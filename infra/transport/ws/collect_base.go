// File: infra/transport/ws/collect_base.go
// Package: ws
//
// 職責 (Responsibility):
//     定義 Collector 與其設定，用來統一管理「WS 訂閱 → 解析 → 發佈」的資料收集流程。
//     將「連線細節 (WSClient)」與「交易所協定 (Adapter)」與「發佈介面 (pubsub.Publisher)」組裝在一起。
//
// 注意事項 (Notes):
//     - Collector 本身不理解業務，只負責流程與錯誤處理、重連與心跳。
//     - 心跳策略優先使用 Adapter 的 Heartbeat 設定，其次才是 CollectorConfig.PingEvery。
//     - ReconnectBackoff 為整個重連與重新訂閱流程的間隔時間。

package ws

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"log"
	"net/http"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	"github.com/gorilla/websocket"

	// === 系統內模組 (Internal Modules) ===
	"dorma_system/infra/pubsub"
)

// CollectorConfig 為 Collector 的靜態設定。
//
// 功能:
//   - 承載連線 URL、HTTP Header、要訂閱的 channels/symbols，以及心跳與重連相關設定。
//
// 欄位說明:
//   - URL:              WebSocket 端點網址。
//   - Headers:          建立連線時附帶的 HTTP Header（如 API key, token 等）。
//   - Channels:         要訂閱的頻道名稱列表（由 Adapter 解讀）。
//   - Symbols:          要訂閱的標的物清單（由 Adapter 解讀）。
//   - PingEvery:        若 Adapter 未提供 heartbeat 間隔，則使用此值作為預設 ping 週期；為 0 則不啟用。
//   - ReconnectBackoff: 當收訊失敗並關閉連線後，等待多少時間再發動重連；若小於等於 0 則會在建構時套用預設值。
//
// 契約 / 限制:
//   - URL 必須為合法 WS URL，否則 Connect 會直接失敗。
//   - Channels 與 Symbols 的具體語意由 Adapter 負責解釋。
//   - ReconnectBackoff 不建議設為過小，避免頻繁重試打爆對端。
//
// 備註:
//   - 若未提供 PingEvery，且 Adapter 也不要求 Heartbeat，則 Collector 不會主動送 ping。
type CollectorConfig struct {
	URL              string
	Headers          http.Header
	Channels         []string
	Symbols          []string
	PingEvery        time.Duration
	ReconnectBackoff time.Duration
}

// Collector 負責整合 WSClient、Adapter 與 pubsub.Publisher 的收集流程。
//
// 功能:
//   - 使用 WSClient 建立與維護 WebSocket 連線。
//   - 透過 Adapter 建構訂閱封包、心跳訊息與資料解析。
//   - 將解析完成的資料發佈到 pubsub.Publisher 介面。
//
// 欄位說明:
//   - ws:   實際的 WebSocket 客戶端實作（例如 GorillaWS），必須實作 WSClient 介面。
//   - adj:  交易所 Adapter，負責協定細節（訂閱格式、心跳、訊息解析）。
//   - pub:  發佈介面（實作 pubsub.Publisher，例如 NATSCoreBus 或 LogBus），負責將處理好的資料送到內部總線。
//   - conf: Collector 行為設定（端點、訂閱標的、心跳、重連策略）。
type Collector struct {
	ws   WSClient
	adj  Adapter
	pub  pubsub.Publisher
	conf CollectorConfig
}

// NewCollector 建立 Collector 實例。
// 功能:
//   - 接收 WSClient、Adapter、Publisher 與 Config，組合成一個 Collector。
//   - 若 ReconnectBackoff 未設定或為非正值，會套用預設 3 秒。
//
// 參數:
//   - ws:   具體 WebSocket 客戶端實作。
//   - adj:  具體交易所 Adapter 實作。
//   - pub:  發佈資料用的 Publisher 實作（例如 NATSCoreBus）。
//   - conf: Collector 的靜態設定。
func NewCollector(ws WSClient, adj Adapter, pub pubsub.Publisher, conf CollectorConfig) *Collector {
	if conf.ReconnectBackoff <= 0 {
		conf.ReconnectBackoff = 3 * time.Second
	}
	return &Collector{ws: ws, adj: adj, pub: pub, conf: conf}
}

// Connect 使用 WSClient 與設定中的 URL/Headers 建立連線。
//
// 功能:
//   - 呼叫 WSClient.Connect 與指定的 WS 端點建立連線。
//
// 參數:
//   - ctx: 上層控制連線動作的 context（可用於逾時或取消）。
//
// 回傳:
//   - error: 若連線建立失敗則回傳錯誤，成功時為 nil。
//
// 備註:
//   - 此函式不會自動訂閱頻道，僅負責建立連線。
func (c *Collector) Connect(ctx context.Context) error {
	return c.ws.Connect(ctx, c.conf.URL, c.conf.Headers)
}

// Subscribe 傳送訂閱封包到 WebSocket 伺服器。
//
// 功能:
//   - 使用 Adapter.BuildSubscribeMsgs 根據 Channels/Symbols 組合出訂閱訊息。
//   - 逐條透過 WSClient.SendJSON 發送訂閱封包。
//   - 透過短暫 Sleep 做輕量節流，避免一次送太多封包壓爆對端。
//
// 參數:
//   - ctx: 上層控制訂閱行為的 context。
//
// 回傳:
//   - error: 若組訂閱封包失敗或任一 SendJSON 出錯，則回傳錯誤；全部成功時為 nil。
//
// 備註:
//   - 若未來需要更精細的節流策略，可以在這裡再包一層 rate limiter。
func (c *Collector) Subscribe(ctx context.Context) error {
	msgs, err := c.adj.BuildSubscribeMsgs(c.conf.Channels, c.conf.Symbols)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if err := c.ws.SendJSON(ctx, m); err != nil {
			return err
		}
		// 避免一次性大量送出訂閱封包造成壓力，做輕量節流。
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

// Run 啟動 Collector 主循環，處理心跳、收訊、重連與發佈。
//
// 功能:
//   - 根據 Adapter / Config 決定是否定期送出 ping 或 heartbeat payload。
//   - 以獨立 reader goroutine 執行阻塞 Recv，並透過 channel 回傳訊息/錯誤供主循環 select。
//   - 當 Recv 發生錯誤時，會關閉連線、等待 backoff，然後嘗試重連與重新訂閱。
//   - 若 ctx 被取消（ctx.Done 收到訊號），則優雅結束主循環。
//
// 參數:
//   - ctx: 控制 Collector 整體生命週期的 context。
//
// 回傳:
//   - error: 正常結束時為 nil；目前實作中重連/解析錯誤都不會直接向上 bubble，除非上層取消 ctx。
//
// 備註:
//   - 重連與重訂閱失敗時不會立刻返回錯誤，而是留給下一輪循環繼續嘗試。
//   - msgType 僅處理 websocket.TextMessage，其他型別目前會被忽略。
//   - 每個「連線生命週期」只會有一個 reader goroutine：讀取錯誤後 reader 結束；重連成功後再啟動新的 reader，避免 concurrent read。
func (c *Collector) Run(ctx context.Context) error {
	// 決定心跳 payload 與間隔：優先採用 Adapter 的設定，沒有再用 config 裡的 PingEvery。
	hbPayload, hbEvery := c.adj.Heartbeat()
	if hbEvery == 0 && c.conf.PingEvery > 0 {
		hbEvery = c.conf.PingEvery
	}

	var hbTicker *time.Ticker
	if hbEvery > 0 {
		hbTicker = time.NewTicker(hbEvery)
		defer hbTicker.Stop()
	}

	// 把 hbTicker.C 抽成可為 nil 的 channel，讓 select 可讀性更高。
	var hbC <-chan time.Time
	if hbTicker != nil {
		hbC = hbTicker.C
	}

	type wsMsg struct {
		msgType int
		data    []byte
	}

	// reader → 主循環事件通道。
	msgCh := make(chan wsMsg, 16)
	errCh := make(chan error, 1)

	// startReader 啟動一個 reader goroutine。
	//
	// 契約:
	//   - 每次只啟動一個 reader；當 Recv 出錯時該 reader 會結束，並回報 errCh。
	//   - 重連成功後，由主循環再次呼叫 startReader() 啟動新的 reader。
	startReader := func() {
		go func() {
			for {
				msgType, data, err := c.ws.Recv(ctx)
				if err != nil {
					// 只送一次錯誤，避免 errCh 塞滿造成阻塞。
					select {
					case errCh <- err:
					default:
					}
					return
				}

				select {
				case msgCh <- wsMsg{msgType: msgType, data: data}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// 第一次啟動 reader（假設上層已先 Connect + Subscribe；若未做，Recv 會回錯並進入重連流程）。
	startReader()

	for {
		select {
		case <-ctx.Done():
			// 上層要求結束，乾淨退出主循環。
			_ = c.ws.Close()
			return nil

		case <-hbC:
			// 到點才送 ping / 心跳封包。
			if hbPayload != "" {
				_ = c.ws.SendJSON(ctx, hbPayload)
			} else {
				_ = c.ws.SendPing(ctx)
			}

		case err := <-errCh:
			log.Printf("[collector] 接收訊息錯誤: %v -> 重新連線中...", err)

			// reader 已結束：接下來由主循環負責重連與重訂閱；成功後再啟新 reader。
			for {
				_ = c.ws.Close() // 先關閉舊連線，避免殘留狀態。

				// 在重連前等待一段 backoff 時間，途中若 ctx 被取消就直接結束。
				select {
				case <-time.After(c.conf.ReconnectBackoff):
				case <-ctx.Done():
					return nil
				}

				// 嘗試重連與重新訂閱；失敗就留在 loop 內繼續 backoff 重試。
				if err := c.Connect(ctx); err != nil {
					continue
				}
				if err := c.Subscribe(ctx); err != nil {
					continue
				}

				// 重連＋重訂閱成功後，啟動新的 reader，恢復收訊。
				startReader()
				break
			}

		case m := <-msgCh:
			// 正常情況下會走到這裡：處理收到的 WS 訊息。
			if m.msgType != websocket.TextMessage {
				continue
			}

			// 交給交易所 Adapter 做解析與路由決策。
			subject, body, err := c.adj.Handle(m.data)
			if err != nil {
				log.Printf("[collector] 處理訊息錯誤: %v", err)
				continue
			}

			// 有主題且有內容才發佈，避免發送空包。
			if subject != "" && len(body) > 0 {
				if err := c.pub.Publish(ctx, subject, body); err != nil {
					log.Printf("[collector] 發佈失敗: %v", err)
				}
			}
		}
	}
}

// Close 關閉底層 WSClient 的連線。
//
// 功能:
//   - 封裝 WSClient.Close，釋放 Collector 所使用的 WebSocket 連線資源。
//
// 參數:
//   - 無。
//
// 回傳:
//   - error: 若 WSClient.Close 發生錯誤則回傳，成功或已關閉時為 nil。
//
// 備註:
//   - 僅關閉 WS 連線，並不會關閉發佈端（例如 NATS 連線）或做其他清理，這部分交由上層管理。
func (c *Collector) Close() error {
	return c.ws.Close()
}
