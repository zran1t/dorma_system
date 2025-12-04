// File: infra/transport/ws/collect_base.go
// Package: ws
//
// 職責 (Responsibility):
//     定義 Collector 與其設定，用來統一管理「WS 訂閱 → 解析 → 發佈」的資料收集流程。
//     將「連線細節 (WSClient)」與「交易所協定 (Adapter)」與「發佈介面 (Bus)」組裝在一起。
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
	// 無

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

// Collector 負責整合 WSClient、Adapter 與 pubsub.Bus 的收集流程。
//
// 功能:
//   - 使用 WSClient 建立與維護 WebSocket 連線。
//   - 透過 Adapter 建構訂閱封包、心跳訊息與資料解析。
//   - 將解析完成的資料發佈到 pubsub.Bus。
//
// 欄位說明:
//   - ws:   實際的 WebSocket 客戶端實作（例如 GorillaWS），必須實作 WSClient 介面。
//   - adj:  交易所 Adapter，負責協定細節（訂閱格式、心跳、訊息解析）。
//   - pub:  發佈介面（例如 NATS Bus），負責將處理好的資料送到內部總線。
//   - conf: Collector 行為設定（端點、訂閱標的、心跳、重連策略）。
//
// 契約 / 限制:
//   - Collector 本身不保證消息一定不丟，只在錯誤時盡量重連與重新訂閱。
//   - Run 須由上層控制生命週期（ctx.Done），否則會持續阻塞在 Recv 。
//
// 備註:
//   - 適合作為「交易所資料收集器」的基底元件，具體策略由 Adapter 決定。
type Collector struct {
	ws   WSClient
	adj  Adapter
	pub  pubsub.Bus
	conf CollectorConfig
}

// NewCollector 建立 Collector 實例。
//
// 功能:
//   - 接收 WSClient、Adapter、Bus 與 Config，組合成一個 Collector。
//   - 若 ReconnectBackoff 未設定或為非正值，會套用預設 3 秒。
//
// 參數:
//   - ws:   具體 WebSocket 客戶端實作。
//   - adj:  具體交易所 Adapter 實作。
//   - pub:  發佈資料用的 Bus 實作。
//   - conf: Collector 的靜態設定。
//
// 回傳:
//   - *Collector: 建構完成的 Collector 實例。
//   - 無 error。
//
// 備註:
//   - 建構後仍需呼叫 Connect 與 Subscribe 再進行 Run。
func NewCollector(ws WSClient, adj Adapter, pub pubsub.Bus, conf CollectorConfig) *Collector {
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
//   - 持續從 WSClient.Recv 取得訊息，交給 Adapter 解析，再用 Bus.Publish 發佈。
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
//   - msgType 僅處理 TextMessage（值為 1），其他型別目前會被忽略。
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

	for {
		select {
		case <-ctx.Done():
			// 上層要求結束，乾淨退出主循環。
			return nil
		default:
			// 保留 default 分支，避免 select 在沒有事件時完全阻塞心跳與 Recv 邏輯。
		}

		// 非阻塞心跳：到點才送 ping / 心跳封包，沒到點就直接略過。
		if hbTicker != nil {
			select {
			case <-hbTicker.C:
				if hbPayload != "" {
					_ = c.ws.SendJSON(ctx, hbPayload)
				} else {
					_ = c.ws.SendPing(ctx)
				}
			default:
				// 還沒到時間就先不動，讓主循環繼續往下跑 Recv。
			}
		}

		// 阻塞等待下一則 WS 訊息。
		msgType, data, err := c.ws.Recv(ctx)
		if err != nil {
			log.Printf("[collector] 接收訊息錯誤: %v -> 重新連線中...", err)
			_ = c.ws.Close() // 先關閉舊連線，避免殘留狀態。

			// 在重連前等待一段 backoff 時間，途中若 ctx 被取消就直接結束。
			select {
			case <-time.After(c.conf.ReconnectBackoff):
			case <-ctx.Done():
				return nil
			}

			// 嘗試重連與重新訂閱，失敗就下一輪再試，不直接中止整個 Collector。
			if err := c.Connect(ctx); err != nil {
				continue
			}
			if err := c.Subscribe(ctx); err != nil {
				continue
			}
			continue
		}

		// 正常情況下會走到這裡：處理收到的 WS 訊息。
		if msgType == 1 /* websocket.TextMessage */ {
			// 交給交易所 Adapter 做解析與路由決策。
			subject, body, err := c.adj.Handle(data)
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
//   - 僅關閉 WS 連線，並不會關閉 pubsub.Bus 或做其他清理。
func (c *Collector) Close() error {
	return c.ws.Close()
}
