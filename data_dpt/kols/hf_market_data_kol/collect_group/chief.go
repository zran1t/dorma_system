// File: data_dpt/kols/hf_market_data_kol/collect_group/chief.go
// Package: collect_group
//
// 職責 (Responsibility):
//     作為 HF market data collect_group 的「總管 (Chief)」，負責：
//       - 依照 exchange / feed 選擇對應 Adapter 與 WS URL
//       - 利用 Resolver 將 canonical 轉成各交易所原生 symbol
//       - 建立並啟動對應的 ws.Collector，並維護其生命週期
//
// 注意事項 (Notes):
//     - 目前一次只允許啟動一個 running Collector，重複 Start 會直接報錯。
//     - exchange / feed 的判斷以小寫後的字串為主，需要新增支援時記得同步擴充 switch。

package collect_group

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"fmt"
	"strings"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	// 無

	// === 系統內模組 (Internal Modules) ===
	"dorma_system/infra/pubsub"
	"dorma_system/infra/symbols"
	"dorma_system/infra/transport/ws"

	binance "dorma_system/data_dpt/kols/hf_market_data_kol/collect_group/exchanges/binance"
	okx "dorma_system/data_dpt/kols/hf_market_data_kol/collect_group/exchanges/okx"
)

// Chief 負責管理單一組「交易所 + feed」的 Collector 實例。
//
// 功能:
//   - 根據 exchange / feed 決定要用哪個 Adapter、連去哪個 WS URL、預設 ping 間隔是什麼。
//   - 使用 Resolver 將 canonical symbol 轉成交易所原生 symbol，再交給 ws.Collector。
//   - 對外提供 Start / Stop，隱藏底層 ws.Collector 細節。
//
// 欄位說明:
//   - wsFactory: 建立 WSClient 的工廠函式，方便注入不同實作（例如測試 stub）。
//   - pub:       對應部門內部使用的 pubsub.Bus，Collector 會往上面發佈 Envelope。
//   - resolver:  用來將 canonical 轉成各交易所原生 symbol 的 Resolver。
//   - running:   當前正在運行中的 ws.Collector 實例，無則為 nil。
//
// 契約 / 限制:
//   - Start 被呼叫時若 running 不為 nil，會直接回傳錯誤，避免重複啟動。
//   - Stop 只會關閉已存在的 collector，不會動到 wsFactory / pub / resolver。
//
// 備註:
//   - 目前支援 OKX / Binance，若要擴增交易所，需同步修改 Start 內的 switch。
type Chief struct {
	wsFactory func() ws.WSClient
	pub       pubsub.Bus
	resolver  symbols.Resolver

	running *ws.Collector
}

// NewChief 建立一個 Chief 實例。
//
// 功能:
//   - 將 wsFactory / pub / resolver 注入，產生對應的 Chief 物件。
//
// 參數:
//   - wsFactory: 建立 WSClient 的工廠函式。
//   - pub:       用來發佈資料的 Bus 實作。
//   - r:         symbol 解析用的 Resolver 實作。
//
// 回傳:
//   - *Chief: 新建立的 Chief 實例。
//   - 無 error。
//
// 備註:
//   - 這個 Chief 預期會長時間存活，由外層控制其 Start / Stop。
func NewChief(wsFactory func() ws.WSClient, pub pubsub.Bus, r symbols.Resolver) *Chief {
	return &Chief{wsFactory: wsFactory, pub: pub, resolver: r}
}

// Start 依據 exchange / feed 啟動一個對應的 Collector。
//
// 功能:
//   - 驗證目前是否已有 Collector 在跑，若是則回傳錯誤。
//   - 依 exchange 選擇 OKX 或 Binance Adapter，以及對應的 WS URL / 預設 ping 間隔。
//   - 對 index-tickers feed 進行 canonical -> INDEX canonical 的轉換。
//   - 使用 resolver.ResolveMany 將 canonical 轉成交易所原生 symbol。
//   - 建立 ws.Collector 實例並啟動 Connect / Subscribe / Run。
//
// 參數:
//   - ctx:      控制整個 Collector 生命週期的 context。
//   - exchange: 交易所名稱（ex: "okx", "binance"），大小寫不敏感。
//   - feed:     要訂閱的資料 feed（ex: "trades-all", "index-tickers"），大小寫不敏感。
//   - canon:    canonical symbol 列表（ex: "BTC-USDT-SWAP" 或 "BTC-USDT" 等）。
//
// 回傳:
//   - error: 若已在執行中、交易所不支援、路由或連線/訂閱失敗，會回傳對應錯誤；成功啟動時為 nil。
//
// 備註:
//   - Collector.Run 會以 goroutine 背景執行，Start 本身不會阻塞。
//   - 若 Subscribe 失敗，會先關閉 collector 再回傳錯誤。
func (c *Chief) Start(ctx context.Context, exchange, feed string, canon []string) error {
	if c.running != nil {
		return fmt.Errorf("already running; stop first")
	}

	ex := strings.ToLower(strings.TrimSpace(exchange))
	fd := strings.ToLower(strings.TrimSpace(feed))

	// 1) 選擇 adapter + wsURL + 預設 ping 間隔（依 feed 決定）
	var (
		adj         ws.Adapter
		url         string
		defaultPing time.Duration
	)
	switch ex {
	case "okx":
		adj = okx.NewAdapter()
		switch fd {
		case "trades-all":
			// trades-all 走 business domain，並搭配定期 ping 維持連線。
			url = "wss://ws.okx.com:8443/ws/v5/business"
			defaultPing = 25 * time.Second
		default:
			// 其他 feed 走 public；OKX public 不特別要求 app-level ping。
			url = "wss://ws.okx.com:8443/ws/v5/public"
			defaultPing = 0
		}
	case "binance":
		adj = binance.NewAdapter()
		url = "wss://stream.binance.com:9443/stream"
		defaultPing = 30 * time.Second
	default:
		return fmt.Errorf("unsupported exchange: %s", exchange)
	}

	// 若是 index-tickers feed，將 canonical 統一轉成 "-INDEX" 結尾。
	if strings.EqualFold(fd, "index-tickers") {
		indexCanon := make([]string, 0, len(canon))
		for _, cSym := range canon {
			parts := strings.Split(strings.TrimSpace(cSym), "-")
			if len(parts) >= 2 {
				indexCanon = append(indexCanon, strings.ToUpper(parts[0]+"-"+parts[1]+"-INDEX"))
			}
		}
		canon = indexCanon
	}

	// 2) 在 Chief 內部反查：canonical -> native
	exSymbols, err := c.resolver.ResolveMany(canon, ex)
	if err != nil {
		return fmt.Errorf("resolve symbols for %s: %w", ex, err)
	}

	// 3) 組 ws.Collector 設定並啟動
	client := c.wsFactory()
	conf := ws.CollectorConfig{
		URL:              url,
		Headers:          nil,
		Channels:         []string{fd},
		Symbols:          exSymbols,
		PingEvery:        defaultPing,     // business/特定 feed 可透過這裡開 ping。
		ReconnectBackoff: 3 * time.Second, // 斷線重連節流。
	}
	collector := ws.NewCollector(client, adj, c.pub, conf)

	if err := collector.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := collector.Subscribe(ctx); err != nil {
		_ = collector.Close()
		return fmt.Errorf("subscribe: %w", err)
	}
	go func() {
		_ = collector.Run(ctx)
	}()

	c.running = collector
	return nil
}

// Stop 停止目前正在執行的 Collector。
//
// 功能:
//   - 若 running 為 nil，表示沒有 Collector 在跑，直接回傳 nil。
//   - 若有執行中的 collector，呼叫其 Close 並將 running 設為 nil。
//
// 參數:
//   - 無。
//
// 回傳:
//   - error: 若 Close 發生錯誤則回傳；正常關閉或本來就沒在跑時為 nil。
//
// 備註:
//   - 不會等待內部 goroutine 完整退出，只負責關閉 WS 連線。
func (c *Chief) Stop() error {
	if c.running == nil {
		return nil
	}
	err := c.running.Close()
	c.running = nil
	return err
}
