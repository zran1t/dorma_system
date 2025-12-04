// File: data_dpt/kols/lf_market_data_kol/collect_group/collector.go
// Package: collect_group
//
// 職責 (Responsibility):
//     - 將 infra 層的 ws.Collector 包裝成「帶 ID 的員工」。
//     - 對外提供 Start / Stop / ID 介面，讓 Chief 用來管理多條收集線。
//
// 注意事項 (Notes):
//     - 本層完全不碰交易所細節，只把參數轉給 ws.NewCollector。
//     - pingEvery 可為 0，代表由 Adapter.Heartbeat 控制心跳或不送。

package collect_group

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"fmt"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	// 無

	// === 系統內模組 (Internal Modules) ===
	"dorma_system/infra/pubsub"
	"dorma_system/infra/transport/ws"
)

// Collector 是一個具體的收集員工，包裝 infra 的 ws.Collector。
//
// 功能:
//   - 代表一條「WS 收集線」，例如 "mark-price-candle|1D"。
//   - 持有實際執行單元 ws.Collector。
//   - 提供 Start / Stop / ID 方法給 Chief 管理。
//
// 欄位說明:
//   - id:  字串形式的工號，例如 "mark-price-candle|1D"。
//   - run: 實際執行收集邏輯的 ws.Collector。
//
// 契約 / 限制:
//   - 一個 Collector 實例預期只會被 Start 一次、Stop 一次。
type Collector struct {
	id  string        // 工號字串（自訂：例如 "mark-price-candle|1D"）
	run *ws.Collector // 實際執行單元
}

// NewCollector 建立一個新的 LF KLine Collector。
//
// 功能:
//   - 呼叫 ws.NewCollector 建立底層收集器。
//   - 封裝成帶 id 的 Collector。
//
// 參數:
//   - id:        自訂工號，用於日誌與識別（例："mark-price-candle|1D"）。
//   - wsFactory: 建立 WSClient 的工廠函數（例：回傳 GorillaWS 實例）。
//   - adj:       交易所 Adapter（負責 build subscribe / heartbeat / handle）。
//   - pub:       pubsub.Bus，用來發佈收集到的 RAW 封包。
//   - url:       WebSocket 端點 URL。
//   - channels:  channel 名稱列表；本案中常態為一個 interval 對應一個 channel。
//   - symbols:   要訂閱的 instId 清單（native symbol 陣列）。
//   - pingEvery: 心跳 ping 間隔；0 代表由 Adapter 控制或不送 ping。
//   - backoff:   斷線重連等待時間。
//
// 回傳:
//   - *Collector: 封裝好的 Collector 實例指標。
//   - 無 error。
//
// 備註:
//   - 本函式不會觸發連線或訂閱，需另外呼叫 Start。
func NewCollector(
	id string,
	wsFactory func() ws.WSClient, // 工廠函數：產生 WSClient（例：Gorilla）
	adj ws.Adapter, // 交易所 adapter（build sub / handle / heartbeat）
	pub pubsub.Bus, // 發佈總線
	url string, // WebSocket 端點
	channels []string, // 通道（可多個；本案每個 collector 一個 interval → 一個 channel）
	symbols []string, // 該 collector 要訂閱的 instId 清單（native）
	pingEvery time.Duration, // ping 間隔（OKX business 建議 ~25s）
	backoff time.Duration, // 斷線重連等待間隔
) *Collector {
	client := wsFactory()
	conf := ws.CollectorConfig{
		URL:              url,
		Channels:         channels,
		Symbols:          symbols,
		PingEvery:        pingEvery,
		ReconnectBackoff: backoff,
	}
	return &Collector{
		id:  id,
		run: ws.NewCollector(client, adj, pub, conf),
	}
}

// ID 回傳 Collector 的工號字串。
//
// 功能:
//   - 提供 Chief 或外層在 log/debug 時辨識此 Collector。
//
// 參數:
//   - 無。
//
// 回傳:
//   - string: 事先設定的 id 字串。
func (c *Collector) ID() string { return c.id }

// Start 啟動此 Collector：連線 + 訂閱 + 啟動 Run() goroutine。
//
// 功能:
//   - 呼叫 run.Connect(ctx) 建立 WS 連線。
//   - 呼叫 run.Subscribe(ctx) 發送訂閱封包。
//   - 若上述兩步成功，啟動 goroutine 執行 run.Run(ctx) 進入主迴圈。
//   - 任一步驟失敗時回傳錯誤；Subscribe 失敗會先嘗試 Close() 清理連線。
//
// 參數:
//   - ctx: 上層 context，控制整條收集線生命週期。
//
// 回傳:
//   - error: connect 或 subscribe 或其他初始化錯誤；成功時為 nil。
//
// 備註:
//   - 此方法不會阻塞在 Run；主迴圈由 goroutine 執行。
func (c *Collector) Start(ctx context.Context) error {
	if err := c.run.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := c.run.Subscribe(ctx); err != nil {
		_ = c.run.Close()
		return fmt.Errorf("subscribe: %w", err)
	}
	go func() { _ = c.run.Run(ctx) }()
	return nil
}

// Stop 停止 Collector，關閉底層 WS 連線。
//
// 功能:
//   - 呼叫 run.Close() 關閉底層 ws.Collector。
//   - 若 run 為 nil，直接視為已停止。
//
// 參數:
//   - 無。
//
// 回傳:
//   - error: 來自 Close() 的錯誤；成功關閉時為 nil。
//
// 備註:
//   - 不會清空 id；Collector 可以在必要時被重新建立。
func (c *Collector) Stop() error {
	if c.run == nil {
		return nil
	}
	return c.run.Close()
}
