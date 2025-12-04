// File: data_dpt/kols/hf_market_data_kol/collect_group/collector.go
// Package: collect_group
//
// 職責 (Responsibility):
//     提供一個比較小顆粒度的 Collector 包裝：
//       - 專注在「單一 id + 單一 ws.Collector」的生命週期管理
//       - 由外層決定 Adapter / URL / ping / backoff 等細節
//
// 注意事項 (Notes):
//     - 和 Chief 不同，這裡不負責選擇 adapter 或 resolver，只負責啟停。

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

// Collector 是對 ws.Collector 的薄封裝，附帶一個簡單的 id。
//
// 功能:
//   - 對 ws.Collector 加上一個字串 id，方便在管理多個 collector 時辨識。
//   - 對外提供 Start / Stop 與 ID 讀取，隱藏 ws.Collector 細節。
//
// 欄位說明:
//   - id:  外部識別用的 collector id（例如 "okx-trades-all" 等）。
//   - run: 實際負責連線與收集的 ws.Collector 實例。
//
// 契約 / 限制:
//   - Start 預期只呼叫一次，多次呼叫可能造成重複 goroutine 啟動（目前未多做防護）。
//   - Stop 只會關閉底層 ws.Collector，不會移除自身結構。
//
// 備註:
//   - 適合被更高階的管理器（例如一個「Collector pool」）拿來統一管理。
type Collector struct {
	id  string
	run *ws.Collector
}

// NewCollector 建立一個包裝後的 Collector 實例。
//
// 功能:
//   - 建立 WSClient、組合 CollectorConfig，然後用 ws.NewCollector 建出實際執行單元。
//   - 將這個執行單元與 id 綁在一起，回傳薄封裝的 Collector。
//
// 參數:
//   - id:        對外用的識別字串。
//   - wsFactory: 建立 WSClient 的工廠函式。
//   - adj:       交易所 Adapter 實作。
//   - pub:       資料發佈用的 Bus 實作。
//   - url:       WebSocket 端點 URL。
//   - channels:  要訂閱的資料頻道列表。
//   - symbols:   要訂閱的標的物列表（原生 symbol 或 canonical 依 Adapter 而定）。
//   - pingEvery: 心跳間隔，0 代表不由 Collector 主動送出 ping。
//   - backoff:   斷線後的重連等待時間。
//
// 回傳:
//   - *Collector: 建立完成的 Collector 實例。
//   - 無 error。
//
// 備註:
//   - 若要自訂 Headers，可在這裡擴充 config 欄位。
func NewCollector(
	id string,
	wsFactory func() ws.WSClient,
	adj ws.Adapter,
	pub pubsub.Bus,
	url string,
	channels []string,
	symbols []string,
	pingEvery time.Duration,
	backoff time.Duration,
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

// ID 回傳 Collector 的識別字串。
//
// 功能:
//   - 讓外部可以取得這個 Collector 的 id，用於 log 或管理用途。
//
// 參數:
//   - 無。
//
// 回傳:
//   - string: 此 Collector 的識別字串。
//
// 備註:
//   - 純 getter，不做任何額外檢查。
func (c *Collector) ID() string { return c.id }

// Start 啟動底層 ws.Collector 的收集流程。
//
// 功能:
//   - 呼叫 ws.Collector 的 Connect 與 Subscribe，成功後在 goroutine 中啟動 Run。
//   - 若 Connect 或 Subscribe 失敗，會回傳錯誤；Subscribe 失敗時會先關閉 ws 連線。
//
// 參數:
//   - ctx: 控制底層 collector 生命週期的 context。
//
// 回傳:
//   - error: 連線或訂閱失敗時回傳錯誤，成功啟動時為 nil。
//
// 備註:
//   - Run 會在背景 goroutine 中運行，Start 本身不會阻塞。
//   - 若之後不需要此 collector，應搭配 Stop 做正確關閉。
func (c *Collector) Start(ctx context.Context) error {
	if err := c.run.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := c.run.Subscribe(ctx); err != nil {
		_ = c.run.Close()
		return fmt.Errorf("subscribe: %w", err)
	}
	go func() {
		_ = c.run.Run(ctx)
	}()
	return nil
}

// Stop 停止底層 ws.Collector。
//
// 功能:
//   - 呼叫 ws.Collector.Close 關閉 WS 連線與相關資源。
//   - 若底層 run 為 nil，代表尚未初始化或已被清理，則直接回傳 nil。
//
// 參數:
//   - 無。
//
// 回傳:
//   - error: 若 Close 發生錯誤則回傳，正常關閉或原本就沒有 collector 時為 nil。
//
// 備註:
//   - 不會將 run 設為 nil，讓呼叫端可以視需求決定是否重複使用同一實例。
func (c *Collector) Stop() error {
	if c.run == nil {
		return nil
	}
	return c.run.Close()
}
