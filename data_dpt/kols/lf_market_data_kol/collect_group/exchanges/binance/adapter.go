// File: data_dpt/kols/lf_market_data_kol/collect_group/exchanges/binance/adapter.go
// Package: binance
//
// 職責 (Responsibility):
//     - 預留 Binance LF KLine collect adapter 的骨架。
//     - 目前只提供介面與基本實作，尚未真正連通 Binance KLine。
//     - 讓上層 Chief 可以先掛上 adapter，不至於編譯失敗。
//
// 注意事項 (Notes):
//     - 所有核心方法目前都回傳「尚未實作」的錯誤或空值。
//     - 未來要支援 Binance KLine（premium index / mark 等）再填入實作。

package binance

import (
	// === 標準函式庫 (Standard Library) ===
	"fmt"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	// 無

	// === 系統內模組 (Internal Modules) ===
	"dorma_system/infra/transport/ws"
)

// NewAdapter 建立一個暫時的 Binance Adapter 骨架。
//
// 功能:
//   - 回傳一個符合 ws.Adapter 介面的物件，但所有方法都尚未實作。
//   - 主要用途是先打通編譯流程與 Chief 的依賴。
//
// 參數:
//   - 無。
//
// 回傳:
//   - ws.Adapter: 實作 ws.Adapter 介面的 pointer。
//   - 無 error（錯誤由各方法自己回傳）。
func NewAdapter() ws.Adapter { return &adapter{} }

// adapter 為 Binance KLine 的暫時骨架實作。
//
// 功能:
//   - 實作 ws.Adapter 必要方法，但內容皆為 stub。
//
// 契約 / 限制:
//   - 任一方法目前都不會真的向 Binance 訂閱或解析資料。
//   - 使用時應預期會收到 "not implemented" 類型錯誤。
type adapter struct{}

// BuildSubscribeMsgs 暫時未實作的 Binance 訂閱組裝器。
//
// 功能:
//   - 目前僅回傳 "binance kline adapter not implemented yet" 錯誤。
//   - 未來需依 Binance 官方 KLine 規則（mark/index）實作 channel 拼法與 payload。
//
// 參數:
//   - channels: 預期的 channel 清單。
//   - symbols:  預期訂閱的 symbol 清單。
//
// 回傳:
//   - []string: 目前一律為 nil。
//   - error:    固定回傳 not implemented 錯誤。
func (a *adapter) BuildSubscribeMsgs(channels, symbols []string) ([]string, error) {
	// 先保留接口；未來要做 Binance KLine（premium index / mark）的 candle 再補
	return nil, fmt.Errorf("binance kline adapter not implemented yet")
}

// Heartbeat 暫時未實作的 Binance WS 心跳策略。
//
// 功能:
//   - 目前不發任何心跳，直接回傳空 payload 與 0 間隔。
//
// 參數:
//   - 無。
//
// 回傳:
//   - string:        空字串（無心跳 payload）。
//   - time.Duration: 0（不啟動心跳）。
func (a *adapter) Heartbeat() (string, time.Duration) { return "", 0 }

// Handle 暫時未實作的 Binance KLine 封裝器。
//
// 功能:
//   - 目前直接回傳空 subject / nil body / nil error，不做任何處理。
//   - 目的只是先填滿 ws.Adapter 介面。
//
// 參數:
//   - _ []byte: 原始 WS 資料（目前未使用）。
//
// 回傳:
//   - string: 空字串（不發佈任何 subject）。
//   - []byte: nil（不產生任何 Envelope）。
//   - error:  nil（視為「靜默略過」）。
func (a *adapter) Handle(_ []byte) (string, []byte, error) {
	// 不處理任何資料
	return "", nil, nil
}

// MakeChannel 暫時未實作的 Binance KLine channel builder。
//
// 功能:
//   - 目前僅回傳空字串，占位用。
//   - 未來需依 Binance stream 命名規則（如 @kline_1m）實作。
//
// 參數:
//   - baseFeed: 預期的 base feed 名稱（目前未使用）。
//   - interval: 預期的 interval 字串（目前未使用）。
//
// 回傳:
//   - string: 目前一律為空字串。
func MakeChannel(baseFeed, interval string) string { return "" }
