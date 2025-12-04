// File: data_dpt/kols/hf_market_data_kol/collect_group/exchanges/binance/adapter.go
// Package: binance
//
// 職責 (Responsibility):
//     Binance 端的 WS Adapter 介面實作。
//     負責把 Collector 給的 channels / symbols 轉成訂閱封包，以及後續心跳與訊息處理策略（目前為 stub）。
//
// 注意事項 (Notes):
//     - 目前僅提供骨架，實際訂閱格式與資料解析尚未實作。
//     - 可以依照 OKX Adapter 的風格，日後逐步補上 Binance 規則。

package binance

import (
	// === 標準函式庫 (Standard Library) ===
	"time"
	// === 第三方套件 (Third-Party Libraries) ===
	// 無
	// === 系統內模組 (Internal Modules) ===
	// 無
)

// Adapter 為 Binance 專用的 WS Adapter 實作骨架。
//
// 功能:
//   - 實作 ws.Adapter 介面，作為 Binance 相關 feed 的協定橋接層。
//   - 未來會負責組訂閱封包、定義心跳策略、解析原始 WS 訊息等。
//
// 欄位說明:
//   - 無（目前不需要狀態，純函式型態）。
//
// 契約 / 限制:
//   - 目前所有方法都回傳空結果，僅作為占位用，不應投入正式環境。
//   - 待實作時，需確保與 Collector / Envelope schema 的契約一致。
//
// 備註:
//   - 可以參考 OKX Adapter 的寫法當作完整版模板。
type Adapter struct{}

// NewAdapter 建立一個 Binance Adapter 實例。
//
// 功能:
//   - 提供給呼叫端方便取得 *Adapter，用來注入 Collector。
//
// 參數:
//   - 無。
//
// 回傳:
//   - *Adapter: 新建立的 Binance Adapter 實例。
//   - 無 error。
//
// 備註:
//   - 目前為無狀態實作，之後若有需要可掛上設定或依賴。
func NewAdapter() *Adapter { return &Adapter{} }

// BuildSubscribeMsgs 依照 channels / symbols 組出 Binance 的訂閱封包。
// （目前為 stub，尚未實作 Binance 實際協定）
//
// 功能:
//   - 符合 ws.Adapter 介面簽名，預留將 channels + symbols 轉成 JSON 訂閱字串的進入點。
//
// 參數:
//   - channels: 要訂閱的資料頻道名稱列表（例如 trades, kline_1m 等）。
//   - symbols:  要訂閱的標的物列表（例如 BTCUSDT, ETHUSDT 等）。
//
// 回傳:
//   - []string: 目前固定回傳空 slice，代表尚未建立任何訂閱封包。
//   - error:    目前固定為 nil，未做任何驗證或錯誤判斷。
//
// 備註:
//   - 正式實作時，應依 Binance 官方文件組合正確的訂閱 JSON，並處理錯誤情境。
func (a *Adapter) BuildSubscribeMsgs(channels []string, symbols []string) ([]string, error) {
	return []string{}, nil
}

// Heartbeat 回傳 Binance 所需的心跳 payload 與間隔。
// （目前為 stub，代表不做 app-level 心跳）
//
// 功能:
//   - 告訴 Collector 是否需要由應用層主動送出心跳訊息。
//   - 目前直接回傳 payload=""、every=0，代表不啟用心跳。
//
// 參數:
//   - 無。
//
// 回傳:
//   - string:        目前固定為空字串，表示沒有自訂心跳 payload。
//   - time.Duration: 目前固定為 0，表示 Collector 不會啟動心跳 ticker。
//
// 備註:
//   - 若未來 Binance 有特別要求 ping/pong 週期，可在這裡定義。
func (a *Adapter) Heartbeat() (string, time.Duration) {
	return "", 0
}

// Handle 處理 Binance 回傳的原始 WS 訊息。
// （目前為 stub，尚未實作實際解析邏輯）
//
// 功能:
//   - 依照 ws.Adapter 介面，將 raw bytes 轉成「要發佈的 subject 與 body」。
//   - 目前直接回傳空 subject / nil body / nil error，等於忽略所有訊息。
//
// 參數:
//   - raw: 來自 WS 的原始訊息內容。
//
// 回傳:
//   - string: 要發佈到 pubsub 的 subject，現在固定為空字串。
//   - []byte: 要發佈的 payload，目前為 nil。
//   - error:  目前固定為 nil，尚未處理任何錯誤情境。
//
// 備註:
//   - 正式實作時，應將 raw 解析成 Envelope（或其他 schema），並決定對應 subject。
func (a *Adapter) Handle(raw []byte) (string, []byte, error) {
	return "", nil, nil
}
