// File: infra/pubsub/bus.go
// Package: pubsub
//
// 職責 (Responsibility):
//     定義系統內部使用的訊息總線抽象層（Bus 介面）。
//     提供統一的 Publish / Request / Subscribe 介面，讓上層不直接依賴特定實作（例如 NATS）。
//
// 注意事項 (Notes):
//     - 這裡只定義介面與基本選項，不負責實際連線與重連邏輯。
//     - Message 結構設計為通用格式，Header 可依需要擴充或映射外部協定。
//     - BusOptions 只是核心連線參數的承載體，預設值與補值行為由實作層決定。

package pubsub

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"time"
	// === 第三方套件 (Third-Party Libraries) ===
	// 無
	// === 系統內模組 (Internal Modules) ===
	// 無
)

// Message 封裝經由 Bus 傳遞的訊息內容。
//
// 功能:
//   - 承載主題、原始資料與基本中繼資訊（例如收到時間）。
//   - 提供上層處理器 (Handler) 統一接收的資料結構。
//
// 欄位說明:
//   - Subject: 訊息主題（依據具體實作對應到 NATS subject 等）。
//   - Data: 原始 payload 資料（通常為序列化後的 JSON / Protobuf 等）。
//   - Header: 額外的標頭欄位，視實作決定是否使用（可為 nil）。
//   - ReceivedAt: 訊息被客戶端接收到的時間。
//
// 契約 / 限制:
//   - Subject 與 Data 一般視為必填欄位；Header 可為空。
//   - ReceivedAt 由實作層決定填入時機，通常為訂閱端收訊時間。
//
// 備註:
//   - 若未使用 Header，可維持為 nil 以避免多餘配置。
//   - 如需保留原始協定 header，可在實作層進行映射再填入。
type Message struct {
	Subject    string
	Data       []byte
	Header     map[string]string
	ReceivedAt time.Time
}

// Handler 是處理訂閱到訊息的函式型別。
//
// 功能:
//   - 定義訂閱回呼的函式簽名，接收 context 與封裝好的 Message。
//
// 參數:
//   - ctx: 用於取消、逾時控制與傳遞請求範圍資料的 context。
//   - m:   接收到的訊息內容指標。
//
// 回傳:
//   - error: 若處理失敗則回傳錯誤，實作端可視需要記錄或採取重試策略；成功時回傳 nil。
//
// 備註:
//   - 具體錯誤處理策略由呼叫端或實作層決定，Bus 介面本身不強制規範。
type Handler func(ctx context.Context, m *Message) error

// Subscription 抽象訂閱的生命周期操作。
//
// 功能:
//   - 提供取消訂閱與優雅 Drain 的基本操作介面。
//
// 契約 / 限制:
//   - Unsubscribe 應中止後續訊息推送，但不保證已排隊訊息一定處理完成。
//   - Drain 應在處理完目前排隊訊息後，才結束訂閱（視實作支援程度）。
//
// 備註:
//   - 實作可對 Unsubscribe 與 Drain 有不同等級的保證，上層需依據實際行為設計關閉流程。
type Subscription interface {
	Unsubscribe() error
	Drain() error
}

// SubOptions 訂閱選項，控制 queue group 與最大併發處理數量。
//
// 功能:
//   - 在 Subscribe 建立時承載額外選項，避免增加介面參數數量。
//   - 透過 SubOpt 函式進行設定，維持 API 簡潔。
//
// 欄位說明:
//   - QueueGroup: 若非空字串，表示使用該 queue group 進行共享訂閱。
//   - MaxInflight: 單一訂閱允許同時處理中的訊息上限（由實作決定是否採用）。
//
// 契約 / 限制:
//   - MaxInflight 小於等於 0 時，實作端可自由決定預設值（例如設為 1）。
//
// 備註:
//   - 目前 NATSCoreBus 只使用 QueueGroup；MaxInflight 可保留未來擴充。
type SubOptions struct {
	QueueGroup  string
	MaxInflight int
}

// SubOpt 是修改 SubOptions 的設定函式型別。
//
// 功能:
//   - 透過一組函式選項的方式設定訂閱行為，避免 Subscribe 介面參數過多。
//
// 參數:
//   - *SubOptions: 指向訂閱選項的指標，供內部修改。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 建議僅在 Subscribe 呼叫前使用，不應在訂閱建立後動態修改。
type SubOpt func(*SubOptions)

// WithQueue 建立設定 QueueGroup 的訂閱選項。
//
// 功能:
//   - 將 SubOptions.QueueGroup 設定為指定的群組名稱，啟用 queue group 模式。
//
// 參數:
//   - group: queue group 名稱；若為空字串，實作端通常視為不啟用 queue group。
//
// 回傳:
//   - SubOpt: 可傳入 Subscribe 的選項函式。
//
// 備註:
//   - queue group 的實際語意依照底層實作（例如 NATS queue group）決定。
func WithQueue(group string) SubOpt {
	return func(o *SubOptions) {
		o.QueueGroup = group
	}
}

// WithMaxInflight 建立設定 MaxInflight 的訂閱選項。
//
// 功能:
//   - 將 SubOptions.MaxInflight 設定為指定數值，用於控制單訂閱的同時處理數量上限。
//
// 參數:
//   - n: 允許的最大 in-flight 訊息數量；小於等於 0 時，實作端可視為不啟用或套用預設值。
//
// 回傳:
//   - SubOpt: 可傳入 Subscribe 的選項函式。
//
// 備註:
//   - 具體行為依實作支援程度而定，目前 NATSCoreBus 尚未使用此欄位。
func WithMaxInflight(n int) SubOpt {
	return func(o *SubOptions) {
		o.MaxInflight = n
	}
}

// Bus 抽象訊息總線介面。
//
// 功能:
//   - 統一封裝訊息發佈、請求/回應與訂閱的行為，隔離具體實作（如 NATS）。
//
// 契約 / 限制:
//   - Publish 應在成功時回傳 nil，錯誤時回傳底層實作的錯誤。
//   - Request 應在超時或錯誤時回傳 error；成功時回傳一則 Message。
//   - Subscribe 應在訂閱建立成功時回傳 Subscription；建立失敗時回傳錯誤與 nil Subscription。
//   - Close 應釋放底層資源，多次呼叫應具備安全性（不得 panic）。
//
// 備註:
//   - 具體的錯誤類型與重試策略由實作層與上層協作決定。
type Bus interface {
	Publish(ctx context.Context, subject string, data []byte) error
	Request(ctx context.Context, subject string, data []byte) (*Message, error)
	Subscribe(ctx context.Context, subject string, h Handler, opts ...SubOpt) (Subscription, error)
	Close()
}

// BusOptions 是建立 Bus 實作（例如 NATSCoreBus）的核心連線設定。
//
// 功能:
//   - 承載連線相關參數，提供實作層初始化時使用。
//
// 欄位說明:
//   - URL: 連線端點，例如 NATS server 的 URL。
//   - Name: 連線名稱，用於監控或除錯時辨識 client。
//   - PingInterval: ping 間隔時間；為 0 時由實作層套用預設值。
//   - ReconnectWait: 重新連線等待時間；為 0 時由實作層套用預設值。
//   - Timeout: Request 等待回應的預設逾時；為 0 時由實作層套用預設值。
//   - MaxReconnects: 最大重連次數，-1 代表無限，0 代表使用 NATS 預設值。
//
// 契約 / 限制:
//   - 欄位為零值時的行為由實作層定義，呼叫端不應過度依賴預設細節。
//   - URL 應為合法的連線字串，否則實作層在連線時會直接回傳錯誤。
//
// 備註:
//   - 若需要更進階的選項（如 TLS 設定），建議透過實作層額外提供建構器或包裝。
type BusOptions struct {
	URL           string
	Name          string
	PingInterval  time.Duration
	ReconnectWait time.Duration
	Timeout       time.Duration
	MaxReconnects int // -1=無限; 0=走 nats 預設
}
