// File: infra/pubsub/bus.go
// Package: pubsub
//
// 職責 (Responsibility):
//     定義系統內部使用的訊息總線抽象層（Publisher / Subscriber / Bus 介面）。
//     提供統一的 Publish / Subscribe 能力，讓上層只依賴自己需要的能力，而不直接綁定 NATS 等具體實作。
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
//   - 具體錯誤處理策略由呼叫端或實作層決定，這裡不強制規範。
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
//   - MaxInflight 目前尚未在 NATSCoreBus 中實作，僅保留於 SubOptions。
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

// Publisher 抽象「只負責發佈」的訊息總線能力。
//
// 功能:
//   - 定義發佈訊息的最小能力集合，提供給只需要送出訊息的元件（例如 Collector）。
//
// 契約 / 限制:
//   - Publish 應在成功時回傳 nil，錯誤時回傳底層實作的錯誤。
//   - 具體的重試與失敗處理策略由呼叫端或實作層決定。
//
// 備註:
//   - 適合用在單向資料流（例如「WS → Bus」），不強迫實作訂閱或關閉行為。
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// Subscriber 抽象「只負責訂閱」的訊息總線能力。
//
// 功能:
//   - 定義訂閱與綁定 Handler 的最小能力集合，供需要從訊息總線接收事件的元件使用。
//
// 契約 / 限制:
//   - Subscribe 應在訂閱建立成功時回傳 Subscription；建立失敗時回傳錯誤與 nil Subscription。
//   - Subscription 的生命周期管理（Unsubscribe / Drain）由呼叫端負責。
//
// 備註:
//   - 適合用在僅需「從 Bus 收訊」的元件，不要求具備發佈能力。
type Subscriber interface {
	Subscribe(ctx context.Context, subject string, h Handler, opts ...SubOpt) (Subscription, error)
}

// Bus 抽象完整訊息總線介面。
//
// 功能:
//   - 統一封裝訊息發佈與訂閱的行為，隔離具體實作（如 NATS）。
//   - 透過組合 Publisher / Subscriber 介面，讓上層可以視需求只依賴部分能力。
//
// 契約 / 限制:
//   - Publish 應在成功時回傳 nil，錯誤時回傳底層實作的錯誤。
//   - Subscribe 應在訂閱建立成功時回傳 Subscription；建立失敗時回傳錯誤與 nil Subscription。
//   - Close 應釋放底層資源，多次呼叫應具備安全性（不得 panic）。
//
// 備註:
//   - 建議上層元件優先依賴 Publisher 或 Subscriber 這類較小介面；
//     只有同時需要收發能力時才依賴整體 Bus。
type Bus interface {
	Publisher
	Subscriber
	Close()
}

// BusOptions 是建立 Bus 實作（例如 NATSCoreBus）的核心連線設定。
//
// 功能:
//   - 承載連線相關參數，提供實作層初始化時使用。
//   - 可選擇提供 OnHandlerError，讓訂閱 handler 的錯誤回報給上層統一處理。
//
// 欄位說明:
//   - URL: 連線端點，例如 NATS server 的 URL。
//   - Name: 連線名稱，用於監控或除錯時辨識 client。
//   - PingInterval: ping 間隔時間；為 0 時由實作層套用預設值。
//   - ReconnectWait: 重新連線等待時間；為 0 時由實作層套用預設值。
//   - Timeout: 連線與請求等操作的預設逾時；為 0 時由實作層套用預設值。
//     在 NATSCoreBus 中會透過 nats.Timeout(opts.Timeout) 套用到 NATS 連線選項，
//     未來也可擴充用在 Request 等需要逾時控制的操作。
//   - MaxReconnects: 最大重連次數，-1 代表無限，0 代表使用 NATS 預設值。
//   - HandlerTimeout: 單筆 handler 的處理逾時；為 0 代表不啟用。
//     用於「每筆訊息獨立生命週期」的場景：在 Subscribe 的 callback 中派生 per-message ctx。
//     注意：ctx 只是訊號，handler 內部的 I/O / queue / lock 必須自行尊重 ctx 才會真的中止。
//   - OnHandlerError: 訂閱 handler 回傳錯誤時的回報鉤子。
//     用於「非同步 callback 無法 return error」的情境，讓上層可以統一做 log/落地/告警。
//     若為 nil，代表不處理（維持 no-op，不會在 pubsub 層自行 log）。
//
// 契約 / 限制:
//   - 欄位為零值時的行為由實作層定義，呼叫端不應過度依賴預設細節。
//   - URL 應為合法的連線字串，否則實作層在連線時會直接回傳錯誤。
//   - HandlerTimeout 若啟用，handler 不應無視 ctx.Done()；
//     否則逾時只會讓 ctx.Done() 變為可讀（發出取消/逾時訊號），
//     不保證能停止阻塞中的工作，也不保證 handler 會自動回傳錯誤。
//   - OnHandlerError 不應做過重或長時間阻塞的工作（避免拖慢訊息處理回呼）；
//     若需要較重的處理，建議在上層自行轉交到 goroutine / queue 再處理。
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
	// HandlerTimeout 是單筆 handler 的處理逾時；為 0 代表不啟用。
	HandlerTimeout time.Duration
	OnHandlerError func(ctx context.Context, subject string, err error)
}
