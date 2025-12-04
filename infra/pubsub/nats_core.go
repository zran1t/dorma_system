// File: infra/pubsub/nats_core.go
// Package: pubsub
//
// 職責 (Responsibility):
//     提供以 NATS 為底層實作的 Bus 版本（NATSCoreBus）。
//     負責建立 NATS 連線、封裝 Publish / Request / Subscribe 與資源關閉邏輯。
//
// 注意事項 (Notes):
//     - 僅處理「Core NATS」功能，不含 JetStream。
//     - BusOptions 的零值會在 NewNATSCoreBus 中套用預設值。
//     - Header 目前尚未從 nats.Msg 映射到 Message.Header，未使用時維持為 nil。

package pubsub

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	"github.com/nats-io/nats.go"
	// === 系統內模組 (Internal Modules) ===
	// 無
)

// natsCoreSub 是 NATS 訂閱的輕量封裝，實作 Subscription 介面。
//
// 功能:
//   - 將 nats.Subscription 轉為符合 Subscription 介面的型別。
//   - 對外僅暴露 Unsubscribe 與 Drain 兩個操作。
//
// 欄位說明:
//   - sub: 內部持有的 NATS 訂閱物件。
//
// 契約 / 限制:
//   - 不保證多次呼叫 Unsubscribe / Drain 的行為，一般應視為一次性操作。
//   - 資源實際釋放時機依據 NATS 實作而定。
//
// 備註:
//   - 此型別不匯出，僅供 NATSCoreBus 內部使用。
type natsCoreSub struct {
	sub *nats.Subscription
}

// Unsubscribe 取消 NATS 訂閱。
//
// 功能:
//   - 呼叫底層 nats.Subscription.Unsubscribe，停止接收後續訊息。
//
// 參數:
//   - 無。
//
// 回傳:
//   - error: 取消訂閱過程中若發生錯誤則回傳，成功時為 nil。
//
// 備註:
//   - 具體行為依 NATS 實作為準，已排隊訊息可能仍會被處理。
func (s *natsCoreSub) Unsubscribe() error { return s.sub.Unsubscribe() }

// Drain 對 NATS 訂閱進行優雅關閉。
//
// 功能:
//   - 呼叫底層 nats.Subscription.Drain，在處理完目前訊息後才關閉訂閱。
//
// 參數:
//   - 無。
//
// 回傳:
//   - error: Drain 過程中若發生錯誤則回傳，成功時為 nil。
//
// 備註:
//   - 適合用在服務關閉流程，希望盡量處理完已收到的訊息再結束。
func (s *natsCoreSub) Drain() error { return s.sub.Drain() }

// NATSCoreBus 是以 NATS 為底層實作的 Bus。
//
// 功能:
//   - 使用 BusOptions 建立 NATS 連線，並提供 Publish / Request / Subscribe / Close 的實作。
//   - 封裝 NATS 連線物件，讓上層只依賴 Bus 介面。
//
// 欄位說明:
//   - nc:   底層 NATS 連線物件。
//   - opts: 建立連線時使用的 BusOptions，含逾時與重連相關設定。
//
// 契約 / 限制:
//   - nc 不應為 nil；若連線建立失敗，應直接在 NewNATSCoreBus 回傳錯誤。
//   - Close 呼叫後，nc 應視為不可再使用。
//
// 備註:
//   - 本實作僅使用 NATS Core API，若需要 JetStream 需另外實作。
type NATSCoreBus struct {
	nc   *nats.Conn
	opts BusOptions
}

// NewNATSCoreBus 建立一個使用 NATS Core 的 Bus 實例。
//
// 功能:
//   - 基於傳入的 BusOptions 初始化 NATS 連線。
//   - 對部分為零值的設定欄位套用預設值（例如 PingInterval、ReconnectWait 等）。
//
// 參數:
//   - opts: 連線設定與行為相關的 BusOptions。
//
// 回傳:
//   - *NATSCoreBus: 成功建立時回傳新的 Bus 實例。
//   - error: 若 NATS 連線建立失敗則回傳錯誤。
//
// 備註:
//   - 預設值行為：
//     PingInterval  為 0 時 → 10 秒。
//     ReconnectWait 為 0 時 → 500 毫秒。
//     Timeout       為 0 時 → 5 秒。
//     MaxReconnects 為 0 時 → -1（視為無限重連）。
func NewNATSCoreBus(opts BusOptions) (*NATSCoreBus, error) {
	if opts.PingInterval == 0 {
		opts.PingInterval = 10 * time.Second
	}
	if opts.ReconnectWait == 0 {
		opts.ReconnectWait = 500 * time.Millisecond
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}
	if opts.MaxReconnects == 0 {
		opts.MaxReconnects = -1
	}

	nc, err := nats.Connect(
		opts.URL,
		nats.Name(opts.Name),
		nats.PingInterval(opts.PingInterval),
		nats.ReconnectWait(opts.ReconnectWait),
		nats.Timeout(opts.Timeout),
		nats.MaxReconnects(opts.MaxReconnects),
	)
	if err != nil {
		return nil, err
	}
	return &NATSCoreBus{nc: nc, opts: opts}, nil
}

// Publish 在 NATS 上發佈一則訊息。
//
// 功能:
//   - 將指定 subject 與 data 送往底層 NATS server。
//
// 參數:
//   - ctx:     呼叫情境；目前僅保留以利未來擴充（例如觀察取消/逾時）。
//   - subject: 發佈目標主題。
//   - data:    要發佈的 payload 資料。
//
// 回傳:
//   - error: 發佈過程若失敗則回傳錯誤，成功時為 nil。
//
// 備註:
//   - 目前未使用 ctx 控制發佈行為，但保留參數以保證介面一致性。
func (b *NATSCoreBus) Publish(ctx context.Context, subject string, data []byte) error {
	_ = ctx // 保留 ctx 參數，之後若要擴充可使用
	return b.nc.Publish(subject, data)
}

// Request 送出一則請求並等待回應。
//
// 功能:
//   - 在指定 subject 上送出請求資料，並在逾時前等待一則回應訊息。
//   - 逾時時間預設來自 BusOptions.Timeout，若 ctx 有 deadline 則優先使用 ctx 的 deadline。
//
// 參數:
//   - ctx:     用於控制逾時與取消的 context；若含 deadline，會覆蓋預設 Timeout。
//   - subject: 請求主題。
//   - data:    請求 payload 資料。
//
// 回傳:
//   - *Message: 成功收到回應時，轉換成 Message 後回傳。
//   - error:   若逾時或底層 NATS 發生錯誤則回傳錯誤。
//
// 備註:
//   - 目前僅填入 Subject、Data、ReceivedAt，Header 尚未從 nats.Msg 映射。
func (b *NATSCoreBus) Request(ctx context.Context, subject string, data []byte) (*Message, error) {
	timeout := b.opts.Timeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return nil, context.DeadlineExceeded
		}
	}
	msg, err := b.nc.Request(subject, data, timeout)
	if err != nil {
		return nil, err
	}
	return &Message{
		Subject:    msg.Subject,
		Data:       msg.Data,
		Header:     nil, // 如需 header，可再映射 msg.Header
		ReceivedAt: time.Now(),
	}, nil
}

// Subscribe 建立一個 NATS 訂閱並綁定 Handler。
//
// 功能:
//   - 在指定 subject 上建立訂閱，收到訊息時呼叫給定的 Handler。
//   - 可透過 SubOpt 選項設定 queue group 與其他訂閱行為。
//
// 參數:
//   - ctx:     處理訊息時共用的 context，傳遞給 Handler 使用。
//   - subject: 要訂閱的主題。
//   - h:       處理訊息的 Handler 函式。
//   - opts:    可選訂閱設定（例如 QueueGroup、MaxInflight 等）。
//
// 回傳:
//   - Subscription: 成功建立訂閱時回傳 natsCoreSub 實例。
//   - error:        若訂閱建立失敗則回傳錯誤與 nil Subscription。
//
// 備註:
//   - 目前將同一個 ctx 傳遞給所有 Handler 呼叫，若 Handler 需要更細粒度控制，可自行派生。
//   - 建立成功後會呼叫一次 nc.Flush，使訂閱盡快生效並及早暴露錯誤。
func (b *NATSCoreBus) Subscribe(ctx context.Context, subject string, h Handler, opts ...SubOpt) (Subscription, error) {
	var so SubOptions
	for _, fn := range opts {
		fn(&so)
	}
	if so.MaxInflight <= 0 {
		so.MaxInflight = 1
	}

	wrap := func(m *nats.Msg) {
		_ = h(ctx, &Message{
			Subject:    m.Subject,
			Data:       m.Data,
			Header:     nil,
			ReceivedAt: time.Now(),
		})
	}

	var (
		sub *nats.Subscription
		err error
	)
	if so.QueueGroup != "" {
		sub, err = b.nc.QueueSubscribe(subject, so.QueueGroup, wrap)
	} else {
		sub, err = b.nc.Subscribe(subject, wrap)
	}
	if err != nil {
		return nil, err
	}

	// Flush 一次，讓訂閱盡快生效並暴露錯誤（若有）
	_ = b.nc.Flush()
	return &natsCoreSub{sub: sub}, nil
}

// Close 關閉 NATSCoreBus 持有的連線。
//
// 功能:
//   - 若連線存在且未關閉，則呼叫 nats.Conn.Close 釋放資源。
//
// 參數:
//   - 無。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 多次呼叫 Close 是安全的；若連線已經關閉或為 nil，將不做任何事。
func (b *NATSCoreBus) Close() {
	if b.nc != nil && !b.nc.IsClosed() {
		b.nc.Close()
	}
}
