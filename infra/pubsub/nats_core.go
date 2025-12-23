// File: infra/pubsub/nats_core.go
// Package: pubsub
//
// 職責 (Responsibility):
//     提供以 NATS 為底層實作的 Bus 版本（NATSCoreBus）。
//     負責建立 NATS 連線、封裝 Publish / Subscribe 與資源關閉邏輯，實作 Bus / Publisher / Subscriber 介面。
//
// 注意事項 (Notes):
//     - 僅處理「Core NATS」功能，不含 JetStream。
//     - BusOptions 的零值會在 NewNATSCoreBus 中套用預設值。
//     - Header 目前尚未從 nats.Msg 映射到 Message.Header，未使用時維持為 nil。

package pubsub

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
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
//   - 使用 BusOptions 建立 NATS 連線，並提供 Publish / Subscribe / Close 的實作。
//   - 封裝 NATS 連線物件，實作 Bus / Publisher / Subscriber 介面，讓上層只依賴抽象而非 NATS。
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
	opts busOptions
}

var (
	_ Bus        = (*NATSCoreBus)(nil)
	_ Publisher  = (*NATSCoreBus)(nil)
	_ Subscriber = (*NATSCoreBus)(nil)
)

// NewNATSCoreBus 建立一個使用 NATS Core 的 Bus 實例。
//
// 功能:
//   - 使用系統內建的預設連線與行為設定，初始化 NATS Core 連線。
//   - 封裝底層 NATS client，提供統一的 Publish / Subscribe / Close 能力。
//
// 參數:
//   - 無。
//
// 回傳:
//   - *NATSCoreBus: 成功建立時回傳新的 Bus 實例。
//   - error: 若 NATS 連線建立失敗則回傳錯誤。
//
// 備註:
//   - 本建構函式採用固定的系統預設值，不提供外部調整連線行為。
//   - 預設值說明：
//     PingInterval  = 10 秒。
//     ReconnectWait = 500 毫秒。
//     Timeout       = 5 秒（套用至 NATS 連線 timeout）。
//     MaxReconnects = -1（無限重連）。
func NewNATSCoreBus() (*NATSCoreBus, error) {
	const (
		// 系統內建的預設 NATS 端點（不依賴第三方套件的 DefaultURL）
		defaultNATSURL = "nats://127.0.0.1:4222"

		// 允許外部覆蓋端點（例如部署環境注入）
		envNATSURLKey = "NATS_URL"

		pingInterval  = 10 * time.Second
		reconnectWait = 500 * time.Millisecond
		timeout       = 5 * time.Second
		maxReconnects = -1
	)

	// 端點來源：優先採用外部注入，其次才使用系統預設值。
	urlStr := strings.TrimSpace(os.Getenv(envNATSURLKey))
	if urlStr == "" {
		urlStr = defaultNATSURL
	}

	// 驗證 URL 格式（支援逗號分隔多個 server URL）
	if err := validateNATSURLList(urlStr); err != nil {
		return nil, err
	}

	nc, err := nats.Connect(
		urlStr,
		nats.PingInterval(pingInterval),
		nats.ReconnectWait(reconnectWait),
		nats.Timeout(timeout),
		nats.MaxReconnects(maxReconnects),
	)
	if err != nil {
		return nil, err
	}

	return &NATSCoreBus{
		nc:   nc,
		opts: busOptions{
			// 目前全走 zero value
			// HandlerTimeout: 0
			// OnHandlerError: nil
		},
	}, nil
}

// validateNATSURLList 驗證 NATS URL（支援逗號分隔多個 URL）。
//
// 規則（可依你系統需要再收緊）：
//   - 每一段必須可被 url.Parse 解析。
//   - scheme 僅允許 nats / tls（避免不預期的 ws/http scheme 進來）。
//   - host 必須存在（避免只有 "nats://" 這類空值）。
func validateNATSURLList(raw string) error {
	parts := strings.Split(raw, ",")
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s == "" {
			return errors.New("invalid NATS_URL: empty entry")
		}

		u, err := url.Parse(s)
		if err != nil {
			return fmt.Errorf("invalid NATS_URL: parse failed: %w", err)
		}

		switch strings.ToLower(u.Scheme) {
		case "nats", "tls":
			// ok
		default:
			return fmt.Errorf("invalid NATS_URL: unsupported scheme=%s", u.Scheme)
		}

		if strings.TrimSpace(u.Host) == "" {
			return fmt.Errorf("invalid NATS_URL: host is empty (url=%s)", s)
		}
	}
	return nil
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
		// 每一筆訊息派生獨立 ctx（可選 timeout），避免整條訂閱共用同一個 ctx。
		// 注意：ctx 只是「取消/逾時訊號」，不會強制中止 handler；
		//       handler 內部的 I/O / queue / lock 必須自行尊重 ctx.Done() 才會真的中止。
		msgCtx := ctx
		var cancel context.CancelFunc
		if b.opts.HandlerTimeout > 0 {
			msgCtx, cancel = context.WithTimeout(ctx, b.opts.HandlerTimeout)
			defer cancel()
		}

		err := h(msgCtx, &Message{
			Subject:    m.Subject,
			Data:       m.Data,
			Header:     nil,
			ReceivedAt: time.Now(),
		})
		if err != nil {
			// 訂閱 handler 的 error 發生在 NATS callback（非同步）中，
			// 無法透過 Subscribe 的 return 往上拋，故透過 BusOptions.OnHandlerError 回報。
			if b.opts.OnHandlerError != nil {
				b.opts.OnHandlerError(msgCtx, m.Subject, err)
			}
		}
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
	if err := b.nc.Flush(); err != nil {
		_ = sub.Unsubscribe() // 避免留下半套訂閱
		return nil, err
	}

	return &natsCoreSub{sub: sub}, nil
}

// Close 關閉 NATSCoreBus 持有的連線。
//
// 功能:
//   - 立即關閉底層 NATS 連線，不等待訂閱 handler 完成。
//   - 用於異常中止或不可恢復錯誤情境。
//
// 參數:
//   - 無。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 多次呼叫是安全的。
//   - 呼叫後將不再接收新的訊息；
//     但已在執行中的 handler 可能仍會跑完（視 NATS client 當下狀態而定）。
//   - Close 不會自動呼叫 Subscription.Drain()；
//     若需要優雅關閉，請由上層先管理訂閱生命週期。
func (b *NATSCoreBus) Close() {
	if b.nc != nil && !b.nc.IsClosed() {
		b.nc.Close()
	}
}
