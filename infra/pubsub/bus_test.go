// File: infra/pubsub/bus_test.go
// Package: pubsub
//
// 職責 (Responsibility):
//
//	測試 pubsub 套件中「純邏輯」的部分，
//	包含 SubOptions / SubOpt 是否正確套用，以及 NATSCoreBus 是否實作預期的介面。
//	不會對外連線、不依賴 NATS server。
package pubsub

import (
	"context"
	"testing"
	"time"
)

// TestSubOptions_WithQueue_And_WithMaxInflight
//
// 功能:
//   - 確認 WithQueue / WithMaxInflight 這兩個 SubOpt 有正確修改 SubOptions。
//
// 說明:
//   - 這支測試跟 NATS 完全無關，只是確保「訂閱選項」這層邏輯沒有寫錯。
func TestSubOptions_WithQueue_And_WithMaxInflight(t *testing.T) {
	var opts SubOptions

	WithQueue("my-queue")(&opts)
	if opts.QueueGroup != "my-queue" {
		t.Fatalf("QueueGroup 預期 = %q, 實際 = %q", "my-queue", opts.QueueGroup)
	}

	WithMaxInflight(10)(&opts)
	if opts.MaxInflight != 10 {
		t.Fatalf("MaxInflight 預期 = %d, 實際 = %d", 10, opts.MaxInflight)
	}
}

// TestHandler_Signature_CompileTimeCheck
//
// 功能:
//   - 用編譯器幫忙檢查 Handler 型別簽名是否符合預期。
//   - 沒有 runtime 行為，重點是「只要編得過就代表簽名沒問題」。
//
// 說明:
//   - 若未來不小心改到 Handler 型別，這裡會直接編譯失敗。
func TestHandler_Signature_CompileTimeCheck(t *testing.T) {
	f := func(ctx context.Context, m *Message) error {
		_ = ctx
		_ = m
		return nil
	}
	var _ Handler = f
}

// TestBusOptions_ZeroValue_IsAccepted
//
// 功能:
//   - 確認 BusOptions 的零值在編譯層面是被接受的，避免未來新增欄位時破壞相容性。
//
// 說明:
//   - 這裡不檢查具體預設值（那是 NewNATSCoreBus 的責任）。
//   - 只要能建立 BusOptions 並傳給 NewNATSCoreBus 就表示欄位設計上沒有奇怪的必填陷阱。
func TestBusOptions_ZeroValue_IsAccepted(t *testing.T) {
	opts := BusOptions{
		// 全部空值，交給 NewNATSCoreBus 做補值。
	}
	_ = opts // 目前不呼叫 NewNATSCoreBus，避免連線外部服務。
}

// TestNATSCoreBus_ImplementsInterfaces
//
// 功能:
//   - 用編譯器檢查 NATSCoreBus 是否實作 Bus / Publisher / Subscriber 三個介面。
//   - 若哪天改了 NATSCoreBus 的方法簽名導致介面不相容，這裡會直接編譯失敗。
func TestNATSCoreBus_ImplementsInterfaces(t *testing.T) {
	var _ Bus = (*NATSCoreBus)(nil)
	var _ Publisher = (*NATSCoreBus)(nil)
	var _ Subscriber = (*NATSCoreBus)(nil)
}

// TestMessage_BasicFields
//
// 功能:
//   - 確認 Message 的基本欄位可以正常賦值與使用，避免 struct tag 或欄位調整時沒注意到。
func TestMessage_BasicFields(t *testing.T) {
	now := time.Now()
	m := &Message{
		Subject:    "test.subject",
		Data:       []byte(`{"hello":"world"}`),
		Header:     map[string]string{"x-id": "123"},
		ReceivedAt: now,
	}

	if m.Subject != "test.subject" {
		t.Fatalf("Subject 預期 = %q, 實際 = %q", "test.subject", m.Subject)
	}
	if string(m.Data) != `{"hello":"world"}` {
		t.Fatalf("Data 預期 = %q, 實際 = %q", `{"hello":"world"}`, string(m.Data))
	}
	if m.Header["x-id"] != "123" {
		t.Fatalf("Header[x-id] 預期 = %q, 實際 = %q", "123", m.Header["x-id"])
	}
	if !m.ReceivedAt.Equal(now) {
		t.Fatalf("ReceivedAt 預期 = %v, 實際 = %v", now, m.ReceivedAt)
	}
}
