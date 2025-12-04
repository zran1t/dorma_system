// File: infra/pubsub/nats_core_test.go
// Package: pubsub
//
// 職責 (Responsibility):
//
//	對 NATSCoreBus 做「真的連線」的整合測試。
//	測試內容包含：
//	  - 能否順利連上本機 NATS server。
//	  - Subscribe 一個 subject，然後 Publish 一筆資料，handler 是否有拿到。
//	  - Close 呼叫後不會 panic，也不會卡住。
//	這支測試「一定會連網」，如果本機沒有 NATS 在 4222 port，測試就會失敗。
package pubsub

import (
	"context"
	"testing"
	"time"
)

// newTestBus 建一個連到本機 NATS 的 NATSCoreBus。
//
// 前提:
//   - 需要有一個 NATS server 在 "nats://127.0.0.1:4222"。
//
// 備註:
//   - 測試失敗時會直接 t.Fatal，避免後面還繼續跑。
func newTestBus(t *testing.T) *NATSCoreBus {
	t.Helper()

	opts := BusOptions{
		URL:           "nats://127.0.0.1:4222",
		Name:          "dorma-test-natscorebus",
		PingInterval:  0, // 讓 NewNATSCoreBus 自己補預設值
		ReconnectWait: 0,
		Timeout:       0,
		MaxReconnects: 0,
	}

	bus, err := NewNATSCoreBus(opts)
	if err != nil {
		t.Fatalf("無法建立 NATSCoreBus，請確認本機 NATS 是否有啟動: %v", err)
	}
	return bus
}

// TestNATSCoreBus_PublishSubscribe
//
// 功能:
//   - 建立一個 NATSCoreBus，對某個 subject 做 Subscribe，然後 Publish 一筆資料。
//   - 驗證 handler 是否有在 timeout 內收到資料，且內容正確。
//
// 說明:
//   - 這是一個「真的連線」的整合測試，適合在你想確認 NATS / 連線 / handler 整條流程
//     是否正常時使用。
//   - 若本機沒開 NATS，這支測試會直接失敗（t.Fatal）。
func TestNATSCoreBus_PublishSubscribe(t *testing.T) {
	bus := newTestBus(t)
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subject := "test.natscorebus.pubsub"
	payload := []byte(`{"kind":"test","msg":"hello"}`)

	// 用 channel 收 handler 收到的訊息，方便測試驗證。
	msgCh := make(chan *Message, 1)

	// 建立訂閱：收到訊息就塞進 channel。
	_, err := bus.Subscribe(ctx, subject, func(ctx context.Context, m *Message) error {
		_ = ctx
		msgCh <- m
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe 失敗: %v", err)
	}

	// 發佈一筆資料。
	if err := bus.Publish(ctx, subject, payload); err != nil {
		t.Fatalf("Publish 失敗: %v", err)
	}

	// 等待 handler 收到訊息。
	select {
	case got := <-msgCh:
		if got.Subject != subject {
			t.Fatalf("Subject 預期 = %q, 實際 = %q", subject, got.Subject)
		}
		if string(got.Data) != string(payload) {
			t.Fatalf("Data 預期 = %q, 實際 = %q", string(payload), string(got.Data))
		}
	case <-ctx.Done():
		t.Fatalf("在 timeout 內沒有收到訊息，可能 NATS 沒開或訂閱/發佈流程有問題: %v", ctx.Err())
	}
}

// TestNATSCoreBus_Close_IsSafe
//
// 功能:
//   - 確認 Close 可以安全呼叫兩次以上，不會 panic。
//   - 並不檢查底層 nats.Conn 的細節，只看你的 Close 寫法是不是 idempotent。
func TestNATSCoreBus_Close_IsSafe(t *testing.T) {
	bus := newTestBus(t)

	// 第一次 Close
	bus.Close()

	// 第二次 Close（理論上不該 panic 或出錯）
	bus.Close()
}
