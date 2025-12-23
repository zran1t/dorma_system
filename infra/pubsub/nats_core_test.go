// File: infra/pubsub/nats_core_test.go
// Package: pubsub
//
// 職責 (Responsibility):
//
//	對 NATSCoreBus 做「真的連線」的整合測試。
//	測試內容包含：
//	  - NewNATSCoreBus 是否能順利建立連線。
//	  - Subscribe 一個 subject，然後 Publish 一筆資料，handler 是否有拿到。
//	  - Close 呼叫後不會 panic，也不會卡住。
//	這支測試「一定會連網」，如果環境中沒有可用的 NATS，測試會被 Skip。
package pubsub

import (
	"context"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	// 測試端點來源：與 NewNATSCoreBus 的策略一致（env 覆蓋 > 預設值）
	testEnvNATSURLKey   = "NATS_URL"
	testDefaultNATSURL  = "nats://127.0.0.1:4222"
	testDefaultNATSPort = "4222"
)

// testNATSURLFromEnvOrDefault 取得測試用 NATS URL 字串（允許逗號分隔多個 server）。
//
// 功能:
//   - 優先讀取環境變數 NATS_URL。
//   - 若為空，使用內建預設值。
//
// 回傳:
//   - string: 原始 URL 字串（可能包含逗號分隔）。
func testNATSURLFromEnvOrDefault() string {
	raw := strings.TrimSpace(os.Getenv(testEnvNATSURLKey))
	if raw == "" {
		raw = testDefaultNATSURL
	}
	return raw
}

// firstNATSURL 取逗號分隔清單中的第一個 URL（trim 後）。
//
// 功能:
//   - 支援 "a,b,c" 形式，取第一個 entry 作為探測目標。
//   - 若第一個為空，視為無效。
//
// 回傳:
//   - string: 第一個 URL。
//   - bool:   是否取得成功。
func firstNATSURL(raw string) (string, bool) {
	parts := strings.Split(raw, ",")
	if len(parts) == 0 {
		return "", false
	}
	first := strings.TrimSpace(parts[0])
	if first == "" {
		return "", false
	}
	return first, true
}

// tcpAddrFromNATSURL 由 NATS URL 推導出 TCP dial 的目標位址。
//
// 功能:
//   - 解析 URL，取得 host[:port]。
//   - 若未提供 port，預設補上 4222。
//
// 回傳:
//   - string: 可用於 net.DialTimeout 的位址字串。
//   - error:  解析失敗時回傳錯誤。
func tcpAddrFromNATSURL(uStr string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(uStr))
	if err != nil {
		return "", err
	}
	host := strings.TrimSpace(u.Host)
	if host == "" {
		return "", &url.Error{Op: "parse", URL: uStr, Err: url.InvalidHostError("")}
	}

	// 若未帶 port，補預設 4222。
	// net.SplitHostPort 對 IPv6/無 port 的行為不同；用最後 fallback 方式處理。
	if _, _, err := net.SplitHostPort(host); err != nil {
		// 缺 port 的情況（例如 "127.0.0.1" 或 "localhost"）
		// 這裡直接補 ":4222"；IPv6 若是 "[::1]" 也可接受。
		host = net.JoinHostPort(strings.Trim(host, "[]"), testDefaultNATSPort)
	}
	return host, nil
}

// requireNATSServer 確認測試目標 NATS server 是否可用；不可用就跳過整合測試。
//
// 功能:
//   - 依照與 NewNATSCoreBus 相同的端點來源策略，取得 NATS URL。
//   - 取第一個 server URL 推導 TCP 位址並嘗試連線。
//   - 若不可用則 t.Skip，避免 CI/本機未啟動 NATS 時整包測試直接 fail。
//
// 參數:
//   - t: 測試物件。
//
// 回傳:
//   - 無。
func requireNATSServer(t *testing.T) {
	t.Helper()

	raw := testNATSURLFromEnvOrDefault()
	first, ok := firstNATSURL(raw)
	if !ok {
		t.Skipf("跳過：NATS_URL 無效（空值或第一個 entry 為空）: %q", raw)
		return
	}

	addr, err := tcpAddrFromNATSURL(first)
	if err != nil {
		t.Skipf("跳過：無法解析 NATS URL（用於探測）: url=%q err=%v", first, err)
		return
	}

	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		t.Skipf("跳過：偵測不到 NATS (%s) from NATS_URL=%q: %v", addr, raw, err)
		return
	}
	_ = conn.Close()
}

// newTestBus 建一個可用的 NATSCoreBus。
//
// 前提:
//   - requireNATSServer 探測成功（否則測試會被 Skip）。
//
// 備註:
//   - 建立失敗時會直接 t.Fatal，避免後面還繼續跑。
func newTestBus(t *testing.T) *NATSCoreBus {
	t.Helper()

	requireNATSServer(t)

	bus, err := NewNATSCoreBus()
	if err != nil {
		t.Fatalf("無法建立 NATSCoreBus: %v", err)
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
//   - 若環境中沒有可用 NATS，本測試會 Skip。
func TestNATSCoreBus_PublishSubscribe(t *testing.T) {
	bus := newTestBus(t)
	defer bus.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	subject := "test.natscorebus.pubsub." + time.Now().Format("150405.000000000")
	payload := []byte(`{"kind":"test","msg":"hello"}`)

	msgCh := make(chan *Message, 1)

	_, err := bus.Subscribe(ctx, subject, func(ctx context.Context, m *Message) error {
		select {
		case msgCh <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatalf("Subscribe 失敗: %v", err)
	}

	if err := bus.Publish(ctx, subject, payload); err != nil {
		t.Fatalf("Publish 失敗: %v", err)
	}

	select {
	case got := <-msgCh:
		if got.Subject != subject {
			t.Fatalf("Subject 預期 = %q, 實際 = %q", subject, got.Subject)
		}
		if string(got.Data) != string(payload) {
			t.Fatalf("Data 預期 = %q, 實際 = %q", string(payload), string(got.Data))
		}
	case <-ctx.Done():
		t.Fatalf("在 timeout 內沒有收到訊息，可能 NATS 不可用或訂閱/發佈流程有問題: %v", ctx.Err())
	}
}

// TestNATSCoreBus_Close_IsSafe
//
// 功能:
//   - 確認 Close 可以安全呼叫兩次以上，不會 panic。
//   - 並不檢查底層 nats.Conn 的細節，只看你的 Close 寫法是不是 idempotent。
//
// 說明:
//   - 若環境中沒有可用 NATS，本測試會 Skip。
func TestNATSCoreBus_Close_IsSafe(t *testing.T) {
	bus := newTestBus(t)

	bus.Close()
	bus.Close()
}
