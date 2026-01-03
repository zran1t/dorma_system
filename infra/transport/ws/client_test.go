// File: infra/transport/ws/client_test.go
// Package: ws
//
// 職責 (Responsibility):
//     測試 GorillaWS 的核心契約：
//       - 連線建立與 swap 行為（基本可用）
//       - SendJSON/SendPing 在多 goroutine 下不會 concurrent write
//       - Recv 會套用 ctx deadline 上限（min(預設 readTimeout, ctx.Deadline)）
//       - Close 會讓阻塞中的 Recv 返回（透過底層 conn close）
//
// 注意事項 (Notes):
//     - 使用 httptest + gorilla/websocket upgrader 建立測試用 WS server。
//     - 測試目標是「正確性與並發安全」而非效能指標。

package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func wsURLFromHTTP(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

type testWSServer struct {
	URL string

	recvTextCh chan string
	pingCh     chan struct{}
	closedCh   chan struct{}
}

func newTestWSServer(t *testing.T) *testWSServer {
	t.Helper()

	s := &testWSServer{
		recvTextCh: make(chan string, 4096),
		pingCh:     make(chan struct{}, 4096),
		closedCh:   make(chan struct{}),
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() {
			_ = c.Close()
			select {
			case <-s.closedCh:
			default:
				close(s.closedCh)
			}
		}()

		// 記錄 ping（並回 pong，保持連線健康）
		c.SetPingHandler(func(appData string) error {
			select {
			case s.pingCh <- struct{}{}:
			default:
			}
			return c.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(1*time.Second))
		})

		for {
			mt, msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.TextMessage {
				select {
				case s.recvTextCh <- string(msg):
				default:
				}
			}
		}
	}))
	t.Cleanup(ts.Close)

	s.URL = wsURLFromHTTP(ts.URL)
	return s
}

func TestGorillaWS_ConcurrentSendJSON_NoConcurrentWritePanic(t *testing.T) {
	srv := newTestWSServer(t)

	g := NewGorillaWS()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := g.Connect(ctx, srv.URL, nil); err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer func() { _ = g.Close() }()

	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			_ = g.SendJSON(ctx, `{"i":`+itoa(i)+`}`)
		}()
	}
	wg.Wait()

	// 確認 server 收到至少 n 筆 text（順序不保證）
	got := 0
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()

	for got < n {
		select {
		case <-deadline.C:
			t.Fatalf("server did not receive enough messages: got=%d want=%d", got, n)
		case <-srv.recvTextCh:
			got++
		}
	}
}

func TestGorillaWS_SendPing_ServerReceivesPing(t *testing.T) {
	srv := newTestWSServer(t)

	g := NewGorillaWS()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := g.Connect(ctx, srv.URL, nil); err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer func() { _ = g.Close() }()

	if err := g.SendPing(ctx); err != nil {
		t.Fatalf("SendPing error: %v", err)
	}

	select {
	case <-srv.pingCh:
	case <-time.After(600 * time.Millisecond):
		t.Fatalf("expected server to receive ping")
	}
}

func TestGorillaWS_Recv_RespectsCtxDeadlineUpperBound(t *testing.T) {
	// server 不送任何訊息，讓 Recv 只能靠 read deadline 返回
	srv := newTestWSServer(t)

	g := NewGorillaWS()
	if err := g.Connect(context.Background(), srv.URL, nil); err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	defer func() { _ = g.Close() }()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(120*time.Millisecond))
	defer cancel()

	start := time.Now()
	_, _, err := g.Recv(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected Recv error due to deadline, got nil")
	}
	// 允許抖動，但應在合理時間內返回
	if elapsed > 900*time.Millisecond {
		t.Fatalf("Recv took too long: %v", elapsed)
	}
}

func TestGorillaWS_Close_UnblocksBlockedRecv(t *testing.T) {
	srv := newTestWSServer(t)

	g := NewGorillaWS()
	if err := g.Connect(context.Background(), srv.URL, nil); err != nil {
		t.Fatalf("Connect error: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := g.Recv(context.Background())
		done <- err
	}()

	time.Sleep(80 * time.Millisecond)

	if err := g.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("expected Recv to return error after Close, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Recv did not return after Close")
	}
}

// 避免引入 strconv（測試檔內小工具即可）
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var b [32]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + (i % 10))
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
