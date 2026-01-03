// File: infra/transport/ws/collect_base_test.go
// Package: ws
//
// 職責 (Responsibility):
//     測試 Collector.Run 的主流程與重連行為：
//       - 收到 TextMessage -> Adapter.Handle -> Publisher.Publish
//       - Recv 回錯 -> 進入重連 loop -> Connect + Subscribe 成功後恢復收訊
//       - Heartbeat tick 會觸發 SendJSON/SendPing（依 Adapter.Heartbeat payload 決定）
//
// 注意事項 (Notes):
//     - 使用 FakeWSClient 來精準控制 Recv 行為與注入斷線。
//     - 測試目標是「狀態機正確性」而非網路真實行為。
//     - Publisher 介面僅有 Publish(ctx, subject, data) 一個方法，測試 stub 只需實作此方法。

package ws

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"dorma_system/infra/pubsub"
)

type fakeWSClient struct {
	connectN  int64
	closeN    int64
	sendJSONN int64
	sendPingN int64

	mu        sync.RWMutex
	curCh     chan fakeRecvItem
	curClosed int32
}

type fakeRecvItem struct {
	msgType int
	data    []byte
	err     error
}

func newFakeWSClient() *fakeWSClient {
	return &fakeWSClient{
		curCh: make(chan fakeRecvItem, 256),
	}
}

func (f *fakeWSClient) Connect(ctx context.Context, url string, hdr http.Header) error {
	atomic.AddInt64(&f.connectN, 1)

	// 模擬「新連線生命週期」：Connect 後換一條新的 recv channel
	f.mu.Lock()
	f.curCh = make(chan fakeRecvItem, 256)
	atomic.StoreInt32(&f.curClosed, 0)
	f.mu.Unlock()

	return nil
}

func (f *fakeWSClient) SendJSON(ctx context.Context, text string) error {
	atomic.AddInt64(&f.sendJSONN, 1)
	return nil
}

func (f *fakeWSClient) SendPing(ctx context.Context) error {
	atomic.AddInt64(&f.sendPingN, 1)
	return nil
}

func (f *fakeWSClient) Close() error {
	atomic.AddInt64(&f.closeN, 1)

	f.mu.Lock()
	if atomic.CompareAndSwapInt32(&f.curClosed, 0, 1) {
		close(f.curCh)
	}
	f.mu.Unlock()

	return nil
}

func (f *fakeWSClient) Recv(ctx context.Context) (int, []byte, error) {
	f.mu.RLock()
	ch := f.curCh
	f.mu.RUnlock()

	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case it, ok := <-ch:
		if !ok {
			return 0, nil, errors.New("fake ws closed")
		}
		if it.err != nil {
			return 0, nil, it.err
		}
		return it.msgType, it.data, nil
	}
}

func (f *fakeWSClient) push(msgType int, data []byte) {
	f.mu.RLock()
	ch := f.curCh
	f.mu.RUnlock()
	ch <- fakeRecvItem{msgType: msgType, data: data}
}

func (f *fakeWSClient) pushErr(err error) {
	f.mu.RLock()
	ch := f.curCh
	f.mu.RUnlock()
	ch <- fakeRecvItem{err: err}
}

type fakeAdapter struct {
	hbPayload string
	hbEvery   time.Duration

	subMsgs []string

	handleSubject string
	handleBody    []byte
	handleErr     error
}

func (a *fakeAdapter) BuildSubscribeMsgs(channels []string, symbols []string) ([]string, error) {
	if a.subMsgs != nil {
		return a.subMsgs, nil
	}
	return []string{`{"op":"sub"}`}, nil
}

func (a *fakeAdapter) Heartbeat() (payload string, every time.Duration) {
	return a.hbPayload, a.hbEvery
}

func (a *fakeAdapter) Handle(raw []byte) (subject string, body []byte, err error) {
	return a.handleSubject, a.handleBody, a.handleErr
}

type fakePublisher struct {
	mu    sync.Mutex
	calls []pubCall
}

type pubCall struct {
	subject string
	data    []byte
}

func (p *fakePublisher) Publish(ctx context.Context, subject string, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, pubCall{
		subject: subject,
		data:    append([]byte(nil), data...),
	})
	return nil
}

var _ pubsub.Publisher = (*fakePublisher)(nil)

func TestCollector_Run_PublishAndReconnect(t *testing.T) {
	wscli := newFakeWSClient()
	adj := &fakeAdapter{
		hbPayload:     "",
		hbEvery:       0,
		handleSubject: "s.test",
		handleBody:    []byte("ok"),
	}
	pub := &fakePublisher{}

	c := NewCollector(wscli, adj, pub, CollectorConfig{
		URL:              "ws://fake",
		Headers:          nil,
		Channels:         []string{"c1"},
		Symbols:          []string{"BTCUSDT"},
		PingEvery:        0,
		ReconnectBackoff: 10 * time.Millisecond,
	})

	// 符合你原先使用習慣：先 Connect + Subscribe，再 Run
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("Connect error: %v", err)
	}
	if err := c.Subscribe(context.Background()); err != nil {
		t.Fatalf("Subscribe error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	// 送第一筆正常訊息 -> 應 publish
	wscli.push(websocket.TextMessage, []byte(`{"x":1}`))
	waitPublished(t, pub, 1, 800*time.Millisecond)

	// 注入 Recv error -> 觸發重連
	wscli.pushErr(errors.New("boom recv"))

	// Run 的重連會呼叫 Connect：Connect 次數應 >= 2
	waitAtLeastInt64(t, &wscli.connectN, 2, 800*time.Millisecond)

	// 重連後再送一筆 -> 應可繼續 publish
	wscli.push(websocket.TextMessage, []byte(`{"x":2}`))
	waitPublished(t, pub, 2, 800*time.Millisecond)

	// 停止
	cancel()

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Run did not exit after cancel")
	}
}

func TestCollector_Run_Heartbeat_PayloadUsesSendJSON(t *testing.T) {
	wscli := newFakeWSClient()
	adj := &fakeAdapter{
		hbPayload:     `{"op":"hb"}`, // payload 非空 -> 走 SendJSON
		hbEvery:       20 * time.Millisecond,
		handleSubject: "s.test",
		handleBody:    []byte("ok"),
	}
	pub := &fakePublisher{}

	c := NewCollector(wscli, adj, pub, CollectorConfig{
		URL:              "ws://fake",
		Headers:          nil,
		Channels:         []string{"c1"},
		Symbols:          []string{"BTCUSDT"},
		PingEvery:        0,
		ReconnectBackoff: 10 * time.Millisecond,
	})

	_ = c.Connect(context.Background())
	_ = c.Subscribe(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	time.Sleep(90 * time.Millisecond)
	cancel()

	<-runDone

	if atomic.LoadInt64(&wscli.sendJSONN) == 0 {
		t.Fatalf("expected heartbeat to trigger SendJSON at least once")
	}
}

func TestCollector_Run_Heartbeat_EmptyPayloadUsesSendPing(t *testing.T) {
	wscli := newFakeWSClient()
	adj := &fakeAdapter{
		hbPayload:     "", // payload 空 -> 走 SendPing
		hbEvery:       20 * time.Millisecond,
		handleSubject: "s.test",
		handleBody:    []byte("ok"),
	}
	pub := &fakePublisher{}

	c := NewCollector(wscli, adj, pub, CollectorConfig{
		URL:              "ws://fake",
		Headers:          nil,
		Channels:         []string{"c1"},
		Symbols:          []string{"BTCUSDT"},
		PingEvery:        0,
		ReconnectBackoff: 10 * time.Millisecond,
	})

	_ = c.Connect(context.Background())
	_ = c.Subscribe(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- c.Run(ctx) }()

	time.Sleep(90 * time.Millisecond)
	cancel()

	<-runDone

	if atomic.LoadInt64(&wscli.sendPingN) == 0 {
		t.Fatalf("expected heartbeat to trigger SendPing at least once")
	}
}

func waitPublished(t *testing.T, pub *fakePublisher, n int, timeout time.Duration) {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		pub.mu.Lock()
		got := len(pub.calls)
		pub.mu.Unlock()

		if got >= n {
			return
		}

		select {
		case <-deadline.C:
			t.Fatalf("publish calls timeout: got=%d want>=%d", got, n)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitAtLeastInt64(t *testing.T, v *int64, want int64, timeout time.Duration) {
	t.Helper()

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		got := atomic.LoadInt64(v)
		if got >= want {
			return
		}

		select {
		case <-deadline.C:
			t.Fatalf("counter timeout: got=%d want>=%d", got, want)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
