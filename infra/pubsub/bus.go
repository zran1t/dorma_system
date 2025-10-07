// infra/pubsub/bus.go
package pubsub

import (
	"context"
	"time"
)

// ─── 訊息/訂閱/處理器 ───────────────────────────────────────────────────────

type Message struct {
	Subject    string
	Data       []byte
	Header     map[string]string
	ReceivedAt time.Time
}

type Handler func(ctx context.Context, m *Message) error

type Subscription interface {
	Unsubscribe() error
	Drain() error
}

type SubOptions struct {
	QueueGroup string
	MaxInflight int
}

type SubOpt func(*SubOptions)

func WithQueue(group string) SubOpt      { return func(o *SubOptions) { o.QueueGroup = group } }
func WithMaxInflight(n int) SubOpt       { return func(o *SubOptions) { o.MaxInflight = n } }

// ─── Bus 介面（上層統一依賴這個） ─────────────────────────────────────────

type Bus interface {
	Publish(ctx context.Context, subject string, data []byte) error
	Request(ctx context.Context, subject string, data []byte) (*Message, error)
	Subscribe(ctx context.Context, subject string, h Handler, opts ...SubOpt) (Subscription, error)
	Close()
}

// ─── Core NATS 連線選項（不含 JetStream） ──────────────────────────────────

type BusOptions struct {
	URL           string
	Name          string
	PingInterval  time.Duration
	ReconnectWait time.Duration
	Timeout       time.Duration
	MaxReconnects int // -1=無限; 0=走 nats 預設
}