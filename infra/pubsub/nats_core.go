// infra/pubsub/nats_core.go
package pubsub

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

type natsCoreSub struct {
	sub *nats.Subscription
}

func (s *natsCoreSub) Unsubscribe() error { return s.sub.Unsubscribe() }
func (s *natsCoreSub) Drain() error       { return s.sub.Drain() }

type NATSCoreBus struct {
	nc   *nats.Conn
	opts BusOptions
}

func NewNATSCoreBus(opts BusOptions) (*NATSCoreBus, error) {
	if opts.PingInterval == 0 { opts.PingInterval = 10 * time.Second }
	if opts.ReconnectWait == 0 { opts.ReconnectWait = 500 * time.Millisecond }
	if opts.Timeout == 0 { opts.Timeout = 5 * time.Second }
	if opts.MaxReconnects == 0 { opts.MaxReconnects = -1 }

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

func (b *NATSCoreBus) Publish(ctx context.Context, subject string, data []byte) error {
	return b.nc.Publish(subject, data)
}

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
		Header:     nil, // 需要可再映射 msg.Header
		ReceivedAt: time.Now(),
	}, nil
}

func (b *NATSCoreBus) Subscribe(ctx context.Context, subject string, h Handler, opts ...SubOpt) (Subscription, error) {
	var so SubOptions
	for _, fn := range opts { fn(&so) }
	if so.MaxInflight <= 0 { so.MaxInflight = 1 }

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
	_ = b.nc.Flush() // 提升可見性
	return &natsCoreSub{sub: sub}, nil
}

func (b *NATSCoreBus) Close() {
	if b.nc != nil && !b.nc.IsClosed() {
		b.nc.Close()
	}
}