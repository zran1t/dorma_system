package book_group

import (
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type Chief struct {
	nc        *nats.Conn
	mu        sync.Mutex
	books     map[string]*OrderBook
	throttle  time.Duration // 每檔 FULL 最短間隔（0=不節流）
	verifyCRC bool          // 是否做 OKX CRC32 驗證
	sub       *nats.Subscription
}

func NewChief(nc *nats.Conn, throttle time.Duration) *Chief {
	return &Chief{
		nc:        nc,
		books:     make(map[string]*OrderBook),
		throttle:  throttle,
		verifyCRC: true,
	}
}

func (c *Chief) Start() error {
	sub, err := c.nc.Subscribe(subjDeltaWildcard, func(m *nats.Msg) {
		if err := c.handleDelta(m); err != nil {
			c.logf("[book_group] handleDelta error: %v", err)
		}
	})
	if err != nil {
		return err
	}
	c.sub = sub
	c.logf("book_group started: delta=%s full=CLEAN.OKX.BOOK.FULL.<BASE>.<QUOTE>.<SUF>", subjDeltaWildcard)
	return nil
}

func (c *Chief) Stop() {
	if c.sub != nil {
		_ = c.sub.Unsubscribe()
		c.sub = nil
	}
}

func (c *Chief) logf(format string, args ...any) {
	log.Printf(format, args...)
}