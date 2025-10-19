// data_dpt/kols/lf_market_data_kol/collect_group/collector.go
package collect_group

import (
	"context"
	"fmt"
	"time"

	"dorma_system/infra/pubsub"
	"dorma_system/infra/transport/ws"
)

// Collector = 一個具體的收集員工，包裝 infra 的 ws.Collector
type Collector struct {
	id  string        // 工號字串（自訂：例如 "mark-price-candle|1D"）
	run *ws.Collector // 實際執行單元
}

func NewCollector(
	id string,
	wsFactory func() ws.WSClient, // 工廠函數：產生 WSClient（例：Gorilla）
	adj ws.Adapter,               // 交易所 adapter（build sub / handle / heartbeat）
	pub pubsub.Bus,               // 發佈總線
	url string,                   // WebSocket 端點
	channels []string,            // 通道（可多個；本案每個 collector 一個 interval → 一個 channel）
	symbols []string,             // 該 collector 要訂閱的 instId 清單（native）
	pingEvery time.Duration,      // ping 間隔（OKX business 建議 ~25s）
	backoff time.Duration,        // 斷線重連等待間隔
) *Collector {
	client := wsFactory()
	conf := ws.CollectorConfig{
		URL:              url,
		Channels:         channels,
		Symbols:          symbols,
		PingEvery:        pingEvery,
		ReconnectBackoff: backoff,
	}
	return &Collector{
		id:  id,
		run: ws.NewCollector(client, adj, pub, conf),
	}
}

func (c *Collector) ID() string { return c.id }

func (c *Collector) Start(ctx context.Context) error {
	if err := c.run.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := c.run.Subscribe(ctx); err != nil {
		_ = c.run.Close()
		return fmt.Errorf("subscribe: %w", err)
	}
	go func() { _ = c.run.Run(ctx) }()
	return nil
}

func (c *Collector) Stop() error {
	if c.run == nil {
		return nil
	}
	return c.run.Close()
}