package collect_group

import (
	"context"
	"fmt"
	"time"

	"dorma_system/infra/pubsub"
	"dorma_system/infra/transport/ws"
)

// Collector = 一個具體的收集員工，包裝 ws.Collector
type Collector struct {
	id  string		  // 工號字串
	run *ws.Collector // infra collect 基底
}

// NewCollector 建立一個收集員工，但還沒連線
func NewCollector(
	id string,
	wsFactory func() ws.WSClient,  // wsFactory: 工廠函數，用來生出一個 WSClient（例如 GorillaWS）
	adj ws.Adapter, // 交易所包 handler,heartbeat,buildsubscribeMsgs
	pub pubsub.Bus, // publish
	url string, 	// ws端點
	channels []string, 
	symbols []string,
	pingEvery time.Duration, 	// ping間隔 
	backoff time.Duration, 		// 錯誤重連等待間隔
) *Collector {
	client := wsFactory() // 生成一個ws客戶端
	conf := ws.CollectorConfig{ // 注入各種參數
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

// Start 會 Connect + Subscribe + 在背景跑 Run()
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

// Stop 會關閉連線，讓 Run() 自行退出
func (c *Collector) Stop() error {
	if c.run == nil {
		return nil
	}
	return c.run.Close()
}