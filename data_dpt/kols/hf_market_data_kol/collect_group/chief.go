package collect_group

import (
	"context"
	"fmt"
	"strings"
	"time"

	"dorma_system/infra/pubsub"
	"dorma_system/infra/symbols"
	"dorma_system/infra/transport/ws"

	binance "dorma_system/data_dpt/kols/hf_market_data_kol/collect_group/exchanges/binance"
	okx "dorma_system/data_dpt/kols/hf_market_data_kol/collect_group/exchanges/okx"
)

type Chief struct {
	wsFactory func() ws.WSClient
	pub       pubsub.Bus
	resolver  symbols.Resolver

	running *ws.Collector
}

func NewChief(wsFactory func() ws.WSClient, pub pubsub.Bus, r symbols.Resolver) *Chief {
	return &Chief{wsFactory: wsFactory, pub: pub, resolver: r}
}

// Start 直接啟動一個收集任務（internal resolve）
func (c *Chief) Start(ctx context.Context, exchange, feed string, canon []string) error {
	if c.running != nil {
		return fmt.Errorf("already running; stop first")
	}

	ex := strings.ToLower(strings.TrimSpace(exchange))
	fd := strings.ToLower(strings.TrimSpace(feed))

	// 1) 選 adapter + wsURL + 預設 ping 間隔（依 feed 決定）
	var (
		adj         ws.Adapter
		url         string
		defaultPing time.Duration
	)
	switch ex {
	case "okx":
		adj = okx.NewAdapter()
		switch fd {
		case "trades-all":
			// trades-all 走 business domain，並開啟定期 ping
			url = "wss://ws.okx.com:8443/ws/v5/business"
			defaultPing = 25 * time.Second
		default:
			// 其餘走 public；OKX public 不強制應用層 ping
			url = "wss://ws.okx.com:8443/ws/v5/public"
			defaultPing = 0
		}
	case "binance":
		adj = binance.NewAdapter()
		url = "wss://stream.binance.com:9443/stream"
		defaultPing = 30 * time.Second
	default:
		return fmt.Errorf("unsupported exchange: %s", exchange)
	}
	
	// 如果是 index-tickers feed，將所有 symbol 從 -SWAP / -SPOT 轉成 -INDEX
	if strings.EqualFold(fd, "index-tickers") {
		indexCanon := make([]string, 0, len(canon))
		for _, c := range canon {
			parts := strings.Split(strings.TrimSpace(c), "-")
			if len(parts) >= 2 {
				indexCanon = append(indexCanon, strings.ToUpper(parts[0]+"-"+parts[1]+"-INDEX"))
			}
		}
		canon = indexCanon
	}

	// 2) 在 Chief 內部反查：canonical -> native
	exSymbols, err := c.resolver.ResolveMany(canon, ex)
	if err != nil {
		return fmt.Errorf("resolve symbols for %s: %w", ex, err)
	}
 
	// 3) 組 ws.Collector 設定並啟動
	client := c.wsFactory()
	conf := ws.CollectorConfig{
		URL:              url,
		Headers:          nil,
		Channels:         []string{fd},
		Symbols:          exSymbols,
		PingEvery:        defaultPing,     // business 會定期送 ping，避免 4004
		ReconnectBackoff: 3 * time.Second, // 斷線回補節流
	}
	collector := ws.NewCollector(client, adj, c.pub, conf)

	if err := collector.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := collector.Subscribe(ctx); err != nil {
		_ = collector.Close()
		return fmt.Errorf("subscribe: %w", err)
	}
	go func() { _ = collector.Run(ctx) }()

	c.running = collector
	return nil
}

func (c *Chief) Stop() error {
	if c.running == nil {
		return nil
	}
	err := c.running.Close()
	c.running = nil
	return err
}