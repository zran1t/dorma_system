// data_dpt/kols/lf_market_data_kol/collect_group/chief.go
package collect_group

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"dorma_system/infra/pubsub"
	"dorma_system/infra/symbols"
	"dorma_system/infra/transport/ws"

	// 交付 OKX；Binance 暫留骨架
	okx "dorma_system/data_dpt/kols/lf_market_data_kol/collect_group/exchanges/okx"
	binance "dorma_system/data_dpt/kols/lf_market_data_kol/collect_group/exchanges/binance"
)

// Chief：管理「多 interval」的 Collector（每個 interval 一支 WS collector）
type Chief struct {
	wsFactory func() ws.WSClient
	pub       pubsub.Bus
	resolver  symbols.Resolver

	// key=interval（"1m","1D","1W"...），value=*Collector
	running map[string]*Collector
}

func NewChief(wsFactory func() ws.WSClient, pub pubsub.Bus, r symbols.Resolver) *Chief {
	return &Chief{
		wsFactory: wsFactory,
		pub:       pub,
		resolver:  r,
		running:   make(map[string]*Collector),
	}
}

// StartMany 啟動多個 interval 收集線（建議用這個）
//   exchange  : "okx"（binance 先骨架）
//   baseFeed  : "mark-price-candle" | "index-candle"
//   intervals : 例如 []string{"1m","5m","1H","1D","1W","1M"...}
//   canon     : 系統標準化 symbol（index 家族會自動轉成 -INDEX 再 resolve）
func (c *Chief) StartMany(ctx context.Context, exchange, baseFeed string, intervals []string, canon []string) error {
	ex := strings.ToLower(strings.TrimSpace(exchange))
	bf := strings.ToLower(strings.TrimSpace(baseFeed))
	if ex == "" || bf == "" {
		return fmt.Errorf("exchange/baseFeed must not be empty")
	}
	if len(intervals) == 0 {
		return fmt.Errorf("intervals must not be empty")
	}
	if len(canon) == 0 {
		return fmt.Errorf("symbols must not be empty")
	}

	// 1) adapter + endpoint
	var (
		adj ws.Adapter
		url string
	)
	switch ex {
	case "okx":
		adj = okx.NewAdapter()
		url = "wss://ws.okx.com:8443/ws/v5/business" // KLine 用 business
	case "binance":
		adj = binance.NewAdapter() // 骨架
		url = "wss://stream.binance.com:9443/stream"
	default:
		return fmt.Errorf("unsupported exchange: %s", exchange)
	}

	// 2) index 家族：canonical 轉 -INDEX；mark family 維持 SWAP/SPOT 原樣
	syms := canon
	if bf == "index-candle" {
		indexCanon := make([]string, 0, len(canon))
		for _, s := range canon {
			parts := strings.Split(strings.TrimSpace(s), "-")
			if len(parts) >= 2 {
				indexCanon = append(indexCanon, strings.ToUpper(parts[0]+"-"+parts[1]+"-INDEX"))
			}
		}
		syms = indexCanon
	}

	// 3) canonical → native instId
	exSymbols, err := c.resolver.ResolveMany(syms, ex)
	if err != nil {
		return fmt.Errorf("resolve symbols for %s: %w", ex, err)
	}

	// 4) 每個 interval 啟一支 collector
	for _, ivRaw := range intervals {
		iv := strings.TrimSpace(ivRaw)
		if iv == "" {
			continue
		}
		if _, exists := c.running[iv]; exists {
			log.Printf("[lf.kline] interval=%s already running, skip", iv)
			continue
		}

		chName := makeOKXChannel(bf, iv) // e.g. "mark-price-candle1m" 或 "index-candle1Dutc"
		id := bf + "|" + iv              // collector id：方便日誌辨識

		col := NewCollector(
			id,
			c.wsFactory,
			adj,
			c.pub,
			url,
			[]string{chName}, // 單 channel（已包含 interval）
			exSymbols,        // 同一個 interval 對多個 instId
			0,                // ✅ WS-level Ping 交給 Adapter.Heartbeat() 做 app ping
			3*time.Second,    // 重連 backoff
		)
		if err := col.Start(ctx); err != nil {
			return fmt.Errorf("start collector (interval=%s): %w", iv, err)
		}
		c.running[iv] = col
		log.Printf("[lf.kline] started interval=%s channel=%s symbols=%d", iv, chName, len(exSymbols))
	}
	return nil
}

// Start：單 interval 薄封裝（與 HF 風格一致）；建議外部還是用 StartMany 批次管控
func (c *Chief) Start(ctx context.Context, exchange, baseFeed, interval string, canon []string) error {
	if strings.TrimSpace(interval) == "" {
		return fmt.Errorf("interval must not be empty")
	}
	return c.StartMany(ctx, exchange, baseFeed, []string{interval}, canon)
}

// Stop：停止全部 interval 的 collector
func (c *Chief) Stop() error {
	for iv, col := range c.running {
		if col == nil {
			continue
		}
		if err := col.Stop(); err != nil {
			log.Printf("[lf.kline] stop interval=%s error: %v", iv, err)
		}
		delete(c.running, iv)
	}
	return nil
}

// makeOKXChannel：把 baseFeed + interval → OKX 最終 channel；
// 只有 OKX 有提供 utc 版本的 interval 才加 "utc"
func makeOKXChannel(baseFeed, interval string) string {
	base := strings.ToLower(strings.TrimSpace(baseFeed))
	iv := strings.TrimSpace(interval)

	// OKX 提供 utc 版的清單（key 使用「標準化大小寫」）
	utcSet := map[string]struct{}{
		"6H": {}, "12H": {},
		"1D": {}, "2D": {}, "3D": {}, "5D": {},
		"1W": {},
		"1M": {}, "3M": {},
		"1Y": {},
	}

	// 分鐘/小時/日週月年的標準化：保持你外部傳入的語意
	norm := strings.ToUpper(iv) // 例如 "1m" → "1M"（分鐘與月份會混？我們額外處理）
	// 分鐘用小寫 m，月份用大寫 M；這裡把常見分鐘規格轉回來
	switch iv {
	case "1m", "3m", "5m", "15m", "30m":
		norm = iv // 分鐘維持原樣（小寫 m）
	case "1h", "2h", "4h", "6h", "12h":
		norm = strings.ToUpper(iv) // "1H"...
	}

	// 是否有 utc 版本
	if _, ok := utcSet[norm]; ok {
		// 月份在 OKX utc 用 "1Mutc/3Mutc"，日/週/小時也是大寫 + utc
		return base + norm + "utc"
	}

	// 其他（分鐘、多數小時）沒有 utc 版
	return base + iv
}