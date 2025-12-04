// File: data_dpt/kols/lf_market_data_kol/collect_group/chief.go
// Package: collect_group
//
// 職責 (Responsibility):
//     - 管理「多 interval」的 LF KLine Collector 群組：
//         - 依 exchange/baseFeed/intervals/canon 建立多支收集線。
//         - 每一個 interval 啟動一支獨立的 WS Collector。
//         - 提供 StartMany / Start / Stop 管理生命週期。
//     - 與 HF collect_group 的 Chief 概念類似，但這裡以 interval 做切分。
//
// 注意事項 (Notes):
//     - index-candle 會自動把 canonical 轉成 -INDEX 再做 symbol 解析。
//     - running map 以 interval 當 key；同一個 interval 不會重複啟動。
//     - Binan ce 目前只有骨架 adapter，還不會真的收資料。

package collect_group

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	// 無

	// === 系統內模組 (Internal Modules) ===
	"dorma_system/infra/pubsub"
	"dorma_system/infra/symbols"
	"dorma_system/infra/transport/ws"

	// 交付 OKX；Binance 暫留骨架
	binance "dorma_system/data_dpt/kols/lf_market_data_kol/collect_group/exchanges/binance"
	okx "dorma_system/data_dpt/kols/lf_market_data_kol/collect_group/exchanges/okx"
)

// Chief 管理「多 interval」的 Collector（每個 interval 一支 WS collector）。
//
// 功能:
//   - 持有 wsFactory / pub / symbols.Resolver。
//   - 以 interval 為 key，記錄當前在跑的 collector。
//   - 提供 StartMany / Start / Stop 管理整組收集線。
//
// 欄位說明:
//   - wsFactory: 產生 WSClient 的工廠（例如回傳 GorillaWS）。
//   - pub:       pubsub.Bus，用來發佈 RAW.* 至 NATS。
//   - resolver:  symbol 解析器，負責 canonical → native instId。
//   - running:   map[interval]*Collector，追蹤目前已啟動的收集員工。
//
// 契約 / 限制:
//   - interval 字串用作 map key，需要呼叫方自己維持命名一致性。
//   - 不會自動重啟失敗 collector；錯誤會回傳給呼叫方。
type Chief struct {
	wsFactory func() ws.WSClient
	pub       pubsub.Bus
	resolver  symbols.Resolver

	// key = interval（"1m","1D","1W"...），value = *Collector
	running map[string]*Collector
}

// NewChief 建立一個 LF KLine Chief 實例。
//
// 功能:
//   - 建立 running map。
//   - 綁定 WSClient 工廠、pubsub.Bus 與 symbol resolver。
//
// 參數:
//   - wsFactory: 建立 WSClient 的工廠方法。
//   - pub:       pubsub.Bus 實例。
//   - r:         symbols.Resolver，進行 canonical 轉 native symbol。
//
// 回傳:
//   - *Chief: 初始化好的 Chief 指標。
//   - 無 error。
func NewChief(wsFactory func() ws.WSClient, pub pubsub.Bus, r symbols.Resolver) *Chief {
	return &Chief{
		wsFactory: wsFactory,
		pub:       pub,
		resolver:  r,
		running:   make(map[string]*Collector),
	}
}

// StartMany 啟動多個 interval 的收集線（建議外部優先用這個）。
//
// 功能:
//   - 依參數決定要啟動哪些 interval 的 collector：
//     exchange  : "okx"（binance 目前是骨架）。
//     baseFeed  : "mark-price-candle" | "index-candle"。
//     intervals : 例如 []string{"1m","5m","1H","1D","1W","1M"}。
//     canon     : canonical symbol 清單。
//   - 對 index-candle：會自動把 canonical 轉成 -INDEX 再做 ResolveMany。
//   - 透過 resolver.ResolveMany 將 canonical → native instId。
//   - 每個 interval 建立一支 Collector，並立即 Start()。
//   - 已經在跑的 interval 會略過，不重複啟動。
//
// 參數:
//   - ctx:       上層 context，用於控制所有 collector 的生命週期。
//   - exchange:  交易所字串，如 "okx" / "binance"。
//   - baseFeed:  KLine 家族鍵，如 "mark-price-candle" / "index-candle"。
//   - intervals: interval 清單，不可為空且每個元素需非空白。
//   - canon:     canonical symbol 清單，不可為空。
//
// 回傳:
//   - error: 任何一個 collector 啟動失敗時回傳錯誤；成功全部啟動則為 nil。
//
// 備註:
//   - 若中途某個 interval 啟動失敗，前面已啟動的不會自動回滾。
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

	// 1) 選擇 adapter + endpoint
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

	// 2) index 家族：canonical 轉 -INDEX；mark family 維持 SWAP/SPOT 原樣。
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

		// 交給各 exchange 套件決定 channel 命名規則
		var chName string
		switch ex {
		case "okx":
			// e.g. "mark-price-candle1m" / "index-candle1Dutc"
			chName = okx.MakeChannel(bf, iv)
		case "binance":
			// 先給骨架，後續可依 Binance 規則實作
			chName = binance.MakeChannel(bf, iv)
		default:
			// 防禦性預設
			chName = bf + strings.TrimSpace(iv)
		}

		id := bf + "|" + iv // collector id：方便日誌辨識

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

// Start 啟動單一 interval 的 collector（薄封裝 StartMany）。
//
// 功能:
//   - 直接呼叫 StartMany(ctx, exchange, baseFeed, []string{interval}, canon)。
//
// 參數:
//   - ctx:       上層 context。
//   - exchange:  交易所字串。
//   - baseFeed:  KLine 家族鍵。
//   - interval:  單一 interval 字串，例："1m"。
//   - canon:     canonical symbol 清單。
//
// 回傳:
//   - error: StartMany 的錯誤直接透傳；成功時為 nil。
//
// 備註:
//   - interval 為空白時會直接回傳錯誤。
func (c *Chief) Start(ctx context.Context, exchange, baseFeed, interval string, canon []string) error {
	if strings.TrimSpace(interval) == "" {
		return fmt.Errorf("interval must not be empty")
	}
	return c.StartMany(ctx, exchange, baseFeed, []string{interval}, canon)
}

// Stop 停止所有 interval 的 collector。
//
// 功能:
//   - 對 running map 中所有 collector 呼叫 Stop()。
//   - Log 出每個 interval 的 stop 結果。
//   - 停止後從 running map 中移除對應 key。
//
// 參數:
//   - 無。
//
// 回傳:
//   - error: 目前一律回傳 nil；各 collector 的錯誤只會寫入 log。
//
// 備註:
//   - 不會關閉 pubsub.Bus 或 WS 工廠本身。
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
