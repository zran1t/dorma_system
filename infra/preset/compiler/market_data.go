// File: infra/preset/compiler/market_data.go
// Package: compiler
//
// 職責 (Responsibility):
//     將 MarketDataPreset 編譯為 MarketDataPacket：
//       - 依 exchange defaults + symbol.market override 合併（deep merge）
//       - 展開 realtime/candle 訂閱
//
// 注意事項 (Notes):
//     - markets.<market> 節點存在即表示啟用該 market。
//     - override 使用 pointer 結構，未提供欄位視為沿用 defaults（only-diff）。

package compiler

import (
	"fmt"

	"dorma_system/infra/preset/packet"
	"dorma_system/infra/preset/reader"
)

func CompileMarketData(p *reader.MarketDataPreset) (*packet.MarketDataPacket, error) {
	out := &packet.MarketDataPacket{
		Version:   p.Version,
		Summary:   p.Summary,
		Exchanges: map[string]packet.MarketDataExchangePacket{},
	}

	for ex, exPreset := range p.Exchanges {
		exOut := packet.MarketDataExchangePacket{
			Exchange:      ex,
			Subscriptions: []packet.MarketDataSubscription{},
		}

		for _, sym := range exPreset.Symbols {
			for marketName, overrideMarket := range sym.Markets {
				market, err := parseMarket(marketName)
				if err != nil {
					return nil, fmt.Errorf("market_data: exchange=%s pair=%s: %w", ex, sym.Pair, err)
				}

				base, err := getDefaultMarket(exPreset.Defaults, market)
				if err != nil {
					return nil, fmt.Errorf("market_data: exchange=%s: %w", ex, err)
				}

				merged := mergeMarketDataMarket(base, overrideMarket)
				exOut.Subscriptions = append(exOut.Subscriptions, expandMarketFeeds(ex, sym.Pair, market, merged)...)
			}
		}

		out.Exchanges[ex] = exOut
	}

	return out, nil
}

func expandMarketFeeds(ex, pair string, market packet.Market, m reader.MarketDataMarketPreset) []packet.MarketDataSubscription {
	var subs []packet.MarketDataSubscription

	if m.Feeds.TradesAll.Enabled {
		subs = append(subs, subSimple(ex, pair, market, "trades_all"))
	}
	if m.Feeds.Trades.Enabled {
		subs = append(subs, subSimple(ex, pair, market, "trades"))
	}
	if m.Feeds.BBO.Enabled {
		subs = append(subs, subSimple(ex, pair, market, "bbo"))
	}
	if m.Feeds.BookDelta.Enabled {
		subs = append(subs, subSimple(ex, pair, market, "book_delta"))
	}
	if m.Feeds.BookFull.Enabled {
		subs = append(subs, subSimple(ex, pair, market, "book_full"))
	}

	subs = append(subs, expandPriceFamily(ex, pair, market, "last", m.Feeds.Last)...)
	subs = append(subs, expandPriceFamily(ex, pair, market, "mark", m.Feeds.Mark)...)
	subs = append(subs, expandPriceFamily(ex, pair, market, "index", m.Feeds.Index)...)

	return subs
}

func expandPriceFamily(ex, pair string, market packet.Market, feed string, f reader.MarketDataPriceFamilyPreset) []packet.MarketDataSubscription {
	var subs []packet.MarketDataSubscription
	if f.Realtime.Enabled {
		subs = append(subs, packet.MarketDataSubscription{
			Exchange: ex, Pair: pair, Market: market, Feed: feed, Mode: "realtime",
		})
	}
	if f.Candle.Enabled {
		subs = append(subs, packet.MarketDataSubscription{
			Exchange: ex, Pair: pair, Market: market, Feed: feed, Mode: "candle",
			Intervals: append([]string{}, f.Candle.Intervals...),
		})
	}
	return subs
}

func subSimple(ex, pair string, market packet.Market, feed string) packet.MarketDataSubscription {
	return packet.MarketDataSubscription{
		Exchange: ex,
		Pair:     pair,
		Market:   market,
		Feed:     feed,
		Mode:     "realtime",
	}
}

func getDefaultMarket(d reader.MarketDataDefaultsPreset, m packet.Market) (reader.MarketDataMarketPreset, error) {
	switch m {
	case packet.MarketSpot:
		return d.Spot, nil
	case packet.MarketSwap:
		return d.Swap, nil
	case packet.MarketIndex:
		return d.Index, nil
	default:
		return reader.MarketDataMarketPreset{}, fmt.Errorf("unknown market: %s", m)
	}
}

func parseMarket(s string) (packet.Market, error) {
	switch s {
	case "spot":
		return packet.MarketSpot, nil
	case "swap":
		return packet.MarketSwap, nil
	case "index":
		return packet.MarketIndex, nil
	default:
		return "", fmt.Errorf("invalid market: %s", s)
	}
}

// mergeMarketDataMarket:
//   - base: defaults 的完整設定
//   - ov  : only-diff override（所有欄位皆可為 nil）
//
// 合併規則：
//   - ov 未提供欄位 ⇒ 沿用 base
//   - ov 提供欄位 ⇒ 覆寫 base（支援 enabled=false / intervals 覆寫）
func mergeMarketDataMarket(base reader.MarketDataMarketPreset, ov reader.MarketDataMarketOverridePreset) reader.MarketDataMarketPreset {
	out := base

	if ov.Feeds == nil {
		return out
	}
	fo := ov.Feeds

	applyFeedEnabled(&out.Feeds.TradesAll, fo.TradesAll)
	applyFeedEnabled(&out.Feeds.Trades, fo.Trades)
	applyFeedEnabled(&out.Feeds.BBO, fo.BBO)
	applyFeedEnabled(&out.Feeds.BookDelta, fo.BookDelta)
	applyFeedEnabled(&out.Feeds.BookFull, fo.BookFull)

	applyPriceFamily(&out.Feeds.Last, fo.Last)
	applyPriceFamily(&out.Feeds.Mark, fo.Mark)
	applyPriceFamily(&out.Feeds.Index, fo.Index)

	return out
}

func applyFeedEnabled(dst *reader.FeedEnabledPreset, ov *reader.FeedEnabledOverridePreset) {
	if ov == nil || ov.Enabled == nil {
		return
	}
	dst.Enabled = *ov.Enabled
}

func applyPriceFamily(dst *reader.MarketDataPriceFamilyPreset, ov *reader.MarketDataPriceFamilyOverridePreset) {
	if ov == nil {
		return
	}
	// realtime.enabled
	if ov.Realtime != nil && ov.Realtime.Enabled != nil {
		dst.Realtime.Enabled = *ov.Realtime.Enabled
	}
	// candle
	if ov.Candle != nil {
		if ov.Candle.Enabled != nil {
			dst.Candle.Enabled = *ov.Candle.Enabled
		}
		// intervals：只要 override 有給（len>0），就視為覆寫；允許顯式清空用 []（YAML 可寫 intervals: []）
		if ov.Candle.Intervals != nil {
			dst.Candle.Intervals = append([]string{}, ov.Candle.Intervals...)
		}
	}
}
