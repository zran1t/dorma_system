// File: infra/preset/compiler/trading_allowlist.go
// Package: compiler
//
// 職責 (Responsibility):
//     將 TradingAllowlistPreset 編譯為 TradingAllowlistPacket：
//       - markets.<market> 節點存在即代表允許交易該 market
//
// 注意事項 (Notes):
//     - allowlist 是治理/風控層資料源（source of truth）。

package compiler

import (
	"fmt"

	"dorma_system/infra/preset/packet"
	"dorma_system/infra/preset/reader"
)

func CompileTradingAllowlist(p *reader.TradingAllowlistPreset) (*packet.TradingAllowlistPacket, error) {
	out := &packet.TradingAllowlistPacket{
		Version:   p.Version,
		Summary:   p.Summary,
		Exchanges: map[string]packet.TradingAllowlistExchangePacket{},
	}

	for ex, exPreset := range p.Exchanges {
		exOut := packet.TradingAllowlistExchangePacket{
			Exchange: ex,
			Allowed:  []packet.TradableInstrument{},
		}

		for _, sym := range exPreset.Symbols {
			for marketName := range sym.Markets {
				market, err := parseMarket(marketName)
				if err != nil {
					return nil, fmt.Errorf("trading_allowlist: exchange=%s pair=%s: %w", ex, sym.Pair, err)
				}
				exOut.Allowed = append(exOut.Allowed, packet.TradableInstrument{
					Exchange: ex,
					Pair:     sym.Pair,
					Market:   market,
				})
			}
		}

		out.Exchanges[ex] = exOut
	}

	return out, nil
}
