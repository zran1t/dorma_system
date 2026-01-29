// File: infra/preset/validator/market_data.go
// Package: validator
//
// 職責 (Responsibility):
//     驗證 MarketDataPreset / MarketDataPacket 的最小合法性。

package validator

import (
	"dorma_system/infra/preset/packet"
	"dorma_system/infra/preset/reader"
)

func ValidateMarketDataPreset(p *reader.MarketDataPreset) error {
	if p.Version <= 0 {
		return errf("market_data preset: invalid version")
	}
	if len(p.Exchanges) == 0 {
		return errf("market_data preset: exchanges is empty")
	}
	for ex, exPreset := range p.Exchanges {
		if len(exPreset.Symbols) == 0 {
			// 允許空（代表此交易所暫不啟用任何 symbol），但至少 defaults 應存在
		}
		_ = ex
	}
	return nil
}

func ValidateMarketDataPacket(p *packet.MarketDataPacket) error {
	if p == nil {
		return errf("market_data packet: nil")
	}
	if p.Version <= 0 {
		return errf("market_data packet: invalid version")
	}
	return nil
}
