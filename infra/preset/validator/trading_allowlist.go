// File: infra/preset/validator/trading_allowlist.go
// Package: validator
//
// 職責 (Responsibility):
//     驗證 TradingAllowlistPreset / TradingAllowlistPacket 的最小合法性。

package validator

import (
	"dorma_system/infra/preset/packet"
	"dorma_system/infra/preset/reader"
)

func ValidateTradingAllowlistPreset(p *reader.TradingAllowlistPreset) error {
	if p.Version <= 0 {
		return errf("trading_allowlist preset: invalid version")
	}
	if len(p.Exchanges) == 0 {
		return errf("trading_allowlist preset: exchanges is empty")
	}
	return nil
}

func ValidateTradingAllowlistPacket(p *packet.TradingAllowlistPacket) error {
	if p == nil {
		return errf("trading_allowlist packet: nil")
	}
	if p.Version <= 0 {
		return errf("trading_allowlist packet: invalid version")
	}
	return nil
}
