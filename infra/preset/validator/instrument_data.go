// File: infra/preset/validator/instrument_data.go
// Package: validator
//
// 職責 (Responsibility):
//     驗證 InstrumentDataPreset / InstrumentDataPacket 的最小合法性。

package validator

import (
	"dorma_system/infra/preset/packet"
	"dorma_system/infra/preset/reader"
)

func ValidateInstrumentDataPreset(p *reader.InstrumentDataPreset) error {
	if p.Version <= 0 {
		return errf("instrument_data preset: invalid version")
	}
	if len(p.Exchanges) == 0 {
		return errf("instrument_data preset: exchanges is empty")
	}
	return nil
}

func ValidateInstrumentDataPacket(p *packet.InstrumentDataPacket) error {
	if p == nil {
		return errf("instrument_data packet: nil")
	}
	if p.Version <= 0 {
		return errf("instrument_data packet: invalid version")
	}
	return nil
}
