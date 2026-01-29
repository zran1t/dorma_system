// File: infra/preset/loader.go
// Package: preset
//
// 職責 (Responsibility):
//     提供對外唯一入口：讀取 system_presets，並編譯為各部門可直接使用的 packets。
//
// 注意事項 (Notes):
//     - Loader 僅負責：I/O → 解析 → 基本驗證 → 編譯輸出。
//     - Adapter 能力驗證（capability table）不在此處做；應由 validator/更上層流程處理。
//     - 若要避免重複讀檔，可使用 LoadAllPackets() 一次讀取並分發。

package preset

import (
	"path/filepath"

	"dorma_system/infra/preset/compiler"
	"dorma_system/infra/preset/packet"
	"dorma_system/infra/preset/reader"
	"dorma_system/infra/preset/validator"
)

type AllPackets struct {
	MarketData     *packet.MarketDataPacket
	InstrumentData *packet.InstrumentDataPacket
	TradingAllow   *packet.TradingAllowlistPacket
}

func LoadMarketDataPacket() (*packet.MarketDataPacket, error) {
	p := filepath.FromSlash(DefaultMarketDataStartupPath)

	pre, err := reader.LoadMarketDataPreset(p)
	if err != nil {
		return nil, err
	}
	if err := validator.ValidateMarketDataPreset(pre); err != nil {
		return nil, err
	}
	out, err := compiler.CompileMarketData(pre)
	if err != nil {
		return nil, err
	}
	if err := validator.ValidateMarketDataPacket(out); err != nil {
		return nil, err
	}
	return out, nil
}

func LoadInstrumentDataPacket() (*packet.InstrumentDataPacket, error) {
	p := filepath.FromSlash(DefaultInstrumentDataStartupPath)

	pre, err := reader.LoadInstrumentDataPreset(p)
	if err != nil {
		return nil, err
	}
	if err := validator.ValidateInstrumentDataPreset(pre); err != nil {
		return nil, err
	}
	out, err := compiler.CompileInstrumentData(pre)
	if err != nil {
		return nil, err
	}
	if err := validator.ValidateInstrumentDataPacket(out); err != nil {
		return nil, err
	}
	return out, nil
}

func LoadTradingAllowlistPacket() (*packet.TradingAllowlistPacket, error) {
	p := filepath.FromSlash(DefaultTradingAllowlistPath)

	pre, err := reader.LoadTradingAllowlistPreset(p)
	if err != nil {
		return nil, err
	}
	if err := validator.ValidateTradingAllowlistPreset(pre); err != nil {
		return nil, err
	}
	out, err := compiler.CompileTradingAllowlist(pre)
	if err != nil {
		return nil, err
	}
	if err := validator.ValidateTradingAllowlistPacket(out); err != nil {
		return nil, err
	}
	return out, nil
}

func LoadAllPackets() (*AllPackets, error) {
	md, err := LoadMarketDataPacket()
	if err != nil {
		return nil, err
	}
	id, err := LoadInstrumentDataPacket()
	if err != nil {
		return nil, err
	}
	ta, err := LoadTradingAllowlistPacket()
	if err != nil {
		return nil, err
	}
	return &AllPackets{
		MarketData:     md,
		InstrumentData: id,
		TradingAllow:   ta,
	}, nil
}
