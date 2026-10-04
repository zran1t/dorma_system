// File: infra/preset/compiler/instrument_data.go
// Package: compiler
//
// 職責 (Responsibility):
//     將 InstrumentDataPreset 編譯為 InstrumentDataPacket：
//       - 以 exchange 為邊界輸出 enabled feeds
//       - symbol-level feed 透過 symbols 過濾 instId
//
// 注意事項 (Notes):
//     - baseline（instrument_info / price_limit）不在此封包中表達；由系統流程強制保證可用。

package compiler

import (
	"dorma_system/infra/preset/packet"
	"dorma_system/infra/preset/reader"
)

func CompileInstrumentData(p *reader.InstrumentDataPreset) (*packet.InstrumentDataPacket, error) {
	out := &packet.InstrumentDataPacket{
		Version:   p.Version,
		Summary:   p.Summary,
		Exchanges: map[string]packet.InstrumentDataExchangePacket{},
	}

	for ex, exPreset := range p.Exchanges {
		exOut := packet.InstrumentDataExchangePacket{
			Exchange: ex,
			Feeds:    []packet.InstrumentFeedIntent{},
		}

		for feedName, feedPreset := range exPreset.Feeds {
			exOut.Feeds = append(exOut.Feeds, packet.InstrumentFeedIntent{
				Exchange: ex,
				Feed:     feedName,
				Enabled:  feedPreset.Enabled,
				Symbols:  append([]string{}, feedPreset.Symbols...),
			})
		}

		out.Exchanges[ex] = exOut
	}

	return out, nil
}
