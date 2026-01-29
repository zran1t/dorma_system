// File: infra/preset/packet/instrument_data_types.go
// Package: packet
//
// 職責 (Responsibility):
//     Instrument Data KOL 所需之啟動封包：描述要訂閱哪些 instrument/derivative public feeds。
//     feed scope 由 preset 決定：market-level 或 symbol-level（symbols filter）。
//
// 注意事項 (Notes):
//     - Symbol-level feeds 使用 Symbols 過濾 instId（例如 BTC-USDT-SWAP）。
//     - Market-level feeds 的 Symbols 通常為空，代表依 adapter 預設 scope 訂閱（例如 instType=SWAP）。

package packet

type InstrumentDataPacket struct {
	Version   int                                     `json:"version"`
	Summary   string                                  `json:"summary"`
	Exchanges map[string]InstrumentDataExchangePacket `json:"exchanges"`
}

type InstrumentDataExchangePacket struct {
	Exchange string                 `json:"exchange"`
	Feeds    []InstrumentFeedIntent `json:"feeds"`
}

type InstrumentFeedIntent struct {
	Exchange string   `json:"exchange"`
	Feed     string   `json:"feed"` // open_interest, liquidation_orders, adl_warning, funding_rate
	Enabled  bool     `json:"enabled"`
	Symbols  []string `json:"symbols,omitempty"`
}
