// File: infra/preset/packet/trading_allowlist_types.go
// Package: packet
//
// 職責 (Responsibility):
//     交易允許清單封包：用於治理/風控層，決定哪些 exchange/pair/market 允許下單。
//
// 注意事項 (Notes):
//     - 「存在即允許」：markets.<market> 節點存在即表示允許交易該 market。
//     - Packet 以扁平清單表示，便於後續建立 map / set。

package packet

type TradingAllowlistPacket struct {
	Version   int                                       `json:"version"`
	Summary   string                                    `json:"summary"`
	Exchanges map[string]TradingAllowlistExchangePacket `json:"exchanges"`
}

type TradingAllowlistExchangePacket struct {
	Exchange string               `json:"exchange"`
	Allowed  []TradableInstrument `json:"allowed"`
}

type TradableInstrument struct {
	Exchange string `json:"exchange"`
	Pair     string `json:"pair"`
	Market   Market `json:"market"`
}
