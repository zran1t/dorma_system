// File: infra/preset/packet/market_data_types.go
// Package: packet
//
// 職責 (Responsibility):
//     Market Data KOL 所需之啟動封包：描述要訂閱哪些 market feeds。
//     已完成 defaults + override merge 與訂閱展開（realtime/candle）。
//
// 注意事項 (Notes):
//     - Mode: "realtime" | "candle"
//     - Candle 需帶 intervals；Realtime 不需 intervals。

package packet

type MarketDataPacket struct {
	Version   int                                 `json:"version"`
	Summary   string                              `json:"summary"`
	Exchanges map[string]MarketDataExchangePacket `json:"exchanges"`
}

type MarketDataExchangePacket struct {
	Exchange      string                   `json:"exchange"`
	Subscriptions []MarketDataSubscription `json:"subscriptions"`
}

type MarketDataSubscription struct {
	Exchange  string   `json:"exchange"`
	Pair      string   `json:"pair"`
	Market    Market   `json:"market"`
	Feed      string   `json:"feed"` // trades_all, trades, bbo, book_delta, book_full, last, mark, index
	Mode      string   `json:"mode"` // realtime | candle
	Intervals []string `json:"intervals,omitempty"`
}
