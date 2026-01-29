// File: infra/preset/reader/market_data.go
// Package: reader
//
// 職責 (Responsibility):
//     讀取並解析 market_data.startup.yaml 對應的 preset 結構。
//     defaults 使用完整結構（值型別），override 使用 pointer 結構（only-diff）。
//
// 注意事項 (Notes):
//     - defaults: 必須描述完整意圖（enabled/intervals 都是實值）。
//     - override: 僅寫差異；未提供欄位視為沿用 defaults。
//     - markets.<market> 節點存在即代表啟用該 market；其內容為 override。

package reader

type MarketDataPreset struct {
	Version   int                                 `yaml:"version"`
	Summary   string                              `yaml:"summary"`
	Exchanges map[string]MarketDataExchangePreset `yaml:"exchanges"`
}

type MarketDataExchangePreset struct {
	Defaults MarketDataDefaultsPreset `yaml:"defaults"`
	Symbols  []MarketDataSymbolPreset `yaml:"symbols"`
}

type MarketDataDefaultsPreset struct {
	Spot  MarketDataMarketPreset `yaml:"spot"`
	Swap  MarketDataMarketPreset `yaml:"swap"`
	Index MarketDataMarketPreset `yaml:"index"`
}

// symbols[*].markets.<market> 存在即表示啟用該 market。
// <market> 內容為 only-diff override（可為 {}）。
type MarketDataSymbolPreset struct {
	Pair    string                                    `yaml:"pair"`
	Markets map[string]MarketDataMarketOverridePreset `yaml:"markets"`
}

// ===== defaults 結構（完整）=====

type MarketDataMarketPreset struct {
	Feeds MarketDataFeedsPreset `yaml:"feeds"`
}

type MarketDataFeedsPreset struct {
	TradesAll FeedEnabledPreset `yaml:"trades_all"`
	Trades    FeedEnabledPreset `yaml:"trades"`
	BBO       FeedEnabledPreset `yaml:"bbo"`
	BookDelta FeedEnabledPreset `yaml:"book_delta"`
	BookFull  FeedEnabledPreset `yaml:"book_full"`

	Last  MarketDataPriceFamilyPreset `yaml:"last"`
	Mark  MarketDataPriceFamilyPreset `yaml:"mark"`
	Index MarketDataPriceFamilyPreset `yaml:"index"`
}

type MarketDataPriceFamilyPreset struct {
	Realtime FeedEnabledPreset      `yaml:"realtime"`
	Candle   MarketDataCandlePreset `yaml:"candle"`
}

type FeedEnabledPreset struct {
	Enabled bool `yaml:"enabled"`
}

type MarketDataCandlePreset struct {
	Enabled   bool     `yaml:"enabled"`
	Intervals []string `yaml:"intervals"`
}

// ===== override 結構（only-diff）=====

type MarketDataMarketOverridePreset struct {
	Feeds *MarketDataFeedsOverridePreset `yaml:"feeds,omitempty"`
}

type MarketDataFeedsOverridePreset struct {
	TradesAll *FeedEnabledOverridePreset `yaml:"trades_all,omitempty"`
	Trades    *FeedEnabledOverridePreset `yaml:"trades,omitempty"`
	BBO       *FeedEnabledOverridePreset `yaml:"bbo,omitempty"`
	BookDelta *FeedEnabledOverridePreset `yaml:"book_delta,omitempty"`
	BookFull  *FeedEnabledOverridePreset `yaml:"book_full,omitempty"`

	Last  *MarketDataPriceFamilyOverridePreset `yaml:"last,omitempty"`
	Mark  *MarketDataPriceFamilyOverridePreset `yaml:"mark,omitempty"`
	Index *MarketDataPriceFamilyOverridePreset `yaml:"index,omitempty"`
}

type FeedEnabledOverridePreset struct {
	Enabled *bool `yaml:"enabled,omitempty"`
}

type MarketDataPriceFamilyOverridePreset struct {
	Realtime *FeedEnabledOverridePreset      `yaml:"realtime,omitempty"`
	Candle   *MarketDataCandleOverridePreset `yaml:"candle,omitempty"`
}

type MarketDataCandleOverridePreset struct {
	Enabled   *bool    `yaml:"enabled,omitempty"`
	Intervals []string `yaml:"intervals,omitempty"`
}

func LoadMarketDataPreset(path string) (*MarketDataPreset, error) {
	var p MarketDataPreset
	if err := readYAMLFile(path, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
