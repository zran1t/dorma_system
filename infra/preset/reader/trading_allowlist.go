// File: infra/preset/reader/trading_allowlist.go
// Package: reader
//
// 職責 (Responsibility):
//     讀取並解析 trading_allowlist.yaml 對應的 preset 結構。

package reader

type TradingAllowlistPreset struct {
	Version   int                                       `yaml:"version"`
	Summary   string                                    `yaml:"summary"`
	Exchanges map[string]TradingAllowlistExchangePreset `yaml:"exchanges"`
}

type TradingAllowlistExchangePreset struct {
	Symbols []TradingAllowlistSymbolPreset `yaml:"symbols"`
}

type TradingAllowlistSymbolPreset struct {
	Pair    string                    `yaml:"pair"`
	Markets map[string]map[string]any `yaml:"markets"` // presence => allowed; value not used
}

func LoadTradingAllowlistPreset(path string) (*TradingAllowlistPreset, error) {
	var p TradingAllowlistPreset
	if err := readYAMLFile(path, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
