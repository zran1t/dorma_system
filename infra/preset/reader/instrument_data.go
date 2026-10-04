// File: infra/preset/reader/instrument_data.go
// Package: reader
//
// 職責 (Responsibility):
//     讀取並解析 instrument_data.startup.yaml 對應的 preset 結構。

package reader

type InstrumentDataPreset struct {
	Version   int                                     `yaml:"version"`
	Summary   string                                  `yaml:"summary"`
	Exchanges map[string]InstrumentDataExchangePreset `yaml:"exchanges"`
}

type InstrumentDataExchangePreset struct {
	Feeds map[string]InstrumentFeedPreset `yaml:"feeds"`
}

type InstrumentFeedPreset struct {
	Enabled bool     `yaml:"enabled"`
	Symbols []string `yaml:"symbols"`
}

func LoadInstrumentDataPreset(path string) (*InstrumentDataPreset, error) {
	var p InstrumentDataPreset
	if err := readYAMLFile(path, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
