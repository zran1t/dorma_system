// File: infra/preset/types.go
// Package: preset
//
// 職責 (Responsibility):
//     定義 Autostart Matrix 的資料結構，對應 YAML 檔內容。
//     包含全域設定、預設值 (defaults) 與每個 symbol 的覆寫。
//     僅作為資料承載層 (Data Holder)，不參與驗證與載入。
//
// 注意事項 (Notes):
//     - 保留舊有語意命名 (hf / price.last/index/mark / tick / candle)。
//     - 不加入邏輯或行為方法，確保結構穩定。
//     - validator 將在上層模組進行欄位合法性檢查。

package preset

// AutostartMatrix 對應 YAML 根節點。
type AutostartMatrix struct {
	// Version 設定版本號，用於追蹤格式相容性。
	Version int `yaml:"version"`

	// Summary 組態摘要；長篇描述請移至 README 或程式檔頭。
	Summary string `yaml:"summary"`

	// Intervals 全域 candle 能力清單。
	Intervals struct {
		// Available 可使用的時間區間；符號應維持固定大小寫規則。
		Available []string `yaml:"available"`
	} `yaml:"intervals"`

	// ExchangesEnabled 啟用的交易所清單。
	// 驗證責任交由 validator 檢查是否為支援名單。
	ExchangesEnabled []string `yaml:"exchanges_enabled"`

	// Defaults 預設值；symbols 若未覆寫則沿用此設定。
	Defaults struct {
		HF struct {
			TradesAll bool `yaml:"trades_all"`
			Trades    bool `yaml:"trades"`
			BBO       bool `yaml:"bbo"`
			BookDelta bool `yaml:"book_delta"`
			BookFull  bool `yaml:"book_full"`
		} `yaml:"hf"`

		Price struct {
			Last  FeedSpec `yaml:"last"`
			Index FeedSpec `yaml:"index"`
			Mark  FeedSpec `yaml:"mark"`
		} `yaml:"price"`
	} `yaml:"defaults"`

	// Symbols 幣種清單；僅需覆寫與 defaults 不同的部分。
	Symbols []struct {
		Pair string `yaml:"pair"`

		// HF 微觀結構族設定；指標類型允許「省略即不覆寫」。
		HF *struct {
			TradesAll *bool `yaml:"trades_all"`
			Trades    *bool `yaml:"trades"`
			BBO       *bool `yaml:"bbo"`
			BookDelta *bool `yaml:"book_delta"`
			BookFull  *bool `yaml:"book_full"`
		} `yaml:"hf,omitempty"`

		// Price 價格族設定；每個成員皆可部分覆寫。
		Price *struct {
			Last  *FeedSpec `yaml:"last,omitempty"`
			Index *FeedSpec `yaml:"index,omitempty"`
			Mark  *FeedSpec `yaml:"mark,omitempty"`
		} `yaml:"price,omitempty"`
	} `yaml:"symbols"`
}

// FeedSpec 描述 tick 與 candle 的設定。
type FeedSpec struct {
	Tick   bool       `yaml:"tick"`
	Candle CandleSpec `yaml:"candle"`
}

// CandleSpec 描述 K 線設定。
// Enabled 為 true 時才啟用；Intervals 為空則建議由上層補全全域預設。
type CandleSpec struct {
	Enabled   bool     `yaml:"enabled"`
	Intervals []string `yaml:"intervals"`
}
