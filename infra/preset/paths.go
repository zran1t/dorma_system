// File: infra/preset/paths.go
// Package: preset
//
// 職責 (Responsibility):
//     集中管理 system_presets 下的「唯一權威」組態路徑，避免外部自訂與誤用。
//
// 注意事項 (Notes):
//     - 路徑為 repo 相對路徑，由 loader 固定使用。
//     - 若未來要支援不同環境切換，請在更上層（啟動器）做切換，不要在此處引入參數化。

package preset

const (
	DefaultMarketDataStartupPath     = "configs/presets/system/market_data.yaml"
	DefaultInstrumentDataStartupPath = "configs/presets/system/instrument_data.yaml"
	DefaultTradingAllowlistPath      = "configs/presets/system/trading_allowlist.yaml"
)
