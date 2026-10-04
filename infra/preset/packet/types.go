// File: infra/preset/packet/types.go
// Package: packet
//
// 職責 (Responsibility):
//     定義 loader 編譯後輸出的「封包 (Packet)」資料結構。
//     Packet 是給各 KOL/部門直接使用的「意圖展開結果」，不再保留 YAML 的 defaults/override 形式。
//
// 注意事項 (Notes):
//     - Packet 應保持語意穩定、可序列化、可測試。
//     - Packet 不承擔驗證與推導邏輯；僅作為資料承載與跨模組契約。

package packet

type Market string

const (
	MarketSpot  Market = "spot"
	MarketSwap  Market = "swap"
	MarketIndex Market = "index"
)
