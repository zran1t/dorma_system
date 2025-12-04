// File: data_dpt/kols/hf_market_data_kol/refine_group/types.go
// Package: refine_group
//
// 職責 (Responsibility):
//     定義 Refine Group 內部使用的介面與通用型別：Out / Handler / ExchangeAdapter / Registrar。
//
// 注意事項 (Notes):
//     - 這些型別是 adapter 與 Refiner 之間的契約，改名或改簽名都要非常小心。

package refine_group

import (
	// === 標準函式庫 (Standard Library) ===
	// 無

	// === 第三方套件 (Third-Party Libraries) ===
	// 無

	// === 系統內模組 (Internal Modules) ===
	"dorma_system/infra/symbols"
	marketcommonv1 "dorma_system/schemas/gen/go/market/common/v1"
)

// Out 代表 handler 產出的一筆結果：要發到哪個 subject + 一個 Envelope。
//
// 功能:
//   - 把「要發佈到哪裡」與「要發佈的內容」綁在一起，方便 Refiner 做最後一哩路的 Publish。
//
// 欄位說明:
//   - Subject: 要發佈到 NATS 的 subject（例如 CLEAN.OKX.TRADES.BTC.USDT.SWAP）。
//   - Msg:     已填好的 Envelope 指標。
//
// 契約 / 限制:
//   - Subject 為空或 Msg 為 nil 的 Out 會在 Refiner 裡被忽略。
//
// 備註:
//   - handler 可以一次回傳多個 Out，Refiner 會逐一發佈。
type Out struct {
	Subject string
	Msg     *marketcommonv1.Envelope
}

// Handler 是「單一 feed handler」的函式型別。
//
// 功能:
//   - 接收一筆 RAW Envelope 與 symbol Resolver，輸出 0~N 筆 Out。
//   - 負責：解析 RawBody、做 ReverseResolve、組乾淨 Envelope、設定 timestamps / messageId 等。
//
// 參數:
//   - env:      指向原始 Envelope 的指標（會帶 Source / Timestamps / Body 等）。
//   - resolver: symbols.Resolver，供 handler 進行 canonical / native 互轉。
//
// 回傳:
//   - []Out: zero 或多筆輸出，如果為 nil 或長度 0 代表「沒東西要發」。
//   - error: 若解析失敗或邏輯出錯時回傳；Refiner 會 log 出錯但仍持續處理下一筆。
//
// 備註:
//   - handler 本身不負責 Subscribe/Publish，只專注在「Raw → Out」。
type Handler func(
	env *marketcommonv1.Envelope,
	resolver symbols.Resolver,
) ([]Out, error)

// ExchangeAdapter 將「一個交易所」底下所有 feed handler 收納在一起。
//
// 功能:
//   - Exchange(): 告訴 Refiner 這組 handler 對應哪個 Exchange 列舉值。
//   - Handlers(): 回傳 feed 名稱 → Handler 的 map。
//
// 欄位說明:
//   - 無（介面型別）。
//
// 契約 / 限制:
//   - Handlers() 回傳的 key 必須跟 Chief / Refiner 那邊使用的 feed key 一致。
//   - Exchange() 必須回傳穩定、唯一的列舉值，否則 dedup key 會亂掉。
//
// 備註:
//   - 每個交易所通常對應一個 Adapter 實作，例如 okx.adapter。
type ExchangeAdapter interface {
	Exchange() marketcommonv1.Exchange
	Handlers() map[string]Handler
}

// Registrar 是可以註冊 ExchangeAdapter 的抽象介面。
//
// 功能:
//   - 提供 RegisterAdapter(ad) 方法，讓外部模組可以把自己的 adapter 掛進來。
//
// 欄位說明:
//   - 無（介面型別）。
//
// 契約 / 限制:
//   - 實作方需要決定「多次註冊同一個 feed」要如何處理（通常是覆寫）。
//
// 備註:
//   - Refiner 本身就是 Registrar 的一個實作。
type Registrar interface {
	RegisterAdapter(ad ExchangeAdapter)
}
