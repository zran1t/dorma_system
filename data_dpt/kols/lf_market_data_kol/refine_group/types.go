// data_dpt/kols/lf_market_data_kol/refine_group/types.go
package refine_group

import (
	marketcommonv1 "dorma_system/schemas/gen/go/market_common_v1"
	"dorma_system/infra/symbols"
)

// Out 是 handler 產生的一筆輸出（含 subject 與封包）
type Out struct {
	Subject string
	Msg     *marketcommonv1.Envelope
}

// Handler：把 RAW Envelope 轉成「一或多筆」輸出
type Handler func(
	env *marketcommonv1.Envelope,
	resolver symbols.Resolver,
) ([]Out, error)

// ExchangeAdapter：一個交易所的所有 feed handlers
type ExchangeAdapter interface {
	Exchange() marketcommonv1.Exchange
	Handlers() map[string]Handler // feed 名 → handler
}

// Registrar：註冊 adapter
type Registrar interface {
	RegisterAdapter(ad ExchangeAdapter)
}