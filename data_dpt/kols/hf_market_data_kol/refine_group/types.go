package refine_group

import (
	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
	"dorma_system/infra/symbols"
)

// Out 是 handler 產生的一筆輸出（含 subject 與封包）
type Out struct {
	Subject string
	Msg     *marketstreamv1.Envelope
}

// Handler：把 RAW Envelope 轉成「一或多筆」輸出
// 之後你要把 BOOK 拆成 FULL / DELTA 兩條 subject，也只要回傳兩種 Out 即可。
type Handler func(
	env *marketstreamv1.Envelope,
	resolver symbols.Resolver,
) ([]Out, error)

// ExchangeAdapter：一個交易所的所有 feed handlers
type ExchangeAdapter interface {
	Exchange() marketstreamv1.Exchange
	Handlers() map[marketstreamv1.Feed]Handler
}

// Registrar：註冊 adapter
type Registrar interface {
	RegisterAdapter(ad ExchangeAdapter)
}