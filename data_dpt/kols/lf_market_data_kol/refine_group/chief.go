// data_dpt/kols/lf_market_data_kol/refine_group/chief.go
package refine_group

import (
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"

	marketcommonv1 "dorma_system/schemas/gen/go/market_common_v1"
	"dorma_system/infra/symbols"
)

type Chief struct {
	nc       *nats.Conn
	resolver symbols.Resolver
	r        *Refiner
}

func NewChief(nc *nats.Conn, resolver symbols.Resolver, ads ...ExchangeAdapter) *Chief {
	r := NewRefiner(nc, resolver)
	for _, ad := range ads {
		r.RegisterAdapter(ad)
	}
	return &Chief{nc: nc, resolver: resolver, r: r}
}

// feed 正規化（LF 只處理 KLINE）
func normFeed(s string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "MARK-CANDLE", "MARK_PRICE_CANDLE", "KLINE.MARKPRICE", "MARKPRICE", "MARK":
		return "KLINE.MARKPRICE", true
	case "INDEX-CANDLE", "INDEX_CANDLE", "KLINE.INDEX", "INDEX":
		return "KLINE.INDEX", true
	default:
		return "", false
	}
}

func rawSubject(exUpper, feedUpper string) string {
	// 直接吃全部 interval / symbol
	switch feedUpper {
	case "KLINE.MARKPRICE":
		return fmt.Sprintf("RAW.%s.MARK-CANDLE.>", exUpper)
	case "KLINE.INDEX":
		return fmt.Sprintf("RAW.%s.INDEX-CANDLE.>", exUpper)
	default:
		return fmt.Sprintf("RAW.%s.%s.>", exUpper, feedUpper)
	}
}

// StartBy：以字串指定交易所與 feeds（允許 ["MARK-CANDLE","INDEX-CANDLE"]）
func (c *Chief) StartBy(ctx context.Context, exchange string, feeds []string) error {
	if len(feeds) == 0 {
		return fmt.Errorf("feeds must not be empty")
	}
	ex := strings.ToUpper(strings.TrimSpace(exchange))

	var subjs []string
	for _, f := range feeds {
		fd, ok := normFeed(f)
		if !ok {
			return fmt.Errorf("unsupported feed %q for %s", f, ex)
		}
		subjs = append(subjs, rawSubject(ex, fd))
	}
	return c.r.Run(ctx, subjs...)
}

// StartEnum：以共用枚舉 Exchange + feeds 字串陣列
func (c *Chief) StartEnum(ctx context.Context, ex marketcommonv1.Exchange, fds []string) error {
	if len(fds) == 0 {
		return fmt.Errorf("feeds empty")
	}
	exStr := strings.ToUpper(strings.TrimPrefix(ex.String(), "EXCHANGE_"))

	var subjs []string
	for _, f := range fds {
		fd, ok := normFeed(f)
		if !ok {
			return fmt.Errorf("unsupported feed %q for %s", f, exStr)
		}
		subjs = append(subjs, rawSubject(exStr, fd))
	}
	return c.r.Run(ctx, subjs...)
}