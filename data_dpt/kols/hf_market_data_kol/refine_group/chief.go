package refine_group

import (
    "context"
    "fmt"
    "strings"
    "github.com/nats-io/nats.go"
    marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
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

func (c *Chief) StartBy(ctx context.Context, exchange string, feeds []string) error {
    if len(feeds) == 0 { return fmt.Errorf("feeds must not be empty") }
    ex := strings.ToUpper(strings.TrimSpace(exchange))
    var subjs []string
    for _, f := range feeds {
        fd := strings.ToUpper(strings.TrimSpace(f))
        switch fd {
        case "TRADES", "TRADES-ALL", "BBO", "BOOK", "BOOKS":
            if fd == "BOOKS" { fd = "BOOK" }
            subjs = append(subjs, fmt.Sprintf("RAW.%s.%s", ex, fd))
        default:
            return fmt.Errorf("unsupported feed %q for %s", fd, ex)
        }
    }
    return c.r.Run(ctx, subjs...)
}

func (c *Chief) StartEnum(ctx context.Context, ex marketstreamv1.Exchange, fds []marketstreamv1.Feed) error {
    if len(fds) == 0 { return fmt.Errorf("feeds empty") }
    exStr := strings.ToUpper(strings.TrimPrefix(ex.String(), "EXCHANGE_"))
    var subjs []string
    for _, fd := range fds {
        fdStr := strings.ToUpper(strings.TrimPrefix(fd.String(), "FEED_"))
        subjs = append(subjs, fmt.Sprintf("RAW.%s.%s", exStr, fdStr))
    }
    return c.r.Run(ctx, subjs...)
}