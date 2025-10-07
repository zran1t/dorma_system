package okx

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
	"google.golang.org/protobuf/proto"

	"github.com/tidwall/gjson"
)

type Adapter struct{}



func NewAdapter() *Adapter { return &Adapter{} }

var chMap = map[string]struct {
	Feed    marketstreamv1.Feed
	Subject string
}{
	"trades":     {marketstreamv1.Feed_FEED_OKX_TRADES, "RAW.OKX.TRADES"},
	"trades-all": {marketstreamv1.Feed_FEED_OKX_TRADES_ALL, "RAW.OKX.TRADES-ALL"},
	"books":      {marketstreamv1.Feed_FEED_OKX_BOOK, "RAW.OKX.BOOK"},
	"bbo-tbt":    {marketstreamv1.Feed_FEED_OKX_BBO, "RAW.OKX.BBO"},
}

func (a *Adapter) BuildSubscribeMsgs(channels, symbols []string) ([]string, error) {
	type arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	}
	type req struct {
		Op   string `json:"op"`
		Args []arg  `json:"args"`
	}

	var out []string
	for _, ch := range channels {
		okxCh := normalizeChannel(ch)
		for _, sym := range symbols {
			r := req{Op: "subscribe", Args: []arg{{Channel: okxCh, InstID: sym}}}
			b, err := json.Marshal(r)
			if err != nil {
				return nil, err
			}
			out = append(out, string(b))
		}
	}
	return out, nil
}

func (a *Adapter) Heartbeat() (string, time.Duration) {
	return "", 0
}

func (a *Adapter) Handle(data []byte) (string, []byte, error) {
	// ---- 記錄 collector 收到時間（微秒, uint64）----
	collectRecvUs := uint64(time.Now().UnixMicro())

	ch := gjson.GetBytes(data, "arg.channel").String()
	if ch == "" {
		return "", nil, nil
	}
	instID := gjson.GetBytes(data, "arg.instId").String()
	ch = normalizeChannel(ch)

	m, ok := chMap[ch]
	if !ok {
		return "", nil, nil
	}

	env := &marketstreamv1.Envelope{
		Version: 1,
		Source: &marketstreamv1.Source{
			Exchange:      marketstreamv1.Exchange_EXCHANGE_OKX,
			Feed:          m.Feed,
			VendorChannel: ch,
		},
		Symbol:     instID,
		MarketType: marketstreamv1.MarketType_MARKET_TYPE_UNSPECIFIED,
		Timestamps: &marketstreamv1.Timestamps{
			// 只寫 collector 層能確定的時間
			CollectRecvUs: collectRecvUs,
			// EventTsUs/Refiner* 留給下游
		},
		Body: &marketstreamv1.Envelope_Raw{
			Raw: &marketstreamv1.RawBody{RawData: data},
		},
	}

	// （可選）seqId 標註：目前沒有欄位就先略過
	if m.Feed == marketstreamv1.Feed_FEED_OKX_BOOK || m.Feed == marketstreamv1.Feed_FEED_OKX_BBO {
		if seq := gjson.GetBytes(data, "data.0.seqId"); seq.Exists() {
			_ = seq // no-op
		}
	}

	// ---- 發佈前一刻補上 publish 時間（微秒, uint64）----
	env.Timestamps.CollectPubUs = uint64(time.Now().UnixMicro())

	pb, err := proto.Marshal(env)
	if err != nil {
		return "", nil, fmt.Errorf("proto marshal envelope: %w", err)
	}
	return m.Subject, pb, nil
}

func normalizeChannel(feed string) string {
	switch strings.ToLower(feed) {
	case "trades":
		return "trades"
	case "books", "book":
		return "books"
	case "bbo", "bbo-tbt":
		return "bbo-tbt"
	case "trades-all", "trades_all":
		return "trades-all"
	default:
		return strings.ToLower(feed)
	}
}