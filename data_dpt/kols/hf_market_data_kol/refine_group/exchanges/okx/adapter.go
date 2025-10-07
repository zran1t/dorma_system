// Package okx
package okx

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"strconv"

	rg "dorma_system/data_dpt/kols/hf_market_data_kol/refine_group"
	"dorma_system/infra/symbols"
	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"

	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
)

const SubjBookDelta = "CLEAN.OKX.BOOK.DELTA"

type adapter struct{}

func New() rg.ExchangeAdapter { return adapter{} }

func (adapter) Exchange() marketstreamv1.Exchange {
	return marketstreamv1.Exchange_EXCHANGE_OKX
}

func (adapter) Handlers() map[marketstreamv1.Feed]rg.Handler {
	return map[marketstreamv1.Feed]rg.Handler{
		marketstreamv1.Feed_FEED_OKX_TRADES_ALL: handleTradesAll,
		marketstreamv1.Feed_FEED_OKX_TRADES:     handleTrades,
		marketstreamv1.Feed_FEED_OKX_BBO:        handleBBO,
		marketstreamv1.Feed_FEED_OKX_BOOK:       handleBook,
	}
}

func Register(r *rg.Refiner) { r.RegisterAdapter(adapter{}) }

// ─────────────────── trades-all ───────────────────

func handleTradesAll(env *marketstreamv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	raw := env.GetRaw()
	if raw == nil {
		return nil, nil
	}
	var head okxTradesAllHead
	if err := json.Unmarshal(raw.RawData, &head); err != nil {
		return nil, err
	}

	vendor := strings.TrimSpace(head.Arg.InstID)
	canon := vendor
	if c, err := res.ReverseResolve("okx", vendor); err == nil && c != "" {
		canon = c
	}
	mtype := inferMarketType(canon)
	if len(head.Data) == 0 {
		return nil, nil
	}

	outs := make([]rg.Out, 0, len(head.Data))
	for _, d := range head.Data {
		ts := env.Timestamps.GetEventTsUs()
		if ts == 0 {
			ts = parseMSasUS(d.TS)
		}
		msg := &marketstreamv1.Envelope{
			Version:    env.Version,
			Source:     env.Source,   // 繼承來源（Exchange/Feed）
			Symbol:     canon,
			MarketType: mtype,
			Timestamps: &marketstreamv1.Timestamps{
				EventTsUs:     ts,
				CollectRecvUs: env.Timestamps.GetCollectRecvUs(),
				CollectPubUs:  env.Timestamps.GetCollectPubUs(),
				RefinerRecvUs: env.Timestamps.GetRefinerRecvUs(),
			},
			Body: &marketstreamv1.Envelope_TradeAll{
				TradeAll: &marketstreamv1.OKXAllTradeBody{
					Version: 1,
					PxE9:    dec1e9(d.Px),
					SzE9:    dec1e9(d.Sz),
					Side:    toSide(d.Side),
					TradeId: d.TradeID,
				},
			},
		}
		// message_id = xxhash128(len|exchange | len|feed | len|body)
		bodyBytes, _ := proto.Marshal(msg.GetTradeAll())
		msg.MessageId = makeMsgID(msg.GetSource().GetExchange().String(), msg.GetSource().GetFeed().String(), bodyBytes)

		outs = append(outs, rg.Out{Subject: "CLEAN.OKX.TRADES-ALL", Msg: msg})
	}
	return outs, nil
}

// ─────────────────── trades ───────────────────

func handleTrades(env *marketstreamv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	raw := env.GetRaw()
	if raw == nil {
		return nil, nil
	}
	var head okxTradesHead
	if err := json.Unmarshal(raw.RawData, &head); err != nil {
		return nil, err
	}

	vendor := strings.TrimSpace(head.Arg.InstID)
	canon := vendor
	if c, err := res.ReverseResolve("okx", vendor); err == nil && c != "" {
		canon = c
	}
	mtype := inferMarketType(canon)
	if len(head.Data) == 0 {
		return nil, nil
	}

	outs := make([]rg.Out, 0, len(head.Data))
	for _, d := range head.Data {
		ts := env.Timestamps.GetEventTsUs()
		if ts == 0 {
			ts = parseMSasUS(d.TS)
		}
		msg := &marketstreamv1.Envelope{
			Version:    env.Version,
			Source:     env.Source,
			Symbol:     canon,
			MarketType: mtype,
			Timestamps: &marketstreamv1.Timestamps{
				EventTsUs:     ts,
				CollectRecvUs: env.Timestamps.GetCollectRecvUs(),
				CollectPubUs:  env.Timestamps.GetCollectPubUs(),
				RefinerRecvUs: env.Timestamps.GetRefinerRecvUs(),
			},
			Body: &marketstreamv1.Envelope_Trade{
				Trade: &marketstreamv1.OKXTradeBody{
					Version: 1,
					PxE9:    dec1e9(d.Px),
					SzE9:    dec1e9(d.Sz),
					Side:    toSide(d.Side),
					Count:   d.Count,
					SeqId:   uint64(d.SeqID),
					TradeId: d.TradeID,
				},
			},
		}
		bodyBytes, _ := proto.Marshal(msg.GetTrade())
		msg.MessageId = makeMsgID(msg.GetSource().GetExchange().String(), msg.GetSource().GetFeed().String(), bodyBytes)

		outs = append(outs, rg.Out{Subject: "CLEAN.OKX.TRADES", Msg: msg})
	}
	return outs, nil
}

// ─────────────────── bbo ───────────────────

func handleBBO(env *marketstreamv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	raw := env.GetRaw()
	if raw == nil {
		return nil, nil
	}
	var head okxBBOHead
	if err := json.Unmarshal(raw.RawData, &head); err != nil {
		return nil, err
	}

	vendor := strings.TrimSpace(head.Arg.InstID)
	canon := vendor
	if c, err := res.ReverseResolve("okx", vendor); err == nil && c != "" {
		canon = c
	}
	mtype := inferMarketType(canon)
	if len(head.Data) == 0 {
		return nil, nil
	}

	outs := make([]rg.Out, 0, len(head.Data))
	for _, d := range head.Data {
		ts := env.Timestamps.GetEventTsUs()
		if ts == 0 {
			ts = parseMSasUS(d.TS)
		}
		msg := &marketstreamv1.Envelope{
			Version:    env.Version,
			Source:     env.Source,
			Symbol:     canon,
			MarketType: mtype,
			Timestamps: &marketstreamv1.Timestamps{
				EventTsUs:     ts,
				CollectRecvUs: env.Timestamps.GetCollectRecvUs(),
				CollectPubUs:  env.Timestamps.GetCollectPubUs(),
				RefinerRecvUs: env.Timestamps.GetRefinerRecvUs(),
			},
			Body: &marketstreamv1.Envelope_Bbo{
				Bbo: &marketstreamv1.OKXBBOBody{
					Version:       1,
					BidPxE9:       dec1e9(d.BidPx),
					BidQtyE9:      dec1e9(d.BidSz),
					AskPxE9:       dec1e9(d.AskPx),
					AskQtyE9:      dec1e9(d.AskSz),
					BidOrderCount: d.BidCnt,
					AskOrderCount: d.AskCnt,
					SeqId:         uint64(d.SeqID),
				},
			},
		}
		bodyBytes, _ := proto.Marshal(msg.GetBbo())
		msg.MessageId = makeMsgID(msg.GetSource().GetExchange().String(), msg.GetSource().GetFeed().String(), bodyBytes)

		outs = append(outs, rg.Out{Subject: "CLEAN.OKX.BBO", Msg: msg})
	}
	return outs, nil
}

// ─────────────────── book ───────────────────

func handleBook(env *marketstreamv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	raw := env.GetRaw()
	if raw == nil { return nil, nil }

	var head okxBookHead
	if err := json.Unmarshal(raw.RawData, &head); err != nil { return nil, err }

	vendor := strings.TrimSpace(head.Arg.InstID)
	canon := vendor
	if c, err := res.ReverseResolve("okx", vendor); err == nil && c != "" { canon = c }
	mtype := inferMarketType(canon)
	if len(head.Data) == 0 { return nil, nil }

	outs := make([]rg.Out, 0, len(head.Data))
	for _, d := range head.Data {
		ts := env.Timestamps.GetEventTsUs()
		if ts == 0 { ts = parseMSasUS(d.TS) }

		msg := &marketstreamv1.Envelope{
			Version:    env.Version,
			Source:     env.Source,
			Symbol:     canon,
			MarketType: mtype,
			Timestamps: &marketstreamv1.Timestamps{
				EventTsUs:     ts,
				CollectRecvUs: env.Timestamps.GetCollectRecvUs(),
				CollectPubUs:  env.Timestamps.GetCollectPubUs(),
				RefinerRecvUs: env.Timestamps.GetRefinerRecvUs(),
			},
			Body: &marketstreamv1.Envelope_Book{
				Book: &marketstreamv1.OKXBooksBody{
					Version:   1,
					Action:    toAction(head.Action), // SNAPSHOT 或 UPDATE
					Bids:      toLevels(d.Bids),
					Asks:      toLevels(d.Asks),
					Checksum:  d.Checksum,
					PrevSeqId: d.PrevSeqID,
					SeqId:     d.SeqID,
				},
			},
		}
		bodyBytes, _ := proto.Marshal(msg.GetBook())
		msg.MessageId = makeMsgID(msg.GetSource().GetExchange().String(), msg.GetSource().GetFeed().String(), bodyBytes)

		// ✅ 一律走 DELTA；FULL 交給 book_group
		outs = append(outs, rg.Out{Subject: "CLEAN.OKX.BOOK.DELTA", Msg: msg})
	}
	return outs, nil
}

// ─────────────────── utilities ───────────────────

// 精確十進位 → E9（不經過 float，不會有 999999/000001 殘差）
// 規則：截斷至 9 位小數（OKX 原始字串本來也在此範圍內），不做四捨五入。
func dec1e9(s string) int64 {
    s = strings.TrimSpace(s)
    if s == "" {
        return 0
    }
    neg := false
    if s[0] == '-' {
        neg = true
        s = s[1:]
    }

    intPart := s
    fracPart := ""
    if dot := strings.IndexByte(s, '.'); dot >= 0 {
        intPart = s[:dot]
        fracPart = s[dot+1:]
    }

    // 保留數字
    digits := func(t string) string {
        b := make([]byte, 0, len(t))
        for i := 0; i < len(t); i++ {
            if t[i] >= '0' && t[i] <= '9' {
                b = append(b, t[i])
            }
        }
        if len(b) == 0 { return "0" }
        return string(b)
    }
    intPart = digits(intPart)
    fracPart = digits(fracPart)

    // 對齊 9 位；超過截斷，不足補 0
    if len(fracPart) > 9 {
        fracPart = fracPart[:9]
    } else if len(fracPart) < 9 {
        fracPart = fracPart + strings.Repeat("0", 9-len(fracPart))
    }

    ip, _ := strconv.ParseInt(intPart, 10, 64)
    fp, _ := strconv.ParseInt(fracPart, 10, 64)

    v := ip*1_000_000_000 + fp
    if neg { v = -v }

    return v
}

func parseMSasUS(msStr string) uint64 {
	ms, err := strconv.ParseInt(msStr, 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	return uint64(ms) * 1000
}

func toSide(s string) marketstreamv1.Side {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "buy", "b":
		return marketstreamv1.Side_SIDE_BUY
	case "sell", "s":
		return marketstreamv1.Side_SIDE_SELL
	default:
		return marketstreamv1.Side_SIDE_UNKNOWN
	}
}

func toAction(s string) marketstreamv1.Action {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "snapshot":
		return marketstreamv1.Action_ACTION_SNAPSHOT
	case "update":
		return marketstreamv1.Action_ACTION_UPDATE
	default:
		return marketstreamv1.Action_ACTION_UNSPECIFIED
	}
}

func toLevels(rows [][]string) []*marketstreamv1.OKXBookLevel {
	out := make([]*marketstreamv1.OKXBookLevel, 0, len(rows))
	for _, r := range rows {
		if len(r) < 2 {
			continue
		}
		out = append(out, &marketstreamv1.OKXBookLevel{
			PxE9:  dec1e9(strings.TrimSpace(r[0])),
			QtyE9: dec1e9(strings.TrimSpace(r[1])),
		})
	}
	return out
}

func inferMarketType(c string) marketstreamv1.MarketType {
	c = strings.ToUpper(strings.TrimSpace(c))
	switch {
	case strings.HasSuffix(c, "-SPOT"):
		return marketstreamv1.MarketType_MARKET_SPOT
	case strings.HasSuffix(c, "-SWAP"):
		return marketstreamv1.MarketType_MARKET_PERPETUAL
	default:
		return marketstreamv1.MarketType_MARKET_TYPE_UNSPECIFIED
	}
}

// xxhash128(len|exchange | len|feed | len|body)
func makeMsgID(exchange, feed string, body []byte) []byte {
	var p bytes.Buffer
	putLen := func(n int) {
		var le [4]byte
		binary.BigEndian.PutUint32(le[:], uint32(n))
		p.Write(le[:])
	}
	putLen(len(exchange))
	p.WriteString(exchange)
	putLen(len(feed))
	p.WriteString(feed)
	putLen(len(body))
	p.Write(body)

	sum := xxh3.Hash128(p.Bytes())
	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[:8], sum.Lo)
	binary.BigEndian.PutUint64(out[8:], sum.Hi)
	return out
}