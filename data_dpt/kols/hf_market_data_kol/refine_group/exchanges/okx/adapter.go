// Package okx
package okx

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	rg "dorma_system/data_dpt/kols/hf_market_data_kol/refine_group"
	"dorma_system/infra/symbols"
	marketcommonv1 "dorma_system/schemas/gen/go/market_common_v1"
	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"

	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)



type adapter struct{}

func New() rg.ExchangeAdapter { return adapter{} }

func (adapter) Exchange() marketcommonv1.Exchange {
	return marketcommonv1.Exchange_EXCHANGE_OKX
}

func (adapter) Handlers() map[string]rg.Handler {
	return map[string]rg.Handler{
		"TRADES-ALL": handleTradesAll,
		"TRADES":     handleTrades,
		"BBO":        handleBBO,
		"BOOK":       handleBook,
        "MARK-PRICE":    handleMarkPrice,
        "INDEX-TICKERS": handleIndexTickers,
	}
}

func Register(r *rg.Refiner) { r.RegisterAdapter(adapter{}) }

// ─────────────────── trades-all ───────────────────

func handleTradesAll(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	// Any → RawBody
	rawBody := &marketstreamv1.RawBody{}
	if env.GetBody() == nil || anypb.UnmarshalTo(env.GetBody(), rawBody, proto.UnmarshalOptions{}) != nil {
		return nil, nil
	}

	var head okxTradesAllHead
	if err := json.Unmarshal(rawBody.RawData, &head); err != nil {
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

		// 具體 body
		tradeAll := &marketstreamv1.OKXAllTradeBody{
			Version: 1,
			PxE9:    dec1e9(d.Px),
			SzE9:    dec1e9(d.Sz),
			Side:    toSide(d.Side),
			TradeId: d.TradeID,
		}
		// 打包 Any
		anyBody, _ := anypb.New(tradeAll)

		msg := &marketcommonv1.Envelope{
			Version:    env.Version,
			Source:     env.Source, // Exchange / Feed（feed 是 string）
			Symbol:     canon,
			MarketType: mtype,
			Timestamps: &marketcommonv1.Timestamps{
				EventTsUs:     ts,
				CollectRecvUs: env.Timestamps.GetCollectRecvUs(),
				CollectPubUs:  env.Timestamps.GetCollectPubUs(),
				RefinerRecvUs: env.Timestamps.GetRefinerRecvUs(),
			},
			Body: anyBody,
		}

		// message_id = xxhash128(len|exchange | len|feed | len|body)
		bodyBytes, _ := proto.Marshal(tradeAll)
		msg.MessageId = makeMsgID(msg.GetSource().GetExchange().String(), msg.GetSource().GetFeed(), bodyBytes)

		outs = append(outs, rg.Out{
    		Subject: buildCleanSubject("TRADES-ALL", canon),
    		Msg:     msg,
		})
	}
	return outs, nil
}

// ─────────────────── trades ───────────────────

func handleTrades(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	// Any → RawBody
	rawBody := &marketstreamv1.RawBody{}
	if env.GetBody() == nil || anypb.UnmarshalTo(env.GetBody(), rawBody, proto.UnmarshalOptions{}) != nil {
		return nil, nil
	}

	var head okxTradesHead
	if err := json.Unmarshal(rawBody.RawData, &head); err != nil {
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

		// 具體 body
		trade := &marketstreamv1.OKXTradeBody{
			Version: 1,
			PxE9:    dec1e9(d.Px),
			SzE9:    dec1e9(d.Sz),
			Side:    toSide(d.Side),
			Count:   d.Count,
			SeqId:   uint64(d.SeqID),
			TradeId: d.TradeID,
		}
		anyBody, _ := anypb.New(trade)

		msg := &marketcommonv1.Envelope{
			Version:    env.Version,
			Source:     env.Source,
			Symbol:     canon,
			MarketType: mtype,
			Timestamps: &marketcommonv1.Timestamps{
				EventTsUs:     ts,
				CollectRecvUs: env.Timestamps.GetCollectRecvUs(),
				CollectPubUs:  env.Timestamps.GetCollectPubUs(),
				RefinerRecvUs: env.Timestamps.GetRefinerRecvUs(),
			},
			Body: anyBody,
		}

		bodyBytes, _ := proto.Marshal(trade)
		msg.MessageId = makeMsgID(msg.GetSource().GetExchange().String(), msg.GetSource().GetFeed(), bodyBytes)

		outs = append(outs, rg.Out{
    		Subject: buildCleanSubject("TRADES", canon),
    		Msg:     msg,
		})
	}
	return outs, nil
}

// ─────────────────── bbo ───────────────────

func handleBBO(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	// Any → RawBody
	rawBody := &marketstreamv1.RawBody{}
	if env.GetBody() == nil || anypb.UnmarshalTo(env.GetBody(), rawBody, proto.UnmarshalOptions{}) != nil {
		return nil, nil
	}

	var head okxBBOHead
	if err := json.Unmarshal(rawBody.RawData, &head); err != nil {
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

		bbo := &marketstreamv1.OKXBBOBody{
			Version:       1,
			BidPxE9:       dec1e9(d.BidPx),
			BidQtyE9:      dec1e9(d.BidSz),
			AskPxE9:       dec1e9(d.AskPx),
			AskQtyE9:      dec1e9(d.AskSz),
			BidOrderCount: d.BidCnt,
			AskOrderCount: d.AskCnt,
			SeqId:         uint64(d.SeqID),
		}
		anyBody, _ := anypb.New(bbo)

		msg := &marketcommonv1.Envelope{
			Version:    env.Version,
			Source:     env.Source,
			Symbol:     canon,
			MarketType: mtype,
			Timestamps: &marketcommonv1.Timestamps{
				EventTsUs:     ts,
				CollectRecvUs: env.Timestamps.GetCollectRecvUs(),
				CollectPubUs:  env.Timestamps.GetCollectPubUs(),
				RefinerRecvUs: env.Timestamps.GetRefinerRecvUs(),
			},
			Body: anyBody,
		}

		bodyBytes, _ := proto.Marshal(bbo)
		msg.MessageId = makeMsgID(msg.GetSource().GetExchange().String(), msg.GetSource().GetFeed(), bodyBytes)

		outs = append(outs, rg.Out{
    		Subject: buildCleanSubject("BBO", canon),
    		Msg:     msg,
		})
	}
	return outs, nil
}

// data_dpt/kols/hf_market_data_kol/refine_group/exchanges/okx/adapter.go
// [片段：handleBook 內覆寫 feed 為 BOOK.DELTA]

func handleBook(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	// Any → RawBody
	rawBody := &marketstreamv1.RawBody{}
	if env.GetBody() == nil || anypb.UnmarshalTo(env.GetBody(), rawBody, proto.UnmarshalOptions{}) != nil {
		return nil, nil
	}

	var head okxBookHead
	if err := json.Unmarshal(rawBody.RawData, &head); err != nil {
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

		book := &marketstreamv1.OKXBooksBody{
			Version:   1,
			Action:    toAction(head.Action),
			Bids:      toLevels(d.Bids),
			Asks:      toLevels(d.Asks),
			Checksum:  d.Checksum,
			PrevSeqId: d.PrevSeqID,
			SeqId:     d.SeqID,
		}
		anyBody, _ := anypb.New(book)

		msg := &marketcommonv1.Envelope{
			Version:    env.Version,
			Source:     env.Source,
			Symbol:     canon,
			MarketType: mtype,
			Timestamps: &marketcommonv1.Timestamps{
				EventTsUs:     ts,
				CollectRecvUs: env.Timestamps.GetCollectRecvUs(),
				CollectPubUs:  env.Timestamps.GetCollectPubUs(),
				RefinerRecvUs: env.Timestamps.GetRefinerRecvUs(),
			},
			Body: anyBody,
		}

		// ---- 新增：覆寫 feed 為 BOOK.DELTA（避免下游只靠 subject 判斷）----
		if msg.Source == nil {
			msg.Source = &marketcommonv1.Source{}
		}
		msg.Source.Feed = "BOOK.DELTA" // English feed name; 對外顯示更精確
		// -----------------------------------------------------------

		// message_id 以「新的 feed 值」計算
		bodyBytes, _ := proto.Marshal(book)
		msg.MessageId = makeMsgID(
			msg.GetSource().GetExchange().String(),
			msg.GetSource().GetFeed(),
			bodyBytes,
		)

		outs = append(outs, rg.Out{
			Subject: buildBookDeltaSubject(canon),
			Msg:     msg,
		})
	}
	return outs, nil
}

// ─────────────────── mark-price ───────────────────
//
// OKX 文檔：channel = "mark-price"
// 推送頻率：價格變動時 200ms；未變動時 10s
// 解析：arg.instId, data[].markPx, data[].ts
// 產出：CLEAN.OKX.MARK-PRICE with Any<OKXMarkPriceBody>
// ───────── mark-price（encoding/json 版本） ─────────
func handleMarkPrice(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
    rawAny := env.GetBody()
    if rawAny == nil { return nil, nil }

    var raw marketstreamv1.RawBody
    if err := anypb.UnmarshalTo(rawAny, &raw, proto.UnmarshalOptions{}); err != nil {
        return nil, err
    }

    var head okxMarkPriceHead
    if err := json.Unmarshal(raw.RawData, &head); err != nil {
        return nil, err
    }
    if len(head.Data) == 0 { return nil, nil }

    vendor := strings.TrimSpace(head.Arg.InstID)
    canon := vendor
    if c, err := res.ReverseResolve("okx", vendor); err == nil && c != "" { canon = c }
    mtype := inferMarketType(canon)

    outs := make([]rg.Out, 0, len(head.Data))
    for _, d := range head.Data {
        tsUS := parseMSasUS(d.TS)
        body := &marketstreamv1.OKXMarkPriceBody{
            Version: 1,
            PxE9:    dec1e9(d.MarkPx),
        }
        anyBody, _ := anypb.New(body)

        out := &marketcommonv1.Envelope{
            Version:    env.Version,
            Source:     env.Source,
            Symbol:     canon,
            MarketType: mtype,
            Timestamps: &marketcommonv1.Timestamps{
                EventTsUs:     tsUS,
                CollectRecvUs: env.GetTimestamps().GetCollectRecvUs(),
                CollectPubUs:  env.GetTimestamps().GetCollectPubUs(),
                RefinerRecvUs: env.GetTimestamps().GetRefinerRecvUs(),
            },
            Body: anyBody,
        }
        // message_id
        bodyBytes, _ := proto.Marshal(body)
        out.MessageId = makeMsgID(out.GetSource().GetExchange().String(), out.GetSource().GetFeed(), bodyBytes)

        outs = append(outs, rg.Out{
    		Subject: buildCleanSubject("MARK", canon),
    		Msg:     out,
		})
    }
    return outs, nil
}

// ─────────────────── index-tickers ───────────────────
//
// OKX 文檔：channel = "index-tickers"
// 推送頻率：變動 100ms；未變動每分鐘一次
// 解析：arg.instId, data[].idxPx, high24h, low24h, open24h, sodUtc0, sodUtc8, ts
// 產出：CLEAN.OKX.INDEX-TICKERS with Any<OKXIndexTickersBody>
// ───────── index-tickers（encoding/json 版本） ─────────
func handleIndexTickers(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
    rawAny := env.GetBody()
    if rawAny == nil { return nil, nil }

    var raw marketstreamv1.RawBody
    if err := anypb.UnmarshalTo(rawAny, &raw, proto.UnmarshalOptions{}); err != nil {
        return nil, err
    }

    var head okxIndexTickersHead
    if err := json.Unmarshal(raw.RawData, &head); err != nil {
        return nil, err
    }
    if len(head.Data) == 0 { return nil, nil }

    vendor := strings.TrimSpace(head.Arg.InstID)
    canon := vendor
    if c, err := res.ReverseResolve("okx", vendor); err == nil && c != "" { canon = c }
    mtype := inferMarketType(canon)

    outs := make([]rg.Out, 0, len(head.Data))
    for _, d := range head.Data {
        tsUS := parseMSasUS(d.TS)
        body := &marketstreamv1.OKXIndexTickersBody{
            Version:    1,
            IdxPxE9:    dec1e9(d.IdxPx),
            High24HE9:  dec1e9(d.High24h),
            Low24HE9:   dec1e9(d.Low24h),
            Open24HE9:  dec1e9(d.Open24h),
            SodUtc0E9:  dec1e9(d.SodUtc0),
            SodUtc8E9:  dec1e9(d.SodUtc8),
        }
        anyBody, _ := anypb.New(body)

        out := &marketcommonv1.Envelope{
            Version:    env.Version,
            Source:     env.Source,
            Symbol:     canon,
            MarketType: mtype,
            Timestamps: &marketcommonv1.Timestamps{
                EventTsUs:     tsUS,
                CollectRecvUs: env.GetTimestamps().GetCollectRecvUs(),
                CollectPubUs:  env.GetTimestamps().GetCollectPubUs(),
                RefinerRecvUs: env.GetTimestamps().GetRefinerRecvUs(),
            },
            Body: anyBody,
        }
        // message_id
        bodyBytes, _ := proto.Marshal(body)
        out.MessageId = makeMsgID(out.GetSource().GetExchange().String(), out.GetSource().GetFeed(), bodyBytes)

        outs = append(outs, rg.Out{
    		Subject: buildCleanSubject("INDEX", canon),
    		Msg:     out,
		})
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
		if len(b) == 0 {
			return "0"
		}
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
	if neg {
		v = -v
	}

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

func inferMarketType(c string) marketcommonv1.MarketType {
	c = strings.ToUpper(strings.TrimSpace(c))
	switch {
	case strings.HasSuffix(c, "-SPOT"):
		return marketcommonv1.MarketType_MARKET_SPOT
	case strings.HasSuffix(c, "-SWAP"):
		return marketcommonv1.MarketType_MARKET_PERPETUAL
	default:
		return marketcommonv1.MarketType_MARKET_TYPE_UNSPECIFIED
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

 // --- subject builder helpers ---

// 拆 canonical（BTC-USDT-SWAP / BTC-USDT-SPOT / 也容忍 BTC-USDT）
func splitCanonical(c string) (base, quote, suf string) {
    parts := strings.Split(strings.ToUpper(strings.TrimSpace(c)), "-")
    if len(parts) == 3 {
        return parts[0], parts[1], parts[2]
    }
    if len(parts) == 2 {
        return parts[0], parts[1], "" // 無尾綴（多見於 index family）
    }
    return c, "", ""
}

// 依 stream 與 canonical 組「逐標的」CLEAN subject。
// stream: TRADES / TRADES-ALL / BBO / BOOK / MARK / INDEX
// 規則：
//  - TRADES/BBO/BOOK 用 canonical 的後綴（SWAP/SPOT）
//  - MARK 強制 SWAP（合約標記價概念）
//  - INDEX 強制 INDEX（指數價）
//  - BOOK.DELTA 留給 book_group 內部匯流，所以還是走固定 "CLEAN.OKX.BOOK.DELTA"
func buildCleanSubject(stream, canonical string) string {
    base, quote, suf := splitCanonical(canonical)
    switch stream {
    case "TRADES", "TRADES-ALL", "BBO", "BOOK":
        if suf == "" { suf = "SPOT" } // 保底
        return fmt.Sprintf("CLEAN.OKX.%s.%s.%s.%s", stream, base, quote, suf)
    case "MARK":
        return fmt.Sprintf("CLEAN.OKX.%s.%s.%s.SWAP", stream, base, quote)
    case "INDEX":
        return fmt.Sprintf("CLEAN.OKX.%s.%s.%s.INDEX", stream, base, quote)
    default:
        // 不該發生：回退成舊總線（避免丟資料）
        return fmt.Sprintf("CLEAN.OKX.%s", stream)
    }
}

// BOOK.DELTA：CLEAN.OKX.BOOK.DELTA.<BASE>.<QUOTE>.<SUF>
func buildBookDeltaSubject(canonical string) string {
    base, quote, suf := splitCanonical(canonical)
    if suf == "" { suf = "SPOT" } // 保底
    return fmt.Sprintf("CLEAN.OKX.BOOK.DELTA.%s.%s.%s", base, quote, suf)
}