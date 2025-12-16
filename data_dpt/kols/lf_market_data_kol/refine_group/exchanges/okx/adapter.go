// data_dpt/kols/lf_market_data_kol/refine_group/exchanges/okx/adapter.go
package okx

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	rg "dorma_system/data_dpt/kols/lf_market_data_kol/refine_group"
	"dorma_system/infra/symbols"
	marketcommonv1 "dorma_system/schemas/gen/go/market/common/v1"
	marketklinev1 "dorma_system/schemas/gen/go/market/kline/v1"
	marketstreamv1 "dorma_system/schemas/gen/go/market/stream/v1"

	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ───────────────── adapter 基本資訊 ─────────────────

type adapter struct{}

func New() rg.ExchangeAdapter { return adapter{} }

func (adapter) Exchange() marketcommonv1.Exchange {
	return marketcommonv1.Exchange_EXCHANGE_OKX
}

// feed key 採「KLINE.MARKPRICE / KLINE.INDEX」與 chief 對應
func (adapter) Handlers() map[string]rg.Handler {
	return map[string]rg.Handler{
		"KLINE.MARKPRICE": handleMarkCandle,
		"KLINE.INDEX":     handleIndexCandle,
	}
}

// ───────────────── handlers ─────────────────

// mark-price-candle*
func handleMarkCandle(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	return handleAnyCandle(env, res, true)
}

// index-candle*
func handleIndexCandle(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	return handleAnyCandle(env, res, false)
}

func handleAnyCandle(env *marketcommonv1.Envelope, res symbols.Resolver, isMark bool) ([]rg.Out, error) {
	rawAny := env.GetBody()
	if rawAny == nil {
		return nil, nil
	}
	var raw marketstreamv1.RawBody
	if err := anypb.UnmarshalTo(rawAny, &raw, proto.UnmarshalOptions{}); err != nil {
		return nil, err
	}

	var head okxCandleHead
	if err := json.Unmarshal(raw.RawData, &head); err != nil {
		return nil, err
	}
	if len(head.Data) == 0 {
		return nil, nil
	}

	vendor := strings.TrimSpace(head.Arg.InstID)
	canon := vendor
	if c, err := res.NativeToCanonical("okx", vendor); err == nil && c != "" {
		canon = c
	}

	// 解析 interval（從 channel 字串尾部取出：mark-price-candle1D / index-candle1Dutc）
	interval := parseInterval(head.Arg.Channel)

	// 收集 confirmed=1 的 bars
	bars := make([]*marketklinev1.KLineBar, 0, len(head.Data))
	for _, row := range head.Data {
		if len(row) < 6 {
			continue
		}
		confirm := strings.TrimSpace(row[5]) == "1"
		if !confirm {
			continue // 只要收定的
		}
		openUS := parseMSasUS(row[0])
		bars = append(bars, &marketklinev1.KLineBar{
			OpenTsUs:   openUS,
			Interval:   interval,
			OpenE9:     dec1e9(row[1]),
			HighE9:     dec1e9(row[2]),
			LowE9:      dec1e9(row[3]),
			CloseE9:    dec1e9(row[4]),
			VolumeE9:   0,      // OKX mark/index 不提供量
			Confirmed:  true,   // 已過濾 confirm=1
			VendorTsUs: openUS, // 來源 open ts
		})
	}
	if len(bars) == 0 {
		return nil, nil
	}

	// 組 body
	var anyBody *anypb.Any
	var bodyBytes []byte
	srcFeed := ""
	subject := ""

	if isMark {
		msg := &marketklinev1.OKXMarkPriceKLineBody{
			SchemaVersion: "",
			Bars:          bars,
		}
		srcFeed = "KLINE.MARKPRICE"
		anyBody, _ = anypb.New(msg)
		bodyBytes, _ = proto.Marshal(msg)
		subject = buildCandleSubject(true, canon, interval)
	} else {
		msg := &marketklinev1.OKXIndexKLineBody{
			SchemaVersion: "",
			Bars:          bars,
		}
		srcFeed = "KLINE.INDEX"
		anyBody, _ = anypb.New(msg)
		bodyBytes, _ = proto.Marshal(msg)
		subject = buildCandleSubject(false, canon, interval)
	}

	// 市場類型
	mtype := inferMarketType(canon)
	if !isMark {
		mtype = marketcommonv1.MarketType_MARKET_INDEX
	}

	// 組 Envelope（覆寫 Source.feed/interval）
	out := &marketcommonv1.Envelope{
		Version:    env.Version,
		Source:     env.Source,
		Symbol:     canonicalSymbolForKline(canon, isMark),
		MarketType: mtype,
		Timestamps: &marketcommonv1.Timestamps{
			EventTsUs:     bars[len(bars)-1].GetOpenTsUs(), // 取最後一根 open ts 做事件時間
			CollectRecvUs: env.GetTimestamps().GetCollectRecvUs(),
			CollectPubUs:  env.GetTimestamps().GetCollectPubUs(),
			RefinerRecvUs: env.GetTimestamps().GetRefinerRecvUs(),
		},
		Body: anyBody,
	}
	if out.Source == nil {
		out.Source = &marketcommonv1.Source{}
	}
	out.Source.Feed = srcFeed
	out.Source.Interval = interval

	// message_id = xxhash128(len|exchange | len|feed | len|body)
	out.MessageId = makeMsgID(
		out.GetSource().GetExchange().String(),
		out.GetSource().GetFeed(),
		bodyBytes,
	)

	return []rg.Out{{
		Subject: subject,
		Msg:     out,
	}}, nil
}

// ───────────────── utilities ─────────────────

// interval 從 channel 名字尾巴剝出
// e.g. "mark-price-candle1Dutc" → "1Dutc"
func parseInterval(ch string) string {
	ch = strings.ToLower(strings.TrimSpace(ch))
	// 找到 "candle" 之後的部分
	i := strings.Index(ch, "candle")
	if i < 0 || i+6 >= len(ch) {
		return ""
	}
	return ch[i+6:]
}

// 精確十進位 → E9（截斷 9 位，不四捨五入）
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
	ms, err := strconv.ParseInt(strings.TrimSpace(msStr), 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	return uint64(ms) * 1000
}

// SWAP/INDEX 規則化
// 強制化 SWAP / INDEX，不吃傳入後綴
func canonicalSymbolForKline(c string, isMark bool) string {
	c = strings.ToUpper(strings.TrimSpace(c))
	base, quote, _ := splitCanonical(c) // 忽略原 suffix
	if isMark {
		return base + "-" + quote + "-SWAP"
	}
	return base + "-" + quote + "-INDEX"
}

func inferMarketType(c string) marketcommonv1.MarketType {
	c = strings.ToUpper(strings.TrimSpace(c))
	switch {
	case strings.HasSuffix(c, "-SPOT"):
		return marketcommonv1.MarketType_MARKET_SPOT
	case strings.HasSuffix(c, "-SWAP"):
		return marketcommonv1.MarketType_MARKET_PERPETUAL
	case strings.HasSuffix(c, "-INDEX"):
		return marketcommonv1.MarketType_MARKET_INDEX
	default:
		return marketcommonv1.MarketType_MARKET_TYPE_UNSPECIFIED
	}
}

// CLEAN subject 命名（新版）：
// CLEAN.OKX.MARK-CANDLE.<BASE>.<QUOTE>.<SUFFIX>.<INTERVAL>
// CLEAN.OKX.INDEX-CANDLE.<BASE>.<QUOTE>.<SUFFIX>.<INTERVAL>
// 例：CLEAN.OKX.MARK-CANDLE.BTC.USDT.SWAP.1m
//
//	CLEAN.OKX.INDEX-CANDLE.BTC.USDT.INDEX.1Dutc
func buildCandleSubject(isMark bool, canonical, interval string) string {
	base, quote, _ := splitCanonical(strings.ToUpper(strings.TrimSpace(canonical)))

	suffix := "SWAP"
	family := "MARK-CANDLE"
	if !isMark {
		suffix = "INDEX"
		family = "INDEX-CANDLE"
	}

	return fmt.Sprintf("CLEAN.OKX.%s.%s.%s.%s.%s", family, base, quote, suffix, interval)
}

// 拆 canonical（BTC-USDT-SWAP / BTC-USDT / …）
func splitCanonical(c string) (base, quote, suf string) {
	parts := strings.Split(strings.ToUpper(strings.TrimSpace(c)), "-")
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}
	if len(parts) == 2 {
		return parts[0], parts[1], ""
	}
	return c, "", ""
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
