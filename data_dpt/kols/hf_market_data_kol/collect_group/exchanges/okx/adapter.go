// collect_group/exchanges/okx/adapter.go
package okx

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	marketcommonv1 "dorma_system/schemas/gen/go/market_common_v1"
	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/tidwall/gjson"
)

type Adapter struct{}
func NewAdapter() *Adapter { return &Adapter{} }

// 將 OKX channel 正規化成我們的鍵
func normalizeChannel(ch string) string {
	switch strings.ToLower(strings.TrimSpace(ch)) {
	case "trades":
		return "trades"
	case "trades-all", "trades_all":
		return "trades-all"
	case "books", "book":
		return "books"
	case "bbo", "bbo-tbt":
		return "bbo-tbt"
	case "mark-price", "mark", "markprice":
		return "mark-price"
	case "index-tickers", "index", "index-ticker", "index_tickers":
		return "index-tickers"
	default:
		return strings.ToLower(ch)
	}
}

// 將 instId → (BASE, QUOTE, SUFFIX)
// 例：BTC-USDT-SWAP → BTC, USDT, SWAP
//     BTC-USDT      → BTC, USDT, SPOT_OR_INDEX (先回 SPOT，交給 caller 決定用 SPOT 還是 INDEX)
func splitInstID(instID string) (base, quote, suffix string) {
	parts := strings.Split(strings.ToUpper(strings.TrimSpace(instID)), "-")
	if len(parts) == 3 { // e.g. BTC-USDT-SWAP
		return parts[0], parts[1], parts[2]
	}
	if len(parts) == 2 { // e.g. BTC-USDT (spot or index family)
		return parts[0], parts[1], "SPOT_OR_INDEX"
	}
	return instID, "", ""
}

// 給定 okxChannel 與 instId，回傳我們的 RAW subject 與 stream 名稱
// stream: TRADES / TRADES-ALL / BOOK / BBO / MARK / INDEX
func buildRAWSubject(okxChannel, instID string) (subject, stream string) {
	ch := normalizeChannel(okxChannel)
	base, quote, suf := splitInstID(instID)

	switch ch {
	case "trades":
		stream = "TRADES"
		// OKX 這邊我們訂的都是合約，所以應該是 SWAP
		if suf == "SPOT_OR_INDEX" { suf = "SPOT" }
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream

	case "trades-all":
		stream = "TRADES-ALL"
		if suf == "SPOT_OR_INDEX" { suf = "SPOT" }
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream

	case "books":
		stream = "BOOK"
		if suf == "SPOT_OR_INDEX" { suf = "SPOT" }
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream

	case "bbo-tbt":
		stream = "BBO"
		if suf == "SPOT_OR_INDEX" { suf = "SPOT" }
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream

	case "mark-price":
		stream = "MARK"
		// mark 是「合約級」，suf 須為 SWAP
		if suf == "SPOT_OR_INDEX" { suf = "SWAP" }
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream

	case "index-tickers":
		stream = "INDEX"
		// index 一定是 …INDEX
		suf = "INDEX"
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream
	}

	// 不認得的 channel：回總線（或直接丟掉）
	return "", ""
}

// --- Adapter ---

// channels: 你的邏輯名稱（"books","mark-price","index-tickers"...）
// symbols: 直接傳 OKX instId（BTC-USDT-SWAP / BTC-USDT）
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
		for _, instID := range symbols {
			b, err := json.Marshal(req{Op: "subscribe", Args: []arg{{Channel: okxCh, InstID: instID}}})
			if err != nil { return nil, fmt.Errorf("marshal subscribe %s %s: %w", okxCh, instID, err) }
			out = append(out, string(b))
		}
	}
	return out, nil
}

func (a *Adapter) Heartbeat() (string, time.Duration) { return "", 0 }

func (a *Adapter) Handle(data []byte) (string, []byte, error) {
	nowUs := uint64(time.Now().UnixMicro())

	ch := gjson.GetBytes(data, "arg.channel").String()
	if ch == "" { return "", nil, nil }
	instID := gjson.GetBytes(data, "arg.instId").String()

	subj, stream := buildRAWSubject(ch, instID)
	if subj == "" { return "", nil, nil }

	// 包成 Any<RawBody>
	raw := &marketstreamv1.RawBody{RawData: data}
	anyBody, err := anypb.New(raw)
	if err != nil { return "", nil, fmt.Errorf("pack raw body: %w", err) }

	env := &marketcommonv1.Envelope{
		Version: 1,
		Source: &marketcommonv1.Source{
			Exchange:      marketcommonv1.Exchange_EXCHANGE_OKX,
			Feed:          stream,             // e.g. "MARK" / "INDEX" / "BOOK"...
			VendorChannel: normalizeChannel(ch),
		},
		Symbol:     instID, // 原生 instId；Refiner 會 ReverseResolve → canonical
		MarketType: marketcommonv1.MarketType_MARKET_TYPE_UNSPECIFIED,
		Timestamps: &marketcommonv1.Timestamps{
			CollectRecvUs: nowUs,
		},
		Body: anyBody,
	}
	env.Timestamps.CollectPubUs = uint64(time.Now().UnixMicro())

	pb, err := proto.Marshal(env)
	if err != nil { return "", nil, fmt.Errorf("proto marshal envelope: %w", err) }
	return subj, pb, nil
}

