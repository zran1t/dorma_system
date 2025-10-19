// data_dpt/kols/lf_market_data_kol/collect_group/exchanges/okx/adapter.go
package okx

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	marketcommonv1 "dorma_system/schemas/gen/go/market_common_v1"
	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"

	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Adapter 實作 WS collect 層的 OKX KLine 轉封裝器。
// 只負責：正規化 channel、抽出 interval、建立 RAW subject、封裝 Envelope。
// 注意：此層不做清洗，Body 一律是 Any<market.stream.v1.RawBody>。
type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

// normalizeFamily 將 OKX candle 家族字串正規化為我們的邏輯鍵。
// 目前支援：mark-price-candle*, index-candle*
func normalizeFamily(ch string) string {
	ch = strings.ToLower(strings.TrimSpace(ch))
	switch {
	case strings.HasPrefix(ch, "mark-price-candle"):
		return "mark-price-candle"
	case strings.HasPrefix(ch, "index-candle"):
		return "index-candle"
	default:
		return ch
	}
}

// parseCandleChannel 將完整 channel 拆成 (family, interval)。
// 例如： "mark-price-candle1m" -> ("mark-price-candle", "1m")
//       "index-candle1Dutc"   -> ("index-candle", "1Dutc")
func parseCandleChannel(full string) (family, interval string) {
	full = strings.TrimSpace(full)
	low := strings.ToLower(full)
	switch {
	case strings.HasPrefix(low, "mark-price-candle"):
		return "mark-price-candle", full[len("mark-price-candle"):]
	case strings.HasPrefix(low, "index-candle"):
		return "index-candle", full[len("index-candle"):]
	default:
		// 無法辨識：回傳原字串與空 interval
		return normalizeFamily(full), ""
	}
}

// splitInstID 將 instId → (BASE, QUOTE, SUFFIX)
func splitInstID(instID string) (base, quote, suffix string) {
	parts := strings.Split(strings.ToUpper(strings.TrimSpace(instID)), "-")
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}
	if len(parts) == 2 {
		return parts[0], parts[1], "SPOT_OR_INDEX"
	}
	return strings.ToUpper(strings.TrimSpace(instID)), "", ""
}

// buildRAWSubject 依 family 與 instId/interval 組 RAW subject。
// 目標：
//   MARKPRICE -> RAW.OKX.MARK-CANDLE.<BASE>.<QUOTE>.<INTERVAL>
//   INDEX     -> RAW.OKX.INDEX-CANDLE.<BASE>.<QUOTE>.<INTERVAL>
func buildRAWSubject(family, instID, interval string) (subject, feed string) {
    base, quote, _ := splitInstID(instID)

    switch family {
    case "mark-price-candle":
        feed = "KLINE.MARKPRICE"
        // 不把 SWAP/INDEX 之類的尾綴塞進 subject（refiner 不需要）
        return fmt.Sprintf("RAW.OKX.MARK-CANDLE.%s.%s.%s", base, quote, interval), feed

    case "index-candle":
        feed = "KLINE.INDEX"
        return fmt.Sprintf("RAW.OKX.INDEX-CANDLE.%s.%s.%s", base, quote, interval), feed
    }
    return "", ""
}

// BuildSubscribeMsgs 將 channels 與 symbols 組成 OKX 訂閱請求。
// channels 例如：["mark-price-candle1m","index-candle1Dutc"]
// symbols  ：native instId（如：BTC-USDT-SWAP / BTC-USDT）
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
		ch = strings.TrimSpace(ch)
		if ch == "" {
			continue
		}
		for _, instID := range symbols {
			b, err := json.Marshal(req{
				Op:   "subscribe",
				Args: []arg{{Channel: ch, InstID: instID}},
			})
			if err != nil {
				return nil, fmt.Errorf("marshal subscribe %q %q: %w", ch, instID, err)
			}
			out = append(out, string(b))
		}
	}
	return out, nil
}

// Heartbeat：OKX business 需要應用層 ping；每 25 秒送一次。
func (a *Adapter) Heartbeat() (string, time.Duration) {
	return `{"op":"ping"}`, 25 * time.Second
}

// Handle 收到 WS 原始資料 → 解析 channel/instId → 建立 RAW subject 與 Envelope。
func (a *Adapter) Handle(data []byte) (string, []byte, error) {
	nowUs := uint64(time.Now().UnixMicro())

	ch := gjson.GetBytes(data, "arg.channel").String()
	if ch == "" {
		return "", nil, nil
	}
	instID := gjson.GetBytes(data, "arg.instId").String()
	if instID == "" {
		return "", nil, nil
	}

	family, interval := parseCandleChannel(ch)
	subj, feed := buildRAWSubject(family, instID, interval)
	if subj == "" {
		return "", nil, nil
	}

	// 包成 Any<RawBody>
	raw := &marketstreamv1.RawBody{RawData: data}
	anyBody, err := anypb.New(raw)
	if err != nil {
		return "", nil, fmt.Errorf("pack raw body: %w", err)
	}

	env := &marketcommonv1.Envelope{
		Version: 1,
		Source: &marketcommonv1.Source{
			Exchange:      marketcommonv1.Exchange_EXCHANGE_OKX,
			Feed:          feed,     // "KLINE.MARKPRICE" / "KLINE.INDEX"
			VendorChannel: ch,       // 例如 "mark-price-candle1m"
			Interval:      interval, // 權威 interval
		},
		Symbol:     instID, // native instId；Refiner ReverseResolve → canonical
		MarketType: marketcommonv1.MarketType_MARKET_TYPE_UNSPECIFIED,
		Timestamps: &marketcommonv1.Timestamps{
			CollectRecvUs: nowUs,
		},
		Body: anyBody,
	}
	env.Timestamps.CollectPubUs = uint64(time.Now().UnixMicro())

	pb, err := proto.Marshal(env)
	if err != nil {
		return "", nil, fmt.Errorf("proto marshal envelope: %w", err)
	}
	return subj, pb, nil
}