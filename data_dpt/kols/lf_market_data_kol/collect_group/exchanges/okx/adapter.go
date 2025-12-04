// File: data_dpt/kols/lf_market_data_kol/collect_group/exchanges/okx/adapter.go
// Package: okx
//
// 職責 (Responsibility):
//     - 實作 LF KLine 用的 WS Adapter：
//         - 正規化 OKX candle channel（mark / index 家族）。
//         - 解析 channel → (family, interval)。
//         - 依 instId / interval 組出 RAW subject 與 feed 名稱。
//         - 封裝 Any<market.stream.v1.RawBody> → Envelope。
//     - 提供 BuildSubscribeMsgs / Heartbeat / Handle 供 ws.Collector 使用。
//
// 注意事項 (Notes):
//     - 此層只做「封裝」與「subject/Feed/Interval 決策」，不做任何清洗或解析 raw 內容。
//     - Body 內容一律維持 RawBody，後續交給 RefineGroup 處理。
//     - Interval 正規化時，會輕度容錯 "UTC" 大小寫，但最終仍以白名單為主。

package okx

import (
	// === 標準函式庫 (Standard Library) ===
	"encoding/json"
	"fmt"
	"strings"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	// === 系統內模組 (Internal Modules) ===
	marketcommonv1 "dorma_system/schemas/gen/go/market/common/v1"
	marketstreamv1 "dorma_system/schemas/gen/go/market/stream/v1"
)

// Adapter 實作 WS collect 層的 OKX KLine 轉封裝器。
//
// 功能:
//   - 正規化 channel（mark-price-candle / index-candle 家族）。
//   - 從 channel 中抽出 interval（例："mark-price-candle1m" → "1m"）。
//   - 建立 RAW subject（RAW.OKX.MARK-CANDLE... / RAW.OKX.INDEX-CANDLE...）。
//   - 封裝 Envelope，Body = Any<market.stream.v1.RawBody>。
//
// 契約 / 限制:
//   - 不負責解析 candle 內容，只保留 raw JSON。
//   - feed/subject/interval 為此層的「權威」輸出，Refiner 直接信任。
type Adapter struct{}

// NewAdapter 建立一個 OKX LF KLine Adapter。
//
// 功能:
//   - 回傳一個無狀態的 Adapter 實例。
//
// 參數:
//   - 無。
//
// 回傳:
//   - *Adapter: OKX KLine Adapter 指標。
//   - 無 error。
func NewAdapter() *Adapter { return &Adapter{} }

// normalizeFamily 將 OKX candle family 字串正規化為邏輯鍵。
//
// 功能:
//   - 把像 "mark-price-candle1m" 這種前綴轉成 "mark-price-candle" 族。
//   - 支援 mark-price-candle* 與 index-candle*。
//   - 其他未識別字串會直接用 lower case 回傳，讓上游監控可以發現異常。
//
// 參數:
//   - ch: 來源 channel 字串。
//
// 回傳:
//   - string: 正規化後的 family 鍵，例如 "mark-price-candle" / "index-candle" / 其他小寫原字串。
//
// 備註:
//   - 僅用於 family 判斷，不負責 interval。
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
//
// 功能:
//   - 例："mark-price-candle1m" → ("mark-price-candle", "1m")。
//   - 例："index-candle1Dutc"  → ("index-candle", "1Dutc")。
//   - 若無法辨識前綴，則 family=normalizeFamily(full)，interval=""。
//
// 參數:
//   - full: OKX 原生 channel 字串。
//
// 回傳:
//   - family:   邏輯家族鍵（"mark-price-candle" / "index-candle" / 其他）。
//   - interval: 剩餘部分（e.g. "1m", "1Dutc", ""）。
//
// 備註:
//   - interval 原樣回傳，不額外改大小寫，之後交給 MakeChannel / 外層處理。
func parseCandleChannel(full string) (family, interval string) {
	full = strings.TrimSpace(full)
	low := strings.ToLower(full)
	switch {
	case strings.HasPrefix(low, "mark-price-candle"):
		return "mark-price-candle", full[len("mark-price-candle"):]
	case strings.HasPrefix(low, "index-candle"):
		return "index-candle", full[len("index-candle"):]
	default:
		// 無法辨識：回傳原字串的 family（normalize 過）與空 interval
		return normalizeFamily(full), ""
	}
}

// splitInstID 將 OKX instId 拆成 (BASE, QUOTE, SUFFIX)。
//
// 功能:
//   - "BTC-USDT-SWAP" → ("BTC","USDT","SWAP")。
//   - "BTC-USDT"      → ("BTC","USDT","SPOT_OR_INDEX")。
//   - 其他格式（含單一字串） → (大寫 instId, "", "")。
//
// 參數:
//   - instID: OKX instId 字串。
//
// 回傳:
//   - base:   基礎貨幣（大寫）。
//   - quote:  報價貨幣（大寫或空）。
//   - suffix: 尾綴（SWAP / SPOT / INDEX / "SPOT_OR_INDEX" / ""）。
//
// 備註:
//   - "SPOT_OR_INDEX" 主要用在後續判斷 default 尾綴時的 fallback。
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

// buildRAWSubject 依 family + instId + interval 組 RAW subject 與 feed 名稱。
// 新規格（對齊 CLEAN 規則）:
//   - MARKPRICE → RAW.OKX.MARK-CANDLE.<BASE>.<QUOTE>.<SWAP>.<INTERVAL>
//   - INDEX     → RAW.OKX.INDEX-CANDLE.<BASE>.<QUOTE>.<INDEX>.<INTERVAL>
//
// 功能:
//   - 根據 family 決定 feed 名稱與 suffix 規則。
//   - 為 mark family 預設 suffix=SWAP，為 index family 直接強制 suffix=INDEX。
//   - 回傳 subject 與 feed（Envelope.Source.Feed 使用）。
//
// 參數:
//   - family:   normalizeFamily 之後的家族鍵。
//   - instID:   OKX instId，例："BTC-USDT-SWAP" / "BTC-USDT"。
//   - interval: Interval 字串，例："1m" / "1Dutc"；可為空。
//
// 回傳:
//   - subject: RAW subject 字串；若 family 不支援則為空字串。
//   - feed:    Feed 名稱（例："KLINE.MARKPRICE" / "KLINE.INDEX"）；若不支援則為空字串。
//
// 備註:
//   - 若 family 不在支援清單內，回傳 ("","") 讓上層直接略過。
func buildRAWSubject(family, instID, interval string) (subject, feed string) {
	base, quote, suf := splitInstID(instID)

	switch family {
	case "mark-price-candle":
		feed = "KLINE.MARKPRICE"
		// MARK 僅接受 SWAP；若 instId 無尾綴或不可辨識，一律規範為 SWAP
		if suf == "" || suf == "SPOT_OR_INDEX" {
			suf = "SWAP"
		}
		return fmt.Sprintf("RAW.OKX.MARK-CANDLE.%s.%s.%s.%s", base, quote, suf, interval), feed

	case "index-candle":
		feed = "KLINE.INDEX"
		// INDEX 家族強制用 INDEX（OKX instId 對 index 常無尾綴）
		suf = "INDEX"
		return fmt.Sprintf("RAW.OKX.INDEX-CANDLE.%s.%s.%s.%s", base, quote, suf, interval), feed
	}
	return "", ""
}

// BuildSubscribeMsgs 將 channels 與 symbols 組成 OKX 訂閱請求。
//
// 功能:
//   - 對每一個 (channel, instId) 組一包 subscribe JSON：
//     {"op":"subscribe","args":[{"channel":ch,"instId":instID}]}
//   - channels 通常已經包含 interval，例："mark-price-candle1m"。
//   - symbols 為 native instId（如：BTC-USDT-SWAP / BTC-USDT）。
//
// 參數:
//   - channels: channel 字串陣列（不得為空字串；空會被略過）。
//   - symbols:  instId 清單，為 OKX 原生 symbol。
//
// 回傳:
//   - []string: JSON 字串陣列，每一個元素是一個 subscribe 請求。
//   - error:    marshal 失敗時回傳錯誤；一般情況為 nil。
//
// 備註:
//   - 沒有做任何合法性檢查，傳什麼就組什麼，方便上層 debug。
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

// Heartbeat 回傳 OKX business WS 需要的 ping payload 與間隔。
//
// 功能:
//   - 對 OKX business 端點，每 25 秒送一次 `{"op":"ping"}`。
//   - 給 ws.Collector 用來決定心跳策略。
//
// 參數:
//   - 無。
//
// 回傳:
//   - string:       心跳 payload 字串（JSON 格式）。
//   - time.Duration: 心跳間隔（目前固定 25 秒）。
//
// 備註:
//   - 若未來 OKX 調整建議間隔，可在此統一修改。
func (a *Adapter) Heartbeat() (string, time.Duration) {
	return `{"op":"ping"}`, 25 * time.Second
}

// MakeChannel: baseFeed + interval → OKX 最終 channel（完整支援官方清單）。
//
// 功能:
//   - 針對 OKX candle family（mark / index），組出合法 channel 名稱：
//     baseFeed + interval（interval 經過白名單檢查與 utc 尾碼正規化）。
//   - 僅允許官方支援的 interval，大小寫與 "utc" 尾碼需要符合清單。
//   - 若 interval 不在白名單中，則直接拼接 baseFeed+interval，方便觀察錯誤輸入。
//
// 參數:
//   - baseFeed: 基礎 feed 名稱，例："mark-price-candle" / "index-candle"（建議小寫）。
//   - interval: interval 字串，例："1m" / "1D" / "1Dutc" 等。
//
// 回傳:
//   - string: 最終 OKX channel 名稱；若 interval 非白名單，仍會直接拼湊回傳。
//
// 備註:
//   - ut c 尾碼會被輕度正規化為 "utc"（大小寫容忍）；其他部分不做轉換。
func MakeChannel(baseFeed, interval string) string {
	base := strings.ToLower(strings.TrimSpace(baseFeed))
	iv := strings.TrimSpace(interval)

	// 允許的 interval（官方原樣白名單）
	okxIntervals := map[string]struct{}{
		// non-UTC
		"1m": {}, "3m": {}, "5m": {}, "15m": {}, "30m": {},
		"1H": {}, "2H": {}, "4H": {}, "6H": {}, "12H": {},
		"1D": {}, "2D": {}, "3D": {}, "5D": {},
		"1W": {},
		"1M": {}, "3M": {}, "1Y": {},

		// UTC variants
		"6Hutc": {}, "12Hutc": {},
		"1Dutc": {}, "2Dutc": {}, "3Dutc": {}, "5Dutc": {},
		"1Wutc": {},
		"1Mutc": {}, "3Mutc": {}, "1Yutc": {},
	}

	// 輕度容錯：把尾碼大小寫正規化為 "utc"
	if strings.HasSuffix(strings.ToLower(iv), "utc") && !strings.HasSuffix(iv, "utc") {
		iv = iv[:len(iv)-3] + "utc"
	}

	// 完整匹配白名單（不自作主張轉大小寫）
	if _, ok := okxIntervals[iv]; ok {
		return base + iv
	}
	// 非法／未支援：照傳入拼，方便動態試錯或快速觀察
	return base + iv
}

// Handle 收到 OKX WS 原始資料 → 解析 → 建立 RAW subject + Envelope。
//
// 功能:
//   - 從 raw JSON 中讀出 arg.channel / arg.instId。
//   - 解析 channel → (family, interval)，再組出 RAW subject / feed。
//   - 將 raw JSON 放入 RawBody，封裝成 Envelope：
//   - Source.Exchange  = EXCHANGE_OKX
//   - Source.Feed      = "KLINE.MARKPRICE" / "KLINE.INDEX"
//   - Source.VendorChannel = 原始 channel
//   - Source.Interval  = interval
//   - Symbol           = instId（native）
//   - Timestamps.CollectRecvUs / CollectPubUs 填當前時間。
//
// 參數:
//   - data: WS 收到的原始 JSON bytes。
//
// 回傳:
//   - string: NATS subject 字串；若無法處理則為空字串。
//   - []byte: 序列化後的 Envelope bytes；若無法處理則為 nil。
//   - error:  封裝或序列化錯誤時回傳；正常下多為 nil。
//
// 備註:
//   - 當 channel 或 instId 缺失 / buildRAWSubject 回傳空 subject 時，會靜默略過（回傳 "", nil, nil）。
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
		Version: "",
		Source: &marketcommonv1.Source{
			Exchange:      marketcommonv1.Exchange_EXCHANGE_OKX,
			Feed:          feed,     // "KLINE.MARKPRICE" / "KLINE.INDEX"
			VendorChannel: ch,       // 例如 "mark-price-candle1m"
			Interval:      interval, // 權威 interval
		},
		Symbol:     instID, // native instId；canonical 由 Refiner ReverseResolve 處理
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
