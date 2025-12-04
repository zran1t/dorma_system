// File: data_dpt/kols/hf_market_data_kol/collect_group/exchanges/okx/adapter.go
// Package: okx
//
// 職責 (Responsibility):
//     實作 OKX 的 WS Adapter：
//       - 組合訂閱封包
//       - 正規化 channel
//       - 將 instId 轉成 RAW subject
//       - 把原始資料包成 Envelope + Any<RawBody> 往內部 bus 丟
//
// 注意事項 (Notes):
//     - 這一層只負責「原樣保留 OKX payload + 加上 Envelope 外殼」，不做業務解析。
//     - symbol canonical / MarketType 等較高階資訊交給 Refiner 透過 ReverseResolve 處理。

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

// Adapter 為 OKX 專用的 WS Adapter 實作。
//
// 功能:
//   - 實作 ws.Adapter 介面，負責 OKX 的訂閱封包格式與回傳訊息包裝。
//   - 將 OKX 的原始 JSON 以 Any<RawBody> 塞進 Envelope，統一交給後續 Refiner 處理。
//
// 欄位說明:
//   - 無（目前為無狀態實作）。
//
// 契約 / 限制:
//   - Handle 若無法判斷 channel 或 subject，會回傳空 subject，讓上游 Collector 自動略過。
//   - Envelope 內部的 Symbol 目前使用 instId 原樣，canonical 交給 Refiner。
//
// 備註:
//   - 若未來需要依不同 feed 拆不同 Adapter，也可以用小型 struct 包裝設定。
type Adapter struct{}

// NewAdapter 建立一個 OKX Adapter 實例。
//
// 功能:
//   - 提供給呼叫端方便取得 *Adapter，用來注入 Collector。
//
// 參數:
//   - 無。
//
// 回傳:
//   - *Adapter: 新建立的 OKX Adapter 實例。
//   - 無 error。
//
// 備註:
//   - 目前為無狀態實作，適合多 goroutine 共用。
func NewAdapter() *Adapter { return &Adapter{} }

// normalizeChannel 將外部輸入的 channel 轉成 OKX 官方約定的 key。
//
// 功能:
//   - 對大小寫與常見別名做正規化，確保後續組訂閱封包與 subject 時使用穩定字串。
//   - 對於不認得的字串，維持小寫原樣，交給上游配置或監控發現問題。
//
// 參數:
//   - ch: 原始 channel 名稱字串（可能包含大小寫或底線等差異）。
//
// 回傳:
//   - string: 正規化後的 channel 名稱。
//
// 備註:
//   - 像 "trades_all" / "trades-all" 都會被折成 "trades-all"。
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
		// 不認得就維持小寫原樣，讓上游配置或監控去發現問題。
		return strings.ToLower(ch)
	}
}

// splitInstID 將 OKX 的 instId 拆解為 base / quote / 後綴。
//
// 功能:
//   - 將像 "BTC-USDT-SWAP" 之類的 instId 拆成三段，供 buildRAWSubject 組 subject 使用。
//   - 若只有兩段（BASE-QUOTE），則 suffix 會標記為 "SPOT_OR_INDEX" 留待後續補判。
//
// 參數:
//   - instID: 來自 OKX 的 instId 字串。
//
// 回傳:
//   - base:   基礎貨幣（大寫）。
//   - quote:  報價貨幣（大寫）。
//   - suffix: 第三段後綴（SWAP / INDEX / SPOT_OR_INDEX / 其他）。
//
// 備註:
//   - 若格式完全不合預期，會回傳 instID 本身當 base，其餘為空字串。
func splitInstID(instID string) (base, quote, suffix string) {
	parts := strings.Split(strings.ToUpper(strings.TrimSpace(instID)), "-")
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}
	if len(parts) == 2 {
		return parts[0], parts[1], "SPOT_OR_INDEX"
	}
	return instID, "", ""
}

// buildRAWSubject 根據 OKX 的 channel 與 instId 生成 RAW subject 與 stream 名稱。
//
// 功能:
//   - 先將 channel 正規化，再拆 instId，最後依照不同資料族群組出 RAW.OKX.* subject。
//   - 同時回傳對應的 stream 名稱（TRADES / BOOK / BBO / MARK / INDEX ...）。
//
// 參數:
//   - okxChannel: 原始或正規化前的 channel 名稱。
//   - instID:     OKX instId，例如 "BTC-USDT-SWAP"。
//
// 回傳:
//   - subject: 完整的 RAW subject 字串，若無法判斷則為空字串。
//   - stream:  上游 schema 內使用的 feed 名稱（TRADES / MARK / INDEX / ...）。
//
// 備註:
//   - 不支援的 channel 會回傳空 subject，讓呼叫端選擇忽略。
func buildRAWSubject(okxChannel, instID string) (subject, stream string) {
	ch := normalizeChannel(okxChannel)
	base, quote, suf := splitInstID(instID)

	switch ch {
	case "trades":
		stream = "TRADES"
		if suf == "SPOT_OR_INDEX" {
			suf = "SPOT"
		}
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream

	case "trades-all":
		stream = "TRADES-ALL"
		if suf == "SPOT_OR_INDEX" {
			suf = "SPOT"
		}
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream

	case "books":
		stream = "BOOK"
		if suf == "SPOT_OR_INDEX" {
			suf = "SPOT"
		}
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream

	case "bbo-tbt":
		stream = "BBO"
		if suf == "SPOT_OR_INDEX" {
			suf = "SPOT"
		}
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream

	case "mark-price":
		stream = "MARK"
		if suf == "SPOT_OR_INDEX" {
			// mark 一般落在合約級別，預設推成 SWAP。
			suf = "SWAP"
		}
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream

	case "index-tickers":
		stream = "INDEX"
		// index 家族統一加上 INDEX 後綴。
		suf = "INDEX"
		return fmt.Sprintf("RAW.OKX.%s.%s.%s.%s", stream, base, quote, suf), stream
	}

	// 不認得的 channel：不發佈（空 subject）
	return "", ""
}

// BuildSubscribeMsgs 根據 channels / symbols 組出 OKX 訂閱 JSON 封包。
//
// 功能:
//   - 對每一個 (channel, symbol) 組成獨立一包 subscribe 訊息，降低單包 payload 大小。
//   - 使用 normalizeChannel 來穩定 channel 名稱。
//   - 若 JSON 序列化失敗，會中止並回傳錯誤。
//
// 參數:
//   - channels: 要訂閱的 channel 名稱列表。
//   - symbols:  要訂閱的 instId 列表。
//
// 回傳:
//   - []string: OKX 規格的 subscribe JSON 字串列表。
//   - error:    若任一封包 marshal 失敗，回傳對應錯誤；全部成功時為 nil。
//
// 備註:
//   - 這邊採用「一個 (ch, inst) 一包」的策略，避免一包塞太多導致限流或丟包。
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
			// 一個 (channel, symbol) 一包，避免一次塞太多造成限流或丟包。
			b, err := json.Marshal(req{Op: "subscribe", Args: []arg{{Channel: okxCh, InstID: instID}}})
			if err != nil {
				return nil, fmt.Errorf("marshal subscribe %s %s: %w", okxCh, instID, err)
			}
			out = append(out, string(b))
		}
	}
	return out, nil
}

// Heartbeat 回傳 OKX 的心跳策略。
//
// 功能:
//   - 告訴 Collector 是否需要由應用層送出特別的 heartbeat payload。
//   - 目前回傳 payload=""、every=0，代表不需要 app-level heartbeat，由 ws ping/pong 自己處理。
//
// 參數:
//   - 無。
//
// 回傳:
//   - string:        目前固定為空字串，表示不送自訂 heartbeat payload。
//   - time.Duration: 目前固定為 0，代表 Collector 不會啟動 heartbeat ticker。
//
// 備註:
//   - 若未來 OKX 針對某些 feed 有特別要求，可在此回傳固定 JSON 串。
func (a *Adapter) Heartbeat() (string, time.Duration) { return "", 0 }

// Handle 將 OKX 的原始 JSON 訊息包裝成 Envelope 並回傳 subject / payload。
//
// 功能:
//   - 從 JSON 中抓出 arg.channel / arg.instId，據此決定 RAW subject 與 stream 名稱。
//   - 將原始 data 塞入 RawBody，再封進 Envelope.Body (Any)。
//   - 設定 Envelope.Source / Symbol / Timestamps 等基本欄位。
//   - 最後回傳 subject 與序列化後的 Envelope bytes。
//
// 參數:
//   - data: 從 WS 收到的原始 JSON bytes。
//
// 回傳:
//   - string: 要發佈的 subject；若無法判斷 channel/subject 則為空字串。
//   - []byte: 封裝好的 Envelope protobuf bytes；若 subject 為空則為 nil。
//   - error:  若封裝 Any 或 marshal Envelope 失敗，回傳錯誤；正常情況為 nil。
//
// 備註:
//   - 不認得的 channel 會直接回傳空 subject，讓 Collector 略過該訊息。
//   - MarketType 目前標為 UNSPECIFIED，由後續 Refiner 根據 instId 再行判斷。
func (a *Adapter) Handle(data []byte) (string, []byte, error) {
	nowUs := uint64(time.Now().UnixMicro())

	ch := gjson.GetBytes(data, "arg.channel").String()
	if ch == "" {
		return "", nil, nil
	}
	instID := gjson.GetBytes(data, "arg.instId").String()

	subj, stream := buildRAWSubject(ch, instID)
	if subj == "" {
		return "", nil, nil
	}

	// Any<RawBody>：保留原封包給 Refiner 做後續解析。
	raw := &marketstreamv1.RawBody{RawData: data}
	anyBody, err := anypb.New(raw)
	if err != nil {
		return "", nil, fmt.Errorf("pack raw body: %w", err)
	}

	env := &marketcommonv1.Envelope{
		Version: "",
		Source: &marketcommonv1.Source{
			Exchange:      marketcommonv1.Exchange_EXCHANGE_OKX,
			Feed:          stream,               // TRADES / MARK / INDEX ...
			VendorChannel: normalizeChannel(ch), // 正規化後的 channel 鍵
		},
		Symbol:     instID, // 原生 instId；canonical 由 Refiner 裡的 ReverseResolve 處理。
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
