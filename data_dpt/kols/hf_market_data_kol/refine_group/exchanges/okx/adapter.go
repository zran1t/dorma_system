// File: data_dpt/kols/hf_market_data_kol/refine_group/exchanges/okx/adapter.go
// Package: okx
//
// 職責 (Responsibility):
//     將 OKX RAW stream 轉成內部 CLEAN envelope：
//       - trades-all / trades / bbo / book / mark-price / index-tickers
//       - 做基本欄位整理（價格縮放、時間戳、symbol 轉 canonical、市場類型判斷...）
//       - 建立 MessageId 與 CLEAN subject。
//     這一層是「Refine engine 的 OKX 插頭」，不碰業務策略，只處理資料型態轉換與去重所需欄位整理。
//
// 注意事項 (Notes):
//     - 所有 handler 都假設 Envelope.Body 是 marketstreamv1.RawBody（由 collect_group 產出）。
//     - symbol canonical 透過 Resolver.ReverseResolve("okx", vendorSymbol) 取得，找不到就退回 vendor 原字串。
//     - MarketType 只靠 canonical 尾綴推斷，若格式錯誤會標為 UNSPECIFIED。
//     - 這裡只組 rg.Out；真正的去重與 publish 是在 Refiner 裡處理。

package okx

import (
	// === 標準函式庫 (Standard Library) ===
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	// === 第三方套件 (Third-Party Libraries) ===
	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	// === 系統內模組 (Internal Modules) ===
	rg "dorma_system/data_dpt/kols/hf_market_data_kol/refine_group"
	"dorma_system/infra/symbols"
	marketcommonv1 "dorma_system/schemas/gen/go/market/common/v1"
	marketstreamv1 "dorma_system/schemas/gen/go/market/stream/v1"
)

// adapter 是 OKX 用的 ExchangeAdapter 實作，對外透過 rg.ExchangeAdapter 介面使用。
//
// 功能:
//   - 讓 Refiner 可以註冊一組「Exchange=OKX + feed→handler」的處理邏輯。
//   - 本身不保存任何狀態，所有處理都在 handler function 內完成。
//
// 欄位說明:
//   - 無（目前為無狀態實作）。
//
// 契約 / 限制:
//   - 必須實作 rg.ExchangeAdapter 介面裡的 Exchange 與 Handlers 方法。
//   - handler 只處理 OKX 專屬資料格式，不應混入其他交易所邏輯。
//
// 備註:
//   - 不導出型別名稱，避免被外部直接依賴，統一透過 New()/Register 注入。
type adapter struct{}

// New 建立一個 OKX 專用的 ExchangeAdapter 實例。
//
// 功能:
//   - 將 adapter 包成 rg.ExchangeAdapter 回傳，方便呼叫端直接註冊進 Refiner。
//
// 參數:
//   - 無。
//
// 回傳:
//   - rg.ExchangeAdapter: 代表 OKX 的 adapter 實例。
//   - 無 error。
//
// 備註:
//   - 建議呼叫端搭配 Register 使用，或直接丟給 NewChief 的 ads 參數。
func New() rg.ExchangeAdapter { return adapter{} }

// Exchange 回報這個 adapter 對應的交易所列舉值。
//
// 功能:
//   - 讓 Refiner 知道此 adapter 是對應哪一個 marketcommonv1.Exchange。
//
// 參數:
//   - 無。
//
// 回傳:
//   - marketcommonv1.Exchange: 目前固定為 EXCHANGE_OKX。
//   - 無 error。
//
// 備註:
//   - 這個值會被用來建立 handlers map 的第一層 key。
func (adapter) Exchange() marketcommonv1.Exchange {
	return marketcommonv1.Exchange_EXCHANGE_OKX
}

// Handlers 回傳此交易所支援的 feed → handler 對應表。
//
// 功能:
//   - 提供一張「邏輯 feed 名」到 handler 的對照表給 Refiner 使用。
//   - key 是內部 feed 鍵，例如 "TRADES-ALL" / "TRADES" / "BBO" / "BOOK" / "MARK-PRICE" / "INDEX-TICKERS"。
//   - 真正訂閱的 RAW subject 會在 Chief 那邊用相同 key 去對應。
//
// 參數:
//   - 無。
//
// 回傳:
//   - map[string]rg.Handler: feed 名稱到 handler 函式的 map。
//   - 無 error。
//
// 備註:
//   - 若要新增 feed，只要增加 map entries 並實作對應 handler 即可。
func (adapter) Handlers() map[string]rg.Handler {
	return map[string]rg.Handler{
		"TRADES-ALL":    handleTradesAll,
		"TRADES":        handleTrades,
		"BBO":           handleBBO,
		"BOOK":          handleBook,
		"MARK-PRICE":    handleMarkPrice,
		"INDEX-TICKERS": handleIndexTickers,
	}
}

// Register 是一個小幫手：直接把 OKX adapter 掛到 Refiner 上。
//
// 功能:
//   - 呼叫 Refiner.RegisterAdapter(adapter{})，簡化呼叫端程式碼。
//   - 讓呼叫端只要帶入 Refiner 指標就能立即啟用 OKX handler。
//
// 參數:
//   - r: *rg.Refiner，Refiner 實例指標。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 嚴格來說只是 syntactic sugar，但能讓初始化比較乾淨。
func Register(r *rg.Refiner) { r.RegisterAdapter(adapter{}) }

// ─────────────────── trades-all ───────────────────

// handleTradesAll 將 OKX trades-all RAW → OKXAllTradeBody → CLEAN subject。
//
// 功能:
//   - 解析 RawBody.RawData，認出 OKX trades-all JSON 結構，轉成多筆 OKXAllTradeBody。
//   - 利用 Resolver 反查 canonical symbol，同時推斷 MarketType。
//   - 建立 Envelope（複製來源 timestamps / source），設定 MessageId 與 CLEAN subject。
//   - 一筆 RAW（通常 data 多筆）會展開成多個 rg.Out。
//
// 參數:
//   - env: collect_group 丟進來的 Envelope（內含 RawBody）。
//   - res: symbols.Resolver，用於 vendor symbol → canonical 的反查。
//
// 回傳:
//   - []rg.Out: 展開後的多筆成交結果，每筆包含 subject 與 Envelope。
//   - error:    JSON 或 proto 處理失敗時回傳錯誤；正常流程為 nil。
//
// 備註:
//   - 若 Body 非 RawBody、JSON 結構不符、或 data 長度為 0，則回傳 nil,nil，表示略過。
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
		// 事件時間：優先吃 Envelope.EventTsUs，沒有就用 vendor ts。
		ts := env.Timestamps.GetEventTsUs()
		if ts == 0 {
			ts = parseMSasUS(d.TS)
		}

		tradeAll := &marketstreamv1.OKXAllTradeBody{
			SchemaVersion: "",
			PxE9:          dec1e9(d.Px),
			SzE9:          dec1e9(d.Sz),
			Side:          toSide(d.Side),
			TradeId:       d.TradeID,
		}
		anyBody, _ := anypb.New(tradeAll)

		msg := &marketcommonv1.Envelope{
			Version:    env.Version,
			Source:     env.Source, // Exchange / Feed 保留原樣
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

		bodyBytes, _ := proto.Marshal(tradeAll)
		msg.MessageId = makeMsgID(
			msg.GetSource().GetExchange().String(),
			msg.GetSource().GetFeed(),
			bodyBytes,
		)

		outs = append(outs, rg.Out{
			Subject: buildCleanSubject("TRADES-ALL", canon),
			Msg:     msg,
		})
	}
	return outs, nil
}

// handleTrades 將 OKX trades（聚合版） RAW → OKXTradeBody → CLEAN subject。
//
// 功能:
//   - 解析 trades 聚合格式 JSON，建立多筆 OKXTradeBody，包含 seqId / count 等欄位。
//   - 推斷 canonical symbol 與 MarketType，複製 timestamps，組成 Envelope。
//   - 計算 MessageId，並回傳多筆 rg.Out。
//
// 參數:
//   - env: 上游包好的 Envelope，Body 預期為 RawBody。
//   - res: symbols.Resolver，用於 vendor → canonical 反查。
//
// 回傳:
//   - []rg.Out: 多筆 TRADES 資料，每筆含 subject 與 Envelope。
//   - error:    JSON 或 proto 相關錯誤；正常情況為 nil。
//
// 備註:
//   - 若 data 为空，或 body 結構錯誤，會直接回 nil,nil。
func handleTrades(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
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

		trade := &marketstreamv1.OKXTradeBody{
			SchemaVersion: "",
			PxE9:          dec1e9(d.Px),
			SzE9:          dec1e9(d.Sz),
			Side:          toSide(d.Side),
			Count:         d.Count,
			SeqId:         uint64(d.SeqID),
			TradeId:       d.TradeID,
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
		msg.MessageId = makeMsgID(
			msg.GetSource().GetExchange().String(),
			msg.GetSource().GetFeed(),
			bodyBytes,
		)

		outs = append(outs, rg.Out{
			Subject: buildCleanSubject("TRADES", canon),
			Msg:     msg,
		})
	}
	return outs, nil
}

// handleBBO 將 OKX bbo RAW → OKXBBOBody → CLEAN subject。
//
// 功能:
//   - 解析 OKX bbo JSON，轉成 OKXBBOBody，包含 bid/ask 價量與掛單數。
//   - 依照 canonical 推斷 MarketType，複製 timestamps、source 等欄位。
//   - 計算 MessageId，回傳多筆 BBO rg.Out。
//
// 參數:
//   - env: Envelope（Body 為 RawBody）。
//   - res: symbols.Resolver，用於 symbol 反查。
//
// 回傳:
//   - []rg.Out: BBO 事件列表。
//   - error:    JSON 解析或 proto 處理錯誤；正常為 nil。
//
// 備註:
//   - 若 data 为空，直接略過。
func handleBBO(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
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
			SchemaVersion: "",
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
		msg.MessageId = makeMsgID(
			msg.GetSource().GetExchange().String(),
			msg.GetSource().GetFeed(),
			bodyBytes,
		)

		outs = append(outs, rg.Out{
			Subject: buildCleanSubject("BBO", canon),
			Msg:     msg,
		})
	}
	return outs, nil
}

// handleBook 將 OKX book RAW → OKXBooksBody（增量）→ BOOK.DELTA subject。
//
// 功能:
//   - 解析 snapshot / update 類型的 order book JSON。
//   - 將 bids/asks 轉成 OKXBookLevel slice，包成 OKXBooksBody。
//   - 將 Source.Feed 強制設成 "BOOK.DELTA"，方便下游直接依 feed 判斷是增量簿。
//   - 設定 MessageId，並輸出到 "CLEAN.OKX.BOOK.DELTA.*" subject。
//
// 參數:
//   - env: Envelope，Body 為 RawBody。
//   - res: symbols.Resolver，用於 canonical 查詢。
//
// 回傳:
//   - []rg.Out: BOOK.DELTA 事件列表。
//   - error:    JSON 或 proto 失敗時回傳錯誤；正常為 nil。
//
// 備註:
//   - Checksum / SeqId / PrevSeqId 會原樣保留，供下游簿聚合層使用。
func handleBook(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
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
			SchemaVersion: "",
			Action:        toAction(head.Action),
			Bids:          toLevels(d.Bids),
			Asks:          toLevels(d.Asks),
			Checksum:      d.Checksum,
			PrevSeqId:     d.PrevSeqID,
			SeqId:         d.SeqID,
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

		if msg.Source == nil {
			msg.Source = &marketcommonv1.Source{}
		}
		msg.Source.Feed = "BOOK.DELTA"

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

// handleMarkPrice 將 OKX mark-price RAW → OKXMarkPriceBody → CLEAN subject。
//
// 功能:
//   - 從 Envelope.Body 解出 RawBody，再解析 mark-price JSON 結構。
//   - 轉成 OKXMarkPriceBody（只關心價格，時間戳由 ts 轉換而來）。
//   - 依 canonical 推斷 MarketType，組成 Envelope 並附上 MessageId。
//   - 輸出到 "CLEAN.OKX.MARK.*.SWAP" subject。
//
// 參數:
//   - env: Envelope，Body 預期為 RawBody。
//   - res: symbols.Resolver，用於 vendor symbol → canonical 反查。
//
// 回傳:
//   - []rg.Out: 多筆 Mark price 事件（通常一筆）。
//   - error:    JSON 解析錯誤等；正常為 nil。
//
// 備註:
//   - Mark price 一般落在合約級別，buildCleanSubject 已固定 suffix=SWAP。
func handleMarkPrice(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	rawAny := env.GetBody()
	if rawAny == nil {
		return nil, nil
	}

	var raw marketstreamv1.RawBody
	if err := anypb.UnmarshalTo(rawAny, &raw, proto.UnmarshalOptions{}); err != nil {
		return nil, err
	}

	var head okxMarkPriceHead
	if err := json.Unmarshal(raw.RawData, &head); err != nil {
		return nil, err
	}
	if len(head.Data) == 0 {
		return nil, nil
	}

	vendor := strings.TrimSpace(head.Arg.InstID)
	canon := vendor
	if c, err := res.ReverseResolve("okx", vendor); err == nil && c != "" {
		canon = c
	}
	mtype := inferMarketType(canon)

	outs := make([]rg.Out, 0, len(head.Data))
	for _, d := range head.Data {
		tsUS := parseMSasUS(d.TS)
		body := &marketstreamv1.OKXMarkPriceBody{
			SchemaVersion: "",
			PxE9:          dec1e9(d.MarkPx),
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

		bodyBytes, _ := proto.Marshal(body)
		out.MessageId = makeMsgID(
			out.GetSource().GetExchange().String(),
			out.GetSource().GetFeed(),
			bodyBytes,
		)

		outs = append(outs, rg.Out{
			Subject: buildCleanSubject("MARK", canon),
			Msg:     out,
		})
	}
	return outs, nil
}

// ─────────────────── index-tickers ───────────────────

// handleIndexTickers 將 OKX index-tickers RAW → OKXIndexTickersBody → CLEAN subject。
//
// 功能:
//   - 解析 index-tickers JSON，轉成 OKXIndexTickersBody（含 24h high/low/open 等）。
//   - 推斷 canonical 與 MarketType，組 Envelope，附上 MessageId。
//   - 輸出到 "CLEAN.OKX.INDEX.*.INDEX" subject。
//
// 參數:
//   - env: Envelope，Body 為 RawBody。
//   - res: symbols.Resolver，用於 symbol 反查。
//
// 回傳:
//   - []rg.Out: 多筆 index ticker 事件。
//   - error:    JSON 或 proto 錯誤；正常為 nil。
//
// 備註:
//   - 若 data 為空，直接略過。
func handleIndexTickers(env *marketcommonv1.Envelope, res symbols.Resolver) ([]rg.Out, error) {
	rawAny := env.GetBody()
	if rawAny == nil {
		return nil, nil
	}

	var raw marketstreamv1.RawBody
	if err := anypb.UnmarshalTo(rawAny, &raw, proto.UnmarshalOptions{}); err != nil {
		return nil, err
	}

	var head okxIndexTickersHead
	if err := json.Unmarshal(raw.RawData, &head); err != nil {
		return nil, err
	}
	if len(head.Data) == 0 {
		return nil, nil
	}

	vendor := strings.TrimSpace(head.Arg.InstID)
	canon := vendor
	if c, err := res.ReverseResolve("okx", vendor); err == nil && c != "" {
		canon = c
	}
	mtype := inferMarketType(canon)

	outs := make([]rg.Out, 0, len(head.Data))
	for _, d := range head.Data {
		tsUS := parseMSasUS(d.TS)
		body := &marketstreamv1.OKXIndexTickersBody{
			SchemaVersion: "",
			IdxPxE9:       dec1e9(d.IdxPx),
			High24HE9:     dec1e9(d.High24h),
			Low24HE9:      dec1e9(d.Low24h),
			Open24HE9:     dec1e9(d.Open24h),
			SodUtc0E9:     dec1e9(d.SodUtc0),
			SodUtc8E9:     dec1e9(d.SodUtc8),
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

		bodyBytes, _ := proto.Marshal(body)
		out.MessageId = makeMsgID(
			out.GetSource().GetExchange().String(),
			out.GetSource().GetFeed(),
			bodyBytes,
		)

		outs = append(outs, rg.Out{
			Subject: buildCleanSubject("INDEX", canon),
			Msg:     out,
		})
	}
	return outs, nil
}

// ─────────────────── utilities ───────────────────

// dec1e9 將十進位字串（可含小數）轉成「乘上 1e9 的 int64」。
//
// 功能:
//   - 將像 "123.45" 類的價格/數量字串，拆成整數與小數，轉成 (intPart*1e9 + fracPart)。
//   - 忽略非 0-9 字元，只保留數字，有點防禦性寫法。
//   - 小數位數不足 9 位時右補 0，多於 9 位則截斷。
//
// 參數:
//   - s: 任意含數字的小數字串，例如 "123.45"、"-0.001" 等。
//
// 回傳:
//   - int64: 乘上 1e9 之後的整數表示。
//   - 無 error（錯誤時會退回 0）。
//
// 備註:
//   - 負號會被單獨處理，內部先轉成正數再加回負號。
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

// parseMSasUS 將毫秒字串轉成微秒（uint64）。
//
// 功能:
//   - 將像 "1699999999999" 之類的毫秒字串轉成微秒，供 Envelope.Timestamps 使用。
//
// 參數:
//   - msStr: 以 10 進位表示的毫秒字串。
//
// 回傳:
//   - uint64: 對應的微秒數；解析失敗或小於等於 0 時回傳 0。
//   - 無 error。
//
// 備註:
//   - 僅做簡單 ParseInt，錯誤不會回傳，只是視為 0。
func parseMSasUS(msStr string) uint64 {
	ms, err := strconv.ParseInt(msStr, 10, 64)
	if err != nil || ms <= 0 {
		return 0
	}
	return uint64(ms) * 1000
}

// toSide 將 OKX side 字串轉成 Side 列舉。
//
// 功能:
//   - 解析 "buy"/"b" / "sell"/"s" 等字串，轉成 marketstreamv1.Side。
//
// 參數:
//   - s: 原始 side 字串。
//
// 回傳:
//   - marketstreamv1.Side: BUY / SELL / UNKNOWN。
//
// 備註:
//   - 若格式不認得，一律標記為 SIDE_UNKNOWN。
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

// toAction 將 snapshot / update 字串轉成 Action 列舉。
//
// 功能:
//   - 將 OKX 替簿資料帶的 "snapshot" / "update" 轉成 enum。
//
// 參數:
//   - s: 原始 action 字串。
//
// 回傳:
//   - marketstreamv1.Action: SNAPSHOT / UPDATE / UNSPECIFIED。
//
// 備註:
//   - 格式不認得時一律標為 ACTION_UNSPECIFIED。
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

// toLevels 將 [[px, qty, ...], ...] 轉成 []*OKXBookLevel。
//
// 功能:
//   - 解析簿資料陣列，每一列至少含 px / qty 兩個欄位。
//   - 利用 dec1e9 做縮放，組成 OKXBookLevel 列表。
//
// 參數:
//   - rows: 由 JSON 解出來的 bids/asks slice，每個元素是一個 string slice。
//
// 回傳:
//   - []*marketstreamv1.OKXBookLevel: 轉換後的價量列表。
//   - 無 error。
//
// 備註:
//   - 若某列長度不足 2，會直接略過。
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

// inferMarketType 從 canonical 尾綴推測市場類型（SPOT / PERPETUAL / 未指定）。
//
// 功能:
//   - 根據 canonical 是否以 "-SPOT" / "-SWAP" 結尾，決定 MarketType。
//   - 其餘狀況一律標為 MARKET_TYPE_UNSPECIFIED。
//
// 參數:
//   - c: canonical symbol 字串。
//
// 回傳:
//   - marketcommonv1.MarketType: SPOT / PERPETUAL / UNSPECIFIED。
//
// 備註:
//   - 若 canonical 格式不乾淨，這裡不會嘗試修正，只會回 UNSPECIFIED。
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

// makeMsgID 依 exchange / feed / body bytes 算出 128-bit 雜湊，當作 MessageId。
//
// 功能:
//   - 將 exchange / feed / body 長度與內容串起來，交給 xxh3.Hash128 做雜湊。
//   - 回傳 16 bytes，前 8 bytes 為 Lo，後 8 bytes 為 Hi（big endian）。
//
// 參數:
//   - exchange: 交易所名稱字串（通常來自 enum.String()）。
//   - feed:     feed 名稱字串。
//   - body:     已序列化後的 protobuf body。
//
// 回傳:
//   - []byte: 16 bytes 的 MessageId。
//   - 無 error。
//
// 備註:
//   - 這個 ID 用來做去重與追蹤，不是密碼學安全雜湊，但碰撞機率足夠低。
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

// splitCanonical 把 canonical（BTC-USDT-SWAP / BTC-USDT-SPOT / BTC-USDT）拆成三段。
//
// 功能:
//   - 將 canonical 拆成 base / quote / suffix，方便組 subject。
//   - 若只有兩段，suffix 留空；若格式怪，base 退回整串，quote/suffix 留空。
//
// 參數:
//   - c: canonical symbol 字串。
//
// 回傳:
//   - base:   基礎貨幣（大寫）。
//   - quote:  報價貨幣（大寫）。
//   - suf:    尾綴（SWAP / SPOT / INDEX / 其他），可能為空字串。
//
// 備註:
//   - 這裡不做合法性檢查，純工具函式。
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

// buildCleanSubject 根據 stream 與 canonical 建出 CLEAN.* subject。
//
// 功能:
//   - 將 stream + canonical 拆成 base/quote/suffix，再依規則組成 CLEAN topic。
//   - TRADES / TRADES-ALL / BBO / BOOK：空 suffix 視為 SPOT。
//   - MARK：固定輸出 SWAP。
//   - INDEX：固定輸出 INDEX。
//   - 其他 stream：退回 "CLEAN.OKX.<stream>"。
//
// 參數:
//   - stream: 邏輯資料流名稱，例如 "TRADES" / "MARK" / "INDEX"。
//   - canonical: canonical symbol 字串。
//
// 回傳:
//   - string: 最終 CLEAN subject 字串。
//
// 備註:
//   - 這裡只處理 OKX 的路由規則，不適用於其他交易所。
func buildCleanSubject(stream, canonical string) string {
	base, quote, suf := splitCanonical(canonical)
	switch stream {
	case "TRADES", "TRADES-ALL", "BBO", "BOOK":
		if suf == "" {
			suf = "SPOT"
		}
		return fmt.Sprintf("CLEAN.OKX.%s.%s.%s.%s", stream, base, quote, suf)
	case "MARK":
		return fmt.Sprintf("CLEAN.OKX.%s.%s.%s.SWAP", stream, base, quote)
	case "INDEX":
		return fmt.Sprintf("CLEAN.OKX.%s.%s.%s.INDEX", stream, base, quote)
	default:
		return fmt.Sprintf("CLEAN.OKX.%s", stream)
	}
}

// buildBookDeltaSubject 建出 BOOK.DELTA 用的 CLEAN subject。
//
// 功能:
//   - 針對簿增量，統一 route 到 "CLEAN.OKX.BOOK.DELTA.<base>.<quote>.<suffix>"。
//   - suffix 若缺省，視為 SPOT。
//
// 參數:
//   - canonical: canonical symbol 字串。
//
// 回傳:
//   - string: CLEAN BOOK.DELTA subject。
//
// 備註:
//   - 下游簿聚合層再決定怎麼處理 snapshot/update 與 checksum。
func buildBookDeltaSubject(canonical string) string {
	base, quote, suf := splitCanonical(canonical)
	if suf == "" {
		suf = "SPOT"
	}
	return fmt.Sprintf("CLEAN.OKX.BOOK.DELTA.%s.%s.%s", base, quote, suf)
}
