// File: data_dpt/kols/hf_market_data_kol/refine_group/exchanges/okx/types.go
// Package: okx
//
// 職責 (Responsibility):
//     定義 OKX RAW JSON payload 對應的中繼 struct，專門用來做 json.Unmarshal。
//     這些型別只存在於 refine_group okx adapter 內部，不應暴露到跨模組 API。
//
// 注意事項 (Notes):
//     - 欄位名稱與 JSON 標籤儘量貼近官方文件，方便對照。
//     - 這些 struct 僅供 json.Unmarshal 使用，不負責商業邏輯。

package okx

// okxTradesAllHead：OKX trades-all 外層 / 內層欄位。
// 例：{"arg":{"channel":"trades-all","instId":"BTC-USDT-SWAP"},"data":[{...}]}
//
// 功能:
//   - 描述 OKX trades-all feed 的 JSON 結構，方便 json.Unmarshal 使用。
//
// 欄位說明:
//   - Arg: 內含 channel / instId。
//   - Data: 多筆成交紀錄，每筆包含價格、數量、方向與時間戳等。
//
// 契約 / 限制:
//   - 僅供 okx adapter 內部使用，不應被其他 package 依賴。
//
// 備註:
//   - 有需要額外欄位再依 OKX 文件補上即可。
type okxTradesAllHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		TradeID string `json:"tradeId"`
		Px      string `json:"px"`
		Sz      string `json:"sz"`
		Side    string `json:"side"`
		TS      string `json:"ts"`
	} `json:"data"`
}

// okxTradesHead：OKX trades（聚合版）外層 / 內層欄位。
//
// 功能:
//   - 對應 OKX trades 聚合 feed 的 JSON 結構，包含 seqId / count 等資訊。
//
// 欄位說明:
//   - Arg: 內含 channel / instId。
//   - Data: 多筆聚合後的成交資料，每筆包含 seqId, count, 價格數量等。
//
// 契約 / 限制:
//   - 僅用於 handleTrades 內部 json.Unmarshal。
//   - Count 使用 `json:"count,string"`，OKX 會以字串形式傳送數字。
//
// 備註:
//   - 若官方欄位有變動，應同步調整這裡與對應 handler。
type okxTradesHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		SeqID   uint64 `json:"seqId"`
		Count   uint32 `json:"count,string"`
		TradeID string `json:"tradeId"`
		Px      string `json:"px"`
		Sz      string `json:"sz"`
		Side    string `json:"side"`
		TS      string `json:"ts"`
	} `json:"data"`
}

// okxBBOHead：OKX bbo 外層 / 內層欄位。
//
// 功能:
//   - 對應 OKX bbo feed 的 JSON 結構，包含最優 bid/ask 價量與掛單數等。
//
// 欄位說明:
//   - Arg: 內含 channel / instId。
//   - Data: 每筆為一個 BBO snapshot，含 bidPx / bidSz / askPx / askSz / bidOrdCt / askOrdCt / seqId / ts。
//
// 契約 / 限制:
//   - 僅用於 handleBBO 內部 json.Unmarshal。
//
// 備註:
//   - 欄位命名盡量保持與官方 JSON key 一致，便於對照。
type okxBBOHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		BidPx  string `json:"bidPx"`
		BidSz  string `json:"bidSz"`
		AskPx  string `json:"askPx"`
		AskSz  string `json:"askSz"`
		BidCnt uint32 `json:"bidOrdCt"`
		AskCnt uint32 `json:"askOrdCt"`
		SeqID  uint64 `json:"seqId"`
		TS     string `json:"ts"`
	} `json:"data"`
}

// okxBookHead：OKX books 外層 / 內層欄位（snapshot / update）。
//
// 功能:
//   - 描述 order book 增量/快照 feed 的 JSON 結構，供 handleBook 使用。
//   - 包含 asks/bids 價量陣列、checksum、seqId 等欄位。
//
// 欄位說明:
//   - Arg:    channel / instId。
//   - Action: "snapshot" or "update"。
//   - Data:   每筆包含 asks/bids 列表、checksum、ts、prevSeqId、seqId。
//
// 契約 / 限制:
//   - 只在 okx adapter 範圍內使用。
//   - asks / bids 內部結構為自由陣列 [[px, qty, ...], ...]，實際解析由 toLevels 負責。
//
// 備註:
//   - prevSeqId 允許為負數（OKX 可能用 -1 表示沒有前一筆）。
type okxBookHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Action string `json:"action"` // "snapshot" or "update"
	Data   []struct {
		Asks      [][]string `json:"asks"` // [[px, qty, ...], ...]
		Bids      [][]string `json:"bids"`
		Checksum  int32      `json:"checksum"`
		TS        string     `json:"ts"`
		PrevSeqID int64      `json:"prevSeqId"`
		SeqID     uint64     `json:"seqId"`
	} `json:"data"`
}

// okxMarkPriceHead：OKX mark-price 外層 / 內層欄位。
//
// 功能:
//   - 對應 mark-price feed 的 JSON 結構，供 handleMarkPrice 使用。
//
// 欄位說明:
//   - Arg:  channel / instId。
//   - Data: 每筆包含 instType / instId / markPx / ts。
//
// 契約 / 限制:
//   - 目前只取 markPx 與 ts，其餘欄位保留以備不時之需。
//
// 備註:
//   - instType 通常是 "SWAP" / "FUTURES" 等，但這裡不直接解讀。
type okxMarkPriceHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		InstType string `json:"instType"`
		InstID   string `json:"instId"`
		MarkPx   string `json:"markPx"`
		TS       string `json:"ts"`
	} `json:"data"`
}

// okxIndexTickersHead：OKX index-tickers 外層 / 內層欄位。
//
// 功能:
//   - 對應 index-tickers feed 的 JSON 結構，供 handleIndexTickers 使用。
//   - 包含 index 價、24 小時高低開盤價與 SOdUtc0/8 等欄位。
//
// 欄位說明:
//   - Arg:  channel / instId。
//   - Data: 每筆為一個 index ticker snapshot，含 idxPx / high24h / low24h / open24h / sodUtc0 / sodUtc8 / ts。
//
// 契約 / 限制:
//   - 僅用於 OKX refine adapter，不做跨模組依賴。
//
// 備註:
//   - 這些欄位會被轉換成 OKXIndexTickersBody 中的對應欄位。
type okxIndexTickersHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		InstID  string `json:"instId"`
		IdxPx   string `json:"idxPx"`
		High24h string `json:"high24h"`
		Low24h  string `json:"low24h"`
		Open24h string `json:"open24h"`
		SodUtc0 string `json:"sodUtc0"`
		SodUtc8 string `json:"sodUtc8"`
		TS      string `json:"ts"`
	} `json:"data"`
}
