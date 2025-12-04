// File: data_dpt/kols/hf_market_data_kol/book_group/orderbook.go
// Package: book_group
//
// 職責 (Responsibility):
//     - 以 in-memory 結構維護單一標的的 order book 狀態。
//     - 提供 snapshot / applySnapshot / applyUpdate 等操作。
//     - 對 OKX 的 seqId/prevSeqId 規則做基本檢查與刷新控制。
//
// 注意事項 (Notes):
//     - 這裡不直接碰 NATS / Envelope，只處理簿本身邏輯。
//     - 若 Stale/needRefresh=true，外層 Chief 應視為「需要重拉 snapshot」。

package book_group

import (
	// === 標準函式庫 (Standard Library) ===
	"sort"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	// 無

	// === 系統內模組 (Internal Modules) ===
	marketstreamv1 "dorma_system/schemas/gen/go/market/stream/v1"
)

// sideMap 是「價 → 量」的簡單簿表示。
//
// 功能:
//   - 將同一個價位的委託量聚合成單一數值，減少結構複雜度。
//
// 欄位說明:
//   - key:   價格，以 1e9 固定小數 int64 表示（pxE9）。
//   - value: 數量，以 1e9 固定小數 int64 表示（qtyE9）。
//
// 契約 / 限制:
//   - key/value 需由外部確保為合理數值。
//
// 備註:
//   - 買/賣兩側都是用 sideMap 做儲存。
type sideMap map[int64]int64 // pxE9 -> qtyE9

// OrderBook 表示單一 symbol 的 order book 狀態。
//
// 功能:
//   - 持有 bid/ask 雙邊價量 map。
//   - 追蹤最新 SeqID / UpdatedAt / lastFullAt。
//   - 提供 applySnapshot / applyUpdate / snapshot 等操作。
//   - 控制 Stale / needRefresh 等狀態，讓上層知道是否需要重拉 snapshot。
//
// 欄位說明:
//   - Symbol:     canonical symbol，例如 "BTC-USDT-SWAP"。
//   - SeqID:      最新成功套用的簿序列號。
//   - Stale:      目前簿是否被視為陳舊（通常配合 needRefresh=true）。
//   - Bids:       買盤 sideMap（價 → 量）。
//   - Asks:       賣盤 sideMap（價 → 量）。
//   - UpdatedAt:  最近一次套用 snapshot 或 update 的時間。
//   - lastFullAt: 最近一次發出 BOOK.FULL snapshot 的時間。
//   - lastSeqSeen: 最近看過的 seqId，用來做去重。
//   - needRefresh: 是否需要重新拉 snapshot（例如 seq 斷層或 prev<0）。
//
// 契約 / 限制:
//   - 外層需保證 applySnapshot / applyUpdate 的呼叫序。
//   - snapshot 返回的 bids/asks 預期是已排序且不為負量。
//
// 備註:
//   - 目前只支援 OKX 的 seqId/prevSeqId 規則。
type OrderBook struct {
	Symbol     string
	SeqID      uint64
	Stale      bool
	Bids       sideMap
	Asks       sideMap
	UpdatedAt  time.Time
	lastFullAt time.Time

	// 去重/刷新控制
	lastSeqSeen uint64
	needRefresh bool
}

// NewOrderBook 建立一個新的 OrderBook 實例。
//
// 功能:
//   - 初始化 Bids/Asks map，設定 Symbol。
//   - 其他欄位使用零值。
//
// 參數:
//   - symbol: canonical symbol 字串。
//
// 回傳:
//   - *OrderBook: 初始化完成的 OrderBook 指標。
//   - 無 error。
//
// 備註:
//   - 呼叫端負責後續套用 snapshot 或 update。
func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		Symbol: symbol,
		Bids:   make(sideMap),
		Asks:   make(sideMap),
	}
}

// applySnapshot 用”整簿快照“覆寫現有 OrderBook。
//
// 功能:
//   - 清空原有 Bids/Asks，再依傳入的 bids/asks 重建簿。
//   - 忽略 QtyE9 <= 0 的價位（視為刪除）。
//   - 更新 SeqID / lastSeqSeen / Stale / needRefresh / UpdatedAt。
//
// 參數:
//   - bids: []*OKXBookLevel，snapshot 的買盤列表。
//   - asks: []*OKXBookLevel，snapshot 的賣盤列表。
//   - seq:  此 snapshot 對應的 seqId。
//
// 回傳:
//   - 無。
//   - 不回傳 error；呼叫端一律視為「狀態已重設」。
//
// 備註:
//   - 一般在收到 snapshot action 或 refresh 後重拉時使用。
func (ob *OrderBook) applySnapshot(bids, asks []*marketstreamv1.OKXBookLevel, seq uint64) {
	ob.Bids = make(sideMap)
	ob.Asks = make(sideMap)
	for _, l := range bids {
		if l.QtyE9 > 0 {
			ob.Bids[l.PxE9] = l.QtyE9
		}
	}
	for _, l := range asks {
		if l.QtyE9 > 0 {
			ob.Asks[l.PxE9] = l.QtyE9
		}
	}
	ob.SeqID = seq
	ob.lastSeqSeen = seq
	ob.Stale = false
	ob.needRefresh = false
	ob.UpdatedAt = time.Now()
}

// applyUpdate 對現有 OrderBook 套用簿增量。
//
// 功能:
//   - 檢查是否已經 Stale/needRefresh，是的話直接拒絕更新。
//   - 檢查 prev：
//   - 若 prev < 0 代表 OKX 通道重置，回傳 false 並標記 needRefresh。
//   - 若 prev != ob.SeqID 代表序列不連續，同樣標記 needRefresh。
//   - 檢查 seq：若 seq <= lastSeqSeen 視為重複或倒退，直接丟棄。
//   - 對 bids/asks 增量套用：QtyE9 <= 0 刪除價位，>0 則覆寫。
//   - 更新 SeqID / lastSeqSeen / UpdatedAt。
//
// 參數:
//   - bids: []*OKXBookLevel，增量買盤。
//   - asks: []*OKXBookLevel，增量賣盤。
//   - prev: 來自 vendor 的 prevSeqId。
//   - seq:  來自 vendor 的 seqId。
//
// 回傳:
//   - bool: true 代表更新成功；false 代表拒絕更新（需要 refresh 或資料已過期）。
//
// 備註:
//   - 回傳 false 時不一定代表錯誤，可能只是「這筆比現有 state 舊」。
func (ob *OrderBook) applyUpdate(
	bids, asks []*marketstreamv1.OKXBookLevel,
	prev int64,
	seq uint64,
) bool {
	if ob.Stale || ob.needRefresh {
		return false
	}
	// OKX：prev=-1 代表通道重置/首次推送，必須重拉 snapshot
	if prev < 0 {
		ob.Stale = true
		ob.needRefresh = true
		return false
	}
	// 連續性檢查
	if uint64(prev) != ob.SeqID {
		ob.Stale = true
		ob.needRefresh = true
		return false
	}
	// 去重（丟掉舊/重複）
	if seq <= ob.lastSeqSeen {
		return false
	}

	for _, l := range bids {
		if l.QtyE9 <= 0 {
			delete(ob.Bids, l.PxE9)
		} else {
			ob.Bids[l.PxE9] = l.QtyE9
		}
	}
	for _, l := range asks {
		if l.QtyE9 <= 0 {
			delete(ob.Asks, l.PxE9)
		} else {
			ob.Asks[l.PxE9] = l.QtyE9
		}
	}
	ob.SeqID = seq
	ob.lastSeqSeen = seq
	ob.UpdatedAt = time.Now()
	return true
}

// snapshot 回傳排序好的簿快照；depth>0 時僅取前 depth 檔。
//
// 功能:
//   - 將 Bids 由高價到低價排序，Asks 由低價到高價排序。
//   - 依 depth 決定取多少檔，depth<=0 代表取全部。
//   - 將 sideMap 轉成 []*OKXBookLevel 結構供外部使用。
//
// 參數:
//   - depth:  要取的檔數上限；>0 時啟用截斷；<=0 代表不截斷。
//
// 回傳:
//   - bids: []*OKXBookLevel，排序好的買盤列表。
//   - asks: []*OKXBookLevel，排序好的賣盤列表。
//   - 無 error。
//
// 備註:
//   - 只回傳 QtyE9 > 0 的價位；非正數價位在更新時即會被移除。
func (ob *OrderBook) snapshot(depth int) (bids, asks []*marketstreamv1.OKXBookLevel) {
	// bids
	if len(ob.Bids) > 0 {
		bp := make([]int64, 0, len(ob.Bids))
		for p := range ob.Bids {
			bp = append(bp, p)
		}
		sort.Slice(bp, func(i, j int) bool { return bp[i] > bp[j] })
		if depth > 0 && len(bp) > depth {
			bp = bp[:depth]
		}
		for _, p := range bp {
			if q := ob.Bids[p]; q > 0 {
				bids = append(bids, &marketstreamv1.OKXBookLevel{PxE9: p, QtyE9: q})
			}
		}
	}
	// asks
	if len(ob.Asks) > 0 {
		ap := make([]int64, 0, len(ob.Asks))
		for p := range ob.Asks {
			ap = append(ap, p)
		}
		sort.Slice(ap, func(i, j int) bool { return ap[i] < ap[j] })
		if depth > 0 && len(ap) > depth {
			ap = ap[:depth]
		}
		for _, p := range ap {
			if q := ob.Asks[p]; q > 0 {
				asks = append(asks, &marketstreamv1.OKXBookLevel{PxE9: p, QtyE9: q})
			}
		}
	}
	return
}
