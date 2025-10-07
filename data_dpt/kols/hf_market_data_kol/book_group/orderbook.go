package book_group

import (
	"sort"
	"time"

	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
)

type sideMap map[int64]int64 // pxE9 -> qtyE9

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

func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		Symbol: symbol,
		Bids:   make(sideMap),
		Asks:   make(sideMap),
	}
}

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

// 回傳排序簿；depth>0 只取前 depth 檔（bids 高→低；asks 低→高）
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