package book_group

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
	marketcommonv1 "dorma_system/schemas/gen/go/market_common_v1"
)

var crcOnce sync.Once


const (
     subjDeltaWildcard = "CLEAN.OKX.BOOK.DELTA.>" // 逐標的 DELTA 全吃
 )

// --- subject builder helpers ---

// BTC-USDT-SWAP / BTC-USDT-SPOT / BTC-USDT → (BTC, USDT, SUF)
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

// 組 book FULL 的逐標的 subject：CLEAN.OKX.BOOK.<BASE>.<QUOTE>.<SUF>
func cleanBookFullSubject(canonical string) string {
    base, quote, suf := splitCanonical(canonical)
    if suf == "" { suf = "SPOT" } // 保底
    return "CLEAN.OKX.BOOK.FULL." + base + "." + quote + "." + suf
}

func (c *Chief) handleDelta(m *nats.Msg) error {
	// Envelope 來自 common（body 使用 Any）
	var env marketcommonv1.Envelope
	if err := proto.Unmarshal(m.Data, &env); err != nil {
		return err
	}

	// Any → 具體 OKXBooksBody；若不是該型別則直接忽略
	if env.GetBody() == nil {
		return nil
	}
	var body marketstreamv1.OKXBooksBody
	if err := anypb.UnmarshalTo(env.GetBody(), &body, proto.UnmarshalOptions{}); err != nil {
		return nil // 非書本更新，略過（需要可改為 log）
	}

	key := env.GetSymbol()

	// 找/建簿
	c.mu.Lock()
	ob, ok := c.books[key]
	if !ok {
		ob = NewOrderBook(key)
		c.books[key] = ob
	}
	c.mu.Unlock()

	switch body.GetAction() {
	case marketstreamv1.Action_ACTION_SNAPSHOT:
		// 更新整簿
		ob.applySnapshot(body.GetBids(), body.GetAsks(), body.GetSeqId())

		// 依「合併後簿」取前 25 檔交錯驗 CRC（與官方描述一致）
		if c.verifyCRC {
			b25, a25 := ob.snapshot(25)
			got := calcOKXChecksumE9(b25, a25)
			want := body.GetChecksum()
			if want != 0 && got != want {
				if !ob.needRefresh {
					c.logf("[book_group] refresh needed: symbol=%s reason=snapshot checksum mismatch", key)
					ob.Stale = true
					ob.needRefresh = true
				}
				dumpCRCOnce(c, key, b25, a25, got, want)
				return nil
			}
		}

		ob.Stale = false
		ob.needRefresh = false
		ob.lastSeqSeen = body.GetSeqId()
		return c.publishFull(&env, ob)

	case marketstreamv1.Action_ACTION_UPDATE:
		if ob.needRefresh {
			return nil
		}
		// 去重
		if body.GetSeqId() <= ob.lastSeqSeen {
			return nil
		}
		// 嚴格序列 / prev=-1
		if ok := ob.applyUpdate(body.GetBids(), body.GetAsks(), body.GetPrevSeqId(), body.GetSeqId()); !ok {
			if !ob.needRefresh {
				c.logf("[book_group] refresh needed: symbol=%s reason=sequence gap or prev<0", key)
				ob.Stale = true
				ob.needRefresh = true
			}
			return nil
		}

		// 增量後再驗一次
		if c.verifyCRC {
			b25, a25 := ob.snapshot(25)
			got := calcOKXChecksumE9(b25, a25)
			want := body.GetChecksum()
			if want != 0 && got != want {
				if !ob.needRefresh {
					c.logf("[book_group] refresh needed: symbol=%s reason=update checksum mismatch", key)
					ob.Stale = true
					ob.needRefresh = true
				}
				return nil
			}
		}

		// 節流（0 表示每筆都發）
		if c.throttle > 0 && time.Since(ob.lastFullAt) < c.throttle {
			return nil
		}
		return c.publishFull(&env, ob)
	}
	return nil
}

// data_dpt/kols/hf_market_data_kol/book_group/handle.go（或你放 publishFull 的檔案）
// [片段：publishFull 內覆寫 feed 為 BOOK.FULL，並用覆寫後的 feed 計算 message_id]

func (c *Chief) publishFull(src *marketcommonv1.Envelope, ob *OrderBook) error {
	bids, asks := ob.snapshot(0)
	b25, a25 := ob.snapshot(25)
	cs := calcOKXChecksumE9(b25, a25)

	concrete := &marketstreamv1.OKXBooksBody{
		Version:   1,
		Action:    marketstreamv1.Action_ACTION_SNAPSHOT,
		Bids:      bids,
		Asks:      asks,
		Checksum:  cs,
		PrevSeqId: 0,
		SeqId:     ob.SeqID,
	}
	anyBody, err := anypb.New(concrete)
	if err != nil {
		return err
	}

	out := &marketcommonv1.Envelope{
		Version:    src.GetVersion(),
		Source:     src.GetSource(), // 先複用來源，再覆寫 feed
		Symbol:     src.GetSymbol(),
		MarketType: src.GetMarketType(),
		Timestamps: &marketcommonv1.Timestamps{
			EventTsUs:     src.GetTimestamps().GetEventTsUs(),
			CollectRecvUs: src.GetTimestamps().GetCollectRecvUs(),
			CollectPubUs:  src.GetTimestamps().GetCollectPubUs(),
			RefinerRecvUs: src.GetTimestamps().GetRefinerRecvUs(),
			RefinerPubUs:  src.GetTimestamps().GetRefinerPubUs(),
		},
		Body: anyBody,
	}

	// ---- 新增：覆寫 feed 為 BOOK.FULL（避免下游只靠 subject 判斷）----
	if out.Source == nil {
		out.Source = &marketcommonv1.Source{}
	}
	out.Source.Feed = "BOOK.FULL"
	// ------------------------------------------------------------

	// 用「覆寫後的 feed」計算 message_id
	bodyBytes, _ := proto.Marshal(concrete)
	msgID := makeMsgID(
		out.GetSource().GetExchange().String(),
		out.GetSource().GetFeed(),
		bodyBytes,
	)
	out.MessageId = msgID

	pb, _ := proto.Marshal(out)
	subject := cleanBookFullSubject(src.GetSymbol())
	if err := c.nc.Publish(subject, pb); err != nil {
		return err
	}
	ob.lastFullAt = time.Now()
	return nil
}

// 交錯（bid, ask, bid, ask…），各自最多 25 檔，字串用 decE9ToString
func calcOKXChecksumE9(bids, asks []*marketstreamv1.OKXBookLevel) int32 {
	nb, na := len(bids), len(asks)
	if nb > 25 {
		nb = 25
	}
	if na > 25 {
		na = 25
	}

	i, j := 0, 0
	var sb strings.Builder
	for i < nb || j < na {
		if i < nb {
			if sb.Len() > 0 {
				sb.WriteByte(':')
			}
			sb.WriteString(decE9ToString(bids[i].PxE9))
			sb.WriteByte(':')
			sb.WriteString(decE9ToString(bids[i].QtyE9))
			i++
		}
		if j < na {
			if sb.Len() > 0 {
				sb.WriteByte(':')
			}
			sb.WriteString(decE9ToString(asks[j].PxE9))
			sb.WriteByte(':')
			sb.WriteString(decE9ToString(asks[j].QtyE9))
			j++
		}
	}
	return int32(crc32.ChecksumIEEE([]byte(sb.String())))
}

// 十進位輸出（去右側 0；整數不帶小數點）
func decE9ToString(v int64) string {
	neg := v < 0
	if neg {
		v = -v
	}
	intPart := v / 1_000_000_000
	frac := v % 1_000_000_000
	if frac == 0 {
		if neg {
			return "-" + itoa(intPart)
		}
		return itoa(intPart)
	}
	s := itoa(frac + 1_000_000_000)[1:] // 固定9位
	s = strings.TrimRight(s, "0")
	if neg {
		return "-" + itoa(intPart) + "." + s
	}
	return itoa(intPart) + "." + s
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// 一次性把交錯字串吐出來協助對拍（可保留）
func dumpCRCOnce(c *Chief, symbol string, bids, asks []*marketstreamv1.OKXBookLevel, got, want int32) {
	crcOnce.Do(func() {
		nb, na := len(bids), len(asks)
		if nb > 25 {
			nb = 25
		}
		if na > 25 {
			na = 25
		}
		i, j := 0, 0
		var sb strings.Builder
		for i < nb || j < na {
			if i < nb {
				if sb.Len() > 0 {
					sb.WriteByte(':')
				}
				sb.WriteString(decE9ToString(bids[i].PxE9))
				sb.WriteByte(':')
				sb.WriteString(decE9ToString(bids[i].QtyE9))
				i++
			}
			if j < na {
				if sb.Len() > 0 {
					sb.WriteByte(':')
				}
				sb.WriteString(decE9ToString(asks[j].PxE9))
				sb.WriteByte(':')
				sb.WriteString(decE9ToString(asks[j].QtyE9))
				j++
			}
		}
		c.logf("[book_group] CRC DEBUG symbol=%s got=%d want=%d str=%q", symbol, got, want, sb.String())
	})
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