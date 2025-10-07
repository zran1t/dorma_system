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

	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
)

var crcOnce sync.Once

const (
	subjDelta = "CLEAN.OKX.BOOK.DELTA"
	subjFull  = "CLEAN.OKX.BOOK.FULL"
)

func (c *Chief) handleDelta(m *nats.Msg) error {
	var env marketstreamv1.Envelope
	if err := proto.Unmarshal(m.Data, &env); err != nil {
		return err
	}
	body := env.GetBook()
	if body == nil {
		return nil
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

func (c *Chief) publishFull(src *marketstreamv1.Envelope, ob *OrderBook) error {
	bids, asks := ob.snapshot(0)
	b25, a25 := ob.snapshot(25)
	cs := calcOKXChecksumE9(b25, a25)

	out := &marketstreamv1.Envelope{
		Version:    src.GetVersion(),
		Source:     src.GetSource(),
		Symbol:     src.GetSymbol(),
		MarketType: src.GetMarketType(),
		Timestamps: &marketstreamv1.Timestamps{
			EventTsUs:     src.GetTimestamps().GetEventTsUs(),
			CollectRecvUs: src.GetTimestamps().GetCollectRecvUs(),
			CollectPubUs:  src.GetTimestamps().GetCollectPubUs(),
			RefinerRecvUs: src.GetTimestamps().GetRefinerRecvUs(),
			RefinerPubUs:  src.GetTimestamps().GetRefinerPubUs(),
		},
		Body: &marketstreamv1.Envelope_Book{
			Book: &marketstreamv1.OKXBooksBody{
				Version:   1,
				Action:    marketstreamv1.Action_ACTION_SNAPSHOT,
				Bids:      bids,
				Asks:      asks,
				Checksum:  cs,
				PrevSeqId: 0,
				SeqId:     ob.SeqID,
			},
		},
	}

	// message_id
	bodyBytes, _ := proto.Marshal(out.GetBook())
	msgID := makeMsgID(src.GetSource().GetExchange().String(), src.GetSource().GetFeed().String(), bodyBytes)
	out.MessageId = msgID

	pb, _ := proto.Marshal(out)
	if err := c.nc.Publish(subjFull, pb); err != nil {
		return err
	}
	ob.lastFullAt = time.Now()
	return nil
}

// 交錯（bid, ask, bid, ask…），各自最多 25 檔，字串用 decE9ToString
func calcOKXChecksumE9(bids, asks []*marketstreamv1.OKXBookLevel) int32 {
	nb, na := len(bids), len(asks)
	if nb > 25 { nb = 25 }
	if na > 25 { na = 25 }

	i, j := 0, 0
	var sb strings.Builder
	for i < nb || j < na {
		if i < nb {
			if sb.Len() > 0 { sb.WriteByte(':') }
			sb.WriteString(decE9ToString(bids[i].PxE9))
			sb.WriteByte(':')
			sb.WriteString(decE9ToString(bids[i].QtyE9))
			i++
		}
		if j < na {
			if sb.Len() > 0 { sb.WriteByte(':') }
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
	if neg { v = -v }
	intPart := v / 1_000_000_000
	frac := v % 1_000_000_000
	if frac == 0 {
		if neg { return "-" + itoa(intPart) }
		return itoa(intPart)
	}
	s := itoa(frac + 1_000_000_000)[1:] // 固定9位
	s = strings.TrimRight(s, "0")
	if neg { return "-" + itoa(intPart) + "." + s }
	return itoa(intPart) + "." + s
}

func itoa(v int64) string {
	if v == 0 { return "0" }
	neg := v < 0
	if neg { v = -v }
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
		if nb > 25 { nb = 25 }
		if na > 25 { na = 25 }
		i, j := 0, 0
		var sb strings.Builder
		for i < nb || j < na {
			if i < nb {
				if sb.Len() > 0 { sb.WriteByte(':') }
				sb.WriteString(decE9ToString(bids[i].PxE9))
				sb.WriteByte(':')
				sb.WriteString(decE9ToString(bids[i].QtyE9))
				i++
			}
			if j < na {
				if sb.Len() > 0 { sb.WriteByte(':') }
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