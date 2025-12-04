// File: data_dpt/kols/hf_market_data_kol/book_group/handle.go
// Package: book_group
//
// 職責 (Responsibility):
//     - 接收 CLEAN.OKX.BOOK.DELTA.* 流，更新 OrderBook 狀態。
//     - 依序列/CRC 判斷是否需要 refresh 或可以正常推進。
//     - 視 throttle 決定何時發出 CLEAN.OKX.BOOK.FULL.* snapshot。
//     - 負責 BOOK.FULL 的 subject / Envelope 組裝與 message_id 生成。
//
// 注意事項 (Notes):
//     - 僅支援 OKX OKXBooksBody；其他 body 會被靜默略過。
//     - CRC mismatch 只會標記 needRefresh，不會主動發出任何 control 訊息。

package book_group

import (
	// === 標準函式庫 (Standard Library) ===
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"strings"
	"sync"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	"github.com/nats-io/nats.go"
	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	// === 系統內模組 (Internal Modules) ===
	marketcommonv1 "dorma_system/schemas/gen/go/market/common/v1"
	marketstreamv1 "dorma_system/schemas/gen/go/market/stream/v1"
)

// crcOnce 確保 dumpCRCOnce 的 debug log 最多只印一次。
//
// 功能:
//   - 作為 sync.Once，用來保護 dumpCRCOnce 只跑一次。
//
// 欄位說明:
//   - 無（全域變數）。
//
// 契約 / 限制:
//   - 只在 CRC mismatch debug 場景使用。
//
// 備註:
//   - 避免大量輸出交錯字串影響 log 可讀性。
var crcOnce sync.Once

// subjDeltaWildcard 是 BOOK.DELTA 的訂閱 pattern。
//
// 功能:
//   - 匹配所有 OKX 的簿增量主題：CLEAN.OKX.BOOK.DELTA.<BASE>.<QUOTE>.<SUF>。
//
// 契約 / 限制:
//   - 目前寫死為 OKX，未來若要支援其他交易所需要拆開。
//
// 備註:
//   - Chief.Start() 會用這個 pattern 來訂閱。
const (
	subjDeltaWildcard = "CLEAN.OKX.BOOK.DELTA.>" // 逐標的 DELTA 全吃
)

// --- subject builder helpers ---

// splitCanonical 將 canonical symbol（BTC-USDT-SWAP / BTC-USDT-SPOT / BTC-USDT）拆成三段。
//
// 功能:
//   - 把如 "BTC-USDT-SWAP" 拆成 (BTC, USDT, SWAP)。
//   - 若只有兩段，尾綴回傳空字串。
//   - 若格式不規則，base 直接回原字串，quote/suf 為空。
//
// 參數:
//   - c: canonical symbol 字串。
//
// 回傳:
//   - base:  基礎貨幣（大寫）。
//   - quote: 報價貨幣（大寫）。
//   - suf:   尾綴（SWAP / SPOT / INDEX / ""）。
//
// 備註:
//   - 僅用於組 BOOK.FULL subject，不作嚴格驗證。
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

// cleanBookFullSubject 組 BOOK.FULL 的逐標的 subject：CLEAN.OKX.BOOK.FULL.<BASE>.<QUOTE>.<SUF>。
//
// 功能:
//   - 用 canonical symbol 拆出 base/quote/suf，產出 BOOK.FULL subject。
//   - 若 suffix 為空，預設 SPOT（保底）。
//
// 參數:
//   - canonical: canonical symbol 字串。
//
// 回傳:
//   - string: BOOK.FULL subject。
//
// 備註:
//   - 目前寫死為 OKX；未來支援其他交易所時可能需要一層 exchange 參數。
func cleanBookFullSubject(canonical string) string {
	base, quote, suf := splitCanonical(canonical)
	if suf == "" {
		suf = "SPOT" // 保底
	}
	return "CLEAN.OKX.BOOK.FULL." + base + "." + quote + "." + suf
}

// handleDelta 處理一筆 BOOK.DELTA 訊息。
//
// 功能:
//   - 反序列化 Envelope，抽出 OKXBooksBody。
//   - 依 Action (SNAPSHOT/UPDATE) 更新對應的 OrderBook。
//   - 做序列/CRC 驗證；若出問題則標註 needRefresh。
//   - 視 throttle 決定是否要呼叫 publishFull 輸出 BOOK.FULL snapshot。
//
// 參數:
//   - m: 來自 NATS 的訊息，預期 subject 為 CLEAN.OKX.BOOK.DELTA.*。
//
// 回傳:
//   - error: proto.Unmarshal 失敗或 publishFull 失敗時回傳；正常邏輯多半是 nil。
//
// 備註:
//   - 非 OKXBooksBody 會被靜默略過（忽略 error），避免影響 pipeline。
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
		// 去重：seqId 不得倒退或重複
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

		// 增量後再驗一次 CRC
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

// publishFull 將 OrderBook 當前狀態輸出成 BOOK.FULL snapshot。
//
// 功能:
//   - 對 OrderBook 做 snapshot，取得完整簿與前 25 檔。
//   - 計算 OKX 標準 CRC checksum。
//   - 組出 OKXBooksBody (SNAPSHOT) + Envelope，並覆寫 feed=BOOK.FULL。
//   - 透過 NATS 發佈到 CLEAN.OKX.BOOK.FULL.<BASE>.<QUOTE>.<SUF>。
//   - 更新 OrderBook.lastFullAt。
//
// 參數:
//   - src:  原始 delta Envelope（用來繼承 timestamps / source / symbol 等資訊）。
//   - ob:   要輸出 snapshot 的 OrderBook。
//
// 回傳:
//   - error: 任何序列化或 NATS 發佈錯誤時回傳；成功時為 nil。
//
// 備註:
//   - 這裡重新計算 checksum，與 OKX 官方規則一致（交錯前 25 檔 bid/ask）。
func (c *Chief) publishFull(src *marketcommonv1.Envelope, ob *OrderBook) error {
	bids, asks := ob.snapshot(0)
	b25, a25 := ob.snapshot(25)
	cs := calcOKXChecksumE9(b25, a25)

	concrete := &marketstreamv1.OKXBooksBody{
		SchemaVersion: "",
		Action:        marketstreamv1.Action_ACTION_SNAPSHOT,
		Bids:          bids,
		Asks:          asks,
		Checksum:      cs,
		PrevSeqId:     0,
		SeqId:         ob.SeqID,
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

	// 覆寫 feed 為 BOOK.FULL（避免下游只靠 subject 判斷）
	if out.Source == nil {
		out.Source = &marketcommonv1.Source{}
	}
	out.Source.Feed = "BOOK.FULL"

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

// calcOKXChecksumE9 依 OKX 規則計算交錯簿的 CRC32 checksum。
//
// 功能:
//   - 對 bids/asks 各取最多 25 檔，交錯成字串：bidPx:bidQty:askPx:askQty:...
//   - 價格與數量使用 decE9ToString 還原十進位表示。
//   - 使用 crc32.ChecksumIEEE 計算 checksum，並轉成 int32。
//
// 參數:
//   - bids: []*OKXBookLevel，已排序的買盤列表。
//   - asks: []*OKXBookLevel，已排序的賣盤列表。
//
// 回傳:
//   - int32: 對應 OKX checksum 欄位的值。
//   - 無 error。
//
// 備註:
//   - 若 bids/asks 長度不足 25 檔，直接以實際長度為準。
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

// decE9ToString 將 1e9 固定小數的 int64 還原成十進位字串。
//
// 功能:
//   - 把例如 123456000000000000000 轉成 "123.456" 這種形式。
//   - 去掉小數部分右側多餘的 0；整數時不帶小數點。
//   - 支援負值。
//
// 參數:
//   - v: 以 1e9 為單位的整數值。
//
// 回傳:
//   - string: 還原後的十進位文字表示。
//   - 無 error。
//
// 備註:
//   - 用來與 OKX 官方 checksum 規則對齊。
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

// itoa 將 int64 轉成十進位字串（不依賴 strconv）。
//
// 功能:
//   - 手寫版本的整數轉字串，避免額外依賴，且對 performance 可預期。
//
// 參數:
//   - v: 來源 int64 值。
//
// 回傳:
//   - string: 十進位字串。
//   - 無 error。
//
// 備註:
//   - 只在 decE9ToString 裡使用。
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

// dumpCRCOnce 一次性輸出交錯簿字串與 checksum 對拍資訊。
//
// 功能:
//   - 將前 25 檔交錯字串與 got/want checksum log 出來，協助對拍 OKX checksum。
//   - 利用 crcOnce 確保僅會輸出一次，避免 log 淹水。
//
// 參數:
//   - c:      Chief 實例，用來呼叫 logf。
//   - symbol: canonical symbol。
//   - bids:   買盤列表。
//   - asks:   賣盤列表。
//   - got:    本地計算出的 checksum。
//   - want:   vendor 提供的 checksum。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 若後續需要多次輸出，可考慮拿掉 sync.Once 或改成有條件重置。
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

// makeMsgID 使用 xxhash128 生成 message_id: hash(len|exchange | len|feed | len|body)。
//
// 功能:
//   - 依序將 exchange / feed / body 的長度與內容寫入 buffer 後，做 xxh3.Hash128。
//   - 將結果拆成 16 byte（BigEndian）：前 8 byte = Lo, 後 8 byte = Hi。
//   - 用於封裝在 Envelope.MessageId，給下游做去重或追蹤。
//
// 參數:
//   - exchange: exchange 字串，例如 "EXCHANGE_OKX"。
//   - feed:     feed 名稱，例如 "BOOK.FULL"。
//   - body:     已序列化完的 body bytes。
//
// 回傳:
//   - []byte: 16-byte 的 message id。
//   - 無 error。
//
// 備註:
//   - 與 refine_group/okx 裡的 makeMsgID 策略保持一致，但互相獨立實作。
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
