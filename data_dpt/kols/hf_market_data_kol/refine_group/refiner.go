// File: data_dpt/kols/hf_market_data_kol/refine_group/refiner.go
// Package: refine_group
//
// 職責 (Responsibility):
//     將 RAW.* Envelope 流，透過各 ExchangeAdapter 的 handler 精煉成 CLEAN.*：
//       - 訂閱 RAW subject
//       - 依 Exchange/Feed 找對 handler
//       - 做去重 (seqId/tradeId based) 與 TTL 管理
//       - 最後把 CLEAN Envelope 發佈出去
//
// 注意事項 (Notes):
//     - 去重機制放在 engine 層（Refiner），避免每個 adapter 自己實作一套。
//     - Run 會阻塞直到 context 結束，呼叫端要用 goroutine 或專用 worker 跑。

package refine_group

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	// === 系統內模組 (Internal Modules) ===
	"dorma_system/infra/symbols"
	marketcommonv1 "dorma_system/schemas/gen/go/market/common/v1"
	marketstreamv1 "dorma_system/schemas/gen/go/market/stream/v1"
)

// Refiner 是整條 RAW → CLEAN 的「引擎」。
//
// 功能:
//   - 持有 NATS 連線與 symbol resolver。
//   - 登記各 ExchangeAdapter 的 handler：handlers[Exchange][feed]。
//   - 負責訂閱 RAW subjects，逐封處理並呼叫對應 handler。
//   - 做基於 seqId / tradeId 的去重、TTL 清理與容量控制。
//   - 發佈處理後的 CLEAN Envelope 到指定 subject。
//
// 欄位說明:
//   - nc:       NATS 連線，用於訂閱 RAW 以及發佈 CLEAN。
//   - resolver: symbol 解析器，轉給 handler 使用。
//   - handlers: 第一層 key = Exchange 列舉，第二層 key = feed 字串。
//   - mu:       去重狀態的 mutex，保護 state map。
//   - state:    per-key 去重資料，key 形式為 "ex|feed|symbol"。
//   - ttl:      去重記錄保留多久；過期會清掉。
//   - capacity: 每個 key 最多保留多少 seqId/tradeId 記錄。
//
// 契約 / 限制:
//   - handler 需遵守「輸入 Envelope / Resolver，回傳 []Out」的介面。
//   - 去重是 best-effort，極端情況下仍可能有少數重複或漏掉（LL 選擇）。
//
// 備註:
//   - 若未來有更多 exchange，可以直接呼叫 RegisterAdapter 掛進來。
type Refiner struct {
	nc       *nats.Conn
	resolver symbols.Resolver

	// handlers: 第一層 key = Exchange（列舉），第二層 key = feed（字串）
	handlers map[marketcommonv1.Exchange]map[string]Handler

	// 去重狀態（放在 engine，不放在各 adapter）
	mu       sync.Mutex
	state    map[string]*dedupState // key = ex|feed|symbol
	ttl      time.Duration          // 記憶多久
	capacity int                    // 每個 key 最多記 N 筆
}

// RefinerOption 用來客製 Refiner 行為（目前只有去重相關）。
//
// 功能:
//   - 讓呼叫端可以用 functional option 的方式調整 ttl / capacity 等參數。
//
// 欄位說明:
//   - 無（函式型別）。
//
// 契約 / 限制:
//   - 只在 NewRefiner 時生效，之後不會再被呼叫。
//
// 備註:
//   - 之後如有新的可調參數，也可以繼續沿用這種型別。
type RefinerOption func(*Refiner)

// WithDedup 調整去重 TTL 與容量。
//
// 功能:
//   - 提供一個 option 讓呼叫端可以覆寫預設的 ttl / capacity。
//
// 參數:
//   - ttl:      去重記錄保留時間，>0 時才會覆寫預設值。
//   - capacity: 每個 key 最大記憶數量，>0 時才會覆寫預設值。
//
// 回傳:
//   - RefinerOption: 可被 NewRefiner 吃進去的 option 函式。
//   - 無 error。
//
// 備註:
//   - 建議 ttl 控制在數分鐘等級，避免 state 膨脹。
func WithDedup(ttl time.Duration, capacity int) RefinerOption {
	return func(r *Refiner) {
		if ttl > 0 {
			r.ttl = ttl
		}
		if capacity > 0 {
			r.capacity = capacity
		}
	}
}

// NewRefiner 建立一個 Refiner 實例（預設 TTL=2 分鐘、capacity=4096）。
//
// 功能:
//   - 設定 NATS 連線與 symbol resolver。
//   - 建立 handlers map 與去重 state map。
//   - 套用所有 RefinerOption，覆寫預設參數。
//
// 參數:
//   - nc:       NATS 連線，用於訂閱 RAW / 發佈 CLEAN。
//   - resolver: symbol 解析器實作。
//   - opts:     可選的 RefinerOption 列表。
//
// 回傳:
//   - *Refiner: 初始化完成的 Refiner 實例。
//   - 無 error。
//
// 備註:
//   - NewRefiner 本身不會註冊任何 adapter，需另外呼叫 RegisterAdapter。
func NewRefiner(nc *nats.Conn, resolver symbols.Resolver, opts ...RefinerOption) *Refiner {
	r := &Refiner{
		nc:       nc,
		resolver: resolver,
		handlers: make(map[marketcommonv1.Exchange]map[string]Handler),
		state:    make(map[string]*dedupState),
		ttl:      2 * time.Minute,
		capacity: 4096,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// RegisterAdapter 把一個 ExchangeAdapter 底下所有 feed handler 掛進來。
//
// 功能:
//   - 透過 ad.Exchange() 取得 exchange key。
//   - 透過 ad.Handlers() 取得 feed → handler map，全部註冊進 handlers。
//
// 參數:
//   - ad: ExchangeAdapter 實作，例如 okx.adapter。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 同一個 feed 重複註冊會被覆寫，後者優先。
func (r *Refiner) RegisterAdapter(ad ExchangeAdapter) {
	ex := ad.Exchange()
	if r.handlers[ex] == nil {
		r.handlers[ex] = make(map[string]Handler)
	}
	for f, h := range ad.Handlers() {
		r.handlers[ex][f] = h
	}
}

// Run 訂閱指定 subjects，並阻塞直到 ctx 結束。
//
// 功能:
//   - 對每個 subject 呼叫 nc.Subscribe，收到訊息時交給 handleMsg 處理。
//   - 訂閱完成後 Flush 一次，讓錯誤早點浮出。
//   - 之後直接阻塞在 <-ctx.Done()，直到上層要求停止。
//
// 參數:
//   - ctx:      控制整個 Refiner 生命週期的 context。
//   - subjects: 要訂閱的 RAW subject pattern 列表。
//
// 回傳:
//   - error: 若 Subscribe 或 ctx 結束時回傳錯誤；正常結束時可能是 context.Canceled 等。
//
// 備註:
//   - 若 subjects 為空，會 log 一行警告並單純等 ctx.Done()。
func (r *Refiner) Run(ctx context.Context, subjects ...string) error {
	if len(subjects) == 0 {
		log.Println("[refiner] 未提供主題; 無法訂閱")
		<-ctx.Done()
		return ctx.Err()
	}
	for _, subj := range subjects {
		if _, err := r.nc.Subscribe(subj, func(m *nats.Msg) {
			if err := r.handleMsg(m); err != nil {
				log.Printf("[refiner] subj=%s err=%v", m.Subject, err)
			}
		}); err != nil {
			return err
		}
	}
	_ = r.nc.Flush()
	<-ctx.Done()
	return ctx.Err()
}

// ────────────────────────── 去重狀態 ──────────────────────────

// dedupState 為單一 key(ex|feed|symbol) 的去重資料。
//
// 功能:
//   - 記住看過的 seqId / tradeId，以及最後一次看到的時間（秒）。
//
// 欄位說明:
//   - seqSeen: seqId → lastSeen(seconds)。
//   - idSeen:  tradeId → lastSeen(seconds)。
//
// 契約 / 限制:
//   - 外部應透過 shouldPass / gcBucket 操作，不直接存取。
//
// 備註:
//   - 存成 int64 秒是為了省空間與計算量。
type dedupState struct {
	seqSeen map[uint64]int64 // seqId -> lastSeen(sec)
	idSeen  map[string]int64 // tradeId -> lastSeen(sec)
}

// bucket 確保 key 對應的 dedupState 存在。
//
// 功能:
//   - 若 state 裡已經有對應 key，就直接回傳。
//   - 若沒有，就建立一個新的 dedupState 存進去後回傳。
//
// 參數:
//   - key: 去重用的 key（ex|feed|symbol）。
//
// 回傳:
//   - *dedupState: 該 key 對應的 dedupState 實例。
//   - 無 error。
//
// 備註:
//   - 會持有 r.mu lock 直到建好為止，外部不用再鎖。
func (r *Refiner) bucket(key string) *dedupState {
	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.state[key]; ok {
		return b
	}
	b := &dedupState{
		seqSeen: make(map[uint64]int64),
		idSeen:  make(map[string]int64),
	}
	r.state[key] = b
	return b
}

// trimMap 粗暴地把 map 修剪到 capacity 以下。
//
// 功能:
//   - 當 map 長度超過 capacity 時，隨機刪除一些 entry，直到剩下約 capacity/2。
//   - 用於避免去重 state 無限成長。
//
// 參數:
//   - m:        任意 key → int64 的 map。
//   - capacity: 上限大小。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 這裡不保證「先刪舊的」，只追求簡單與足夠。
func trimMap[K comparable](m map[K]int64, capacity int) {
	if len(m) <= capacity {
		return
	}
	target := capacity / 2
	for k := range m {
		delete(m, k)
		if len(m) <= target {
			break
		}
	}
}

// gcBucket 針對單一 dedupState 做 TTL + 容量控制。
//
// 功能:
//   - 刪除 lastSeen < expireAt 的 seqId / tradeId。
//   - 若仍超過容量上限，就呼叫 trimMap 做粗略修剪。
//
// 參數:
//   - b:      要清理的 dedupState。
//   - nowSec: 目前時間（秒）。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 預期由 shouldPass 內部定期呼叫。
func (r *Refiner) gcBucket(b *dedupState, nowSec int64) {
	expireAt := nowSec - int64(r.ttl.Seconds())

	for s, t := range b.seqSeen {
		if t < expireAt {
			delete(b.seqSeen, s)
		}
	}
	for id, t := range b.idSeen {
		if t < expireAt {
			delete(b.idSeen, id)
		}
	}

	trimMap(b.seqSeen, r.capacity)
	trimMap(b.idSeen, r.capacity)
}

// shouldPass 依 seqId / tradeId 判斷是否放行，並更新去重 state。
//
// 功能:
//   - 先做 TTL cleanup，再檢查 seqId / tradeId 是否看過：
//     1) 若有 seqId 且出現在 seqSeen，直接丟掉。
//     2) 若有 tradeId 且出現在 idSeen，直接丟掉。
//     3) 若兩者都沒有命中，則更新對應 entry，然後回傳 true。
//   - 讓上層在發佈前決定「這筆到底該不該出門」。
//
// 參數:
//   - key:     去重 key（ex|feed|symbol）。
//   - seqId:   交易序列號，0 代表沒有。
//   - tradeId: 交易 ID，空字串代表沒有。
//   - nowSec:  目前時間（秒）。
//
// 回傳:
//   - bool: true 代表這筆應該往下送；false 代表判定為重複。
//   - 無 error。
//
// 備註:
//   - 如果兩個 id 都存在，只要任何一個被視為重複就丟（偏保守）。
func (r *Refiner) shouldPass(key string, seqId uint64, tradeId string, nowSec int64) bool {
	b := r.bucket(key)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.gcBucket(b, nowSec)

	if seqId != 0 {
		if _, ok := b.seqSeen[seqId]; ok {
			return false
		}
	}
	if tradeId != "" {
		if _, ok := b.idSeen[tradeId]; ok {
			return false
		}
	}

	if seqId != 0 {
		b.seqSeen[seqId] = nowSec
	}
	if tradeId != "" {
		b.idSeen[tradeId] = nowSec
	}
	return true
}

// ────────────────────────── 消息處理 ──────────────────────────

// handleMsg：收 RAW → 找 handler → 拿 []Out → 去重 → 發佈 CLEAN。
//
// 功能:
//   - 將 NATS 訊息的 Data 反序列化成 Envelope。
//   - 根據 Envelope.Source.Exchange / Feed 找出對應 handler。
//   - 呼叫 handler(env, resolver) 拿到 []Out。
//   - 對每一筆 Out 做 timestamp 補齊 / 去重 / Publish。
//   - 出錯時寫 log，但不會中止整體流程（除非 Publish 回錯）。
//
// 參數:
//   - m: 來自 NATS 的 RAW 訊息。
//
// 回傳:
//   - error: Unmarshal 或 Publish 失敗時回傳錯誤；多數情況下為 nil。
//
// 備註:
//   - handler 回傳 0 筆或 nil 時會被視為無輸出，靜默略過。
func (r *Refiner) handleMsg(m *nats.Msg) error {
	nowUS := time.Now().UnixNano() / 1e3
	nowSec := nowUS / 1_000_000

	var env marketcommonv1.Envelope
	if err := proto.Unmarshal(m.Data, &env); err != nil {
		return err
	}
	if env.Timestamps == nil {
		env.Timestamps = &marketcommonv1.Timestamps{}
	}
	env.Timestamps.RefinerRecvUs = uint64(nowUS)

	src := env.GetSource()
	if src == nil {
		return nil
	}
	ex := src.GetExchange()
	fd := src.GetFeed()

	table := r.handlers[ex]
	if table == nil {
		return nil
	}
	h, ok := table[fd]
	if !ok || h == nil {
		return nil
	}

	outs, err := h(&env, r.resolver)
	if err != nil || len(outs) == 0 {
		return err
	}

	for _, o := range outs {
		if o.Msg == nil || o.Subject == "" {
			continue
		}
		out := o.Msg
		subj := o.Subject

		if out.Timestamps == nil {
			out.Timestamps = &marketcommonv1.Timestamps{}
		}
		if out.Timestamps.RefinerRecvUs == 0 {
			out.Timestamps.RefinerRecvUs = uint64(nowUS)
		}

		// key = ex|feed|symbol
		sym := out.GetSymbol()
		key := makeKey(ex, fd, sym)

		// 從 body 抽出去重 id（KLINE 類通常會回 0,""）
		seqId, tradeId := extractIds(out)
		if !r.shouldPass(key, seqId, tradeId, nowSec) {
			continue
		}

		out.Timestamps.RefinerPubUs = uint64(time.Now().UnixNano() / 1e3)

		pb, _ := proto.Marshal(out)
		if err := r.nc.Publish(subj, pb); err != nil {
			return err
		}
	}
	return nil
}

// makeKey 組出去重用的 key（exchange|feed|symbol）。
//
// 功能:
//   - 把 Exchange 列舉值轉成 10 進位字串，和 feed / symbol 串在一起。
//
// 參數:
//   - ex:     marketcommonv1.Exchange 列舉值。
//   - fd:     feed 名稱字串。
//   - symbol: canonical symbol 字串。
//
// 回傳:
//   - string: 去重用 key。
//   - 無 error。
//
// 備註:
//   - 這個 key 用在 state map 與 shouldPass。
func makeKey(ex marketcommonv1.Exchange, fd string, symbol string) string {
	return strconv.FormatInt(int64(ex), 10) + "|" + fd + "|" + symbol
}

// extractIds 從 Envelope.Body 拿出 seqId / tradeId（若有）。
//
// 功能:
//   - 根據 Any.type_url 判斷 body 具體型別。
//   - 針對 OKX*Body 族群抽取適用的去重欄位：
//   - OKXTradeBody:        seqId + tradeId
//   - OKXAllTradeBody:     tradeId
//   - OKXBBOBody:          seqId
//   - OKXBooksBody:        seqId
//   - OKXMarkPriceBody:    無
//   - OKXIndexTickersBody: 無
//   - 若 type_url 前綴有差異，會退而求其次逐一嘗試解碼。
//
// 參數:
//   - out: 乾淨的 Envelope 指標。
//
// 回傳:
//   - seq:     抽出的 seqId，沒有則為 0。
//   - tradeId: 抽出的 tradeId，沒有則為空字串。
//
// 備註:
//   - 若完全無法解析，會回傳 (0, "")，代表不做去重。
func extractIds(out *marketcommonv1.Envelope) (seq uint64, tradeId string) {
	a := out.GetBody()
	if a == nil {
		return 0, ""
	}
	tu := a.GetTypeUrl()

	switch {
	case hasTypeSuffix(tu, ".OKXTradeBody"):
		var t marketstreamv1.OKXTradeBody
		if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
			return t.GetSeqId(), t.GetTradeId()
		}
	case hasTypeSuffix(tu, ".OKXAllTradeBody"):
		var t marketstreamv1.OKXAllTradeBody
		if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
			return 0, t.GetTradeId()
		}
	case hasTypeSuffix(tu, ".OKXBBOBody"):
		var t marketstreamv1.OKXBBOBody
		if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
			return t.GetSeqId(), ""
		}
	case hasTypeSuffix(tu, ".OKXBooksBody"):
		var t marketstreamv1.OKXBooksBody
		if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
			return t.GetSeqId(), ""
		}
	case hasTypeSuffix(tu, ".OKXMarkPriceBody"):
		return 0, ""
	case hasTypeSuffix(tu, ".OKXIndexTickersBody"):
		return 0, ""
	default:
		// 後備保險：逐一嘗試各型別，避免 type_url 前綴略有差異時完全漏解。
		{
			var t marketstreamv1.OKXTradeBody
			if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
				return t.GetSeqId(), t.GetTradeId()
			}
		}
		{
			var t marketstreamv1.OKXAllTradeBody
			if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
				return 0, t.GetTradeId()
			}
		}
		{
			var t marketstreamv1.OKXBBOBody
			if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
				return t.GetSeqId(), ""
			}
		}
		{
			var t marketstreamv1.OKXBooksBody
			if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
				return t.GetSeqId(), ""
			}
		}
		{
			var t marketstreamv1.OKXMarkPriceBody
			if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
				return 0, ""
			}
		}
		{
			var t marketstreamv1.OKXIndexTickersBody
			if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
				return 0, ""
			}
		}
	}
	return 0, ""
}

// hasTypeSuffix 檢查 Any.type_url 是否以指定 suffix 結尾。
//
// 功能:
//   - 用 string 後綴比對判斷型別，避免被不同 prefix 影響。
//
// 參數:
//   - typeURL: Any.type_url。
//   - suffix:  要匹配的後綴字串。
//
// 回傳:
//   - bool: true 代表 type_url 是以 suffix 結尾。
//   - 無 error。
//
// 備註:
//   - 這裡不用 strings.HasSuffix 是單純沿用現有寫法，行為等價。
func hasTypeSuffix(typeURL, suffix string) bool {
	if len(typeURL) < len(suffix) {
		return false
	}
	return typeURL[len(typeURL)-len(suffix):] == suffix
}
