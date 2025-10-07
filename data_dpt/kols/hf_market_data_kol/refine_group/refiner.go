package refine_group

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
	"dorma_system/infra/symbols"
)

// ────────────────────────── Refiner ──────────────────────────

type Refiner struct {
	nc       *nats.Conn
	resolver symbols.Resolver
	handlers map[marketstreamv1.Exchange]map[marketstreamv1.Feed]Handler

	// 去重狀態（放 engine，不汙染 adapter）
	mu       sync.Mutex
	state    map[string]*dedupState // key = ex|feed|symbol
	ttl      time.Duration          // 記憶多久
	capacity int                    // 每個 key 最多記 N 筆
}

type RefinerOption func(*Refiner)

// WithDedup 調整去重 TTL 與容量
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

// NewRefiner：預設 TTL 2 分鐘、容量 4096
func NewRefiner(nc *nats.Conn, resolver symbols.Resolver, opts ...RefinerOption) *Refiner {
	r := &Refiner{
		nc:       nc,
		resolver: resolver,
		handlers: make(map[marketstreamv1.Exchange]map[marketstreamv1.Feed]Handler),
		state:    make(map[string]*dedupState),
		ttl:      2 * time.Minute,
		capacity: 4096,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

func (r *Refiner) RegisterAdapter(ad ExchangeAdapter) {
	ex := ad.Exchange()
	if r.handlers[ex] == nil {
		r.handlers[ex] = make(map[marketstreamv1.Feed]Handler)
	}
	for f, h := range ad.Handlers() {
		r.handlers[ex][f] = h
	}
}

// Run：訂閱並阻塞到 ctx 結束
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

type dedupState struct {
	seqSeen   map[uint64]int64 // seqId -> lastSeen(sec)
	idSeen    map[string]int64 // tradeId -> lastSeen(sec)
}

// 確保 key 有狀態
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

// 泛型修剪，避免 map[any]int64 帶來的型別不相容
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

// 清理過期或超過容量的紀錄（簡單版：逐步淘汰）
func (r *Refiner) gcBucket(b *dedupState, nowSec int64) {
	expireAt := nowSec - int64(r.ttl.Seconds())

	// TTL 清理
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

	// 容量控制（粗略）：若超過 cap，刪到接近 cap/2
	trimMap(b.seqSeen, r.capacity)
	trimMap(b.idSeen, r.capacity)
}

// shouldPass：依規則判斷是否放行，並記錄
// 規則：
// 1) 若有 seqId：以 seqId 去重（看過就丟）
// 2) 若有 tradeId：再以 tradeId 去重（看過就丟）
// 3) 若兩者皆有，兩層都檢（任一命中即丟）
// 4) 若兩者皆無，直接放行（不記錄）
func (r *Refiner) shouldPass(key string, seqId uint64, tradeId string, nowSec int64) bool {
	b := r.bucket(key)

	r.mu.Lock()
	defer r.mu.Unlock()

	// 先做 GC
	r.gcBucket(b, nowSec)

	// seqId 檢查
	if seqId != 0 {
		if _, ok := b.seqSeen[seqId]; ok {
			return false
		}
	}

	// tradeId 檢查
	if tradeId != "" {
		if _, ok := b.idSeen[tradeId]; ok {
			return false
		}
	}

	// 記錄（兩者有就都記）
	if seqId != 0 {
		b.seqSeen[seqId] = nowSec
	}
	if tradeId != "" {
		b.idSeen[tradeId] = nowSec
	}
	return true
}

// ────────────────────────── 消息處理 ──────────────────────────

// handleMsg：收 RAW → 交給對應 handler → 拿到 []Out → 去重與發佈
func (r *Refiner) handleMsg(m *nats.Msg) error {
	nowUS := time.Now().UnixNano() / 1e3
	nowSec := nowUS / 1_000_000

	var env marketstreamv1.Envelope
	if err := proto.Unmarshal(m.Data, &env); err != nil {
	 return err
	}
	if env.Timestamps == nil {
	 env.Timestamps = &marketstreamv1.Timestamps{}
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
	h := table[fd]
	if h == nil {
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

		// 補時間戳
		if out.Timestamps == nil {
			out.Timestamps = &marketstreamv1.Timestamps{}
		}
		if out.Timestamps.RefinerRecvUs == 0 {
			out.Timestamps.RefinerRecvUs = uint64(nowUS)
		}

		// key：ex|feed|symbol
		sym := out.GetSymbol()
		key := makeKey(ex, fd, sym)

		// 取去重 id
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

func makeKey(ex marketstreamv1.Exchange, fd marketstreamv1.Feed, symbol string) string {
	// 用數字字串，避免 string(rune(x)) 造成單字元碰撞
	return strconv.FormatInt(int64(ex), 10) + "|" +
		strconv.FormatInt(int64(fd), 10) + "|" +
		symbol
}

// 從 Envelope 抽取 seqId / tradeId（若無則為 0 / ""）
func extractIds(out *marketstreamv1.Envelope) (seq uint64, tradeId string) {
	switch b := out.Body.(type) {
	case *marketstreamv1.Envelope_Trade:
		if b.Trade != nil {
			seq = b.Trade.GetSeqId()
			tradeId = b.Trade.GetTradeId()
		}
	case *marketstreamv1.Envelope_TradeAll:
		if b.TradeAll != nil {
			// OKX trades-all 通常沒有 seqId，就只用 tradeId
			seq = 0
			tradeId = b.TradeAll.GetTradeId()
		}
	case *marketstreamv1.Envelope_Bbo:
		if b.Bbo != nil {
			seq = b.Bbo.GetSeqId()
			// BBO 沒 tradeId
		}
	case *marketstreamv1.Envelope_Book:
		if b.Book != nil {
			seq = b.Book.GetSeqId()
			// BOOK 沒 tradeId；SNAPSHOT/UPDATE 特殊流程之後再加
		}
	default:
		// 其他型別：不去重
	}
	return
}