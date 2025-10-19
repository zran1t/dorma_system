// data_dpt/kols/lf_market_data_kol/refine_group/refiner.go
package refine_group

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	marketcommonv1 "dorma_system/schemas/gen/go/market_common_v1"
	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
	marketklinev1 "dorma_system/schemas/gen/go/market_kline_v1"
	"dorma_system/infra/symbols"
)

// ────────────────────────── Refiner ──────────────────────────

type Refiner struct {
	nc       *nats.Conn
	resolver symbols.Resolver

	handlers map[marketcommonv1.Exchange]map[string]Handler

	mu       sync.Mutex
	state    map[string]*dedupState // key = ex|feed|symbol
	ttl      time.Duration
	capacity int
}

type RefinerOption func(*Refiner)

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

func (r *Refiner) RegisterAdapter(ad ExchangeAdapter) {
	ex := ad.Exchange()
	if r.handlers[ex] == nil {
		r.handlers[ex] = make(map[string]Handler)
	}
	for f, h := range ad.Handlers() {
		r.handlers[ex][f] = h
	}
}

func (r *Refiner) Run(ctx context.Context, subjects ...string) error {
	if len(subjects) == 0 {
		log.Println("[lf/refiner] 未提供主題; 無法訂閱")
		<-ctx.Done()
		return ctx.Err()
	}
	for _, subj := range subjects {
		if _, err := r.nc.Subscribe(subj, func(m *nats.Msg) {
			if err := r.handleMsg(m); err != nil {
				log.Printf("[lf/refiner] subj=%s err=%v", m.Subject, err)
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
	seqSeen map[uint64]int64 // seqId -> lastSeen(sec)
	idSeen  map[string]int64 // tradeId -> lastSeen(sec)
}

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

		// 補時間戳
		if out.Timestamps == nil {
			out.Timestamps = &marketcommonv1.Timestamps{}
		}
		if out.Timestamps.RefinerRecvUs == 0 {
			out.Timestamps.RefinerRecvUs = uint64(nowUS)
		}

		// key：ex|feed|symbol
		sym := out.GetSymbol()
		key := makeKey(ex, fd, sym)

		// 取去重 id（KLINE 無鍵 → 0,""）
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

func makeKey(ex marketcommonv1.Exchange, fd string, symbol string) string {
	return strconv.FormatInt(int64(ex), 10) + "|" + fd + "|" + symbol
}

// extractIds：支援 HF（trades/bbo/book）與 LF（kline）
// KLINE 系列無 seqId / tradeId → 回傳 0,""
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

	// LF KLINE
	case hasTypeSuffix(tu, ".OKXMarkPriceKLineBody"):
		return 0, ""
	case hasTypeSuffix(tu, ".OKXIndexKLineBody"):
		return 0, ""

	// 後備保險
	default:
		{
			var t marketklinev1.OKXMarkPriceKLineBody
			if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
				return 0, ""
			}
		}
		{
			var t marketklinev1.OKXIndexKLineBody
			if err := anypb.UnmarshalTo(a, &t, proto.UnmarshalOptions{}); err == nil {
				return 0, ""
			}
		}
	}
	return 0, ""
}

func hasTypeSuffix(typeURL, suffix string) bool {
	if len(typeURL) < len(suffix) {
		return false
	}
	return typeURL[len(typeURL)-len(suffix):] == suffix
}