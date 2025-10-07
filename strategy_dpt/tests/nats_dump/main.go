package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
)

type okxEnvelopeHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
}

type okxTradesPush struct {
	Arg  struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		InstID  string `json:"instId"`
		TradeID string `json:"tradeId"`
		TS      string `json:"ts"`
	} `json:"data"`
}

type okxTsPush struct {
	Arg  struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		TS string `json:"ts"`
	} `json:"data"`
}

type key struct {
	feed   string
	symbol string
}

type lruDedup struct {
	set   map[uint32]int
	order []uint32
	cap   int
}

func newLRU(capacity int) *lruDedup {
	if capacity <= 0 {
		capacity = 1024
	}
	return &lruDedup{
		set:   make(map[uint32]int),
		order: make([]uint32, 0, capacity),
		cap:   capacity,
	}
}
func (l *lruDedup) seen(h uint32) bool {
	_, ok := l.set[h]
	return ok
}
func (l *lruDedup) add(h uint32) {
	if _, ok := l.set[h]; ok {
		return
	}
	if len(l.order) >= l.cap {
		ev := l.order[0]
		l.order = l.order[1:]
		delete(l.set, ev)
	}
	l.order = append(l.order, h)
	l.set[h] = len(l.order) - 1
}

type perKeyStats struct {
	Count             int64
	Bytes             int64
	LastArrive        time.Time
	LastTS            int64
	LastTradeID       int64
	IDGaps            int64
	OutOfOrder        int64
	SilenceEvents     int64
	Duplicates        int64
	MaxInterarrivalMs int64
}

type counters struct {
	mu         sync.Mutex
	totalMsgs  int64
	totalBytes int64

	bySubject map[string]int64
	perKey    map[key]*perKeyStats
	dedup     map[key]*lruDedup

	gapTrades time.Duration
	gapBBO    time.Duration
	gapBook   time.Duration

	dedupCap int
}

func newCounters(gapTrades, gapBBO, gapBook time.Duration) *counters {
	return &counters{
		bySubject: make(map[string]int64),
		perKey:    make(map[key]*perKeyStats),
		dedup:     make(map[key]*lruDedup),
		gapTrades: gapTrades,
		gapBBO:    gapBBO,
		gapBook:   gapBook,
		dedupCap:  parseIntEnv("DEDUP_LRU", 1024),
	}
}
func (c *counters) getGapThreshold(feed string) time.Duration {
	feed = strings.ToUpper(feed)
	switch {
	case strings.Contains(feed, "TRADES"):
		return c.gapTrades
	case strings.Contains(feed, "BBO"):
		return c.gapBBO
	case strings.Contains(feed, "BOOK"):
		return c.gapBook
	default:
		return 3 * time.Second
	}
}
func (c *counters) getStats(k key) *perKeyStats {
	s, ok := c.perKey[k]
	if !ok {
		s = &perKeyStats{LastTradeID: -1}
		c.perKey[k] = s
	}
	return s
}
func (c *counters) getDedup(k key) *lruDedup {
	d, ok := c.dedup[k]
	if !ok {
		d = newLRU(c.dedupCap)
		c.dedup[k] = d
	}
	return d
}

func (c *counters) addMsg(subject string, env *marketstreamv1.Envelope, raw []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.totalMsgs++
	c.totalBytes += int64(len(raw))
	c.bySubject[subject]++

	feed := env.GetSource().GetFeed().String()
	symbol := env.GetSymbol()
	k := key{feed: feed, symbol: symbol}

	ps := c.getStats(k)

	// arrival gap & dedup
	now := time.Now()
	if !ps.LastArrive.IsZero() {
		d := now.Sub(ps.LastArrive)
		if d > c.getGapThreshold(feed) {
			ps.SilenceEvents++
		}
		if ms := d.Milliseconds(); ms > ps.MaxInterarrivalMs {
			ps.MaxInterarrivalMs = ms
		}
	}
	ps.LastArrive = now

	h := crc32.ChecksumIEEE(raw)
	if c.getDedup(k).seen(h) {
		ps.Duplicates++
	} else {
		c.getDedup(k).add(h)
	}

	ps.Count++
	ps.Bytes += int64(len(raw))

	// 解析 raw
	var head okxEnvelopeHead
	if err := json.Unmarshal(raw, &head); err != nil {
		return
	}
	ch := head.Arg.Channel
	switch ch {
	case "trades-all":
		var t okxTradesPush
		if err := json.Unmarshal(raw, &t); err != nil || len(t.Data) == 0 {
			return
		}
		td := t.Data[0]
		if tsms, err := strconv.ParseInt(td.TS, 10, 64); err == nil {
			ps.LastTS = tsms
		}
		if idNum, err := strconv.ParseInt(td.TradeID, 10, 64); err == nil {
			if ps.LastTradeID >= 0 {
				if idNum <= ps.LastTradeID {
					ps.OutOfOrder++
				} else if idNum-ps.LastTradeID > 1 {
					ps.IDGaps++
				}
			}
			ps.LastTradeID = idNum
		}
	case "trades":
		var t okxTradesPush
		if err := json.Unmarshal(raw, &t); err != nil || len(t.Data) == 0 {
			return
		}
		for _, td := range t.Data {
			if tsms, err := strconv.ParseInt(td.TS, 10, 64); err == nil {
				ps.LastTS = tsms
			}
			if idNum, err := strconv.ParseInt(td.TradeID, 10, 64); err == nil {
				if ps.LastTradeID >= 0 {
					if idNum <= ps.LastTradeID {
						ps.OutOfOrder++
					} else if idNum-ps.LastTradeID > 1 {
						ps.IDGaps++
					}
				}
				ps.LastTradeID = idNum
			}
		}
	default:
		var t okxTsPush
		if err := json.Unmarshal(raw, &t); err == nil && len(t.Data) > 0 {
			if tsms, err := strconv.ParseInt(t.Data[0].TS, 10, 64); err == nil {
				ps.LastTS = tsms
			}
		}
	}
}

func (c *counters) snapshot() (int64, int64, map[string]int64, []struct {
	key
	*perKeyStats
}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	bySub := make(map[string]int64, len(c.bySubject))
	for k, v := range c.bySubject {
		bySub[k] = v
	}
	var rows []struct {
		key
		*perKeyStats
	}
	for k, v := range c.perKey {
		rows = append(rows, struct {
			key
			*perKeyStats
		}{k, &perKeyStats{
			Count:             v.Count,
			Bytes:             v.Bytes,
			LastArrive:        v.LastArrive,
			LastTS:            v.LastTS,
			LastTradeID:       v.LastTradeID,
			IDGaps:            v.IDGaps,
			OutOfOrder:        v.OutOfOrder,
			SilenceEvents:     v.SilenceEvents,
			Duplicates:        v.Duplicates,
			MaxInterarrivalMs: v.MaxInterarrivalMs,
		}})
	}
	return c.totalMsgs, c.totalBytes, bySub, rows
}

func printEvery(c *counters, interval time.Duration) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	lastWall := time.Now()

	var prevTotalMsgs int64
	var prevTotalBytes int64
	prevBySub := make(map[string]int64)
	prevPerKeyCount := make(map[key]int64)
	prevPerKeyBytes := make(map[key]int64)
	prevIDGaps := make(map[key]int64)
	prevOOO := make(map[key]int64)
	prevSil := make(map[key]int64)
	prevDup := make(map[key]int64)

	for range t.C {
		now := time.Now()
		elapsed := now.Sub(lastWall)
		if elapsed < interval {
			continue
		}
		lastWall = now

		totalMsgs, totalBytes, bySub, rows := c.snapshot()

		dMsgs := totalMsgs - prevTotalMsgs
		dBytes := totalBytes - prevTotalBytes
		prevTotalMsgs = totalMsgs
		prevTotalBytes = totalBytes

		pps := float64(dMsgs) / elapsed.Seconds()
		mbps := (float64(dBytes) / (1024 * 1024)) / elapsed.Seconds()
		fmt.Printf("%s | pps=%.0f, MB/s=%.2f, msgs=%d, window=%.0fms\n",
			now.Format("15:04:05.000"), pps, mbps, dMsgs, float64(elapsed.Milliseconds()))

		type kv struct{ k string; v int64 }
		var subj []kv
		for k, v := range bySub {
			delta := v - prevBySub[k]
			if delta > 0 {
				subj = append(subj, kv{k, delta})
			}
			prevBySub[k] = v
		}
		sort.Slice(subj, func(i, j int) bool { return subj[i].v > subj[j].v })
		for i := 0; i < len(subj) && i < 4; i++ {
			fmt.Printf("  • %-12s : %6d\n", subj[i].k, subj[i].v)
		}

		type rowAgg struct {
			k      key
			score  int64
			gaps   int64
			ooo    int64
			sil    int64
			dups   int64
			count  int64
			bytes  int64
			maxGap int64
		}
		var agg []rowAgg
		for _, r := range rows {
			dCount := r.Count - prevPerKeyCount[r.key]
			dBytes := r.Bytes - prevPerKeyBytes[r.key]
			dGaps := r.IDGaps - prevIDGaps[r.key]
			dOOO := r.OutOfOrder - prevOOO[r.key]
			dSil := r.SilenceEvents - prevSil[r.key]
			dDup := r.Duplicates - prevDup[r.key]

			prevPerKeyCount[r.key] = r.Count
			prevPerKeyBytes[r.key] = r.Bytes
			prevIDGaps[r.key] = r.IDGaps
			prevOOO[r.key] = r.OutOfOrder
			prevSil[r.key] = r.SilenceEvents
			prevDup[r.key] = r.Duplicates

			if dCount <= 0 && dGaps <= 0 && dOOO <= 0 && dSil <= 0 && dDup <= 0 {
				continue
			}
			agg = append(agg, rowAgg{
				k:      r.key,
				score:  dGaps + dOOO + dSil,
				gaps:   dGaps,
				ooo:    dOOO,
				sil:    dSil,
				dups:   dDup,
				count:  dCount,
				bytes:  dBytes,
				maxGap: r.MaxInterarrivalMs,
			})
		}
		sort.Slice(agg, func(i, j int) bool {
			if agg[i].score == agg[j].score {
				return agg[i].count > agg[j].count
			}
			return agg[i].score > agg[j].score
		})
		maxTop := 5
		for i := 0; i < len(agg) && i < maxTop; i++ {
			a := agg[i]
			fmt.Printf("    - %-16s %-20s x %6d | gaps=%d ooo=%d sil=%d dup=%d maxΔms=%d\n",
				a.k.feed, a.k.symbol, a.count, a.gaps, a.ooo, a.sil, a.dups, a.maxGap)
		}
	}
}

func writeReportFiles(ctx context.Context, c *counters, every time.Duration, outJSON, outCSV string) {
	t := time.NewTicker(every)
	defer t.Stop()

	ensureDir := func(p string) error {
		dir := filepath.Dir(p)
		if dir == "." || dir == "" {
			return nil
		}
		return os.MkdirAll(dir, 0o755)
	}

	// 前一輪快照（做增量）
	var prevTotalMsgs int64
	var prevTotalBytes int64
	prevPerKeyCount := make(map[key]int64)
	prevPerKeyBytes := make(map[key]int64)
	prevIDGaps := make(map[key]int64)
	prevOOO := make(map[key]int64)
	prevSil := make(map[key]int64)
	prevDup := make(map[key]int64)

	lastTick := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			now := time.Now()
			elapsed := now.Sub(lastTick)
			if elapsed <= 0 {
				elapsed = every
			}
			lastTick = now

			totalMsgs, totalBytes, _, rows := c.snapshot()

			dTotalMsgs := totalMsgs - prevTotalMsgs
			dTotalBytes := totalBytes - prevTotalBytes
			prevTotalMsgs = totalMsgs
			prevTotalBytes = totalBytes

			// ---- JSON ----
			if err := ensureDir(outJSON); err == nil {
				if f, err := os.Create(outJSON); err == nil {
					enc := json.NewEncoder(f)
					enc.SetIndent("", "  ")
					type rowJSON struct {
						Feed               string `json:"feed"`
						Symbol             string `json:"symbol"`
						Count              int64  `json:"count"`
						Bytes              int64  `json:"bytes"`
						DeltaCount         int64  `json:"delta_count"`
						DeltaBytes         int64  `json:"delta_bytes"`
						IDGaps             int64  `json:"id_gaps"`
						OutOfOrder         int64  `json:"out_of_order"`
						SilenceEvents      int64  `json:"silence_events"`
						Duplicates         int64  `json:"duplicates"`
						DeltaIDGaps        int64  `json:"delta_id_gaps"`
						DeltaOutOfOrder    int64  `json:"delta_out_of_order"`
						DeltaSilenceEvents int64  `json:"delta_silence_events"`
						DeltaDuplicates    int64  `json:"delta_duplicates"`
						LastTSMs           int64  `json:"last_ts_ms"`
						LastArrivalUnix    int64  `json:"last_arrival_unix"`
						MaxInterarrivalMs  int64  `json:"max_interarrival_ms"`
					}
					out := struct {
						GeneratedAt     string    `json:"generated_at"`
						WindowSeconds   float64   `json:"window_seconds"`
						TotalMsgs       int64     `json:"total_msgs"`
						TotalBytes      int64     `json:"total_bytes"`
						DeltaTotalMsgs  int64     `json:"delta_total_msgs"`
						DeltaTotalBytes int64     `json:"delta_total_bytes"`
						Rows            []rowJSON `json:"rows"`
					}{
						GeneratedAt:     now.Format(time.RFC3339),
						WindowSeconds:   elapsed.Seconds(),
						TotalMsgs:       totalMsgs,
						TotalBytes:      totalBytes,
						DeltaTotalMsgs:  dTotalMsgs,
						DeltaTotalBytes: dTotalBytes,
					}
					for _, r := range rows {
						k := r.key
						rc := rowJSON{
							Feed:               k.feed,
							Symbol:             k.symbol,
							Count:              r.Count,
							Bytes:              r.Bytes,
							DeltaCount:         r.Count - prevPerKeyCount[k],
							DeltaBytes:         r.Bytes - prevPerKeyBytes[k],
							IDGaps:             r.IDGaps,
							OutOfOrder:         r.OutOfOrder,
							SilenceEvents:      r.SilenceEvents,
							Duplicates:         r.Duplicates,
							DeltaIDGaps:        r.IDGaps - prevIDGaps[k],
							DeltaOutOfOrder:    r.OutOfOrder - prevOOO[k],
							DeltaSilenceEvents: r.SilenceEvents - prevSil[k],
							DeltaDuplicates:    r.Duplicates - prevDup[k],
							LastTSMs:           r.LastTS,
							LastArrivalUnix:    r.LastArrive.Unix(),
							MaxInterarrivalMs:  r.MaxInterarrivalMs,
						}
						out.Rows = append(out.Rows, rc)

						// 更新 prev
						prevPerKeyCount[k] = r.Count
						prevPerKeyBytes[k] = r.Bytes
						prevIDGaps[k] = r.IDGaps
						prevOOO[k] = r.OutOfOrder
						prevSil[k] = r.SilenceEvents
						prevDup[k] = r.Duplicates
					}
					_ = enc.Encode(out)
					_ = f.Close()
				}
			}

			// ---- CSV（覆寫：每輪完整快照 + 增量欄位）----
			if err := ensureDir(outCSV); err == nil {
				if f, err := os.Create(outCSV); err == nil {
					w := csv.NewWriter(f)
					_ = w.Write([]string{
						"generated_at",
						"window_seconds",
						"feed",
						"symbol",
						"count",
						"bytes",
						"delta_count",
						"delta_bytes",
						"id_gaps",
						"out_of_order",
						"silence_events",
						"duplicates",
						"delta_id_gaps",
						"delta_out_of_order",
						"delta_silence_events",
						"delta_duplicates",
						"last_ts_ms",
						"last_arrival_unix",
						"max_interarrival_ms",
						"total_msgs",
						"total_bytes",
						"delta_total_msgs",
						"delta_total_bytes",
					})
					gen := now.Format(time.RFC3339)
					win := fmt.Sprintf("%.3f", elapsed.Seconds())
					for _, r := range rows {
						k := r.key
						_ = w.Write([]string{
							gen,
							win,
							k.feed,
							k.symbol,
							strconv.FormatInt(r.Count, 10),
							strconv.FormatInt(r.Bytes, 10),
							strconv.FormatInt(r.Count-prevPerKeyCount[k], 10),
							strconv.FormatInt(r.Bytes-prevPerKeyBytes[k], 10),
							strconv.FormatInt(r.IDGaps, 10),
							strconv.FormatInt(r.OutOfOrder, 10),
							strconv.FormatInt(r.SilenceEvents, 10),
							strconv.FormatInt(r.Duplicates, 10),
							strconv.FormatInt(r.IDGaps-prevIDGaps[k], 10),
							strconv.FormatInt(r.OutOfOrder-prevOOO[k], 10),
							strconv.FormatInt(r.SilenceEvents-prevSil[k], 10),
							strconv.FormatInt(r.Duplicates-prevDup[k], 10),
							strconv.FormatInt(r.LastTS, 10),
							strconv.FormatInt(r.LastArrive.Unix(), 10),
							strconv.FormatInt(r.MaxInterarrivalMs, 10),
							strconv.FormatInt(totalMsgs, 10),
							strconv.FormatInt(totalBytes, 10),
							strconv.FormatInt(dTotalMsgs, 10),
							strconv.FormatInt(dTotalBytes, 10),
						})
						// 更新 prev
						prevPerKeyCount[k] = r.Count
						prevPerKeyBytes[k] = r.Bytes
						prevIDGaps[k] = r.IDGaps
						prevOOO[k] = r.OutOfOrder
						prevSil[k] = r.SilenceEvents
						prevDup[k] = r.Duplicates
					}
					w.Flush()
					_ = f.Close()
				}
			}
		}
	}
}

func main() {
	var (
		natsURL   string
		subject   string
		intervalS int
		jsonPath  string
		csvPath   string
		repEveryS int
	)
	flag.StringVar(&natsURL, "nats", getenv("NATS_URL", "nats://127.0.0.1:4222"), "NATS url")
	flag.StringVar(&subject, "subj", "RAW.>", "NATS subject (e.g. RAW.>)")
	flag.IntVar(&intervalS, "print", 1, "print summary every N seconds")
	flag.StringVar(&jsonPath, "json", "nats_dump_report.json", "report json path")
	flag.StringVar(&csvPath, "csv", "nats_dump_report.csv", "report csv path")
	flag.IntVar(&repEveryS, "report", 60, "write report every N seconds")
	flag.Parse()

	gapTrades := parseDurEnv("GAP_TRADES_MS", 2000*time.Millisecond)
	gapBBO := parseDurEnv("GAP_BBO_MS", 2000*time.Millisecond)
	gapBook := parseDurEnv("GAP_BOOK_MS", 5000*time.Millisecond)

	nc, err := nats.Connect(natsURL, nats.Name("raw monitor (proto decode)"))
	if err != nil {
		log.Fatalf("NATS connect failed: %v", err)
	}
	defer nc.Drain()
	log.Printf("connected to %s; subject=%s; print=%ds; report=%ds", natsURL, subject, intervalS, repEveryS)

	cs := newCounters(gapTrades, gapBBO, gapBook)

	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		var env marketstreamv1.Envelope
		if err := proto.Unmarshal(m.Data, &env); err != nil {
			return
		}
		raw := env.GetRaw().GetRawData()
		cs.addMsg(m.Subject, &env, raw)
	})
	if err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go printEvery(cs, time.Duration(intervalS)*time.Second)
	go writeReportFiles(ctx, cs, time.Duration(repEveryS)*time.Second, jsonPath, csvPath)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("bye")
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func parseDurEnv(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return def
}
func parseIntEnv(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}