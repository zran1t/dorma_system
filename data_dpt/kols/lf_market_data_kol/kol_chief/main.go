package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"

	// collect / refine（LF）
	lcollect "dorma_system/data_dpt/kols/lf_market_data_kol/collect_group"
	lrefine "dorma_system/data_dpt/kols/lf_market_data_kol/refine_group"
	okxlf "dorma_system/data_dpt/kols/lf_market_data_kol/refine_group/exchanges/okx"

	// infra
	"dorma_system/infra/pubsub"
	"dorma_system/infra/symbols"
	"dorma_system/infra/transport/ws"
)

func main() {
	// ────────────────────── lifecycle ──────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("[lf/kol_chief] shutting down…")
		cancel()
	}()

	// ────────────────────── NATS ──────────────────────
	nurl := os.Getenv("NATS_URL")
	if nurl == "" {
		nurl = "nats://127.0.0.1:4222"
	}

	// bus for collectors (pub only)
	bus, err := pubsub.NewNATSCoreBus()
	if err != nil {
		log.Fatalf("NATS connect (collectors) failed: %v", err)
	}
	log.Printf("[lf/kol_chief] NATS connected (collectors): %s", nurl)

	// conn for refiner (sub RAW.* → pub CLEAN.*)
	ncRef, err := nats.Connect(nurl, nats.Name("lf_market_data_kol.refiner"))
	if err != nil {
		log.Fatalf("NATS connect (refiner) failed: %v", err)
	}
	defer ncRef.Drain()

	// ────────────────────── symbols resolver ──────────────────────
	res := symbols.NewInMemoryResolver()
	if err := res.LoadFromDefaultYAML(); err != nil {
		log.Fatalf("load symbol mappings failed: %v", err)
	}

	// ────────────────────── config：標的 + intervals ──────────────────────
	canon := []string{
		"BTC-USDT-SWAP", "ETH-USDT-SWAP", "XRP-USDT-SWAP",
		"SOL-USDT-SWAP", "ADA-USDT-SWAP", "DOGE-USDT-SWAP",
	}

	intervals := []string{
		"1m", "5m", "15m", "1H", "4H", "1Dutc", "1Wutc",
	}

	// ────────────────────── start refiner ──────────────────────
	rchief := lrefine.NewChief(ncRef, res, okxlf.New())
	go func() {
		if err := rchief.StartBy(ctx, "okx", []string{"MARK-CANDLE", "INDEX-CANDLE"}); err != nil {
			log.Fatalf("[lf/refiner] start failed: %v", err)
		}
	}()
	log.Printf("[lf/kol_chief] refiner started: exchange=okx feeds=%v", []string{"MARK-CANDLE", "INDEX-CANDLE"})

	// ────────────────────── start collectors（正確：baseFeed + interval） ──────────────────────
	for _, iv := range intervals {
		// MARK-CANDLE
		chiefMark := lcollect.NewChief(func() ws.WSClient { return ws.NewGorillaWS() }, bus, res)
		if err := chiefMark.Start(ctx, "okx", "mark-price-candle", iv, canon); err != nil {
			log.Fatalf("[lf/collector] start mark feed interval=%s failed: %v", iv, err)
		}
		log.Printf("[lf/collector] started: feed=mark-price-candle interval=%s symbols=%s",
			iv, strings.Join(canon, ","))
		time.Sleep(80 * time.Millisecond)

		// INDEX-CANDLE
		chiefIdx := lcollect.NewChief(func() ws.WSClient { return ws.NewGorillaWS() }, bus, res)
		if err := chiefIdx.Start(ctx, "okx", "index-candle", iv, canon); err != nil {
			log.Fatalf("[lf/collector] start index feed interval=%s failed: %v", iv, err)
		}
		log.Printf("[lf/collector] started: feed=index-candle interval=%s symbols=%s",
			iv, strings.Join(canon, ","))
		time.Sleep(80 * time.Millisecond)
	}

	// ────────────────────── block ──────────────────────
	<-ctx.Done()
	log.Println("[lf/kol_chief] exit")
}
