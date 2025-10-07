// data_dpt/kols/hf_market_data_kol/kol_chief/main.go
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

	bookgroup "dorma_system/data_dpt/kols/hf_market_data_kol/book_group"
	"dorma_system/data_dpt/kols/hf_market_data_kol/collect_group"
	"dorma_system/data_dpt/kols/hf_market_data_kol/refine_group"
	okxref "dorma_system/data_dpt/kols/hf_market_data_kol/refine_group/exchanges/okx"

	"dorma_system/infra/pubsub"
	"dorma_system/infra/symbols"
	"dorma_system/infra/transport/ws"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("shutting down...")
		cancel()
	}()

	nurl := os.Getenv("NATS_URL")
	if nurl == "" {
		nurl = "nats://127.0.0.1:4222"
	}

	// bus for collectors
	bus, err := pubsub.NewNATSCoreBus(pubsub.BusOptions{
		URL:  nurl,
		Name: "hf_market_data_kol.collectors",
	})
	if err != nil { log.Fatalf("NATS connect failed: %v", err) }
	log.Printf("NATS connected (collect): %s", nurl)

	res := symbols.NewInMemoryResolver()
	if err := res.LoadDefaultsFromDisk(); err != nil {
		log.Fatalf("load symbol mappings failed: %v", err)
	}

	// 前 10 大
	canon := []string{
		"BTC-USDT-SWAP", "ETH-USDT-SWAP", "BNB-USDT-SWAP", "XRP-USDT-SWAP",
		"SOL-USDT-SWAP", "ADA-USDT-SWAP", "DOGE-USDT-SWAP", "MATIC-USDT-SWAP",
		"DOT-USDT-SWAP", "LTC-USDT-SWAP",
	}

	feeds := []string{"trades", "bbo", "books", "trades-all"}

	// 連 NATS（refiner / book_group 共用這條）
	ncRef, err := nats.Connect(nurl, nats.Name("hf_market_data_kol.refiner"))
	if err != nil { log.Fatalf("NATS connect (refiner) failed: %v", err) }
	defer ncRef.Drain()

	// ❶ 先啟 book_group（確保不 miss 第一個 snapshot）
	bg := bookgroup.NewChief(ncRef, 1000*time.Millisecond) // 0 = 不節流，驗證期建議先開著
	go func() {
		if err := bg.Start(); err != nil {
			log.Fatalf("book_group start failed: %v", err)
		}
	}()
	log.Printf("book_group started: delta=CLEAN.OKX.BOOK.DELTA full=CLEAN.OKX.BOOK.FULL")


	// ❸ 最後啟 refiner（CLEAN.OKX.*.DELTA）
	rchief := refine_group.NewChief(ncRef, res, okxref.New())
	go func() {
		if err := rchief.StartBy(ctx, "okx", feeds); err != nil {
			log.Fatalf("refiner start failed: %v", err)
		}
	}()
	log.Printf("refiner started: exchange=okx feeds=%v", feeds)


	// ❷ 再啟 collectors（RAW.*）
	for _, feed := range feeds {
		chief := collect_group.NewChief(
			func() ws.WSClient { return ws.NewGorillaWS() },
			bus, res,
		)
		if err := chief.Start(ctx, "okx", feed, canon); err != nil {
			log.Fatalf("start %s failed: %v", feed, err)
		}
		log.Printf("collector started: feed=%s symbols=%s", feed, strings.Join(canon, ","))
	}



	<-ctx.Done()
	log.Println("exit")
}