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
	bus, err := pubsub.NewNATSCoreBus()
	if err != nil {
		log.Fatalf("NATS connect failed: %v", err)
	}
	log.Printf("NATS connected (collect): %s", nurl)

	res := symbols.NewInMemoryResolver()
	if err := res.LoadFromDefaultYAML(); err != nil {
		log.Fatalf("load symbol mappings failed: %v", err)
	}

	// 交易用 instrument（永續）
	canon := []string{
		"BTC-USDT-SWAP", "ETH-USDT-SWAP", "BNB-USDT-SWAP", "XRP-USDT-SWAP",
		"SOL-USDT-SWAP", "ADA-USDT-SWAP", "DOGE-USDT-SWAP", "MATIC-USDT-SWAP",
		"DOT-USDT-SWAP", "LTC-USDT-SWAP",
	}
	// 指數頻道用（去掉 -SWAP，保留 BASE-QUOTE）
	indexSymbols := make([]string, 0, len(canon))
	for _, s := range canon {
		// 期待格式 BASE-QUOTE-SWAP
		parts := strings.Split(strings.TrimSpace(s), "-")
		if len(parts) == 3 && strings.EqualFold(parts[2], "SWAP") {
			indexSymbols = append(indexSymbols, strings.ToUpper(parts[0]+"-"+parts[1]))
		} else {
			// 後備：如果不是 SWAP，就嘗試直接取前兩段
			if len(parts) >= 2 {
				indexSymbols = append(indexSymbols, strings.ToUpper(parts[0]+"-"+parts[1]))
			}
		}
	}

	// feeds：新增 mark-price / index-tickers
	feeds := []string{"trades", "bbo", "books", "trades-all", "mark-price", "index-tickers"}

	// 連 NATS（refiner / book_group 共用這條）
	ncRef, err := nats.Connect(nurl, nats.Name("hf_market_data_kol.refiner"))
	if err != nil {
		log.Fatalf("NATS connect (refiner) failed: %v", err)
	}
	defer ncRef.Drain()

	// ❶ 先啟 book_group（確保不 miss 第一個 snapshot）
	bg := bookgroup.NewChief(ncRef, 1000*time.Millisecond) // 0 = 不節流
	go func() {
		if err := bg.Start(); err != nil {
			log.Fatalf("book_group start failed: %v", err)
		}
	}()
	log.Printf("book_group started: delta=CLEAN.OKX.BOOK.DELTA full=CLEAN.OKX.BOOK.FULL")

	// ❸ 啟 refiner（吃 RAW.*，出 CLEAN.*）
	rchief := refine_group.NewChief(ncRef, res, okxref.New())
	go func() {
		if err := rchief.StartBy(ctx, "okx", feeds); err != nil {
			log.Fatalf("refiner start failed: %v", err)
		}
	}()
	log.Printf("refiner started: exchange=okx feeds=%v", feeds)

	// ❷ collectors（RAW.*）
	for _, feed := range feeds {
		chief := collect_group.NewChief(
			func() ws.WSClient { return ws.NewGorillaWS() },
			bus, res,
		)

		// index-tickers 要用 Index 的 instId（BASE-QUOTE）
		syms := canon
		if strings.EqualFold(feed, "index-tickers") {
			syms = indexSymbols
		}

		if err := chief.Start(ctx, "okx", feed, syms); err != nil {
			log.Fatalf("start %s failed: %v", feed, err)
		}
		log.Printf("collector started: feed=%s symbols=%s", feed, strings.Join(syms, ","))
	}

	<-ctx.Done()
	log.Println("exit")
}
