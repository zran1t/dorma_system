package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
)

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://127.0.0.1:4222"
	}
	subject := os.Getenv("SUBJECT")
	if subject == "" {
		subject = "CLEAN.OKX.BBO"
	}

	// 新增：從環境變數或預設取得要接收幾個封包
	msgLimit := int64(5)
	if s := os.Getenv("MSG_LIMIT"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
			msgLimit = v
		}
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer nc.Drain()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var count int64

	// 訂閱 subject
	_, err = nc.Subscribe(subject, func(msg *nats.Msg) {
		var env marketstreamv1.Envelope
		if err := proto.Unmarshal(msg.Data, &env); err != nil {
			log.Printf("unmarshal failed: %v", err)
			return
		}

		j, _ := protojson.MarshalOptions{
			Multiline: true,
			Indent:    "  ",
		}.Marshal(&env)

		fmt.Println("----- message -----")
		fmt.Println(string(j))

		if atomic.AddInt64(&count, 1) >= msgLimit {
			log.Printf("received %d messages, stopping...", msgLimit)
			stop()
		}
	})
	if err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}

	log.Printf("listening on %s ... (waiting for %d messages)", subject, msgLimit)
	<-ctx.Done()
	log.Println("exit")
}