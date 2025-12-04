// data_dpt/kols/lf_market_data_kol/tests/sniff_json/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	marketcommonv1 "dorma_system/schemas/gen/go/market/common/v1"
	_ "dorma_system/schemas/gen/go/market/kline/v1"
	_ "dorma_system/schemas/gen/go/market/stream/v1"
)

// =========================
// 🔧 可調整參數集中於此
// =========================
var (
	// NATS 伺服器位置
	DefaultNATSURL = "nats://127.0.0.1:4222"

	// 預設訂閱 subject（使用萬用字元可監聽多個）
	DefaultSubject = "CLEAN.OKX.TRADES.BTC.USDT.SWAP"

	// 要接收的封包數量
	DefaultMsgLimit = int64(6)
)

// =========================
// main 這個func is test
// =========================
func main() {
	natsURL := getEnvOrDefault("NATS_URL", DefaultNATSURL)
	subject := getEnvOrDefault("SUBJECT", DefaultSubject)
	msgLimit := getEnvInt64OrDefault("MSG_LIMIT", DefaultMsgLimit)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("❌ NATS 連線失敗: %v", err)
	}
	defer nc.Drain()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var count int64

	_, err = nc.Subscribe(subject, func(msg *nats.Msg) {
		var env marketcommonv1.Envelope
		if err := proto.Unmarshal(msg.Data, &env); err != nil {
			log.Printf("⚠️ 解包 Envelope 失敗: %v", err)
			return
		}

		// 用 protojson 直接序列化整個封包，不做任何加工
		j, err := protojson.MarshalOptions{
			Multiline:       true,
			Indent:          "  ",
			UseProtoNames:   true,
			EmitUnpopulated: true,
		}.Marshal(&env)
		if err != nil {
			log.Printf("⚠️ JSON 序列化失敗: %v", err)
			return
		}

		// 印出結果
		fmt.Println("===== 新封包 =====")
		fmt.Printf("Subject: %s\n", msg.Subject)
		fmt.Println(string(j))
		fmt.Println("==================")

		if atomic.AddInt64(&count, 1) >= msgLimit {
			log.Printf("📦 已接收 %d 則封包，準備結束…", msgLimit)
			stop()
		}
	})
	if err != nil {
		log.Fatalf("❌ 訂閱失敗: %v", err)
	}

	log.Printf("🚀 開始監聽 %s（最多 %d 則）", subject, msgLimit)
	<-ctx.Done()
	log.Println("✅ sniff_json 結束")
}

// getEnvOrDefault 這個func
func getEnvOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func getEnvInt64OrDefault(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil && i > 0 {
			return i
		}
	}
	return def
}
