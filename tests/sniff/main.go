// data_dpt/kols/lf_market_data_kol/tests/sniff_kline/main.go
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
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
	"google.golang.org/protobuf/types/known/anypb"

	marketcommonv1 "dorma_system/schemas/gen/go/market_common_v1"
	marketstreamv1 "dorma_system/schemas/gen/go/market_stream_v1"
	marketklinev1 "dorma_system/schemas/gen/go/market_kline_v1"
)

// prettyProto 以漂亮 JSON 印出 protobuf message
func prettyProto(m proto.Message) string {
	j, err := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(m)
	if err != nil {
		return fmt.Sprintf("<<JSON 序列化失敗: %v>>", err)
	}
	return string(j)
}

// prettyJSONRaw 針對原始 JSON bytes 進行縮排，美觀輸出
func prettyJSONRaw(b []byte) string {
	var any interface{}
	if err := json.Unmarshal(b, &any); err != nil {
		return string(b)
	}
	out, err := json.MarshalIndent(any, "", "  ")
	if err != nil {
		return string(b)
	}
	return string(out)
}

// hasTypeSuffix 檢查 Any 的 type_url 是否以指定後綴結尾
func hasTypeSuffix(typeURL, suffix string) bool {
	if len(typeURL) < len(suffix) {
		return false
	}
	return typeURL[len(typeURL)-len(suffix):] == suffix
}

// unpackAndDescribe 依據 Any 的 type_url 嘗試解包並回傳（標題, 內容字串）
func unpackAndDescribe(a *anypb.Any) (title string, body string) {
	if a == nil {
		return "Body", "(nil)"
	}
	tu := a.GetTypeUrl()

	// 先處理 LF/KLine 型別
	switch {
	case hasTypeSuffix(tu, ".OKXMarkPriceKLineBody"):
		var m marketklinev1.OKXMarkPriceKLineBody
		if err := anypb.UnmarshalTo(a, &m, proto.UnmarshalOptions{}); err != nil {
			return "Body(type=OKXMarkPriceKLineBody) 解析失敗", err.Error()
		}
		return "Body(type=OKXMarkPriceKLineBody)", prettyProto(&m)
	case hasTypeSuffix(tu, ".OKXIndexKLineBody"):
		var m marketklinev1.OKXIndexKLineBody
		if err := anypb.UnmarshalTo(a, &m, proto.UnmarshalOptions{}); err != nil {
			return "Body(type=OKXIndexKLineBody) 解析失敗", err.Error()
		}
		return "Body(type=OKXIndexKLineBody)", prettyProto(&m)
	}

	// HF/Stream 型別（沿用舊 sniff）
	switch {
	case hasTypeSuffix(tu, ".RawBody"):
		var raw marketstreamv1.RawBody
		if err := anypb.UnmarshalTo(a, &raw, proto.UnmarshalOptions{}); err != nil {
			return "Body(type=RawBody) 解析失敗", err.Error()
		}
		return "Body(type=RawBody)", prettyJSONRaw(raw.GetRawData())
	case hasTypeSuffix(tu, ".OKXTradeBody"):
		var m marketstreamv1.OKXTradeBody
		if err := anypb.UnmarshalTo(a, &m, proto.UnmarshalOptions{}); err != nil {
			return "Body(type=OKXTradeBody) 解析失敗", err.Error()
		}
		return "Body(type=OKXTradeBody)", prettyProto(&m)
	case hasTypeSuffix(tu, ".OKXAllTradeBody"):
		var m marketstreamv1.OKXAllTradeBody
		if err := anypb.UnmarshalTo(a, &m, proto.UnmarshalOptions{}); err != nil {
			return "Body(type=OKXAllTradeBody) 解析失敗", err.Error()
		}
		return "Body(type=OKXAllTradeBody)", prettyProto(&m)
	case hasTypeSuffix(tu, ".OKXBBOBody"):
		var m marketstreamv1.OKXBBOBody
		if err := anypb.UnmarshalTo(a, &m, proto.UnmarshalOptions{}); err != nil {
			return "Body(type=OKXBBOBody) 解析失敗", err.Error()
		}
		return "Body(type=OKXBBOBody)", prettyProto(&m)
	case hasTypeSuffix(tu, ".OKXBooksBody"):
		var m marketstreamv1.OKXBooksBody
		if err := anypb.UnmarshalTo(a, &m, proto.UnmarshalOptions{}); err != nil {
			return "Body(type=OKXBooksBody) 解析失敗", err.Error()
		}
		return "Body(type=OKXBooksBody)", prettyProto(&m)
	case hasTypeSuffix(tu, ".OKXMarkPriceBody"):
		var m marketstreamv1.OKXMarkPriceBody
		if err := anypb.UnmarshalTo(a, &m, proto.UnmarshalOptions{}); err != nil {
			return "Body(type=OKXMarkPriceBody) 解析失敗", err.Error()
		}
		return "Body(type=OKXMarkPriceBody)", prettyProto(&m)
	case hasTypeSuffix(tu, ".OKXIndexTickersBody"):
		var m marketstreamv1.OKXIndexTickersBody
		if err := anypb.UnmarshalTo(a, &m, proto.UnmarshalOptions{}); err != nil {
			return "Body(type=OKXIndexTickersBody) 解析失敗", err.Error()
		}
		return "Body(type=OKXIndexTickersBody)", prettyProto(&m)
	}

	// 後備嘗試
	try := []struct {
		name string
		msg  proto.Message
	}{
		{"OKXMarkPriceKLineBody", &marketklinev1.OKXMarkPriceKLineBody{}},
		{"OKXIndexKLineBody", &marketklinev1.OKXIndexKLineBody{}},
		{"OKXTradeBody", &marketstreamv1.OKXTradeBody{}},
		{"OKXAllTradeBody", &marketstreamv1.OKXAllTradeBody{}},
		{"OKXBBOBody", &marketstreamv1.OKXBBOBody{}},
		{"OKXBooksBody", &marketstreamv1.OKXBooksBody{}},
		{"OKXMarkPriceBody", &marketstreamv1.OKXMarkPriceBody{}},
		{"OKXIndexTickersBody", &marketstreamv1.OKXIndexTickersBody{}},
		{"RawBody", &marketstreamv1.RawBody{}},
	}
	for _, t := range try {
		if err := anypb.UnmarshalTo(a, t.msg, proto.UnmarshalOptions{}); err == nil {
			return "Body(type=" + t.name + ")", prettyProto(t.msg)
		}
	}

	return "Body(未知型別)", fmt.Sprintf("{\"type_url\":%q, \"value_len\": %d}", tu, len(a.GetValue()))
}

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://127.0.0.1:4222"
	}
	subject := os.Getenv("SUBJECT")
	if strings.TrimSpace(subject) == "" {
		// 預設看 LF 的 Mark KLine 全幣（可用 >）
		subject = "CLEAN.OKX.MARK-CANDLE.>"
	}

	// 取得要接收幾個封包
	msgLimit := int64(5)
	if s := os.Getenv("MSG_LIMIT"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v > 0 {
			msgLimit = v
		}
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("NATS 連線失敗: %v", err)
	}
	defer nc.Drain()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var count int64

	// 訂閱 subject（支持萬用字元）
	_, err = nc.Subscribe(subject, func(msg *nats.Msg) {
		var env marketcommonv1.Envelope
		if err := proto.Unmarshal(msg.Data, &env); err != nil {
			log.Printf("解包 Envelope 失敗: %v", err)
			return
		}

		fmt.Println("===== 新訊息 =====")
		fmt.Printf("Subject: %s\n", msg.Subject)
		fmt.Printf("Symbol:  %s\n", env.GetSymbol())
		fmt.Printf("Market:  %v\n", env.GetMarketType())
		if mid := env.GetMessageId(); len(mid) == 16 {
			fmt.Printf("MsgID:   %s\n", hex.EncodeToString(mid))
		}
		if src := env.GetSource(); src != nil {
			fmt.Printf("Source:  exchange=%s feed=%s interval=%s vendor_channel=%s\n",
				src.GetExchange().String(),
				src.GetFeed(),
				src.GetInterval(),
				src.GetVendorChannel(),
			)
		}
		if ts := env.GetTimestamps(); ts != nil {
			fmt.Printf("TS(us):  event=%d collect_recv=%d collect_pub=%d refiner_recv=%d refiner_pub=%d\n",
				ts.GetEventTsUs(), ts.GetCollectRecvUs(), ts.GetCollectPubUs(), ts.GetRefinerRecvUs(), ts.GetRefinerPubUs(),
			)
		}

		// 先顯示 Envelope（不含 body）供參考
		fmt.Println("--- Envelope (無 body) ---")
		envCopy := proto.Clone(&env).(*marketcommonv1.Envelope)
		envCopy.Body = nil
		fmt.Println(prettyProto(envCopy))

		// 解 Any，顯示 body
		fmt.Println("--- Envelope.Body ---")
		if env.GetBody() == nil {
			fmt.Println("(無 body)")
		} else {
			fmt.Printf("type_url: %s\n", env.GetBody().GetTypeUrl())
			title, content := unpackAndDescribe(env.GetBody())
			fmt.Println(title)
			fmt.Println(content)
		}

		if atomic.AddInt64(&count, 1) >= msgLimit {
			log.Printf("已接收 %d 則訊息，準備結束…", msgLimit)
			stop()
		}
	})
	if err != nil {
		log.Fatalf("訂閱失敗: %v", err)
	}

	log.Printf("開始監聽 %s（目標 %d 則）", subject, msgLimit)
	<-ctx.Done()
	log.Println("結束")
}