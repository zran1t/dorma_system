package ws

import (
	"context"
	"net/http"
	"time"
)

// 連線介面：infra 提供 gorilla 版實作
type WSClient interface {
	Connect(ctx context.Context, url string, hdr http.Header) error
	SendJSON(ctx context.Context, text string) error
	Recv(ctx context.Context) (msgType int, data []byte, err error)
	Close() error
	SendPing (ctx context.Context) error
}

// Adapter 由「交易所包」實作：告訴 Collector 要怎麼訂閱/心跳/解析
type Adapter interface {
	// BuildSubscribeMsgs：把使用者輸入的 channels/symbols 轉成交易所訂閱封包（多個 JSON 字串）
	BuildSubscribeMsgs(channels []string, symbols []string) ([]string, error)
	// Heartbeat：若交易所需要 app-level ping，回傳要送的字串 & 間隔；不需要就返回間隔=0
	Heartbeat() (payload string, every time.Duration)
	// Handle：收原始 bytes，決定要發到哪個 subject（回傳 subject 與 body）
	Handle(raw []byte) (subject string, body []byte, err error)
}
