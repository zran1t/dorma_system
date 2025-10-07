package binance

import (
	"time"
)

// Adapter 是占位用的空實作，先讓整體能編譯通過。
// 之後要接幣安時，再把三個方法補成真的邏輯。
type Adapter struct{}

func NewAdapter() *Adapter { return &Adapter{} }

// BuildSubscribeMsgs 回傳要送到 WS 的訂閱封包字串清單。
// 先回空切片代表「不送訂閱」，Collector.Subscribe() 會直接略過。
func (a *Adapter) BuildSubscribeMsgs(channels []string, symbols []string) ([]string, error) {
	return []string{}, nil
}

// Heartbeat 回傳心跳封包與週期。0 代表不送心跳。
func (a *Adapter) Heartbeat() (string, time.Duration) {
	return "", 0
}

// Handle 處理收到的 WS 訊息，回傳 (subject, body)。
// 這裡回空 subject 與 nil body，Collector 會自動跳過不發佈。
func (a *Adapter) Handle(raw []byte) (string, []byte, error) {
	return "", nil, nil
}