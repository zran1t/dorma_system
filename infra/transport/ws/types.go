// File: infra/transport/ws/types.go
// Package: ws
//
// 職責 (Responsibility):
//     定義 WebSocket 傳輸層的抽象介面，包括 WSClient 與 Adapter。
//     讓上層只依賴這兩個介面就能組裝 Collector，而不綁定具體實作。
//
// 注意事項 (Notes):
//     - WSClient 負責連線與 I/O；Adapter 負責協定與資料解析。
//     - 介面本身只有簽名與語意，不關心具體錯誤處理策略。

package ws

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"net/http"
	"time"
	// === 第三方套件 (Third-Party Libraries) ===
	// 無
	// === 系統內模組 (Internal Modules) ===
	// 無
)

// WSClient 抽象 WebSocket 客戶端行為。
//
// 功能:
//   - 定義建立連線、送文字、收訊息、送 ping 與關閉等基本操作。
//
// 契約 / 限制:
//   - 實作必須是 goroutine-safe（尤其是 SendJSON / Recv 可被多 goroutine 呼叫時）。
//   - 若尚未連線時進行收發，應回傳明確錯誤，而不是悄悄吞掉。
//
// 備註:
//   - GorillaWS 是此介面的其中一個實作，未來可替換為測試 stub 或其他套件。
type WSClient interface {
	// Connect 建立與指定 URL 的 WebSocket 連線。
	Connect(ctx context.Context, url string, hdr http.Header) error

	// SendJSON 送出一段文字訊息（通常是 JSON）。
	SendJSON(ctx context.Context, text string) error

	// Recv 阻塞等待下一則訊息，回傳訊息型別與內容。
	Recv(ctx context.Context) (msgType int, data []byte, err error)

	// Close 關閉連線並釋放資源。
	Close() error

	// SendPing 主動送出 ping 控制訊息。
	SendPing(ctx context.Context) error
}

// Adapter 定義與特定交易所協定相關的行為。
//
// 功能:
//   - 負責將「頻道＋標的物」轉換為訂閱封包。
//   - 定義心跳 payload 與間隔（若需要 app-level ping）。
//   - 將原始 WS 訊息解析並轉成「要發佈到哪個 subject、內容為何」。
//
// 契約 / 限制:
//   - BuildSubscribeMsgs 應對輸入的 channels/symbols 有明確定義（缺失/錯誤時需回傳錯誤）。
//   - Heartbeat 若回傳 every=0，表示不需要由 Collector 主動送心跳。
//   - Handle 出錯時不應 panic，應回傳錯誤讓 Collector 決定要不要丟棄該訊息。
//
// 備註:
//   - 一個 Adapter 通常對應一個交易所與一組資料型別（例如 kline / orderbook）。
type Adapter interface {
	// BuildSubscribeMsgs 把 channels + symbols 組合成訂閱用 JSON 字串列表。
	BuildSubscribeMsgs(channels []string, symbols []string) ([]string, error)

	// Heartbeat 回傳心跳 payload 與間隔；payload 為空字串時表示改用 WS ping。
	Heartbeat() (payload string, every time.Duration)

	// Handle 將原始 WS 訊息解析成要發佈的 subject 與 body。
	Handle(raw []byte) (subject string, body []byte, err error)
}
