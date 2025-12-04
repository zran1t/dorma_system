// File: infra/transport/ws/client.go
// Package: ws
//
// 職責 (Responsibility):
//     封裝基於 gorilla/websocket 的 WebSocket 客戶端實作。
//     提供連線管理、訊息收發、ping 心跳與安全關閉等基礎能力。
//
// 注意事項 (Notes):
//     - 透過 RWMutex + Mutex 確保多 goroutine 下的讀寫安全。
//     - ctx 目前僅保留在介面中，尚未用來控制 gorilla 的 I/O，之後可再擴充。
//     - 單一訊息設有最大長度限制，避免吃下異常大的 payload。

package ws

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	"github.com/gorilla/websocket"
	// === 系統內模組 (Internal Modules) ===
	// 無
)

// GorillaWS 是基於 gorilla/websocket 的 WSClient 實作。
//
// 功能:
//   - 管理 WebSocket 連線的建立、收發與關閉。
//   - 透過鎖避免多 goroutine 同時寫入造成競態問題。
//
// 欄位說明:
//   - dialer:  用於建立 WebSocket 連線的 gorilla Dialer 實例。
//   - conn:    當前建立好的 WebSocket 連線指標，尚未連線時為 nil。
//   - mu:      保護 conn 讀寫的 RWMutex，允許多讀單寫。
//   - writeMu: 針對 gorilla WriteMessage 的序列化寫鎖，避免並行寫入。
//
// 契約 / 限制:
//   - 所有對 conn 的讀寫都必須透過鎖保護，避免資料競態。
//   - Close 之後 conn 會被設為 nil，不可再使用舊連線物件。
//
// 備註:
//   - 不直接處理業務邏輯，只專注在傳輸層行為。
type GorillaWS struct {
	dialer  *websocket.Dialer
	conn    *websocket.Conn
	mu      sync.RWMutex
	writeMu sync.Mutex
}

// NewGorillaWS 建立一個預設設定的 GorillaWS 實例。
//
// 功能:
//   - 初始化 gorilla Dialer，設定基礎 Handshake timeout。
//   - 回傳尚未連線的 GorillaWS 物件。
//
// 參數:
//   - 無。
//
// 回傳:
//   - *GorillaWS: 新建的 WebSocket 客戶端實例。
//   - 無 error。
//
// 備註:
//   - 後續仍需呼叫 Connect 才會真正建立連線。
func NewGorillaWS() *GorillaWS {
	return &GorillaWS{
		dialer: &websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
		},
	}
}

// Connect 與指定的 WebSocket 端點建立連線。
//
// 功能:
//   - 使用內部的 Dialer 依照 ctx 與 HandshakeTimeout 完成握手。
//   - 設定 ReadLimit 與初始 ReadDeadline，並安裝 PongHandler。
//   - 將成功建立的連線寫入 GorillaWS 實例。
//
// 參數:
//   - ctx: 用於控制握手過程的逾時與取消。
//   - url: WebSocket 端點 URL。
//   - hdr: 連線時附帶的 HTTP Header（如授權 token 等）。
//
// 回傳:
//   - error: 若連線失敗則回傳錯誤，成功時為 nil。
//
// 備註:
//   - 若已有舊連線存在，目前實作會直接覆蓋 conn，不會自動關閉舊連線。
func (g *GorillaWS) Connect(ctx context.Context, url string, hdr http.Header) error {
	// 建立 WS 連線：使用 Dialer 來尊重 Handshake timeout 與 ctx。
	conn, _, err := g.dialer.DialContext(ctx, url, hdr)
	if err != nil {
		return err
	}

	// 控制單一訊息最大尺寸，避免意外吃到過大的 payload。
	conn.SetReadLimit(16 << 20) // 16 MiB

	// 初始讀取 deadline：長時間無訊息時讓連線自然 timeout。
	_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))

	// 伺服器只要有回 pong，就往後延長讀取 deadline。
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})

	// 將新的連線安全地放進 GorillaWS 實例。
	g.mu.Lock()
	g.conn = conn
	g.mu.Unlock()

	return nil
}

// SendJSON 將文字（通常是 JSON）以 Text frame 送出。
//
// 功能:
//   - 透過目前的 WebSocket 連線送出一段文字訊息。
//   - 對同一連線的寫入操作進行序列化，避免 gorilla 競態限制。
//
// 參數:
//   - ctx: 目前未實際使用，只保留介面一致性，未來可擴充寫入逾時或取消邏輯。
//   - text: 要送出的文字內容（例如 JSON 字串）。
//
// 回傳:
//   - error: 若連線不存在或寫入失敗則回傳錯誤，成功時為 nil。
//
// 備註:
//   - 若尚未建立連線，會回傳 "ws 未連線" 錯誤。
//   - 設定 5 秒 WriteDeadline，避免寫入操作無限卡死。
func (g *GorillaWS) SendJSON(ctx context.Context, text string) error {
	_ = ctx // 目前未使用 ctx，只保留介面一致性

	g.mu.RLock()
	c := g.conn
	g.mu.RUnlock()
	if c == nil {
		return errors.New("ws 未連線")
	}

	// gorilla 對同一條連線的寫入必須串行，不然會觸發競態。
	g.writeMu.Lock()
	defer g.writeMu.Unlock()

	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return c.WriteMessage(websocket.TextMessage, []byte(text))
}

// Recv 阻塞等待下一則 WebSocket 訊息。
//
// 功能:
//   - 從目前的 WebSocket 連線讀取一則訊息。
//   - 在每次讀取前更新 ReadDeadline，避免長時間無流量時整條連線卡死。
//
// 參數:
//   - ctx: 目前未實際使用，只保留介面一致性，未來可擴充讀取逾時或取消邏輯。
//
// 回傳:
//   - int:   gorilla 定義的訊息型別（例如 TextMessage / BinaryMessage）。
//   - []byte: 訊息 payload 的原始內容。
//   - error: 若尚未連線或讀取失敗則回傳錯誤，成功時為 nil。
//
// 備註:
//   - 若未建立連線，會回傳 "ws 未連線" 錯誤。
//   - 目前 read timeout 固定設為 90 秒。
func (g *GorillaWS) Recv(ctx context.Context) (int, []byte, error) {
	_ = ctx // 目前未使用 ctx，只保留介面一致性

	g.mu.RLock()
	c := g.conn
	g.mu.RUnlock()
	if c == nil {
		return 0, nil, errors.New("ws 未連線")
	}

	_ = c.SetReadDeadline(time.Now().Add(90 * time.Second))
	return c.ReadMessage()
}

// Close 關閉 WebSocket 連線並釋放資源。
//
// 功能:
//   - 安全地關閉目前持有的 WebSocket 連線，並將 conn 設為 nil。
//   - 確保在關閉期間不會有其他 goroutine 同時寫入。
//
// 參數:
//   - 無。
//
// 回傳:
//   - error: 若底層 Close 發生錯誤則回傳，成功或無連線時為 nil。
//
// 備註:
//   - 多次呼叫是安全的，若 conn 已為 nil 則不做任何事。
func (g *GorillaWS) Close() error {
	// 先鎖住 writeMu，確保沒有 writer 正在操作。
	g.writeMu.Lock()
	defer g.writeMu.Unlock()

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.conn != nil {
		err := g.conn.Close()
		g.conn = nil
		return err
	}
	return nil
}

// SendPing 主動向伺服器送出 Ping 控制訊息。
//
// 功能:
//   - 透過 WriteControl 發送 Ping frame，讓對端回 Pong 以維持連線活性。
//   - 可搭配上層心跳邏輯定期呼叫。
//
// 參數:
//   - ctx: 目前未實際使用，只保留介面一致性，未來可用於控制 Ping 行為。
//
// 回傳:
//   - error: 若尚未連線或寫入失敗則回傳錯誤，成功時為 nil。
//
// 備註:
//   - 這裡使用固定 5 秒的 deadline，避免 Ping 操作卡死。
func (g *GorillaWS) SendPing(ctx context.Context) error {
	_ = ctx // 目前未使用 ctx，只保留介面一致性

	g.mu.RLock()
	c := g.conn
	g.mu.RUnlock()
	if c == nil {
		return errors.New("ws 未連線")
	}

	deadline := time.Now().Add(5 * time.Second)
	return c.WriteControl(websocket.PingMessage, []byte("ping"), deadline)
}
