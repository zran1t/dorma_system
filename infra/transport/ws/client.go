// File: infra/transport/ws/client.go
// Package: ws
//
// 職責 (Responsibility):
//     封裝基於 gorilla/websocket 的 WebSocket 客戶端實作。
//     提供連線管理、訊息收發、ping 心跳與安全關閉等基礎能力。
//
// 注意事項 (Notes):
//     - 透過 RWMutex + 寫入序列化機制確保多 goroutine 下的讀寫安全。
//     - ctx 已用於控制：等待寫入鎖的取消，以及 WriteDeadline 的上限（取 min(預設 timeout, ctx.Deadline)）；
//       ReadDeadline 會在每次 Recv 進入讀取前設定一次，但可能因 PongHandler 刷新而延後。
//     - gorilla 的阻塞 I/O 不會因 ctx.Done 直接中止；需透過 deadline timeout 或 Close() 來打斷。
//     - 單一訊息設有最大長度限制，避免吃下異常大的 payload。
//     - ctx 的 Done 只影響「等待 writeSem」與「deadline 上限」；不保證立刻中止 gorilla 的 ReadMessage/WriteMessage，
//       若需立即中止，需依賴 deadline 到期或由上層呼叫 Close()。

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

const (
	handshakeTimeout = 10 * time.Second

	maxMessageSize = 16 << 20 // 16 MiB

	readTimeout  = 90 * time.Second
	writeTimeout = 5 * time.Second
	pingTimeout  = 5 * time.Second
)

// GorillaWS 是基於 gorilla/websocket 的 WSClient 實作。
//
// 功能:
//   - 管理 WebSocket 連線的建立、收發與關閉。
//   - 透過鎖避免多 goroutine 同時寫入造成競態問題。
//
// 欄位說明:
//   - dialer:    用於建立 WebSocket 連線的 gorilla Dialer 實例。
//   - conn:      當前建立好的 WebSocket 連線指標，尚未連線時為 nil。
//   - mu:        保護 conn 讀寫的 RWMutex，允許多讀單寫。
//   - writeOnce: 確保 writeSem 只被初始化一次，避免非 NewGorillaWS 建構造成 nil semaphore。
//   - writeSem:  寫入序列化 semaphore（容量 1），用來串行化所有寫入（WriteMessage / WriteControl）；支援 ctx 取消等待鎖。
//
// 契約 / 限制:
//   - 所有對 conn 的讀寫都必須透過 mu 保護，避免資料競態。
//   - 所有對 conn 的寫入都必須透過 writeSem 串行化，避免 gorilla concurrent write。
//   - Close 之後 conn 會被設為 nil，不可再使用舊連線物件。
//
// 備註:
//   - 不直接處理業務邏輯，只專注在傳輸層行為。
//   - Connect 會在成功建立新連線後，自動替換 conn 並關閉舊連線，避免資源殘留；替換過程會與 writer 串行化。
//   - Connect/Close 會在完成 swap 後才關閉舊 conn，以縮短 writeSem 的持有時間。
type GorillaWS struct {
	dialer *websocket.Dialer
	conn   *websocket.Conn
	mu     sync.RWMutex

	writeOnce sync.Once
	writeSem  chan struct{}
}

// NewGorillaWS 建立一個預設設定的 GorillaWS 實例。
//
// 功能:
//   - 初始化 gorilla Dialer，設定基礎 Handshake timeout。
//   - 初始化寫入序列化機制（writeSem），確保後續寫入可被串行化。
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
	g := &GorillaWS{
		dialer: &websocket.Dialer{
			HandshakeTimeout: handshakeTimeout,
		},
	}
	g.initWriteSem()
	return g
}

// initWriteSem 初始化寫入序列化 semaphore。
//
// 功能:
//   - 使用 sync.Once 確保 writeSem 只會初始化一次。
//   - writeSem 容量為 1，並預先放入 token，作為可取得的寫入鎖。
//
// 參數:
//   - 無。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 若 GorillaWS 是以 &GorillaWS{} 非 NewGorillaWS 建構，仍可透過此函式確保 writeSem 可用。
func (g *GorillaWS) initWriteSem() {
	g.writeOnce.Do(func() {
		g.writeSem = make(chan struct{}, 1)
		g.writeSem <- struct{}{}
	})
}

// lockWrite 取得寫入序列化鎖（支援 ctx 取消）。
//
// 功能:
//   - 等待取得 writeSem token，以串行化後續的寫入行為。
//   - 若 ctx.Done 先觸發，則回傳 ctx.Err，避免無限等待寫入鎖。
//
// 參數:
//   - ctx: 控制等待寫入鎖的逾時與取消。
//
// 回傳:
//   - error: 取得鎖成功為 nil；ctx 取消/逾時則回傳 ctx.Err。
//
// 備註:
//   - 取得成功後，呼叫端必須搭配 unlockWrite() 釋放 token。
//   - 若 ctx 為 nil，會視為 context.Background()，避免 nil dereference。
func (g *GorillaWS) lockWrite(ctx context.Context) error {
	g.initWriteSem()
	if ctx == nil {
		ctx = context.Background()
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.writeSem:
		return nil
	}
}

// unlockWrite 釋放寫入序列化鎖。
//
// 功能:
//   - 將 token 放回 writeSem，允許下一個 writer 進入寫入區段。
//
// 參數:
//   - 無。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 呼叫端必須確保 unlockWrite 與 lockWrite 成對使用，避免 token 遺失造成永久阻塞。
func (g *GorillaWS) unlockWrite() {
	g.initWriteSem()
	g.writeSem <- struct{}{}
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
//   - 若已有舊連線存在，會在成功建立新連線後替換 conn，並自動關閉舊連線（與 writer 串行化）。
//   - swap 完成後才會關閉舊 conn，以縮短 writeSem 持有時間。
//   - 若 ctx 為 nil，會視為 context.Background()，避免 DialContext 使用 nil ctx。
func (g *GorillaWS) Connect(ctx context.Context, url string, hdr http.Header) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// 建立 WS 連線：使用 Dialer 來尊重 Handshake timeout 與 ctx。
	conn, _, err := g.dialer.DialContext(ctx, url, hdr)
	if err != nil {
		return err
	}

	// 控制單一訊息最大尺寸，避免意外吃到過大的 payload。
	conn.SetReadLimit(maxMessageSize)

	// 初始讀取 deadline：長時間無訊息時讓連線自然 timeout。
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))

	// 伺服器只要有回 pong，就往後延長讀取 deadline。
	// 注意：此刷新可能延後 Recv 設定的更早 ReadDeadline，因此 ctx.Deadline 並非硬上限；
	// 若需要「ctx.Done 立即中止讀取」，由上層在 ctx.Done 時呼叫 Close()。
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})

	// 替換 conn 前先取得寫入序列化鎖，避免替換期間仍有 writer 操作舊連線。
	if err := g.lockWrite(ctx); err != nil {
		_ = conn.Close() // 新連線已建立但無法進行替換，避免 leak
		return err
	}

	unlocked := false
	defer func() {
		if !unlocked {
			g.unlockWrite()
		}
	}()

	// swap：用 mu 保護 conn 指標的一致性。
	var old *websocket.Conn
	g.mu.Lock()
	old = g.conn
	g.conn = conn
	g.mu.Unlock()

	// swap 完成即可釋放寫入鎖，避免把舊連線 close 的時間算進 writer 的臨界區。
	g.unlockWrite()
	unlocked = true

	// 關閉舊連線：在鎖外進行 I/O，避免拖慢後續 writer。
	if old != nil {
		_ = old.Close()
	}

	return nil
}

// SendJSON 將文字（通常是 JSON）以 Text frame 送出。
//
// 功能:
//   - 透過目前的 WebSocket 連線送出一段文字訊息。
//   - 對同一連線的寫入操作進行序列化，避免 gorilla 競態限制。
//   - 尊重 ctx：等待寫入鎖可取消；若 ctx 有 deadline，會縮短 WriteDeadline 上限。
//
// 參數:
//   - ctx: 用於控制等待寫入鎖與寫入操作的逾時/取消（包含 deadline 與 Done）。
//   - text: 要送出的文字內容（例如 JSON 字串）。
//
// 回傳:
//   - error: 若連線不存在、ctx 取消/逾時、或寫入失敗則回傳錯誤；成功時為 nil。
//
// 備註:
//   - 若尚未建立連線，會回傳 "ws 未連線" 錯誤。
//   - 寫入 deadline 會取較早者：min(預設 writeTimeout, ctx.Deadline)。
//   - ctx.Done 不保證立刻中止 WriteMessage；若需立即中止，需依賴 deadline 到期或由上層 Close()。
//   - 為確保一致性：取得寫入鎖後才讀取 conn，避免寫入落到舊連線。
func (g *GorillaWS) SendJSON(ctx context.Context, text string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := g.lockWrite(ctx); err != nil {
		return err
	}
	defer g.unlockWrite()

	// 取得連線指標：在 write lock 範圍內讀取 conn，確保使用的是最新 conn。
	g.mu.RLock()
	c := g.conn
	g.mu.RUnlock()
	if c == nil {
		return errors.New("ws 未連線")
	}

	// 寫入 deadline：取較早者（預設 writeTimeout vs ctx deadline）。
	deadline := time.Now().Add(writeTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = c.SetWriteDeadline(deadline)

	return c.WriteMessage(websocket.TextMessage, []byte(text))
}

// SendPing 主動向伺服器送出 Ping 控制訊息。
//
// 功能:
//   - 透過 WriteControl 發送 Ping frame，讓對端回 Pong 以維持連線活性。
//   - 對同一連線的寫入操作進行序列化，避免 gorilla 競態限制。
//   - 尊重 ctx：等待寫入鎖可取消；若 ctx 有 deadline，會縮短 WriteDeadline 上限。
//
// 參數:
//   - ctx: 用於控制等待寫入鎖與 Ping 寫入的逾時/取消（包含 deadline 與 Done）。
//
// 回傳:
//   - error: 若尚未連線、ctx 取消/逾時、或寫入失敗則回傳錯誤；成功時為 nil。
//
// 備註:
//   - Ping deadline 會取較早者：min(預設 pingTimeout, ctx.Deadline)。
//   - ctx.Done 不保證立刻中止 WriteControl；若需立即中止，需依賴 deadline 到期或由上層 Close()。
//   - 為確保一致性：取得寫入鎖後才讀取 conn，避免寫入落到舊連線。
func (g *GorillaWS) SendPing(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := g.lockWrite(ctx); err != nil {
		return err
	}
	defer g.unlockWrite()

	// 取得連線指標：在 write lock 範圍內讀取 conn，確保使用的是最新 conn。
	g.mu.RLock()
	c := g.conn
	g.mu.RUnlock()
	if c == nil {
		return errors.New("ws 未連線")
	}

	deadline := time.Now().Add(pingTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = c.SetWriteDeadline(deadline)

	return c.WriteControl(websocket.PingMessage, []byte("ping"), deadline)
}

// Recv 阻塞等待下一則 WebSocket 訊息。
//
// 功能:
//   - 從目前的 WebSocket 連線讀取一則訊息。
//   - 在每次讀取前更新 ReadDeadline，避免長時間無流量時整條連線卡死。
//   - 若 ctx 有 deadline，會先用它縮短本次 ReadDeadline 的初始值。
//
// 參數:
//   - ctx: 用於控制讀取 deadline 的上限（包含 deadline）。
//
// 回傳:
//   - int:    gorilla 定義的訊息型別（例如 TextMessage / BinaryMessage）。
//   - []byte: 訊息 payload 的原始內容。
//   - error:  若尚未連線或讀取失敗則回傳錯誤；成功時為 nil。
//
// 備註:
//   - 若未建立連線，會回傳 "ws 未連線" 錯誤。
//   - 讀取 deadline 會先取較早者：min(預設 readTimeout, ctx.Deadline)。
//   - 若連線啟用 PongHandler，對端回 pong 可能刷新 ReadDeadline（延長 readTimeout），因此 ctx.Deadline 並非硬上限。
//   - 若需「ctx.Done 立刻打斷阻塞讀取」，建議由上層在 ctx.Done 時呼叫 Close() 來中止 ReadMessage。
func (g *GorillaWS) Recv(ctx context.Context) (int, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	g.mu.RLock()
	c := g.conn
	g.mu.RUnlock()
	if c == nil {
		return 0, nil, errors.New("ws 未連線")
	}

	// 讀取 deadline：先取較早者（預設 readTimeout vs ctx deadline）。
	deadline := time.Now().Add(readTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = c.SetReadDeadline(deadline)

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
//   - Close 不接受 ctx：會阻塞等待 writeSem token，以確保沒有 writer 正在進行。
//   - swap 完成後才會關閉舊 conn，以縮短 writeSem 持有時間。
func (g *GorillaWS) Close() error {
	g.initWriteSem()

	// 取得寫入鎖：確保沒有 writer 正在進行。
	<-g.writeSem

	// swap：在寫入鎖保護下把 conn 設成 nil，確保不會再有新 writer 取得舊 conn。
	var c *websocket.Conn
	g.mu.Lock()
	c = g.conn
	g.conn = nil
	g.mu.Unlock()

	// 釋放 token，縮短 writer 的阻塞時間。
	g.writeSem <- struct{}{}

	if c != nil {
		return c.Close()
	}
	return nil
}
