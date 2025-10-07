package ws

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type GorillaWS struct {
	dialer  *websocket.Dialer // 執行連線的工具物件
	conn    *websocket.Conn   // 連線到的物件
	mu      sync.RWMutex      // 保護鎖 避免同時寫入或是同時讀跟寫
	writeMu sync.Mutex        // 串行化對同一個conn 的WriteMessage
}

// 初始化建立 GorillaWS 實例
func NewGorillaWS() *GorillaWS {
	return &GorillaWS{dialer: &websocket.Dialer{HandshakeTimeout: 10 * time.Second}}
}

// 使用初始化完後的 GorillaWS 實例 建立與ws端點的連線
func (g *GorillaWS) Connect(ctx context.Context, url string, hdr http.Header) error {
	conn, _, err := g.dialer.DialContext(ctx, url, hdr) // 用g(GorillaWS)裡面的dialer去連線 (生命週期管理,端點url,header)
	if err != nil {                                     // 有錯誤就return 會報錯誤 停止進程
		return err
	}
	conn.SetReadLimit(16 << 20)                                // 單一訊息超過 16mb 就會直接丟棄   20 -> 1mb的意思
	_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second)) // 設定超時 如果90秒沒有訊息就會自動超時

	conn.SetPongHandler(func(string) error { // 確定伺服器有回傳pong就會延長 超時時間90秒
		_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		return nil
	})
	// 把物件存進GorillaWS
	g.mu.Lock()   // 先鎖住避免多進程打架
	g.conn = conn // 放進成功連線到的物件
	g.mu.Unlock() // 放進去後解鎖
	return nil
}

// 使用初始化完後的 GorillaWS 實例 傳入資料 此為訂閱封包（json)
func (g *GorillaWS) SendJSON(ctx context.Context, text string) error {
	g.mu.RLock()   // 閱讀鎖 讓多個goroutine可以同時讀 但不能寫
	c := g.conn    // 把連線物件讀出來
	g.mu.RUnlock() // 把閱讀鎖解開
	if c == nil {  // 如果沒取到連線物件 就代表還沒連線
		return errors.New("ws 未連線")
	}
	g.writeMu.Lock()               // ⬅️ 串行化寫入
    defer g.writeMu.Unlock()
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))    // 設定這次寫入操作最多等待五秒 超過就停止避免goroutine永遠卡死
	return c.WriteMessage(websocket.TextMessage, []byte(text)) // 把訂閱請求送出去 websocket.TextMessage 文字框架 內容是json字串的bytes
}

// 使用初始化完後的 GorillaWS 實例 接收回傳的封包
func (g *GorillaWS) Recv(ctx context.Context) (int, []byte, error) {
	g.mu.RLock()   // 鎖上閱讀鎖
	c := g.conn    // 抓連線物件
	g.mu.RUnlock() // 解開閱讀鎖
	if c == nil {  // 抓不到ws連線物件就回傳為連線
		return 0, nil, errors.New("ws 未連線")
	}
	_ = c.SetReadDeadline(time.Now().Add(90 * time.Second))
	return c.ReadMessage() // 阻塞等待接收下一個封包
}
	
// 關閉
func (g *GorillaWS) Close() error {
	g.writeMu.Lock()        // 確保沒有 writer 正在寫
    defer g.writeMu.Unlock()

	g.mu.Lock()         // 上鎖
	defer g.mu.Unlock() // 確保最後一定會解鎖 避免中間卡住 後面沒解鎖到

	if g.conn != nil {  // 先看是否連線物件還有 無就執行關閉
		err := g.conn.Close() // 關閉ws連線物
		g.conn = nil          // 把連線物件設為空值
		return err
	}
	return nil
}

// 主動ping ws端
func (g *GorillaWS) SendPing(ctx context.Context) error {
    g.mu.RLock()
    c := g.conn
    g.mu.RUnlock()
    if c == nil {
        return errors.New("ws 未連線")
    }
    // Ping 控制幀，5 秒 timeout
    deadline := time.Now().Add(5 * time.Second)
    return c.WriteControl(websocket.PingMessage, []byte("ping"), deadline)
}