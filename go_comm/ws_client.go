// data_dpt/go_comm/ws_client.go
// 說明: 上游 WebSocket 訂閱抽象層（讀取 → 交由回呼處理）
// 功能: 建立連線、心跳保活、自動重連、讀取訊息（[]byte 原樣交付）
// 注意: 僅處理「連線/讀取」，資料解碼與後續發到 NATS 由呼叫端負責
// 最後修改: 2025-09-12 by AI Writer (Ticket ID: GO-COMM-WS-0001)

package go_comm

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WSClientConfig
// 說明: WebSocket 連線設定（常用項目精簡）
// 備註: Header 可放授權資訊（例如 API-Key）
type WSClientConfig struct {
	URL              string            // 例如: wss://stream.binance.com:9443/ws/btcusdt@trade
	Header           map[string]string // 自定 HTTP Header
	HandshakeTimeout time.Duration     // 預設 5s
	DialTimeout      time.Duration     // 預設 5s（TCP 連線）
	ReadLimit        int64             // 每則訊息最大位元組數（預設 1MB）
	PingInterval     time.Duration     // 發送 ping 間隔（預設 15s）
	PongWait         time.Duration     // 等待 pong 超時（預設 45s）
	ReconnectBackoff time.Duration     // 重連初始間隔（指數退避，預設 1s）
	TLSConfig        *tls.Config       // 若需客製 TLS
}

// WSClient
// 說明: 管理單一 WS 連線與讀取 loop
type WSClient struct {
	cfg    WSClientConfig
	dialer *websocket.Dialer

	mu   sync.Mutex
	conn *websocket.Conn

	closed  chan struct{}
	started bool
}

// NewWSClient: 產生一個 WSClient（未連線）
func NewWSClient(cfg WSClientConfig) *WSClient {
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = 5 * time.Second
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ReadLimit == 0 {
		cfg.ReadLimit = 1 << 20 // 1MB
	}
	if cfg.PingInterval == 0 {
		cfg.PingInterval = 15 * time.Second
	}
	if cfg.PongWait == 0 {
		cfg.PongWait = 45 * time.Second
	}
	if cfg.ReconnectBackoff == 0 {
		cfg.ReconnectBackoff = 1 * time.Second
	}

	d := &websocket.Dialer{
		HandshakeTimeout: cfg.HandshakeTimeout,
		TLSClientConfig:  cfg.TLSConfig,
		Proxy:            http.ProxyFromEnvironment,
	}

	return &WSClient{
		cfg:    cfg,
		dialer: d,
		closed: make(chan struct{}),
	}
}

// Start
// 說明: 啟動 WS 連線 + 讀取 loop；若中斷會自動重連（指數退避）
// 參數:
//   - ctx: 取消時會關閉連線與停止 loop
//   - onMessage: 每當收到一則上游訊息（text/binary），把原始 bytes 傳給你
//   - onError: 非致命錯誤回報（例如短暫斷線/重連等）
//
// 回傳: error（僅在「初次連線就無法建立」時回傳）
func (c *WSClient) Start(ctx context.Context, onMessage func([]byte), onError func(error)) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("WSClient 已啟動")
	}
	c.started = true
	c.mu.Unlock()

	// 初次連線（若失敗，直接回錯）
	if err := c.connect(); err != nil {
		return fmt.Errorf("WS 初次連線失敗: %w", err)
	}

	// 背景：讀取 + 心跳 + 重連
	go c.run(ctx, onMessage, onError)
	return nil
}

// Close: 主動關閉
func (c *WSClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
		// 已關閉
	default:
		close(c.closed)
		if c.conn != nil {
			_ = c.conn.Close()
			c.conn = nil
		}
	}
}

// --- 內部實作 ---

// connect: 嘗試建立連線
func (c *WSClient) connect() error {
	h := http.Header{}
	for k, v := range c.cfg.Header {
		h.Set(k, v)
	}
	// Dial 加上總體超時
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.DialTimeout)
	defer cancel()

	conn, _, err := c.dialer.DialContext(ctx, c.cfg.URL, h)
	if err != nil {
		return err
	}

	conn.SetReadLimit(c.cfg.ReadLimit)
	// Pong handler：收到 pong 就延長期限
	_ = conn.SetReadDeadline(time.Now().Add(c.cfg.PongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(c.cfg.PongWait))
	})

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return nil
}

// run: 讀取 loop + 心跳 + 自動重連
func (c *WSClient) run(ctx context.Context, onMessage func([]byte), onError func(error)) {
	backoff := c.cfg.ReconnectBackoff

	pingTicker := time.NewTicker(c.cfg.PingInterval)
	defer pingTicker.Stop()

readLoop:
	for {
		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()

		// 無連線（例如剛重連失敗）→ 等待下一輪
		if conn == nil {
			select {
			case <-time.After(backoff):
				if err := c.connect(); err != nil {
					if onError != nil {
						onError(fmt.Errorf("WS 重連失敗: %w", err))
					}
					// 指數退避（上限 30s）
					if backoff < 30*time.Second {
						backoff *= 2
					}
					continue
				}
				// 重連成功 → 重置退避
				backoff = c.cfg.ReconnectBackoff
				pingTicker.Reset(c.cfg.PingInterval)
				continue
			case <-ctx.Done():
				c.Close()
				return
			case <-c.closed:
				return
			}
		}

		// 有連線 → 讀取與心跳
		for {
			select {
			case <-pingTicker.C:
				// 送 ping；若失敗視為斷線
				if err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(3*time.Second)); err != nil {
					if onError != nil {
						onError(fmt.Errorf("WS ping 失敗，將重連: %w", err))
					}
					_ = conn.Close()
					c.mu.Lock()
					c.conn = nil
					c.mu.Unlock()
					continue readLoop
				}
			case <-ctx.Done():
				c.Close()
				return
			case <-c.closed:
				return
			default:
				// 讀取一則訊息（text/binary 均可）
				msgType, data, err := conn.ReadMessage()
				if err != nil {
					if onError != nil {
						onError(fmt.Errorf("WS 讀取錯誤，將重連: %w", err))
					}
					_ = conn.Close()
					c.mu.Lock()
					c.conn = nil
					c.mu.Unlock()
					continue readLoop
				}
				// 只轉發 text/binary；其他控制訊息忽略
				if msgType == websocket.TextMessage || msgType == websocket.BinaryMessage {
					if onMessage != nil {
						onMessage(data)
					}
				}
			}
		}
	}
}
