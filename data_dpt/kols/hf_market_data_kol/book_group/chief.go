// File: data_dpt/kols/hf_market_data_kol/book_group/chief.go
// Package: book_group
//
// 職責 (Responsibility):
//     負責訂閱 OKX BOOK.DELTA 流，維護一組 OrderBook 池，並控制是否要節流發出 BOOK.FULL。
//     - 建立/持有單一 NATS 訂閱 (sub)
//     - 把每一筆 delta 交給 handleDelta 處理
//
// 注意事項 (Notes):
//     - Chief 本身不做簿邏輯，簿細節交給 OrderBook。
//     - Stop 只會取消訂閱，不會關閉 NATS 連線。

package book_group

import (
	// === 標準函式庫 (Standard Library) ===
	"log"
	"sync"
	"time"

	// === 第三方套件 (Third-Party Libraries) ===
	"github.com/nats-io/nats.go"
	// === 系統內模組 (Internal Modules) ===
	// 無
)

// Chief 負責管理多檔 order book 的聚合與輸出。
//
// 功能:
//   - 持有 NATS 訂閱，接收 BOOK.DELTA 流。
//   - 管理每檔 symbol 對應的 OrderBook 實例。
//   - 控制 BOOK.FULL 的發佈頻率（throttle）。
//   - CRC 驗證失敗時標記 needRefresh，等待上游重新拉 snapshot。
//
// 欄位說明:
//   - nc:        NATS 連線，用來訂閱/發佈訊息。
//   - mu:        保護 books map 的互斥鎖。
//   - books:     symbol → OrderBook 的快取池。
//   - throttle:  BOOK.FULL 最短間隔；0 代表每次更新都發。
//   - verifyCRC: 是否啟用 OKX CRC32 驗證邏輯。
//   - sub:       當前 NATS 訂閱物件，用於 Stop 時取消訂閱。
//
// 契約 / 限制:
//   - 一個 Chief 目前只會建立一個 subject 訂閱 (subjDeltaWildcard)。
//   - books map 不會自動清理，長時間跑需要外層考慮是否要加淘汰策略。
//
// 備註:
//   - Chief 是 long-running component，Start 之後預期會常駐。
type Chief struct {
	nc        *nats.Conn
	mu        sync.Mutex
	books     map[string]*OrderBook
	throttle  time.Duration // 每檔 FULL 最短間隔（0=不節流）
	verifyCRC bool          // 是否做 OKX CRC32 驗證
	sub       *nats.Subscription
}

// NewChief 建立一個新的 Chief 實例。
//
// 功能:
//   - 初始化 books map 與基本參數。
//   - 預設 verifyCRC = true。
//
// 參數:
//   - nc:       NATS 連線實例。
//   - throttle: 控制 BOOK.FULL 發佈的節流間隔；0 代表無節流。
//
// 回傳:
//   - *Chief: 初始化完成的 Chief 實例指標。
//   - 無 error。
//
// 備註:
//   - 本函式不會建立 NATS 訂閱，需額外呼叫 Start() 才會開始運作。
func NewChief(nc *nats.Conn, throttle time.Duration) *Chief {
	return &Chief{
		nc:        nc,
		books:     make(map[string]*OrderBook),
		throttle:  throttle,
		verifyCRC: true,
	}
}

// Start 啟動 Chief，訂閱 BOOK.DELTA 主題。
//
// 功能:
//   - 呼叫 nc.Subscribe(subjDeltaWildcard, ...) 建立訂閱。
//   - 每當收到 NATS 訊息時，呼叫 handleDelta 做簿更新與 FULL 發佈。
//   - 將 subscription 存進 c.sub，供 Stop 時取消。
//
// 參數:
//   - 無。
//
// 回傳:
//   - error: 訂閱失敗時回傳錯誤；成功時為 nil。
//
// 備註:
//   - Start 不會阻塞；訂閱 callback 由 NATS 內部 goroutine 執行。
//   - 重複呼叫 Start 目前沒有特別防護，外層應避免重複啟動。
func (c *Chief) Start() error {
	sub, err := c.nc.Subscribe(subjDeltaWildcard, func(m *nats.Msg) {
		if err := c.handleDelta(m); err != nil {
			c.logf("[book_group] handleDelta error: %v", err)
		}
	})
	if err != nil {
		return err
	}
	c.sub = sub
	c.logf("book_group started: delta=%s full=CLEAN.OKX.BOOK.FULL.<BASE>.<QUOTE>.<SUF>", subjDeltaWildcard)
	return nil
}

// Stop 停止 Chief，取消 NATS 訂閱。
//
// 功能:
//   - 若已建立 subscription，呼叫 Unsubscribe() 並清空 c.sub。
//
// 參數:
//   - 無。
//
// 回傳:
//   - 無（Unsubscribe 的錯誤目前被忽略）。
//
// 備註:
//   - 不會關閉 NATS 連線，也不會清空 books map。
func (c *Chief) Stop() {
	if c.sub != nil {
		_ = c.sub.Unsubscribe()
		c.sub = nil
	}
}

// logf 是 book_group 專用的 log helper。
//
// 功能:
//   - 將 log.Printf 封裝起來，未來若要導到其他 logger 比較好改。
//
// 參數:
//   - format: 格式化字串，與 fmt.Printf 相同。
//   - args:   對應的參數列表。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 目前直接轉呼叫標準庫 log.Printf。
func (c *Chief) logf(format string, args ...any) {
	log.Printf(format, args...)
}
