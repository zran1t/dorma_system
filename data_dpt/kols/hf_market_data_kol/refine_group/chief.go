// File: data_dpt/kols/hf_market_data_kol/refine_group/chief.go
// Package: refine_group
//
// 職責 (Responsibility):
//     Refine Group 的「Chief」：
//       - 幫忙根據 exchange / feed 字串，組出 RAW.* 訂閱 subject
//       - 呼叫 Refiner.Run 去訂閱並啟動整個精煉流程
//
// 注意事項 (Notes):
//     - 這一層只處理 subject pattern 與 feed 正規化，不涉入實際解碼邏輯。

package refine_group

import (
	// === 標準函式庫 (Standard Library) ===
	"context"
	"fmt"
	"strings"

	// === 第三方套件 (Third-Party Libraries) ===
	"github.com/nats-io/nats.go"

	// === 系統內模組 (Internal Modules) ===
	"dorma_system/infra/symbols"
	marketcommonv1 "dorma_system/schemas/gen/go/market/common/v1"
)

// Chief 是 refine_group 的門面，負責把 RAW.* 主題交給 Refiner 處理。
//
// 功能:
//   - 根據 exchange / feed 字串，生成 RAW.<EX>.<FEED>.> pattern。
//   - 將這些 pattern 丟給 Refiner.Run 來訂閱並處理訊息。
//   - 對外提供 StartBy / StartEnum 兩種呼叫方式（字串版與列舉版）。
//
// 欄位說明:
//   - nc:       連到 NATS 的連線物件，用於訂閱與發佈。
//   - resolver: symbol 解析器，雖然 Chief 自己不用，但會注入到 Refiner。
//   - r:        Refiner 實例指標，負責真正的 RAW → CLEAN 邏輯。
//
// 契約 / 限制:
//   - Chief 不會自己直接訂閱 NATS，而是完全委託 Refiner。
//   - feeds 需要經過 normFeed 成功轉換，否則會回錯誤。
//
// 備註:
//   - 如果未來有多個 Refiner 實例，也可以讓 Chief 持有多組 r。
type Chief struct {
	nc       *nats.Conn
	resolver symbols.Resolver
	r        *Refiner
}

// NewChief 建立一個 Chief 實例並順便註冊一批 ExchangeAdapter。
//
// 功能:
//   - 用給定的 NATS 連線與 Resolver 建一個 Refiner。
//   - 將 ads 底下所有 adapter 全部註冊進 Refiner。
//   - 最後包成 Chief 回傳。
//
// 參數:
//   - nc:  NATS 連線，用來做訂閱與發佈。
//   - resolver: symbols.Resolver，供 Refiner 與 adapter 使用。
//   - ads: 可變參數，一組或多組 ExchangeAdapter 實作。
//
// 回傳:
//   - *Chief: 初始化完成的 Chief 實例。
//   - 無 error。
//
// 備註:
//   - 若要調整 Refiner 的去重參數，應改用 NewRefiner 的 opts 版再塞進來。
func NewChief(nc *nats.Conn, resolver symbols.Resolver, ads ...ExchangeAdapter) *Chief {
	r := NewRefiner(nc, resolver)
	for _, ad := range ads {
		r.RegisterAdapter(ad)
	}
	return &Chief{nc: nc, resolver: resolver, r: r}
}

// normFeed 將外部輸入的 feed 字串正規化，收斂成少數鍵。
//
// 功能:
//   - 將 TRADES / TRADES_ALL / trades 等字串統一成內部 key，例如 "TRADES"。
//   - 幫助 Chief 與 Refiner 之間用固定的 feed 名稱溝通。
//
// 參數:
//   - s: 來源 feed 字串。
//
// 回傳:
//   - string: 正規化後的 feed 名。
//   - bool:   是否成功對應；false 代表不支援此 feed。
//
// 備註:
//   - 大小寫 / 底線 / dash 差異會被吸收掉。
func normFeed(s string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "TRADES":
		return "TRADES", true
	case "TRADES-ALL", "TRADES_ALL":
		return "TRADES-ALL", true
	case "BBO", "BBO-TBT":
		return "BBO", true
	case "BOOK", "BOOKS":
		return "BOOK", true
	case "MARK-PRICE", "MARK":
		return "MARK-PRICE", true
	case "INDEX-TICKERS", "INDEX":
		return "INDEX-TICKERS", true
	default:
		return "", false
	}
}

// rawSubject 組出 RAW.<EX>.<FEED>.> 的訂閱 pattern。
//
// 功能:
//   - 只負責 string format，不做任何驗證。
//
// 參數:
//   - exUpper: 已經是大寫的交易所字串（例如 "OKX"）。
//   - feedUpper: 已經是正規化後的 feed 名（例如 "TRADES-ALL"）。
//
// 回傳:
//   - string: RAW.<EX>.<FEED>.> pattern 字串。
//   - 無 error。
//
// 備註:
//   - Chief.Start* 會用這個來組 NATS subject。
func rawSubject(exUpper, feedUpper string) string {
	return fmt.Sprintf("RAW.%s.%s.>", exUpper, feedUpper)
}

// StartBy 以「字串版 exchange + feeds」啟動 Refiner 訂閱流程。
//
// 功能:
//   - 檢查 feeds 是否為空，為空直接回錯。
//   - 將 exchange 正規化成大寫字串，feed 透過 normFeed 收斂。
//   - 組出一組或多組 RAW.* subject 後，呼叫 Refiner.Run。
//
// 參數:
//   - ctx:      控制整個訂閱生命週期的 context。
//   - exchange: 交易所名稱字串，例如 "okx"。
//   - feeds:    feed 名稱列表。
//
// 回傳:
//   - error: feeds 為空、不支援的 feed、或 Refiner.Run 出錯時回傳錯誤；成功啟動時為 nil。
//
// 備註:
//   - 這個方法偏向「人類友善」，適合 CLI / config 驅動。
func (c *Chief) StartBy(ctx context.Context, exchange string, feeds []string) error {
	if len(feeds) == 0 {
		return fmt.Errorf("feeds must not be empty")
	}
	ex := strings.ToUpper(strings.TrimSpace(exchange))

	var subjs []string
	for _, f := range feeds {
		fd, ok := normFeed(f)
		if !ok {
			return fmt.Errorf("unsupported feed %q for %s", f, ex)
		}
		subjs = append(subjs, rawSubject(ex, fd))
	}
	return c.r.Run(ctx, subjs...)
}

// StartEnum 以 Exchange 列舉 + feeds 啟動 Refiner 訂閱流程。
//
// 功能:
//   - 和 StartBy 類似，只是 exchange 改用列舉型別。
//   - 內部會把列舉字串 EXCHANGE_XYZ 轉成 "XYZ" 當成 exchange 字串來組 subject。
//
// 參數:
//   - ctx:  控制生命週期的 context。
//   - ex:   marketcommonv1.Exchange 列舉值。
//   - fds:  feed 名稱列表。
//
// 回傳:
//   - error: feeds 為空、不支援 feed、或 Refiner.Run 失敗時回傳錯誤；否則為 nil。
//
// 備註:
//   - 適合上游已經用 protobuf 列舉在傳遞 exchange 的情境。
func (c *Chief) StartEnum(ctx context.Context, ex marketcommonv1.Exchange, fds []string) error {
	if len(fds) == 0 {
		return fmt.Errorf("feeds empty")
	}
	exStr := strings.ToUpper(strings.TrimPrefix(ex.String(), "EXCHANGE_"))

	var subjs []string
	for _, f := range fds {
		fd, ok := normFeed(f)
		if !ok {
			return fmt.Errorf("unsupported feed %q for %s", f, exStr)
		}
		subjs = append(subjs, rawSubject(exStr, fd))
	}
	return c.r.Run(ctx, subjs...)
}
