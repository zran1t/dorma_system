// File: infra/symbols/resolver.go
// Package: symbols
//
// 職責 (Responsibility):
//     定義 symbol 解析介面 (Resolver) 與 inMemoryResolver 實作。
//     負責 canonical ↔ exchange native symbol 的查找與反查，不碰 YAML 與 IO。
//
// 注意事項 (Notes):
//     - 不處理任何磁碟 / YAML 載入邏輯（改由 loader.go 或外部元件負責）。
//     - inMemoryResolver 使用 RWMutex，併發使用時須透過指標傳遞。

package symbols

import (
	// === 標準函式庫 (Standard Library) ===
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Resolver 定義 symbol 解析與反解析的介面。
//
// 功能:
//   - 提供 canonical → exchange native 與 native → canonical 的查詢能力。
//   - 支援單筆與批次解析。
//
// 契約 / 限制:
//   - 若查詢不到對應資料，須回傳 ErrNotFound 包裝在錯誤內。
//   - BatchCanonicalToNative 應維持輸出順序與輸入 canonical 順序一致。
//
// 備註:
//   - inMemoryResolver 為此介面的其中一種實作，亦可替換為其他 back-end（例如 Redis）。
type Resolver interface {
	// CanonicalToNative 將單一 canonical 轉成指定交易所的原生 symbol。
	CanonicalToNative(canonical, exchange string) (string, error)

	// BatchCanonicalToNative 批次將多個 canonical 轉成指定交易所的原生 symbol。
	// 輸出順序必須與輸入的 canonicals slice 順序一致。
	BatchCanonicalToNative(canonicals []string, exchange string) ([]string, error)

	// NativeToCanonical 將交易所原生 symbol 反查為 canonical。
	// 例如 OKX 的 "BTC-USDT-SWAP" 反查為 "BTC-USDT-SWAP"（或其他系統 canonical）。
	NativeToCanonical(exchange, native string) (string, error)
}

// inMemoryStore 是 inMemoryResolver 使用的底層儲存結構。
//
// 功能:
//   - 以 canonical 為第一層 key，交易所名稱為第二層 key，value 為原生 symbol。
//
// 欄位說明:
//   - map[string]map[string]string:
//     第一層 key: canonical（大寫字串）。
//     第二層 key: exchange 名稱（小寫字串）。
//     value:      對應的原生 symbol。
//
// 備註:
//   - 不匯出，僅供 inMemoryResolver 內部使用。
type inMemoryStore map[string]map[string]string

// inMemoryResolver 是以記憶體 map 為基礎的 Resolver 實作。
//
// 功能:
//   - 將 canonical 與各交易所原生 symbol 的對照存放於記憶體中。
//   - 提供 register / CanonicalToNative / NativeToCanonical 等基本操作。
//
// 欄位說明:
//   - mu:    讀寫鎖，確保並發存取時的安全性。
//   - store: 實際儲存 canonical 與原生 symbol 對應的 inMemoryStore。
//
// 契約 / 限制:
//   - 所有讀寫 store 的操作都必須透過 mu 保護。
//   - 不處理持久化，程式重啟後資料需重新載入。
//
// 備註:
//   - 適合作為小型系統或測試環境的 resolver 實作。
type inMemoryResolver struct {
	mu    sync.RWMutex
	store inMemoryStore
}

// NewInMemoryResolver 建立一個新的 inMemoryResolver 實例。
//
// 功能:
//   - 初始化內部的 inMemoryStore，準備接收後續註冊資料。
//
// 參數:
//   - 無。
//
// 回傳:
//   - *inMemoryResolver: 新建立的 resolver 實例。
//
// 備註:
//   - 典型使用方式為啟動時建立一個實例，並搭配外部 loader 或手動 register。
func NewInMemoryResolver() *inMemoryResolver {
	return &inMemoryResolver{store: make(inMemoryStore)}
}

// register 註冊單一 canonical 與交易所原生 symbol 的對應。
//
// 功能:
//   - 將傳入的 canonical / exchange / native 正規化後寫入 store。
//   - 若該 canonical 尚未存在，會自動建立新的 row。
//
// 參數:
//   - canonical: 標準化交易對代碼字串，會轉成大寫並 TrimSpace。
//   - exchange:  交易所名稱，會轉成小寫並 TrimSpace。
//   - native:    該交易所在此 canonical 對應的原生 symbol，會 TrimSpace。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 若同一 canonical + exchange 重複註冊，後一次會覆蓋前一次。
func (r *inMemoryResolver) register(canonical, exchange, native string) {
	c := strings.ToUpper(strings.TrimSpace(canonical))
	e := strings.ToLower(strings.TrimSpace(exchange))
	n := strings.TrimSpace(native)

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.store[c]; !ok {
		r.store[c] = make(map[string]string)
	}
	r.store[c][e] = n
}

// ErrNotFound 表示未找到對應的標準標的物代碼。
//
// 功能:
//   - 作為 Resolver 實作在查無對應資料時包裝使用的基礎錯誤。
//
// 備註:
//   - 外層可使用 errors.Is(err, ErrNotFound) 判斷是否為「找不到」類錯誤。
var ErrNotFound = errors.New("未找到對應的標準標的物代碼")

// CanonicalToNative 將 canonical 轉成指定交易所的原生 symbol。
//
// 功能:
//   - 依照 canonical 與 exchange 在 store 中查詢對應的原生 symbol。
//
// 參數:
//   - canonical: 標準化交易對代碼字串，會轉成大寫並 TrimSpace。
//   - exchange:  交易所名稱，會轉成小寫並 TrimSpace。
//
// 回傳:
//   - string: 查詢成功時的原生 symbol，失敗時為空字串。
//   - error:  若無對應資料，回傳包含 ErrNotFound 的錯誤。
//
// 備註:
//   - canonical 與 exchange 必須已先透過內部 register 或 loader 註冊過。
func (r *inMemoryResolver) CanonicalToNative(canonical, exchange string) (string, error) {
	c := strings.ToUpper(strings.TrimSpace(canonical))
	e := strings.ToLower(strings.TrimSpace(exchange))

	r.mu.RLock()
	defer r.mu.RUnlock()

	row, ok := r.store[c]
	if !ok {
		return "", fmt.Errorf("%w: canonical=%s", ErrNotFound, c)
	}
	native, ok := row[e]
	if !ok || native == "" {
		return "", fmt.Errorf("%w: canonical=%s exchange=%s", ErrNotFound, c, e)
	}
	return native, nil
}

// BatchCanonicalToNative 批次解析多個 canonical。
//
// 功能:
//   - 依序對輸入 slice 中的每一個 canonical 呼叫 CanonicalToNative。
//   - 維持輸出順序與輸入順序一致。
//
// 參數:
//   - canonicals: 要解析的 canonical 清單。
//   - exchange:   交易所名稱。
//
// 回傳:
//   - []string: 已成功解析的原生 symbol 清單，長度可能小於 canonicals（若中途遇錯）。
//   - error:    若其中一筆解析失敗，回傳對應錯誤，並保留已成功解析的部分結果。
//
// 備註:
//   - 一旦遇到錯誤就停止後續解析，呼叫端可依需求自行決定是否重試或略過。
func (r *inMemoryResolver) BatchCanonicalToNative(canonicals []string, exchange string) ([]string, error) {
	out := make([]string, 0, len(canonicals))
	for _, c := range canonicals {
		n, err := r.CanonicalToNative(c, exchange)
		if err != nil {
			return out, err
		}
		out = append(out, n)
	}
	return out, nil
}

// NativeToCanonical 將交易所原生 symbol 反查為 canonical。
//
// 功能:
//   - 針對指定交易所與原生 symbol，掃描 store 內所有 canonical，尋找符合的對應。
//   - 找到第一筆符合 exchange+native 的紀錄即回傳。
//
// 參數:
//   - exchange: 交易所名稱，會轉成小寫並 TrimSpace。
//   - native:   原生 symbol 字串，會 TrimSpace。
//
// 回傳:
//   - string: 查詢成功時的 canonical，失敗時為空字串。
//   - error:  若找不到對應，回傳包含 ErrNotFound 的錯誤。
//
// 備註:
//   - 此實作為線性掃描，資料量大時效能有限；若有大量反查需求，建議建立反向索引。
func (r *inMemoryResolver) NativeToCanonical(exchange, native string) (string, error) {
	e := strings.ToLower(strings.TrimSpace(exchange))
	n := strings.TrimSpace(native)

	r.mu.RLock()
	defer r.mu.RUnlock()

	for canon, row := range r.store {
		if v, ok := row[e]; ok && v == n {
			return canon, nil
		}
	}
	return "", fmt.Errorf("%w: exchange=%s native=%s", ErrNotFound, e, n)
}
