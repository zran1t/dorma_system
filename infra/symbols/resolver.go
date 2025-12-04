// File: infra/symbols/resolver.go
// Package: symbols
//
// 職責 (Responsibility):
//     管理「標準化交易對代碼 (canonical)」與各交易所原生 symbol 之間的轉換。
//     提供從 YAML 設定載入對照表的能力，並以記憶體內部結構進行查找與反查。
//
// 注意事項 (Notes):
//     - canonical 格式固定為 "BASE-QUOTE-TYPE"，其中 TYPE 為 SPOT / SWAP / INDEX。
//     - 非 INDEX 類型時，Quote 僅允許 USDT；INDEX 可為 USD/USDT/BTC/USDC 等。
//     - YAML 檔的路徑可透過 ConfigureYAMLDir/Path/Paths 進行調整。
//     - InMemoryResolver 使用 RWMutex 進行併發保護，可安全在多 goroutine 環境下使用。

package symbols

import (
	// === 標準函式庫 (Standard Library) ===
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	// === 第三方套件 (Third-Party Libraries) ===
	"gopkg.in/yaml.v3"
	// === 系統內模組 (Internal Modules) ===
	// 無
)

// perExchangeYAML 對應單一交易所 YAML 檔的結構。
//
// 功能:
//   - 承載同一個交易所底下，現貨 / 永續 / 指數三種市場型別的 symbol 對照表。
//   - 讓 yaml.Unmarshal 可以直接解析成內部用的結構。
//
// 欄位說明:
//   - Spot:  現貨市場 mapping，key 通常為 canonical（含 -SPOT 後綴），value 為該交易所原生 symbol。
//   - Perp:  永續合約市場 mapping，key 通常為 canonical（含 -SWAP 後綴）。
//   - Index: 指數市場 mapping，key 通常為 canonical（含 -INDEX 後綴）。
//
// 契約 / 限制:
//   - key 應已帶上對應後綴（-SPOT / -SWAP / -INDEX），後續會有檢查。
//   - yaml tag 不可隨意變更，否則載入程式會失敗。
//
// 備註:
//   - 此結構不匯出，僅供 InMemoryResolver 內部使用。
type perExchangeYAML struct {
	Spot  map[string]string `yaml:"spot"`
	Perp  map[string]string `yaml:"perp"`
	Index map[string]string `yaml:"index"`
}

// defaultYAMLPaths 紀錄各交易所對應的預設 YAML 檔路徑。
//
// 功能:
//   - 做為 LoadDefaultsFromDisk 的預設路徑來源。
//   - 可透過 ConfigureYAMLDir/Path/Paths 在啟動時進行覆寫。
//
// 契約 / 限制:
//   - key 為交易所名稱的小寫版本（例如 "okx"、"binance"）。
//   - value 為經過 filepath.FromSlash 處理過的相對或絕對路徑。
//
// 備註:
//   - 路徑僅是預設值，實際檔案是否存在由載入階段決定。
var defaultYAMLPaths = map[string]string{
	"okx":     filepath.FromSlash("configs/symbol_mapping/okx.yaml"),
	"binance": filepath.FromSlash("configs/symbol_mapping/binance.yaml"),
}

// ConfigureYAMLDir 設定多個交易所 YAML 檔所在的目錄。
//
// 功能:
//   - 依據傳入的目錄與交易所清單，批次更新 defaultYAMLPaths 中的路徑。
//   - 若未指定 exchanges，則預設使用 ["okx", "binance"]。
//
// 參數:
//   - dir:       YAML 檔所在的目錄路徑。
//   - exchanges: 要更新路徑的交易所名稱清單，若為空則使用預設列表。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 設定的路徑為 dir/<exchange>.yaml，exchange 會先轉成小寫。
//   - 若某交易所原本不存在於 defaultYAMLPaths，會直接新增一筆。
func ConfigureYAMLDir(dir string, exchanges ...string) {
	if len(exchanges) == 0 {
		exchanges = []string{"okx", "binance"}
	}
	for _, ex := range exchanges {
		defaultYAMLPaths[strings.ToLower(ex)] = filepath.Join(dir, strings.ToLower(ex)+".yaml")
	}
}

// ConfigureYAMLPath 設定單一交易所的 YAML 檔路徑。
//
// 功能:
//   - 將指定交易所的 YAML 檔路徑覆蓋為傳入的 path。
//
// 參數:
//   - exchange: 交易所名稱，會轉成小寫後做為 key 存入。
//   - path:     該交易所使用的 YAML 檔路徑。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 若該交易所尚未存在於 defaultYAMLPaths 內，會直接新增。
func ConfigureYAMLPath(exchange, path string) {
	defaultYAMLPaths[strings.ToLower(exchange)] = path
}

// ConfigureYAMLPaths 批次設定多個交易所的 YAML 檔路徑。
//
// 功能:
//   - 針對 map 中每一個交易所名稱與路徑，依序更新 defaultYAMLPaths。
//
// 參數:
//   - m: key 為交易所名稱，value 為 YAML 檔路徑。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 交易所名稱會先轉成小寫後再存入 defaultYAMLPaths。
func ConfigureYAMLPaths(m map[string]string) {
	for ex, p := range m {
		defaultYAMLPaths[strings.ToLower(ex)] = p
	}
}

// LoadYAMLPerExchange 從單一交易所的 YAML 內容載入 symbol 對照表。
//
// 功能:
//   - 將 YAML bytes 解析為 perExchangeYAML 結構。
//   - 檢查 key 是否符合預期後綴（-SPOT / -SWAP / -INDEX）。
//   - 逐一呼叫 Register 將 canonical → native 對應寫入 InMemoryResolver。
//
// 參數:
//   - exchange: 交易所名稱，用於寫入 store 時做為 key（會轉成小寫）。
//   - b:        YAML 檔案的原始內容（byte slice）。
//
// 回傳:
//   - error: 若 YAML 解析失敗、或 key 未帶正確後綴，則回傳錯誤；成功時為 nil。
//
// 備註:
//   - canonical key 會被以原樣（但經過 TrimSpace/ToUpper）寫入 store。
//   - 若 YAML 中出現不符合規範的 key，整體載入會失敗。
func (r *InMemoryResolver) LoadYAMLPerExchange(exchange string, b []byte) error {
	ex := strings.ToLower(strings.TrimSpace(exchange))
	var cfg perExchangeYAML
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return err
	}

	for canon, native := range cfg.Spot {
		if !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(canon)), "-SPOT") {
			return fmt.Errorf("yaml mismatch: %s not suffixed with -SPOT", canon)
		}
		r.Register(canon, ex, native)
	}
	for canon, native := range cfg.Perp {
		if !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(canon)), "-SWAP") {
			return fmt.Errorf("yaml mismatch: %s not suffixed with -SWAP", canon)
		}
		r.Register(canon, ex, native)
	}
	for canon, native := range cfg.Index {
		if !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(canon)), "-INDEX") {
			return fmt.Errorf("yaml mismatch: %s not suffixed with -INDEX", canon)
		}
		r.Register(canon, ex, native)
	}
	return nil
}

// LoadYAMLPerExchangeBatch 批次載入多個交易所的 YAML 內容。
//
// 功能:
//   - 對 map 中每一個交易所名稱與檔案內容呼叫 LoadYAMLPerExchange。
//   - 任一交易所載入失敗即回傳錯誤，並中止後續載入。
//
// 參數:
//   - files: key 為交易所名稱，value 為該交易所 YAML 檔內容。
//
// 回傳:
//   - error: 若任一交易所載入失敗，回傳對應錯誤；全部成功則為 nil。
//
// 備註:
//   - 若需要「部分成功」的行為，可在外層自行切分呼叫。
func (r *InMemoryResolver) LoadYAMLPerExchangeBatch(files map[string][]byte) error {
	for ex, b := range files {
		if err := r.LoadYAMLPerExchange(ex, b); err != nil {
			return fmt.Errorf("load %s yaml: %w", ex, err)
		}
	}
	return nil
}

// LoadDefaultsFromDisk 依照 defaultYAMLPaths 從磁碟載入對照表。
//
// 功能:
//   - 根據指定的交易所清單或預設表，計算出實際要讀取的檔案路徑集合。
//   - 逐一讀取檔案內容並呼叫 LoadYAMLPerExchange 進行解析與註冊。
//
// 參數:
//   - exchanges: 要載入的交易所清單；若為空，則使用 defaultYAMLPaths 中的全部交易所。
//
// 回傳:
//   - error: 若讀檔或解析任一 YAML 檔失敗，回傳錯誤；全部成功則為 nil。
//
// 備註:
//   - 若呼叫端未對某交易所事先設定路徑，會退回 config/symbol_mapping/<exchange>.yaml。
//   - 此函式會在第一個錯誤發生時立刻中止並回傳。
func (r *InMemoryResolver) LoadDefaultsFromDisk(exchanges ...string) error {
	targets := make(map[string]string)
	if len(exchanges) == 0 {
		for ex, p := range defaultYAMLPaths {
			targets[ex] = p
		}
	} else {
		for _, ex := range exchanges {
			exLow := strings.ToLower(ex)
			if p, ok := defaultYAMLPaths[exLow]; ok {
				targets[exLow] = p
			} else {
				// 若呼叫端未事先設定路徑，退回到預設 config 目錄
				targets[exLow] = filepath.FromSlash(filepath.Join("config/symbol_mapping", exLow+".yaml"))
			}
		}
	}

	for ex, path := range targets {
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read yaml %s (%s): %w", ex, path, err)
		}
		if err := r.LoadYAMLPerExchange(ex, data); err != nil {
			return fmt.Errorf("parse yaml %s (%s): %w", ex, path, err)
		}
	}
	return nil
}

// MarketType 表示市場型別。
// 目前支援現貨 (SPOT)、永續 (SWAP)、指數 (INDEX) 三種。
type MarketType string

const (
	// MarketSPOT 代表現貨市場。
	MarketSPOT MarketType = "SPOT"
	// MarketSWAP 代表永續合約市場。
	MarketSWAP MarketType = "SWAP"
	// MarketINDEX 代表指數市場。
	MarketINDEX MarketType = "INDEX"
)

// Canonical 表示標準化後的交易對資訊。
//
// 功能:
//   - 將字串形式的 canonical 解析後拆成 Base / Quote / MarketType 三個欄位。
//   - 作為 DefaultNative 等工具函式的輸入型別。
//
// 欄位說明:
//   - Base:       基礎貨幣（例如 BTC、ETH）。
//   - Quote:      報價貨幣（大多為 USDT，INDEX 類型可為其他貨幣）。
//   - MarketType: 市場類型（SPOT / SWAP / INDEX）。
//
// 契約 / 限制:
//   - Base 與 Quote 預期為大寫（ParseCanonical 會做 ToUpper）。
//   - MarketType 僅允許使用 MarketSPOT / MarketSWAP / MarketINDEX 三種值。
//
// 備註:
//   - 此型別為純資料結構，本身不包含任何驗證邏輯。
type Canonical struct {
	Base       string
	Quote      string
	MarketType MarketType
}

// ParseCanonical 將字串形式的 canonical 解析為 Canonical 結構。
//
// 功能:
//   - 解析形如 "BASE-QUOTE-TYPE" 的字串，拆出 Base/Quote/MarketType。
//   - 檢查市場類型是否在支援範圍內。
//   - 對非 INDEX 類型，限制報價貨幣必須為 USDT。
//
// 參數:
//   - s: 待解析的標的物代碼字串，預期格式為 "BASE-QUOTE-TYPE"。
//
// 回傳:
//   - Canonical: 解析成功後的結構值，若失敗則為零值。
//   - error:     若格式錯誤、類型不支援或報價貨幣不符合限制，回傳對應錯誤。
//
// 備註:
//   - 會先對輸入字串做 TrimSpace，再以 "-" split。
//   - 目前的貨幣約束偏向策略需求，可視需求調整或放寬。
func ParseCanonical(s string) (Canonical, error) {
	parts := strings.Split(strings.TrimSpace(s), "-")
	if len(parts) != 3 {
		return Canonical{}, fmt.Errorf("無效的標的物代碼: %q", s)
	}
	base, quote, t := strings.ToUpper(parts[0]), strings.ToUpper(parts[1]), strings.ToUpper(parts[2])

	mt := MarketType(t)
	switch mt {
	case MarketSPOT, MarketSWAP, MarketINDEX:
	default:
		return Canonical{}, fmt.Errorf("無效的市場類型: %s", t)
	}

	// 非 INDEX 類型時，報價貨幣必須為 USDT；INDEX 可允許 USD/USDT/BTC/USDC 等。
	if mt != MarketINDEX && quote != "USDT" {
		return Canonical{}, fmt.Errorf("結餘貨幣必須為USDT (偵測到 %s)", quote)
	}
	return Canonical{Base: base, Quote: quote, MarketType: mt}, nil
}

// Resolver 定義 symbol 解析與反解析的介面。
//
// 功能:
//   - 提供 canonical → exchange native 與 native → canonical 的查詢能力。
//   - 支援單筆與批次解析。
//
// 契約 / 限制:
//   - 若查詢不到對應資料，須回傳 ErrNotFound 包裝在錯誤內。
//   - ResolveMany 應維持輸出順序與輸入 canonical 順序一致。
//
// 備註:
//   - InMemoryResolver 為此介面的其中一種實作，亦可替換為其他 back-end（例如 Redis）。
type Resolver interface {
	// Resolve 將 canonical 轉成指定交易所的原生 symbol。
	Resolve(canonical, exchange string) (string, error)
	// ResolveMany 批次解析多個 canonical，順序與輸入相同。
	ResolveMany(canonicals []string, exchange string) ([]string, error)

	// ReverseResolve 將交易所原生 symbol 反查為 canonical。
	// 例如 OKX 的 "BTC-USDT-SWAP" 反查為 "BTC-USDT-SWAP"。
	ReverseResolve(exchange, native string) (string, error)
}

// inMemoryStore 是 InMemoryResolver 使用的底層儲存結構。
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
//   - 不匯出，僅供 InMemoryResolver 內部使用。
type inMemoryStore map[string]map[string]string

// InMemoryResolver 是以記憶體 map 為基礎的 Resolver 實作。
//
// 功能:
//   - 將 canonical 與各交易所原生 symbol 的對照存放於記憶體中。
//   - 提供 Register / Resolve / ReverseResolve 等基本操作。
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
type InMemoryResolver struct {
	mu    sync.RWMutex
	store inMemoryStore
}

// NewInMemoryResolver 建立一個新的 InMemoryResolver 實例。
//
// 功能:
//   - 初始化內部的 inMemoryStore，準備接收後續註冊資料。
//
// 參數:
//   - 無。
//
// 回傳:
//   - *InMemoryResolver: 新建立的 resolver 實例。
//   - 無 error。
//
// 備註:
//   - 典型使用方式為啟動時建立一個實例，並搭配 LoadDefaultsFromDisk 或手動 Register。
func NewInMemoryResolver() *InMemoryResolver {
	return &InMemoryResolver{store: make(inMemoryStore)}
}

// Register 註冊單一 canonical 與交易所原生 symbol 的對應。
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
func (r *InMemoryResolver) Register(canonical, exchange, native string) {
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

// RegisterRow 批次註冊同一 canonical 在多個交易所的對應。
//
// 功能:
//   - 將傳入 map 中的多個交易所原生 symbol 一次寫入同一 canonical row。
//
// 參數:
//   - canonical: 標準化交易對代碼字串，會轉成大寫並 TrimSpace。
//   - m:         key 為交易所名稱，value 為原生 symbol。
//
// 回傳:
//   - 無。
//
// 備註:
//   - 若 row 尚未存在會建立；若已存在，新資料會覆蓋相同 exchange 的舊值。
func (r *InMemoryResolver) RegisterRow(canonical string, m map[string]string) {
	c := strings.ToUpper(strings.TrimSpace(canonical))
	r.mu.Lock()
	defer r.mu.Unlock()
	row, ok := r.store[c]
	if !ok {
		row = make(map[string]string)
		r.store[c] = row
	}
	for ex, native := range m {
		row[strings.ToLower(strings.TrimSpace(ex))] = strings.TrimSpace(native)
	}
}

// ErrNotFound 表示未找到對應的標準標的物代碼。
//
// 功能:
//   - 作為 Resolver 實作在查無對應資料時包裝使用的基礎錯誤。
//
// 備註:
//   - 外層可使用 errors.Is(err, ErrNotFound) 判斷是否為「找不到」類錯誤。
var ErrNotFound = errors.New("未找到對應的標準標的物代碼")

// Resolve 將 canonical 轉成指定交易所的原生 symbol。
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
//   - canonical 與 exchange 必須已先透過 Register / Load* 系列函式註冊過。
func (r *InMemoryResolver) Resolve(canonical, exchange string) (string, error) {
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

// ResolveMany 批次解析多個 canonical。
//
// 功能:
//   - 依序對輸入 slice 中的每一個 canonical 呼叫 Resolve。
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
func (r *InMemoryResolver) ResolveMany(canonicals []string, exchange string) ([]string, error) {
	out := make([]string, 0, len(canonicals))
	for _, c := range canonicals {
		n, err := r.Resolve(c, exchange)
		if err != nil {
			return out, err
		}
		out = append(out, n)
	}
	return out, nil
}

// ReverseResolve 將交易所原生 symbol 反查為 canonical。
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
func (r *InMemoryResolver) ReverseResolve(exchange, native string) (string, error) {
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

// DefaultNative 根據 canonical 與交易所規則產生預設原生 symbol。
//
// 功能:
//   - 依照不同交易所的命名慣例，從 Canonical 結構組合出預設的原生 symbol 字串。
//   - 用於尚未明確提供 mapping 時建立合理預設值。
//
// 參數:
//   - exchange: 交易所名稱，會轉成小寫後判斷。
//   - canon:    已解析完成的 Canonical 結構。
//
// 回傳:
//   - string: 組合後的預設原生 symbol 字串。
//
// 備註:
//   - okx:     SPOT/INDEX → "BASE-QUOTE"，SWAP → "BASE-QUOTE-SWAP"。
//   - binance: 直接使用 base+quote 並轉成小寫。
//   - 其他交易所: 使用 base+quote 並轉成大寫。
func DefaultNative(exchange string, canon Canonical) string {
	switch strings.ToLower(exchange) {
	case "okx":
		switch canon.MarketType {
		case MarketSPOT, MarketINDEX:
			return fmt.Sprintf("%s-%s", canon.Base, canon.Quote) // e.g. BTC-USDT
		case MarketSWAP:
			return fmt.Sprintf("%s-%s-SWAP", canon.Base, canon.Quote) // e.g. BTC-USDT-SWAP
		default:
			return fmt.Sprintf("%s-%s", canon.Base, canon.Quote)
		}
	case "binance":
		return strings.ToLower(canon.Base + canon.Quote)
	default:
		return strings.ToUpper(canon.Base + canon.Quote)
	}
}

// UpsertDefaultRow 依照預設規則產生多個交易所的原生 symbol 並寫入 store。
//
// 功能:
//   - 先解析 canonical 字串，取得 Canonical 結構。
//   - 針對 exchanges 清單中的每一個交易所，呼叫 DefaultNative 產生預設原生 symbol。
//   - 最後將整列資料透過 RegisterRow 寫入 InMemoryResolver。
//
// 參數:
//   - canonical: 標準化交易對代碼字串（會先經過 ParseCanonical 驗證）。
//   - exchanges: 要產生預設原生 symbol 的交易所清單。
//
// 回傳:
//   - error: 若 canonical 無法成功解析，則回傳錯誤；成功時為 nil。
//
// 備註:
//   - 若某交易所在 store 中已存在同一 canonical 的資料，新資料會覆蓋舊資料。
//   - 適合在初始化階段用於快速建立一批預設 mapping。
func (r *InMemoryResolver) UpsertDefaultRow(canonical string, exchanges ...string) error {
	canon, err := ParseCanonical(canonical)
	if err != nil {
		return err
	}
	row := make(map[string]string)
	for _, ex := range exchanges {
		row[strings.ToLower(ex)] = DefaultNative(ex, canon)
	}
	r.RegisterRow(canonical, row)
	return nil
}
