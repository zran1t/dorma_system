// ============================
// File: dorma_system/infra/symbols/resolver.go
// ============================
package symbols

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// ---- 符號對照：設定與載入（YAML）----

//   spot:
//     BTC-USDT-SPOT: BTCUSDT
//   perp:
//     BTC-USDT-SWAP: BTCUSDT
type perExchangeYAML struct {
	Spot map[string]string `yaml:"spot"`
	Perp map[string]string `yaml:"perp"`
}

// 所有交易所 mapping 的預設儲存路徑
var defaultYAMLPaths = map[string]string{
	"okx":     filepath.FromSlash("configs/symbol_mapping/okx.yaml"),
	"binance": filepath.FromSlash("configs/symbol_mapping/binance.yaml"),
}

func ConfigureYAMLDir(dir string, exchanges ...string) {
	if len(exchanges) == 0 {
		exchanges = []string{"okx", "binance"}
	}
	for _, ex := range exchanges {
		defaultYAMLPaths[strings.ToLower(ex)] = filepath.Join(dir, strings.ToLower(ex)+".yaml")
	}
}
func ConfigureYAMLPath(exchange, path string) { defaultYAMLPaths[strings.ToLower(exchange)] = path }
func ConfigureYAMLPaths(m map[string]string) {
	for ex, p := range m {
		defaultYAMLPaths[strings.ToLower(ex)] = p
	}
}

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
	return nil
}

func (r *InMemoryResolver) LoadYAMLPerExchangeBatch(files map[string][]byte) error {
	for ex, b := range files {
		if err := r.LoadYAMLPerExchange(ex, b); err != nil {
			return fmt.Errorf("load %s yaml: %w", ex, err)
		}
	}
	return nil
}

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

// ---- 規格與介面 ----

type MarketType string

const (
	MarketSPOT MarketType = "SPOT"
	MarketSWAP MarketType = "SWAP"
)

type Canonical struct {
	Base       string
	Quote      string
	MarketType MarketType
}

func ParseCanonical(s string) (Canonical, error) {
	parts := strings.Split(strings.TrimSpace(s), "-")
	if len(parts) != 3 {
		return Canonical{}, fmt.Errorf("無效的標的物代碼: %q", s)
	}
	base, quote, t := strings.ToUpper(parts[0]), strings.ToUpper(parts[1]), strings.ToUpper(parts[2])
	if quote != "USDT" {
		return Canonical{}, fmt.Errorf("結餘貨幣必須為USDT (偵測到 %s)", quote)
	}
	switch MarketType(t) {
	case MarketSPOT, MarketSWAP:
	default:
		return Canonical{}, fmt.Errorf("無效的市場類型: %s", t)
	}
	return Canonical{Base: base, Quote: quote, MarketType: MarketType(t)}, nil
}

// 反查器介面：加入 ReverseResolve（native -> canonical）
type Resolver interface {
	// canonical -> native
	Resolve(canonical, exchange string) (string, error)
	ResolveMany(canonicals []string, exchange string) ([]string, error)

	// native -> canonical（例如 OKX 的 "BTC-USDT-SWAP" 反查為 "BTC-USDT-SWAP"）
	ReverseResolve(exchange, native string) (string, error)
}

// ---- in-memory 實作 ----

type inMemoryStore map[string]map[string]string

type InMemoryResolver struct {
	mu    sync.RWMutex
	store inMemoryStore
}

func NewInMemoryResolver() *InMemoryResolver {
	return &InMemoryResolver{store: make(inMemoryStore)}
}

// 註冊
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

var ErrNotFound = errors.New("未找到對應的標準標的物代碼")

// canonical -> native
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

// native -> canonical
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

// 小工具：預設 native 生成
func DefaultNative(exchange string, canon Canonical) string {
	switch strings.ToLower(exchange) {
	case "okx":
		// 你若想 OKX SWAP 也不加 -SWAP，就把 SWAP 的 return 改成一樣
		return fmt.Sprintf("%s-%s", canon.Base, canon.Quote)
	case "binance":
		return strings.ToLower(canon.Base + canon.Quote)
	default:
		return strings.ToUpper(canon.Base + canon.Quote)
	}
}

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