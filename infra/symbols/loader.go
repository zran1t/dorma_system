// File: infra/symbols/loader.go
// Package: symbols
//
// 職責 (Responsibility):
//     負責從 YAML 檔載入各交易所的 symbol mapping，寫入 inMemoryResolver。
//     將 IO / 檔案路徑等細節收斂在單一檔案，不外放設定入口。
//
// 注意事項 (Notes):
//     - 預設 YAML 檔放在 "configs/symbol_mapping" 目錄下（例如 okx.yaml、binance.yaml）。
//     - 僅支援 inMemoryResolver，此檔案直接依賴 inMemoryResolver 內部的 register 方法。
//     - 僅處理讀檔與 YAML 解析，不做進一步業務層驗證（例如幣種白名單等）。

package symbols

import (
	// === 標準函式庫 (Standard Library) ===
	"fmt"
	"os"
	"path/filepath"
	"strings"

	// === 第三方套件 (Third-Party Libraries) ===
	"gopkg.in/yaml.v3"
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
//   - 此結構不匯出，僅供 inMemoryResolver 內部使用。
type perExchangeYAML struct {
	Spot  map[string]string `yaml:"spot"`
	Perp  map[string]string `yaml:"perp"`
	Index map[string]string `yaml:"index"`
}

// applyExchangeYAML 從單一交易所的 YAML 內容載入 symbol 對照表。
//
// 功能:
//   - 將 YAML bytes 解析為 perExchangeYAML 結構。
//   - 檢查 key 是否符合預期後綴（-SPOT / -SWAP / -INDEX）。
//   - 逐一呼叫 register 將 canonical → native 對應寫入 inMemoryResolver。
//
// 參數:
//   - exchange: 交易所名稱，用於寫入 store 時做為 key（會轉成小寫）。
//   - data:     YAML 檔案的原始內容（byte slice）。
//
// 回傳:
//   - error: 若 YAML 解析失敗、或 key 未帶正確後綴，則回傳錯誤；成功時為 nil。
//
// 備註:
//   - canonical key 會被以原樣（但經過 TrimSpace/ToUpper）寫入 store。
//   - 若 YAML 中出現不符合規範的 key，整體載入會失敗。
func (r *inMemoryResolver) applyExchangeYAML(exchange string, data []byte) error {
	ex := strings.ToLower(strings.TrimSpace(exchange))

	var cfg perExchangeYAML
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return err
	}

	for canon, native := range cfg.Spot {
		if !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(canon)), "-SPOT") {
			return fmt.Errorf("yaml mismatch: %s not suffixed with -SPOT", canon)
		}
		r.register(canon, ex, native)
	}
	for canon, native := range cfg.Perp {
		if !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(canon)), "-SWAP") {
			return fmt.Errorf("yaml mismatch: %s not suffixed with -SWAP", canon)
		}
		r.register(canon, ex, native)
	}
	for canon, native := range cfg.Index {
		if !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(canon)), "-INDEX") {
			return fmt.Errorf("yaml mismatch: %s not suffixed with -INDEX", canon)
		}
		r.register(canon, ex, native)
	}
	return nil
}

// LoadFromDefaultYAML 依照內建的預設路徑從磁碟載入對照表。
//
// 功能:
//   - 根據指定的交易所清單或內建預設表，計算出實際要讀取的檔案路徑集合。
//   - 逐一讀取檔案內容並套用交易所 YAML 設定到 inMemoryResolver。
//
// 參數:
//   - exchanges: 要載入的交易所清單；若為空，則使用內建預設表中的全部交易所。
//
// 回傳:
//   - error: 若讀檔或解析任一 YAML 檔失敗，回傳錯誤；全部成功則為 nil。
//
// 備註:
//   - 若遇到未內建的交易所名稱，會退回到 config/symbol_mapping/<exchange>.yaml。
//   - 此函式會在第一個錯誤發生時立刻中止並回傳。
func (r *inMemoryResolver) LoadFromDefaultYAML(exchanges ...string) error {
	defaultYAMLPaths := map[string]string{
		"okx":     filepath.FromSlash("configs/symbol_mapping/okx.yaml"),
		"binance": filepath.FromSlash("configs/symbol_mapping/binance.yaml"),
	}

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
		if err := r.applyExchangeYAML(ex, data); err != nil {
			return fmt.Errorf("parse yaml %s (%s): %w", ex, path, err)
		}
	}
	return nil
}
