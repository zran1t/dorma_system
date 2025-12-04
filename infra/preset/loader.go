// File: infra/preset/loader.go
// Package: preset
//
// 職責 (Responsibility):
//     讀取並解析系統預設的 Autostart Matrix 組態。
//     此模組為唯一權威路徑，外部不得指定自訂位置。
//
// 注意事項 (Notes):
//     - 若檔案遺失或解析錯誤，由上層判斷是否中止啟動。
//     - 不做自動補檔或 fallback；系統應確保配置正確。
//     - 僅負責 I/O 與反序列化，驗證與修正留給 validator。

package preset

import (
	// === 標準函式庫 (Standard Library) ===
	"os"
	"path/filepath"

	// === 第三方套件 (Third-Party Libraries) ===
	"gopkg.in/yaml.v3"
	// === 系統內模組 (Internal Modules) ===
	// 無
)

// defaultAutostartMatrixPath 系統唯一權威組態位置。
// 不匯出以避免外部存取或誤用。
const defaultAutostartMatrixPath = "configs/system_presets/startup_matrix.yaml"

// LoadAutostartMatrix 載入系統預設的啟動矩陣。
//
// 功能:
//   - 從固定路徑讀取 YAML 檔。
//   - 將內容解析為 AutostartMatrix 結構並回傳。
//
// 回傳:
//   - *AutostartMatrix: 成功解析後的設定結構。
//   - error: 若檔案不存在或內容錯誤則回傳錯誤。
//
// 備註:
//   - 不處理任何驗證與預設填補；此階段僅保證 I/O 成功。
func LoadAutostartMatrix() (*AutostartMatrix, error) {
	p := filepath.FromSlash(defaultAutostartMatrixPath)

	// 僅嘗試讀取，不進行存在性檢查；讓錯誤自然回傳給上層。
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}

	var m AutostartMatrix

	// 使用 YAML 解構，保留註解友善特性與階層語意。
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
