// File: infra/symbols/loader_test.go
// Package: symbols
//
// 職責 (Responsibility):
//
//	測試 symbols 套件中「載入 YAML → inMemoryResolver」這一層的行為，
//	包含 applyExchangeYAML 與 LoadFromDefaultYAML 是否能正確把設定寫進記憶體。
//
// 注意事項 (Notes):
//   - 這裡會測兩種層級：
//     1) 純 in-memory YAML 字串（不碰檔案系統）。
//     2) 使用專案內的 configs/symbol_mapping/*.yaml 做整合測試。

package symbols

import (
	"os"
	"path/filepath"
	"testing"
)

// TestApplyExchangeYAML_Basic
//
// 功能:
//   - 使用手工建立的 YAML 字串，驗證 applyExchangeYAML 能正確載入 spot / perp / index 三種 mapping。
//   - 驗證載入後，CanonicalToNative 可以查到對應的原生 symbol。
//
// 說明:
//   - 這支測試完全使用記憶體中的 []byte，不依賴實體檔案，適合作為「單元測試」。
func TestApplyExchangeYAML_Basic(t *testing.T) {
	r := NewInMemoryResolver()

	// 手工準備一份簡單的 YAML 內容。
	yamlData := []byte(`
spot:
  BTC-USDT-SPOT: BTC-USDT
perp:
  BTC-USDT-SWAP: BTC-USDT-SWAP
index:
  BTC-USDT-INDEX: BTC-USDT-INDEX
`)

	if err := r.applyExchangeYAML("okx", yamlData); err != nil {
		t.Fatalf("applyExchangeYAML 不預期失敗: %v", err)
	}

	// 驗證三種市場型別都能查到對應原生 symbol。
	tests := []struct {
		canonical string
		exchange  string
		want      string
	}{
		{"BTC-USDT-SPOT", "okx", "BTC-USDT"},
		{"BTC-USDT-SWAP", "okx", "BTC-USDT-SWAP"},
		{"BTC-USDT-INDEX", "okx", "BTC-USDT-INDEX"},
	}

	for _, tc := range tests {
		got, err := r.CanonicalToNative(tc.canonical, tc.exchange)
		if err != nil {
			t.Fatalf("CanonicalToNative(%q, %q) 不預期失敗: %v", tc.canonical, tc.exchange, err)
		}
		if got != tc.want {
			t.Fatalf("CanonicalToNative(%q, %q) 預期 = %q, 實際 = %q", tc.canonical, tc.exchange, tc.want, got)
		}
	}
}

// TestApplyExchangeYAML_InvalidSuffix
//
// 功能:
//   - 確認當 YAML 裡的 key 沒有帶上正確的後綴（-SPOT / -SWAP / -INDEX）時，applyExchangeYAML 會回傳錯誤。
//
// 說明:
//   - 這支測試覆蓋三種市場型別各一個錯誤案例，確保 suffix 檢查有實際生效。
//   - 每個子測試名稱會清楚說明「哪裡錯」以及「預期要回錯」。
func TestApplyExchangeYAML_InvalidSuffix(t *testing.T) {
	r := NewInMemoryResolver()

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "spot 缺少 -SPOT suffix_應回傳錯誤",
			data: []byte(`
spot:
  BTC-USDT: BTC-USDT
`),
		},
		{
			name: "perp 缺少 -SWAP suffix_應回傳錯誤",
			data: []byte(`
perp:
  BTC-USDT-SPOT: BTC-USDT-SWAP
`),
		},
		{
			name: "index 缺少 -INDEX suffix_應回傳錯誤",
			data: []byte(`
index:
  BTC-USDT: BTC-USDT-INDEX
`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := r.applyExchangeYAML("okx", tc.data)
			if err == nil {
				t.Fatalf("預期 applyExchangeYAML 應該回傳錯誤，實際為 nil")
			}
		})
	}
}

// TestLoadFromDefaultYAML_UsingRealConfigs
//
// 功能:
//   - 使用專案內的 configs/symbol_mapping/okx.yaml、binance.yaml，
//     驗證 LoadFromDefaultYAML 能順利載入並寫入 inMemoryResolver。
//   - 檢查載入後 store 不是空的，代表至少有一筆 mapping 進來。
//
// 說明:
//   - 這是一個偏「整合測試」等級的檢查，會真的打開專案裡的 YAML 檔案。
//   - 若測試環境缺少這些檔案，或目前工作目錄不是專案根，這支測試會嘗試往上切換目錄；
//     若仍找不到 configs，就直接 Skip，而不是標記為 FAIL。
func TestLoadFromDefaultYAML_UsingRealConfigs(t *testing.T) {
	r := NewInMemoryResolver()

	// 記錄原本的工作目錄，方便測完切回來。
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("取得工作目錄失敗: %v", err)
	}
	defer func() { _ = os.Chdir(wd) }()

	// 嘗試找出專案根目錄（要有 configs/symbol_mapping 這個資料夾）。
	projectRoot := ""
	tryDirs := []string{
		wd,
		filepath.Dir(wd),               // 上一層
		filepath.Dir(filepath.Dir(wd)), // 上兩層
	}

	for _, dir := range tryDirs {
		okxPath := filepath.Join(dir, "configs", "symbol_mapping", "okx.yaml")
		if _, err := os.Stat(okxPath); err == nil {
			projectRoot = dir
			break
		}
	}

	if projectRoot == "" {
		t.Skip("找不到 configs/symbol_mapping/okx.yaml，略過整合測試（可能不是在專案根底下執行）")
	}

	// 切到推定的專案根目錄，讓 LoadFromDefaultYAML 使用相對路徑時能找到檔案。
	if err := os.Chdir(projectRoot); err != nil {
		t.Fatalf("切換到專案根目錄失敗: %v", err)
	}

	if err := r.LoadFromDefaultYAML(); err != nil {
		t.Fatalf("LoadFromDefaultYAML 不預期失敗，請確認 configs/symbol_mapping/*.yaml 是否存在且格式正確: %v", err)
	}

	if len(r.store) == 0 {
		t.Fatalf("LoadFromDefaultYAML 執行後，inMemoryResolver.store 不應該是空的")
	}
}
