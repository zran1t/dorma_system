// File: infra/symbols/resolver_test.go
// Package: symbols
//
// 職責 (Responsibility):
//
//	測試 symbols 套件中「純 in-memory 解析邏輯」的部分，
//	包含 inMemoryResolver 的註冊 / 正向解析 / 反向解析與介面實作檢查。
//	不碰磁碟、不處理 YAML IO。
package symbols

import (
	"errors"
	"testing"
)

// TestInMemoryResolver_CanonicalToNative_And_NativeToCanonical
//
// 功能:
//   - 確認 inMemoryResolver 能正確將 canonical ↔ native 互轉。
//   - 同一個 canonical 在不同交易所可以對應到不同的原生 symbol。
//
// 說明:
//   - 這支測試只驗證「記憶體內 mapping」是否正確，不依賴任何 YAML 或外部設定檔。
func TestInMemoryResolver_CanonicalToNative_And_NativeToCanonical(t *testing.T) {
	r := NewInMemoryResolver()

	// 安裝一組測試資料：同一個 canonical，在 okx / binance 各有一個 native symbol。
	r.register("BTC-USDT-SPOT", "okx", "BTC-USDT")
	r.register("BTC-USDT-SPOT", "binance", "btcusdt")

	// 正向解析：canonical → native
	okxNative, err := r.CanonicalToNative("BTC-USDT-SPOT", "okx")
	if err != nil {
		t.Fatalf("CanonicalToNative(okx) 不預期失敗: %v", err)
	}
	if okxNative != "BTC-USDT" {
		t.Fatalf("okx native 預期 = %q, 實際 = %q", "BTC-USDT", okxNative)
	}

	binanceNative, err := r.CanonicalToNative("BTC-USDT-SPOT", "binance")
	if err != nil {
		t.Fatalf("CanonicalToNative(binance) 不預期失敗: %v", err)
	}
	if binanceNative != "btcusdt" {
		t.Fatalf("binance native 預期 = %q, 實際 = %q", "btcusdt", binanceNative)
	}

	// 反向解析：native → canonical
	canonFromOkx, err := r.NativeToCanonical("okx", "BTC-USDT")
	if err != nil {
		t.Fatalf("NativeToCanonical(okx) 不預期失敗: %v", err)
	}
	if canonFromOkx != "BTC-USDT-SPOT" {
		t.Fatalf("從 okx 反查 canonical 預期 = %q, 實際 = %q", "BTC-USDT-SPOT", canonFromOkx)
	}

	canonFromBinance, err := r.NativeToCanonical("binance", "btcusdt")
	if err != nil {
		t.Fatalf("NativeToCanonical(binance) 不預期失敗: %v", err)
	}
	if canonFromBinance != "BTC-USDT-SPOT" {
		t.Fatalf("從 binance 反查 canonical 預期 = %q, 實際 = %q", "BTC-USDT-SPOT", canonFromBinance)
	}
}

// TestInMemoryResolver_BatchCanonicalToNative_PreservesOrder
//
// 功能:
//   - 確認 BatchCanonicalToNative 會維持輸入 slice 的順序，不會打亂。
//   - 同時驗證多筆 canonical 解析時，結果與單筆解析一致。
//
// 說明:
//   - 這支測試主要鎖定「順序」這個契約，避免之後內部實作調整時破壞不易察覺。
func TestInMemoryResolver_BatchCanonicalToNative_PreservesOrder(t *testing.T) {
	r := NewInMemoryResolver()

	// 準備三組不同 canonical 的 mapping。
	r.register("BTC-USDT-SPOT", "okx", "BTC-USDT")
	r.register("ETH-USDT-SPOT", "okx", "ETH-USDT")
	r.register("SOL-USDT-SPOT", "okx", "SOL-USDT")

	inputs := []string{
		"ETH-USDT-SPOT",
		"BTC-USDT-SPOT",
		"SOL-USDT-SPOT",
	}

	want := []string{
		"ETH-USDT",
		"BTC-USDT",
		"SOL-USDT",
	}

	got, err := r.BatchCanonicalToNative(inputs, "okx")
	if err != nil {
		t.Fatalf("BatchCanonicalToNative 不預期失敗: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("回傳 slice 長度不符，預期 = %d, 實際 = %d", len(want), len(got))
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("索引 %d 的結果不符，預期 = %q, 實際 = %q", i, want[i], got[i])
		}
	}
}

// TestInMemoryResolver_ErrNotFound_Behavior
//
// 功能:
//   - 確認查不到資料時，會回傳含有 ErrNotFound 的錯誤。
//   - 同時覆蓋三種情境：
//     1) canonical 根本不存在（應回傳 ErrNotFound）。
//     2) canonical 存在但指定交易所沒有對應（應回傳 ErrNotFound）。
//     3) NativeToCanonical 查不到反向 mapping（應回傳 ErrNotFound）。
//
// 說明:
//   - 這支測試鎖定錯誤型別，未來若改錯誤訊息內容，這裡不會壞；
//     只要 ErrNotFound 的 semantics 沒變就可以。
//   - 子測試名稱會明確寫出「情境 + 預期行為」，方便從測試輸出直接看懂意圖。
func TestInMemoryResolver_ErrNotFound_Behavior(t *testing.T) {
	r := NewInMemoryResolver()

	// 僅註冊一筆 okx 資料，方便測三種不同缺失情境。
	r.register("BTC-USDT-SPOT", "okx", "BTC-USDT")

	tests := []struct {
		name string
		fn   func() error
	}{
		{
			name: "canonical 不存在_應回傳 ErrNotFound",
			fn: func() error {
				_, err := r.CanonicalToNative("ETH-USDT-SPOT", "okx")
				return err
			},
		},
		{
			name: "canonical 存在但交易所沒有資料_應回傳 ErrNotFound",
			fn: func() error {
				_, err := r.CanonicalToNative("BTC-USDT-SPOT", "binance")
				return err
			},
		},
		{
			name: "NativeToCanonical 找不到對應_應回傳 ErrNotFound",
			fn: func() error {
				_, err := r.NativeToCanonical("okx", "UNKNOWN-SYMBOL")
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Fatalf("預期應該回傳錯誤，實際為 nil")
			}
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("錯誤應該要包含 ErrNotFound，實際為: %v", err)
			}
		})
	}
}

// TestInMemoryResolver_ImplementsResolver
//
// 功能:
//   - 用編譯器檢查 inMemoryResolver 是否有實作 Resolver 介面。
//   - 若將來不小心改壞方法簽名，這裡會直接編譯失敗。
func TestInMemoryResolver_ImplementsResolver(t *testing.T) {
	var _ Resolver = (*inMemoryResolver)(nil)
}
