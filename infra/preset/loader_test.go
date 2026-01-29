// File: infra/preset/loader_test.go
// Package: preset
//
// 職責 (Responsibility):
//     測試 preset pipeline 的正確性（reader → validator → compiler → packet）。
//     透過測試用 YAML（寫入 temp file）驗證：
//       - market_data: defaults + only-diff override merge 與訂閱展開（realtime/candle）
//       - instrument_data: enabled feeds 與 symbol-level symbols filter
//       - trading_allowlist: markets.<market> presence → allowed 清單展開
//
// 注意事項 (Notes):
//     - 不依賴 repo 內真實 YAML 檔，避免測試對環境/檔案內容產生耦合。
//     - 本測試不呼叫 preset.Load*Packet()（因其使用固定路徑常數）；改以 reader 直接讀 temp 檔。

package preset

import (
	"os"
	"path/filepath"
	"testing"

	"dorma_system/infra/preset/compiler"
	"dorma_system/infra/preset/packet"
	"dorma_system/infra/preset/reader"
	"dorma_system/infra/preset/validator"
)

func TestPipeline_MarketDataPreset_CompileAndExpand(t *testing.T) {
	yml := `
version: 1
summary: "test market data"

exchanges:
  okx:
    defaults:
      spot:
        feeds:
          trades_all: { enabled: true }
          trades:     { enabled: true }
          bbo:        { enabled: true }
          book_delta: { enabled: true }
          book_full:  { enabled: true }
          last:
            realtime: { enabled: true }
            candle: { enabled: true, intervals: [1m, 5m] }
          mark:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }
          index:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }

      swap:
        feeds:
          trades_all: { enabled: true }
          trades:     { enabled: true }
          bbo:        { enabled: true }
          book_delta: { enabled: true }
          book_full:  { enabled: true }
          last:
            realtime: { enabled: true }
            candle: { enabled: true, intervals: [1m, 5m] }
          mark:
            realtime: { enabled: true }
            candle: { enabled: true, intervals: [1m, 5m, 15m] }
          index:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }

      index:
        feeds:
          trades_all: { enabled: false }
          trades:     { enabled: false }
          bbo:        { enabled: false }
          book_delta: { enabled: false }
          book_full:  { enabled: false }
          last:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }
          mark:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }
          index:
            realtime: { enabled: true }
            candle: { enabled: true, intervals: [1m, 5m] }

    symbols:
      - pair: BTC-USDT
        markets:
          spot: {}
          swap:
            feeds:
              trades:
                enabled: false
              last:
                candle:
                  intervals: [1m]   # 覆寫 intervals（only-diff）
          index: {}
`

	p := mustWriteTempYAML(t, "market_data.startup.yaml", yml)
	pre, err := reader.LoadMarketDataPreset(p)
	if err != nil {
		t.Fatalf("LoadMarketDataPreset error: %v", err)
	}
	if err := validator.ValidateMarketDataPreset(pre); err != nil {
		t.Fatalf("ValidateMarketDataPreset error: %v", err)
	}

	out, err := compiler.CompileMarketData(pre)
	if err != nil {
		t.Fatalf("CompileMarketData error: %v", err)
	}
	if err := validator.ValidateMarketDataPacket(out); err != nil {
		t.Fatalf("ValidateMarketDataPacket error: %v", err)
	}

	ex := out.Exchanges["okx"]
	if ex.Exchange != "okx" {
		t.Fatalf("exchange mismatch: %s", ex.Exchange)
	}

	// 建 set 方便無序比較
	got := map[string]packet.MarketDataSubscription{}
	for _, s := range ex.Subscriptions {
		got[subKey(s)] = s
	}

	// spot(BTC): 5 simple + last(realtime,candle) = 7
	// swap(BTC): 4 simple (trades=false) + last(realtime,candle) + mark(realtime,candle) = 8
	// index(BTC): index(realtime,candle) = 2
	// total = 17
	if len(got) != 17 {
		t.Fatalf("subscriptions count mismatch: got=%d want=%d", len(got), 17)
	}

	// spot BTC-USDT trades should exist
	assertHasSub(t, got, "okx|BTC-USDT|spot|trades|realtime", nil)

	// swap BTC-USDT trades should be disabled by override
	assertNotHasSub(t, got, "okx|BTC-USDT|swap|trades|realtime")

	// swap BTC-USDT last candle intervals should be overridden to [1m]
	assertHasSub(t, got, "okx|BTC-USDT|swap|last|candle", []string{"1m"})

	// swap BTC-USDT mark realtime should exist (defaults enabled)
	assertHasSub(t, got, "okx|BTC-USDT|swap|mark|realtime", nil)

	// index BTC-USDT index candle should exist
	assertHasSub(t, got, "okx|BTC-USDT|index|index|candle", []string{"1m", "5m"})
}

func TestPipeline_InstrumentDataPreset_Compile(t *testing.T) {
	yml := `
version: 1
summary: "test instrument data"

exchanges:
  okx:
    feeds:
      open_interest:
        enabled: true
      liquidation_orders:
        enabled: true
      adl_warning:
        enabled: false
      funding_rate:
        enabled: true
        symbols: [BTC-USDT-SWAP, ETH-USDT-SWAP]
`

	p := mustWriteTempYAML(t, "instrument_data.startup.yaml", yml)
	pre, err := reader.LoadInstrumentDataPreset(p)
	if err != nil {
		t.Fatalf("LoadInstrumentDataPreset error: %v", err)
	}
	if err := validator.ValidateInstrumentDataPreset(pre); err != nil {
		t.Fatalf("ValidateInstrumentDataPreset error: %v", err)
	}

	out, err := compiler.CompileInstrumentData(pre)
	if err != nil {
		t.Fatalf("CompileInstrumentData error: %v", err)
	}
	if err := validator.ValidateInstrumentDataPacket(out); err != nil {
		t.Fatalf("ValidateInstrumentDataPacket error: %v", err)
	}

	ex := out.Exchanges["okx"]
	if ex.Exchange != "okx" {
		t.Fatalf("exchange mismatch: %s", ex.Exchange)
	}

	// feeds 無序，做 map
	got := map[string]packet.InstrumentFeedIntent{}
	for _, f := range ex.Feeds {
		got[f.Feed] = f
	}

	if !got["open_interest"].Enabled {
		t.Fatalf("open_interest enabled mismatch: got=%v want=true", got["open_interest"].Enabled)
	}
	if got["adl_warning"].Enabled {
		t.Fatalf("adl_warning enabled mismatch: got=%v want=false", got["adl_warning"].Enabled)
	}
	if !got["funding_rate"].Enabled {
		t.Fatalf("funding_rate enabled mismatch: got=%v want=true", got["funding_rate"].Enabled)
	}
	assertSliceEq(t, got["funding_rate"].Symbols, []string{"BTC-USDT-SWAP", "ETH-USDT-SWAP"})
}

func TestPipeline_TradingAllowlistPreset_Compile(t *testing.T) {
	yml := `
version: 1
summary: "test allowlist"

exchanges:
  okx:
    symbols:
      - pair: BTC-USDT
        markets:
          spot: {}
          swap: {}
      - pair: ETH-USDT
        markets:
          swap: {}
`

	p := mustWriteTempYAML(t, "trading_allowlist.yaml", yml)
	pre, err := reader.LoadTradingAllowlistPreset(p)
	if err != nil {
		t.Fatalf("LoadTradingAllowlistPreset error: %v", err)
	}
	if err := validator.ValidateTradingAllowlistPreset(pre); err != nil {
		t.Fatalf("ValidateTradingAllowlistPreset error: %v", err)
	}

	out, err := compiler.CompileTradingAllowlist(pre)
	if err != nil {
		t.Fatalf("CompileTradingAllowlist error: %v", err)
	}
	if err := validator.ValidateTradingAllowlistPacket(out); err != nil {
		t.Fatalf("ValidateTradingAllowlistPacket error: %v", err)
	}

	ex := out.Exchanges["okx"]
	if ex.Exchange != "okx" {
		t.Fatalf("exchange mismatch: %s", ex.Exchange)
	}

	// allowed 應為：BTC spot+swap、ETH swap => 3
	if len(ex.Allowed) != 3 {
		t.Fatalf("allowed count mismatch: got=%d want=%d", len(ex.Allowed), 3)
	}

	got := map[string]packet.TradableInstrument{}
	for _, a := range ex.Allowed {
		got[allowKey(a)] = a
	}

	if _, ok := got["okx|BTC-USDT|spot"]; !ok {
		t.Fatalf("missing allow: okx BTC spot")
	}
	if _, ok := got["okx|BTC-USDT|swap"]; !ok {
		t.Fatalf("missing allow: okx BTC swap")
	}
	if _, ok := got["okx|ETH-USDT|swap"]; !ok {
		t.Fatalf("missing allow: okx ETH swap")
	}
}

func TestPipeline_InvalidMarket_ShouldError(t *testing.T) {
	yml := `
version: 1
summary: "test invalid market"

exchanges:
  okx:
    defaults:
      spot:
        feeds:
          trades_all: { enabled: true }
          trades:     { enabled: true }
          bbo:        { enabled: true }
          book_delta: { enabled: true }
          book_full:  { enabled: true }
          last:
            realtime: { enabled: true }
            candle: { enabled: true, intervals: [1m] }
          mark:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }
          index:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }
      swap:
        feeds:
          trades_all: { enabled: false }
          trades:     { enabled: false }
          bbo:        { enabled: false }
          book_delta: { enabled: false }
          book_full:  { enabled: false }
          last:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }
          mark:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }
          index:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }
      index:
        feeds:
          trades_all: { enabled: false }
          trades:     { enabled: false }
          bbo:        { enabled: false }
          book_delta: { enabled: false }
          book_full:  { enabled: false }
          last:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }
          mark:
            realtime: { enabled: false }
            candle: { enabled: false, intervals: [] }
          index:
            realtime: { enabled: true }
            candle: { enabled: true, intervals: [1m] }

    symbols:
      - pair: BTC-USDT
        markets:
          swappp: {}   # invalid
`

	p := mustWriteTempYAML(t, "market_data_invalid.yaml", yml)
	pre, err := reader.LoadMarketDataPreset(p)
	if err != nil {
		t.Fatalf("LoadMarketDataPreset error: %v", err)
	}

	_, err = compiler.CompileMarketData(pre)
	if err == nil {
		t.Fatalf("expected error for invalid market, got nil")
	}
}

// ===== helpers =====

func mustWriteTempYAML(t *testing.T, name string, content string) string {
	t.Helper()

	dir := t.TempDir()
	p := filepath.Join(dir, name)

	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp yaml error: %v", err)
	}
	return p
}

func subKey(s packet.MarketDataSubscription) string {
	return s.Exchange + "|" + s.Pair + "|" + string(s.Market) + "|" + s.Feed + "|" + s.Mode
}

func allowKey(a packet.TradableInstrument) string {
	return a.Exchange + "|" + a.Pair + "|" + string(a.Market)
}

func assertHasSub(t *testing.T, got map[string]packet.MarketDataSubscription, key string, intervals []string) {
	t.Helper()

	s, ok := got[key]
	if !ok {
		t.Fatalf("missing subscription: %s", key)
	}
	if intervals != nil {
		assertSliceEq(t, s.Intervals, intervals)
	}
}

func assertNotHasSub(t *testing.T, got map[string]packet.MarketDataSubscription, key string) {
	t.Helper()

	if _, ok := got[key]; ok {
		t.Fatalf("unexpected subscription exists: %s", key)
	}
}

func assertSliceEq(t *testing.T, got []string, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("slice len mismatch: got=%v want=%v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("slice value mismatch: got=%v want=%v", got, want)
		}
	}
}
