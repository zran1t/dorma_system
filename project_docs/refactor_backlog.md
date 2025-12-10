# 🗂 Item Format

此格式用於記錄專案中的重構項目（Refactor Items）或技術債（Technical Debt）。
欄位名稱以英文為主，內容以中文描述，方便維護與跨團隊閱讀。

```md
## ID: RF-XXXX
**Title:** <short title>
**CreatedAt:** YYYY-MM-DD
**Status:** Pending | In-Progress | Completed | Dropped
**Branch:** <branch-name> (optional until started)
**StartedAt:** YYYY-MM-DD (optional)
**EndedAt:** YYYY-MM-DD (optional)

### Description
<詳細描述該項目的技術債 / 問題 / 架構缺陷 / 重構需求>

### Expected Outcome
<完成此項目後，系統應達成的目標、改善的行為或架構提升>

### Notes
<補充說明，例如依賴其他項目、風險、重構策略等>
```

---

## ID: RF-0001
**Title:** Symbol Pipeline 正規化與 YAML 驅動架構
**CreatedAt:** 2025-12-10
**Status:** Pending
**Branch:** <branch-name>
**StartedAt:** YYYY-MM-DD
**EndedAt:** YYYY-MM-DD

### Description
目前 `hf_market_data_kol`、`lf_market_data_kol`、`ref_data_kol` 存在同一類 symbol 處理問題，屬於跨模組的架構性技術債：

- symbol 清單（如 `BTC-USDT-SWAP`）直接硬寫在 `kol_chief/main.go`
- index feed 需要 `BASE-QUOTE`，但目前是用字串 `split("-")` 粗暴拆解
- symbol 相關的 domain knowledge 分散於 `main`、`collector`、`refiner`，缺乏統一入口
- `resolver` 雖支援 YAML，但僅處理 **canonical ↔ native** 對照，不支援描述「KOL 要訂閱哪些 symbol」
- `collect_group` 的 API 僅接受 `[]string`，但內容有時是 canonical、有時又像 native，語意不一致且容易出錯
- 因缺乏「symbol 設定的單一真實來源（Single Source of Truth）」
  → 無法實現「只需指定 YAML → 自動配置 resolver → 自動產生可訂閱 symbol → 自動建立訂閱計畫」的完整資料管線

此項目屬於架構級技術債，牽涉多個 KOL pipeline，不適合在 review 階段大量改動，需獨立啟動 refactor phase。

### Expected Outcome
- 建立統一的 **`SymbolPlan` 抽象層**，讓所有 KOL 使用共同的 symbol pipeline
- 所有 symbol 清單從硬編改為 **YAML 驅動 (YAML-driven symbols)**
- `resolver` 增加批次載入、批次正規化能力，可處理整包 YAML 並轉成 canonical
- `collector` / `refiner` 改為吃結構化的 **symbol 訂閱計畫**，避免再操作原始 `[]string`
- canonical parsing、market-type 轉換等 domain logic 完全收斂到 `infra/symbols`
- `kol_chief` 不再包含任何 symbol 操作，只負責載入 SymbolPlan 並啟動模組

### Notes
- 建議先建立 `SymbolPlan` interface，再用「硬編版 Provider → YAML Provider」的方式逐步遷移
- 影響範圍橫跨 hf / lf / ref_data KOL，需要一次規劃、同步推進
- 適合在整體 review 結束後啟動的 dedicated refactor（避免邊 review 邊大幅更動架構）
