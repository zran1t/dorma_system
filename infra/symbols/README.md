# `infra/symbols` 套件說明

> **角色定位：**
> 提供系統內部統一的 **symbol 解析層**，負責 *canonical ↔ exchange native symbol* 的查找與反查。
> 核心邏輯是 `inMemoryResolver`（純記憶體），載入入口則由 `loader.go` 讀取 YAML 並寫入 resolver。

---

## 1. 套件職責（What）

這個 package 只做兩件事：

1. **定義 symbol 解析介面與 in-memory 實作**
   - `Resolver` 介面：canonical → native、native → canonical、批次解析
   - `inMemoryResolver`：用 `map` + `RWMutex` 實作查找與註冊

2. **提供 YAML 載入器（loader）把設定寫進 resolver**
   - `applyExchangeYAML`：從單一交易所 YAML bytes 解析並 `register`
   - `LoadFromDefaultYAML`：從預設路徑讀檔、逐一套用到 `inMemoryResolver`

> 這個 package **不**做：幣種白名單、業務層驗證、持久化、版本管理等。

---

## 2. 設計動機與理由（Why）

### 2.1 為什麼要把「解析」和「載入」分開？

- `resolver.go`：純查找/反查（**不碰 IO**），容易測試與替換
- `loader.go`：把「檔案路徑 / 讀檔 / YAML 解析」收斂在單一檔案，避免上層四處散落 IO 細節

這樣做的好處是：
你可以在測試裡直接塞 `[]byte` YAML（不碰檔案系統），也可以在服務啟動時從 `configs/` 讀取完整設定。

### 2.2 為什麼 `inMemoryResolver` 不匯出？

`inMemoryResolver` 是內部細節（store 結構、鎖、register 行為）。對外只需要：

- 用建構器拿到 resolver：`NewInMemoryResolver()`
- 透過 `Resolver` 介面使用：`CanonicalToNative` / `NativeToCanonical` / `BatchCanonicalToNative`

這樣上層不會依賴 `store` 內部形狀，也方便未來替換 backend（例如 Redis）。

---

## 3. 目錄結構（Layout）

```text
infra/symbols/
  ├── loader.go         // YAML/讀檔載入 → 寫入 inMemoryResolver（只做 IO + schema 檢查）
  ├── resolver.go       // Resolver 介面 + inMemoryResolver 實作（純邏輯、不碰 IO）
  ├── loader_test.go    // loader 層測試（in-memory YAML + configs 整合測試）
  ├── resolver_test.go  // resolver 層測試（查找/反查/批次/ErrNotFound）
  └── README.md         // 本文件
```

---

## 4. 主要型別與介面（API Overview）

### 4.1 `Resolver` 介面

```go
type Resolver interface {
    CanonicalToNative(canonical, exchange string) (string, error)
    BatchCanonicalToNative(canonicals []string, exchange string) ([]string, error)
    NativeToCanonical(exchange, native string) (string, error)
}
```

- `CanonicalToNative`：查 canonical 在某交易所對應的 native symbol
- `BatchCanonicalToNative`：保持輸出順序與輸入 slice 一致（契約）
- `NativeToCanonical`：反向查找（目前為線性掃描）

### 4.2 `inMemoryResolver`

- 使用 `map[string]map[string]string`
  - 第一層 key：canonical（**大寫**）
  - 第二層 key：exchange（**小寫**）
  - value：native symbol（TrimSpace 後的字串）
- 透過 `RWMutex` 確保併發安全

> 註：`register` 是內部方法，用於 loader 或測試安裝資料。

### 4.3 `ErrNotFound`

```go
var ErrNotFound = errors.New("未找到對應的標準標的物代碼")
```

- 查不到資料時，錯誤必須包住 `ErrNotFound`
- 外層用 `errors.Is(err, ErrNotFound)` 判斷即可
  （不依賴錯誤訊息字串）

---

## 5. YAML Schema（設定格式）

單一交易所 YAML 以三段為主：

- `spot`
- `perp`
- `index`

範例：

```yaml
spot:
  BTC-USDT-SPOT: BTC-USDT
perp:
  BTC-USDT-SWAP: BTC-USDT-SWAP
index:
  BTC-USDT-INDEX: BTC-USDT-INDEX
```

### 5.1 key / value 規則

- **key（canonical）必須帶 suffix：**
  - spot：`-SPOT`
  - perp：`-SWAP`
  - index：`-INDEX`
- value 是該交易所的原生 symbol（native symbol，**不可為空字串**）
- loader 只做 suffix 檢查與 YAML 解析，不做更高層業務驗證

---

## 6. 預設設定檔路徑（Default Config Paths）

`LoadFromDefaultYAML` 目前內建：

- `configs/symbol_mapping/okx.yaml`
- `configs/symbol_mapping/binance.yaml`

如果呼叫 `LoadFromDefaultYAML("someExchange")` 且沒有內建路徑，會退回：

- `configs/symbol_mapping/<exchange>.yaml`

---

## 7. 使用範例（Examples）

### 7.1 啟動時從預設 YAML 載入

```go
package main

import (
    "log"

    "dorma_system/infra/symbols"
)

func main() {
    r := symbols.NewInMemoryResolver()

    // 從 configs/symbol_mapping/*.yaml 載入
    if err := r.LoadFromDefaultYAML(); err != nil {
        log.Fatalf("load symbols failed: %v", err)
    }

    native, err := r.CanonicalToNative("BTC-USDT-SPOT", "okx")
    if err != nil {
        log.Fatalf("resolve failed: %v", err)
    }
    log.Println("okx native =", native)
}
```

### 7.2 測試/嵌入式：直接用 in-memory YAML bytes

```go
r := symbols.NewInMemoryResolver()

data := []byte(`
spot:
  BTC-USDT-SPOT: BTC-USDT
`)

if err := r.applyExchangeYAML("okx", data); err != nil {
    // applyExchangeYAML 為內部方法：
    // 僅建議在 symbols package 內（或測試）使用，
    // 上層服務請使用 LoadFromDefaultYAML 或自行包裝 loader。
}

native, _ := r.CanonicalToNative("BTC-USDT-SPOT", "okx")
```

---

## 8. 行為與限制（Behavior & Constraints）

### 8.1 正規化（Normalization）

- canonical：`strings.ToUpper(strings.TrimSpace(canonical))`
- exchange：`strings.ToLower(strings.TrimSpace(exchange))`
- native：`strings.TrimSpace(native)`

也就是說：外部輸入大小寫、前後空白不需要完全一致。

### 8.2 NativeToCanonical 的效能

`NativeToCanonical` 目前是 **線性掃描**：

- 小資料量 OK（你現在預期交易所數量/幣對數不大）
- 若未來反查需求變大，建議新增反向索引（例如 `map[exchange]map[native]canonical`）

### 8.3 BatchCanonicalToNative 的錯誤行為

- 一旦遇到第一筆錯誤，會中止並回傳：
  - `out`（已成功解析的部分結果）
  - `err`
- 此設計讓呼叫端能自行決定是否忽略錯誤、重試、或回退整批結果。

這點在 resolver 註解與測試中都有明確覆蓋。

### 8.4 Thread-safety

- `inMemoryResolver` 用 `RWMutex` 保護 store
- 讀寫在多 goroutine 下是安全的（前提：都走 resolver 方法）

---

## 9. 測試（Testing）

### 9.1 執行測試

```bash
go test ./infra/symbols -v
```

### 9.2 測試策略

- `resolver_test.go`
  - 正向解析 / 反向解析
  - 批次解析順序契約
  - ErrNotFound 的行為（反例測試：預期回錯）

- `loader_test.go`
  - `applyExchangeYAML` 的基本載入（in-memory YAML）
  - suffix 檢查反例（預期回錯）
  - `LoadFromDefaultYAML` 整合測試：
    - 會嘗試尋找 `configs/symbol_mapping/okx.yaml`
    - 找不到則 `t.Skip`（避免因執行目錄不是專案根而 fail）

---

## 10. 命名與使用規範（Guidelines）

- 上層服務優先依賴 `Resolver` 介面，而不是依賴 `inMemoryResolver` 內部行為
- YAML key 一律使用「canonical + suffix」：
  - `-SPOT / -SWAP / -INDEX`
- loader 僅負責 schema 檢查與載入，不在這層塞業務驗證
  （要加白名單/黑名單，請在更上層做）

---

## 11. 未來擴充方向

- 新增反向索引，加速 `NativeToCanonical`
- 新增其他 backend resolver（Redis/DB）但維持同一個 `Resolver` 介面
- 增加更嚴謹的 YAML 驗證（重複 mapping、空值、格式一致性等）
