# `infra/preset` 套件說明

> **角色定位：**
> `infra/preset` 提供「系統啟動預設 (system_presets)」的**唯一權威讀取與編譯入口**：將 YAML 的「訂閱/治理意圖」解析、做最小合法性檢查，並**編譯成各部門（KOL / 初始化流程）可直接使用的 Packet**，避免上層直接依賴 YAML 結構與 merge 規則。

---

## 1. 套件職責（What）

1. **讀取 system_presets（I/O + YAML 反序列化）**
   - 將 `configs/system_presets/*.yaml` 解析為 preset 結構（`reader`）。
2. **最小合法性驗證（格式/最小語意）**
   - 驗證版本、是否存在 exchanges 等「最低限度」條件（`validator`）。
3. **編譯為封包（Packet）**
   - 將 preset（defaults + override / presence semantic）編譯為可直接執行的封包：
     - Market Data Packet：展開成 subscriptions（realtime/candle + intervals）
     - Instrument Data Packet：輸出 enabled feeds + symbol filter
     - Trading Allowlist Packet：展開允許交易清單（presence => allowed）
   - 編譯邏輯集中在 `compiler`。
4. **對外唯一入口（Loader API）**
   - 封裝讀檔路徑（`paths.go`）與 pipeline（reader → validator → compiler → packet）。

---

## 2. 設計動機與理由（Why）

### 2.1 為什麼要有這個 package？

- **切斷上層對 YAML 的耦合**：上層（KOL / 初始化部門）只拿 Packet，不需要理解 defaults/override、presence semantic 等規則。
- **降低重複與錯誤擴散**：所有 YAML 解析與 merge 規則集中在一處，避免每個部門各自讀檔/merge。
- **提供穩定跨模組契約**：Packet 結構相對穩定，上層可依賴 JSON tag / struct 語意，不必跟 YAML schema 共同演進。

### 2.2 為什麼拆成 reader / validator / compiler？

- **reader**：只做 I/O + Unmarshal，保持純資料層。
- **validator**：只做「最小必要檢查」，避免把 adapter capability / 業務規則塞進 infra。
- **compiler**：集中所有「推導 / 展開 / merge」行為，輸出可執行的 Packet。
- 這個拆法的目標是：責任邊界清楚、容易測試、容易替換某一段行為。

### 2.3 為什麼 Packet 不保留 defaults/override？

- Packet 是「**意圖展開結果**」，給下游直接使用：
  - Market Data：已完成 merge 並展開為 subscriptions；下游只需迭代訂閱。
  - Allowlist：已展開成 allowed instruments；下游直接建立 set/map。
- defaults/override 只對「人類撰寫」與「loader 編譯」有價值；對下游執行期價值低、反而增加理解成本。

---

## 3. 目錄結構（Layout）

```text
infra/preset/
  ├── loader.go                 // 對外唯一入口：Load*Packet / LoadAllPackets
  ├── paths.go                  // system_presets 唯一權威路徑常數
  ├── loader_test.go            // pipeline 測試（使用 temp YAML）
  │
  ├── reader/                   // YAML → Preset structs（只做 I/O + Unmarshal）
  │   ├── common.go             // readYAMLFile(...)（共用讀檔/解碼）
  │   ├── market_data.go
  │   ├── instrument_data.go
  │   └── trading_allowlist.go
  │
  ├── validator/                // 最小合法性檢查（不含 adapter capability）
  │   ├── common.go
  │   ├── market_data.go
  │   ├── instrument_data.go
  │   └── trading_allowlist.go
  │
  ├── compiler/                 // Preset → Packet（merge / expand / flatten）
  │   ├── market_data.go
  │   ├── instrument_data.go
  │   └── trading_allowlist.go
  │
  └── packet/                   // 對外契約（下游只依賴這層）
      ├── types.go              // Market enum
      ├── market_data_types.go
      ├── instrument_data_types.go
      └── trading_allowlist_types.go
```

---

## 4. 主要型別與介面（API Overview）

### 4.1 核心資料結構（Packets）

#### `packet.MarketDataPacket`

```go
type MarketDataPacket struct {
    Version   int
    Summary   string
    Exchanges map[string]MarketDataExchangePacket
}
```

- `Exchanges[exchange].Subscriptions` 已是展開後的訂閱清單。
- 每筆 subscription 具備：
  - `Exchange`, `Pair`, `Market`, `Feed`
  - `Mode`: `realtime` / `candle`
  - `Intervals`（僅 candle 需要）

#### `packet.InstrumentDataPacket`

```go
type InstrumentDataPacket struct {
    Version   int
    Summary   string
    Exchanges map[string]InstrumentDataExchangePacket
}
```

- `Feeds` 是 feed intent 列表：
  - `Enabled=true` 代表應訂閱
  - `Symbols` 非空時表示 symbol-level filter（例如 funding_rate）

#### `packet.TradingAllowlistPacket`

```go
type TradingAllowlistPacket struct {
    Version   int
    Summary   string
    Exchanges map[string]TradingAllowlistExchangePacket
}
```

- `Allowed` 為展開後的扁平清單（便於建立 set/map）：
  - `Exchange`, `Pair`, `Market`

### 4.2 Loader 對外入口

```go
func LoadMarketDataPacket() (*packet.MarketDataPacket, error)
func LoadInstrumentDataPacket() (*packet.InstrumentDataPacket, error)
func LoadTradingAllowlistPacket() (*packet.TradingAllowlistPacket, error)
func LoadAllPackets() (*AllPackets, error)
```

- 全部使用 `paths.go` 中的「唯一權威路徑」。
- 內部 pipeline：`reader → validator → compiler → validator(packet)`。

### 4.3 Options / Config 類型

- 本 package **不提供 Options**：路徑固定、契約固定。
- 若要做環境切換（dev/prod），建議在更上層（啟動器）切換工作目錄或替換配置檔，不要在 `infra/preset` 內參數化。

---

## 5. 具體實作（Implementation）

### 5.1 Pipeline 概觀

以 `LoadMarketDataPacket()` 為例：

1. `reader.LoadMarketDataPreset(path)`
2. `validator.ValidateMarketDataPreset(preset)`
3. `compiler.CompileMarketData(preset)`
   - 取 defaults
   - 對每個 `symbols[*].markets.<market>` 做 merge（only-diff）
   - 產生 subscriptions（realtime/candle）
4. `validator.ValidateMarketDataPacket(packet)`

### 5.2 Market Data 的合併策略（only-diff override）

- defaults 使用「值型別」完整描述
- override 使用「pointer 結構」描述差異
- 合併語意：
  - override 未提供欄位 ⇒ 沿用 defaults
  - override 提供 `enabled=false` ⇒ 覆寫為 false
  - candle intervals：
    - override 有給 `intervals: [...]` ⇒ 覆寫
    - override 有給 `intervals: []` ⇒ 明確清空

### 5.3 Instrument Data 的 scope 處理

- feed 依其天然粒度使用 preset 表達：
  - market-level / instType-level：通常不帶 symbols
  - symbol-level：帶 `symbols: [instId...]`（例如 funding_rate）

---

## 6. 使用範例（Examples）

### 6.1 最小可用：讀取 Market Data Packet

```go
md, err := preset.LoadMarketDataPacket()
if err != nil { /* handle */ }

for _, ex := range md.Exchanges {
    for _, sub := range ex.Subscriptions {
        // 直接把 sub 丟給你的 collector / adapter subscribe
    }
}
```

### 6.2 初始化流程：一次讀三份 Packet

```go
all, err := preset.LoadAllPackets()
if err != nil { /* handle */ }

// allowlist 給下單風控/初始化
allow := all.TradingAllow

// market_data 給 market_data_kol
md := all.MarketData

// instrument_data 給 instrument_data_kol
ins := all.InstrumentData
```

### 6.3 下游建立加速索引（典型）

```go
allowed := map[string]struct{}{}
for _, ex := range allow.Exchanges {
    for _, a := range ex.Allowed {
        key := a.Exchange + "|" + a.Pair + "|" + string(a.Market)
        allowed[key] = struct{}{}
    }
}
```

---

## 7. 行為與限制（Behavior & Constraints）

- Thread-safety：
  - Loader 函式無共享狀態，**可併發呼叫**（但會重複讀檔）。
- Lifecycle：
  - 無需 Close；純讀檔 + 結構編譯。
- Error handling：
  - 讀檔/解碼錯誤會直接回傳。
  - compiler 遇到 invalid market 會回傳錯誤。
- 限制：
  - capability 驗證（交易所支援哪些 feed）**不在此層**，應由 adapter capability table 或更上層 validator 負責。

---

## 8. 測試（Testing）

- 測試策略：
  - 使用 temp YAML（寫入 `t.TempDir()`），避免依賴 repo 真實配置。
  - 覆蓋讀取、merge、展開、presence semantic、錯誤情境（invalid market）。
- 執行指令：

```bash
go test ./infra/preset -v
```

---

## 9. 未來擴充方向

- capability table 驗證整合：
  - 在更上層引入 adapter 能力表，對 feed / market / intervals 做一致性驗證。
- 增加更多 preset 類型：
  - 例如 risk / strategy bootstrap 類 presets（但仍維持 Packet 契約輸出）。
- 增加 cache（避免重複讀檔）：
  - 若未來有性能需求，可在更上層做一次載入並分發，或在此層加 memoization（需注意 reload 策略）。

---

## 10. 命名與使用規範（Guidelines）

- 抽象層：
  - `packet/*` 是下游唯一應依賴的資料結構（除非你在寫 loader/validator）。
- loader/reader/compiler 命名規則：
  - `reader/*`：只做 YAML → preset struct
  - `compiler/*`：只做 preset → packet
  - `validator/*`：只做最小合法性檢查
- presets 檔名規則（system_presets）：
  - `market_data.startup.yaml`
  - `instrument_data.startup.yaml`
  - `trading_allowlist.yaml`
