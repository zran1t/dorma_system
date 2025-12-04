# hf_market_data_kol — High-Frequency Market Data Pipeline (KOL Division)

---

## 🧩 模組概觀

`hf_market_data_kol` 是高頻行情資料部門的核心模組，負責從各交易所收集、清洗、聚合即時行情，並以統一的 **Envelope** 結構發佈至 NATS。
主要目標：

> **以統一封包結構與 Subject 命名規範，實現跨交易所、跨市場的高效即時資料流通。**

模組分層：

```
┌───────────────────────┐
│ Collect Group (收集層) │ → RAW JSON
└──────────┬────────────┘
           ↓
┌───────────────────────┐
│ Refine Group (精煉層)  │ → CLEAN Envelopes
└──────────┬────────────┘
           ↓
┌───────────────────────┐
│ Book Group (簿聚合層) │ → FULL Books
└──────────┬────────────┘
           ↓
        下游系統
```

---

## ⚙️ 資料流總覽

### 1. **Collect Group — WebSocket 收集層**

- 每個交易所、每個 feed（`trades`、`books`、`mark-price`…）對應一條 WebSocket。
- 一條 WS 同時訂閱多個 symbol。

- 收到原始 JSON 後即封裝成：

- 封包型態：
  ```proto
  Envelope {
    source.feed = "BOOK"
    body = RawBody{ raw_data: bytes }
  }
  ```
- gjson做淺層解析，提取channel與instid去判斷該送往的subject，原始封包原封不動塞進RawBody

---

### 2. **Refine Group — 精煉與標準化**

- 對應各交易所 adapter（例如 `refine_group/exchanges/okx`）。
- 訂閱 `RAW.<EXCHANGE>.<FEED>.>`。
- 清洗流程：
  1. 解析 JSON → 對應 Proto 結構 (`OKXTradeBody`, `OKXBooksBody`, ... )。
  2. 透過 `symbols.Resolver` 將交易所 symbol 轉為內部標準符號(symbol_mapping)。
  3. 計算 `message_id`（xxh3-128，包含 feed 與 body）。
  4. 補齊 `Timestamps`。
  5. 發佈至 CLEAN 層。

- 產出 Subject 範例：
  ```
  CLEAN.OKX.TRADES
  CLEAN.OKX.BBO
  CLEAN.OKX.BOOK.DELTA.BTC.USDT.SWAP
  CLEAN.OKX.MARK-PRICE
  CLEAN.OKX.INDEX-TICKERS
  ```

- **BOOK 特例**
  - Refiner 產出的封包代表「增量簿 (delta)」
  - `Source.feed = "BOOK.DELTA"`
  - Subject = `CLEAN.OKX.BOOK.DELTA.<SYMBOL>`
  - Body = `OKXBooksBody{Action: SNAPSHOT/UPDATE}`

---

### 3. **Book Group — 訂單簿聚合層**

- 訂閱：`CLEAN.OKX.BOOK.DELTA.>`
- 以 symbol 為 key 維護內部 `OrderBook` 狀態：
  ```go
  map[string]*OrderBook
  ```

- 每收到一筆 DELTA：
  1. 解析 `OKXBooksBody`
  2. 驗證 `seqId` / `prev_seqId`
  3. 更新 bids/asks map
  4. 計算 OKX checksum（前 25 檔交錯組合）
  5. 若 CRC 不符 → 標記 `needRefresh=true`（稍後觸發重連）

- **節流 (Throttle)：**
  - FULL 發佈頻率受節流控制（例如 1 秒一次）。
  - DELTA 永不丟棄，只暫緩 FULL 發佈。
  - 最終輸出：
    ```
    CLEAN.OKX.BOOK.FULL.<SYMBOL>
    Source.feed = "BOOK.FULL"
    Body = OKXBooksBody{Action: SNAPSHOT, 全簿400檔}
    ```

---

## 🧠 封包結構 (Envelope)

```proto
message Envelope {
  uint32 version = 1;
  Source source = 2;         // 來源資訊
  string symbol = 3;         // 標準化交易對
  MarketType market_type = 4;// 市場類型
  bytes message_id = 5;      // xxh3-128(exchange|feed|body)
  Timestamps timestamps = 6; // 延遲追蹤
  google.protobuf.Any body = 20;
}

message Source {
  Exchange exchange = 1;     // e.g. OKX
  string feed = 2;           // e.g. BOOK.DELTA / BOOK.FULL
  string vendor_channel = 3; // 來源頻道名
}
```

---

## 🧮 Subject 命名規範

| 層級 | 範例 Subject | 描述 |
|------|---------------|------|
| RAW | `RAW.OKX.BOOK` | WS 原始 JSON |
| CLEAN (Refine) | `CLEAN.OKX.BOOK.DELTA.BTC.USDT.SWAP` | 清洗後增量簿 |
| CLEAN (BookGroup) | `CLEAN.OKX.BOOK.FULL.BTC.USDT.SWAP` | 聚合後全簿 |
| CLEAN (Others) | `CLEAN.OKX.TRADES`, `CLEAN.OKX.MARK-PRICE` | 其他行情 |

MarketType 對應：
- SPOT → `.SPOT`
- SWAP → `.SWAP`
- INDEX → `.INDEX`

---

## 🧾 Timestamps 鏈

| 欄位 | 說明 | 單位 |
|------|------|------|
| event_ts_us | 交易所事件時間 | µs |
| collect_recv_us | 收集組接收 WS | µs |
| collect_pub_us | 收集組推送 RAW | µs |
| refiner_recv_us | 精煉組接收 RAW | µs |
| refiner_pub_us | 精煉組推送 CLEAN | µs |

> 可用來計算端對端延遲，例如
> `refiner_pub_us - collect_recv_us = total_latency_us`

---

## 🧱 去重與狀態清理

- **Refiner**
  - Key: `(exchange|feed|symbol)`
  - 雙層去重：`seqId` + `tradeId`
  - 內建 TTL/容量 GC，僅在活躍 feed 滾動修剪。
  - 無 background GC，避免浪費 CPU。

- **BookGroup**
  - 每 symbol 各自 `OrderBook`
  - 單一訂閱 callback 序列執行 → 無需加鎖
  - CRC mismatch 僅標記，未觸發即時重連（介面已保留）

---

## 🧮 message_id 計算規則

```
len(exchange) | exchange | len(feed) | feed | len(body) | body_bytes
→ xxh3_128 hash → 16 bytes message_id
```

此設計確保同一筆資料在 feed 或內容變動時 ID 不重複。

---

## 📡 執行緒模型與效能設計

| 層 | 執行粒度 | 說明 |
|----|-----------|------|
| Collect | feed 級 goroutine | 每 feed 一條 WS；多 symbol 訂閱 |
| Refine | feed 級 goroutine | 每 feed 一條 NATS 訂閱；callback 串行執行 |
| BookGroup | 單 goroutine | 訂閱 `BOOK.DELTA.>`；所有 symbol 順序處理 |

> 優點：
> - 保證資料序列一致性
> - 無鎖設計
> - Feed 間天然併行
> - 無 goroutine 爆炸與 channel 同步開銷

---

## 📊 觀測與監控

- 延遲監控：透過 Envelope.timestamps
- 錯誤監控：CRC mismatch counter、sequence gap counter
- 跨部門通道（建議命名 `INTER.MONITOR.*`）
  - 用於發送異常事件（CRC mismatch、refresh request…）
  - 監控部門訂閱即可接收

---

## ⚖️ 設計理念總結

| 面向 | 策略 | 實現 |
|------|------|------|
| 高內聚 | 每層職責單一 | Collect / Refine / Book 各自獨立 |
| 低耦合 | 全部透過 NATS 傳遞 | Envelope + Subject schema |
| 可擴充 | 新交易所可插入 adapter | 實作 ExchangeAdapter |
| 可觀測 | timestamps + monitor 通道 | 全鏈路延遲可追蹤 |
| 穩定性 | checksum + refresh flag | 準備重連邏輯 |
| 性能 | feed 級 goroutine + throttle | 無 busy loop |

---

## 🪜 未來可擴充項目

- **Refresh Control**
  - BookGroup 偵測 CRC mismatch 後向 Collect 發送 control subject 請求重拉 snapshot。
- **Validator Layer**
  - Refine 發佈前自動檢查 proto schema、欄位範圍。
- **Prometheus Exporter**
  - 收集 feed QPS、FULL 節流次數、CRC 錯誤率。

---

## 🧾 sniff 範例

```bash
# 看增量簿
SUBJECT=CLEAN.OKX.BOOK.DELTA.BTC.USDT.SWAP go run tests/sniff/main.go

# 看聚合後全簿
SUBJECT=CLEAN.OKX.BOOK.FULL.BTC.USDT.SWAP go run tests/sniff/main.go
```

範例輸出：
```
===== 新訊息 =====
Subject: CLEAN.OKX.BOOK.FULL.BTC.USDT.SWAP
Source:  exchange=OKX feed=BOOK.FULL vendor_channel=books
Symbol:  BTC-USDT-SWAP
Body(type=OKXBooksBody)
bids[0]: 65300.1 ...
asks[0]: 65300.3 ...
```

---

## 📘 作者註記

> **hf_market_data_kol** 模組由 Data Department / KOL Market Data 維護。
>
> 職責：
> - Collect 組：交易所 WS 收集與重連機制
> - Refine 組：標準化與封包一致性
> - Book 組：訂單簿合併、校驗與節流
>
> 所有資料最終均以 `Envelope` 為唯一通訊格式。
>
> 修改前請閱讀本文件後再查看各層 Chief / Adapter 內部註解。
