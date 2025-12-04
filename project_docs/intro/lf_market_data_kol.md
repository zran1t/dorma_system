# lf_market_data_kol — Low-Frequency Market Data Pipeline (KOL Division)

> **目標**：以統一的 Envelope 結構與 Subject 命名，收集 OKX 的 **KLine (MARK/INDEX)** 並清洗成一致的 `CLEAN` 訊流，供下游即時處理與分析。

---

## 🧩 模組分層

```
┌────────────────────────┐
│ Collect Group (收集層)  │ → RAW bytes(JSON wrapped)
└──────────┬─────────────┘
           ↓
┌────────────────────────┐
│ Refine Group (精煉層)   │ → CLEAN Envelopes
└──────────┬─────────────┘
           ↓
        下游系統
```

- **Collect Group**：連線交易所 WS（OKX business ws），訂閱 kline 頻道，將原始 JSON 包入 `Envelope.Body = Any<RawBody>` 後發佈到 `RAW.*`。
- **Refine Group**：訂閱 `RAW.*`，解析 JSON、規範 interval / symbol / 市場別，產出標準化 `CLEAN.*`。

---

## ⚙️ 目前交付的行情與範圍

- **交易所**：`OKX`
- **行情家族（feeds）**：
  - `MARK-CANDLE` — Mark Price KLine（永續合約的標記價K線）
  - `INDEX-CANDLE` — Index KLine（指數價K線，由各交易對現貨加權組成）
- **標的集合（canonical symbols）**：
  - `BTC-USDT-SWAP`, `ETH-USDT-SWAP`, `BNB-USDT-SWAP`, `XRP-USDT-SWAP`,
    `SOL-USDT-SWAP`, `ADA-USDT-SWAP`, `DOGE-USDT-SWAP`, `MATIC-USDT-SWAP`,
    `DOT-USDT-SWAP`, `LTC-USDT-SWAP`
  - INDEX 家族會自動由 SWAP 標的轉換為 `-INDEX` 結尾：
    ```
    BTC-USDT-SWAP → BTC-USDT-INDEX
    ETH-USDT-SWAP → ETH-USDT-INDEX
    ...
    ```

- **可用的時間週期（intervals）**
  下列 **兩個家族（MARK / INDEX）均支援相同集合**。系統在組裝 channel 時**完整保留外部傳入格式**（分鐘用小寫 `m`；小時/日/週/月用大寫；帶 `utc` 尾碼的維持原樣）。

  - **非 UTC 版本**
    - 分鐘：`1m`, `3m`, `5m`, `15m`, `30m`
    - 小時：`1H`, `2H`, `4H`, `6H`, `12H`
    - 日週月季年：`1D`, `2D`, `3D`, `5D`, `1W`, `1M`, `3M`

  - **UTC 版本**
    - 小時（UTC）：`6Hutc`, `12Hutc`
    - 日（UTC）：`1Dutc`, `2Dutc`, `3Dutc`, `5Dutc`
    - 週（UTC）：`1Wutc`
    - 月（UTC）：`1Mutc`, `3Mutc`
    - 年（UTC）：`1Yutc`

> ✅ 經過測試：adapter 會將 interval 精確對應官方命名；白名單校驗避免非法或拼寫錯誤的訂閱。

---

## 🧮 Subject 命名規範（**新版：RAW/CLEAN 都含市場後綴**）

### Collect（RAW 層）
- MARK KLine：
  `RAW.OKX.MARK-CANDLE.<BASE>.<QUOTE>.<SWAP>.<INTERVAL>`
  例：`RAW.OKX.MARK-CANDLE.BTC.USDT.SWAP.1m`

- INDEX KLine：
  `RAW.OKX.INDEX-CANDLE.<BASE>.<QUOTE>.<INDEX>.<INTERVAL>`
  例：`RAW.OKX.INDEX-CANDLE.BTC.USDT.INDEX.1m`

> RAW 層只承載 **RawBody**（`RawBody.raw_data` 為交易所原始 JSON）。

### Refine（CLEAN 層）
- MARK：`CLEAN.OKX.MARK-CANDLE.<BASE>.<QUOTE>.<SWAP>.<INTERVAL>`
  例：`CLEAN.OKX.MARK-CANDLE.BTC.USDT.SWAP.1m` → `Symbol = BTC-USDT-SWAP`，`MarketType = MARKET_PERPETUAL`

- INDEX：`CLEAN.OKX.INDEX-CANDLE.<BASE>.<QUOTE>.<INDEX>.<INTERVAL>`
  例：`CLEAN.OKX.INDEX-CANDLE.BTC.USDT.INDEX.1m` → `Symbol = BTC-USDT-INDEX`，`MarketType = MARKET_INDEX`

> **說明**：為了與 CLEAN 完全對齊與更直觀的篩選/監控，RAW 也改為含市場後綴（SWAP/INDEX），使上下游命名一致。

---

## 🧠 Envelope 結構（關鍵欄位）

```proto
message Envelope {
  uint32 version = 1;
  Source source = 2;           // exchange, feed, vendor_channel, interval
  string symbol = 3;           // 規範化後符號（強制 SWAP/INDEX）
  MarketType market_type = 4;  // SPOT / PERPETUAL / INDEX
  bytes message_id = 5;        // xxh3_128(exchange|feed|body)
  Timestamps timestamps = 6;   // 全鏈路延遲追蹤
  google.protobuf.Any body = 20;
}

message Source {
  Exchange exchange = 1;       // EXCHANGE_OKX
  string feed = 2;             // "KLINE.MARKPRICE" 或 "KLINE.INDEX"
  string vendor_channel = 3;   // 例如 "mark-price-candle1m"
  string interval = 4;         // 權威 interval，保留原樣（e.g. 1m / 1Dutc）
}

message Timestamps {
  uint64 event_ts_us      = 1; // 事件時間（開盤時間）
  uint64 collect_recv_us  = 2; // Collect 收到 ws
  uint64 collect_pub_us   = 3; // Collect 發 RAW
  uint64 refiner_recv_us  = 4; // Refiner 收 RAW
  uint64 refiner_pub_us   = 5; // Refiner 發 CLEAN
}
```

### CLEAN Body（KLine）

- **MARK** → `type.googleapis.com/market.kline.v1.OKXMarkPriceKLineBody`
- **INDEX** → `type.googleapis.com/market.kline.v1.OKXIndexKLineBody`

共同欄位：

```proto
message KLineBar {
  uint64 open_ts_us  = 1;   // 開盤時間（us）
  string interval    = 2;   // "1m" / "1Dutc"（與 Source.interval 一致）
  int64  open_e9     = 3;
  int64  high_e9     = 4;
  int64  low_e9      = 5;
  int64  close_e9    = 6;
  int64  volume_e9   = 7;   // MARK/INDEX 量為 0
  bool   confirmed   = 8;   // 已收定
  uint64 vendor_ts_us= 9;   // 交易所事件時間（通常與 open_ts_us 對齊）
}
```

> **Refiner 僅發佈 `confirmed=true` 的 bar**。

---

## 🔄 Symbol 與市場別規則

- Collect 層：`Envelope.symbol` = 交易所 `instId`（如 `BTC-USDT-SWAP` / `BTC-USDT`）。
- Refine 層：
  - 先嘗試 `resolver.ReverseResolve("okx", instId)` 取得 canonical；若無，取 vendor 原字串。
  - **KLine 強制規範化：**
    - MARK → `BASE-QUOTE-SWAP`
    - INDEX → `BASE-QUOTE-INDEX`
- `MarketType`：根據 `Symbol` 的後綴判斷（SWAP→PERPETUAL；INDEX→INDEX；SPOT→SPOT）。

---

## ⏱️ Timestamps 鏈與延遲

- 事件時間：取 bar 的 `open_ts_us`（最後一根 bar 的開盤時間）
- 可計算端對端延遲：
  `latency_us = refiner_pub_us - collect_recv_us`

---

## 🧾 message_id（去重鍵）

```
len(exchange) | exchange | len(feed) | feed | len(body) | body_bytes
→ xxh3_128 → 16 bytes → Envelope.message_id
```

> feed 或 body 任一變動，`message_id` 皆不同。

---

## ▶️ 快速啟動

### 1) 啟動 kol_chief

```bash
NATS_URL="nats://127.0.0.1:4222" go run data_dpt/kols/lf_market_data_kol/kol_chief/main.go
```

預期日誌：

```
[lf/kol_chief] NATS connected (collectors): nats://127.0.0.1:4222
[lf/kol_chief] refiner started: exchange=okx feeds=[MARK-CANDLE INDEX-CANDLE]
[lf.kline] started interval=1m channel=mark-price-candle1m symbols=10
[lf.kline] started interval=1m channel=index-candle1m symbols=10
...
```

### 2) 驗證 CLEAN

```bash
SUBJECT=CLEAN.OKX.MARK-CANDLE.BTC.USDT.SWAP.1m   go run tests/sniff/main.go
SUBJECT=CLEAN.OKX.INDEX-CANDLE.BTC.USDT.INDEX.1m go run tests/sniff/main.go
```

輸出片段（示意）：

```
Subject: CLEAN.OKX.MARK-CANDLE.BTC.USDT.SWAP.1m
Symbol:  BTC-USDT-SWAP
Market:  MARKET_PERPETUAL
Source:  feed=KLINE.MARKPRICE interval=1m vendor_channel=mark-price-candle1m
Body(type=OKXMarkPriceKLineBody) bars[0].confirmed=true ...
```

### 3) 驗證 RAW（選看）

```bash
nats sub "RAW.OKX.MARK-CANDLE.BTC.USDT.SWAP.1m"
nats sub "RAW.OKX.INDEX-CANDLE.BTC.USDT.INDEX.1m"
```

> 會看到 protobuf Envelope 的 bytes 與原始 JSON 混合輸出（CLI 會顯示部分不可見字元），屬正常現象。

---

## 🧪 測試重點清單

- `CLEAN.OKX.MARK-CANDLE.*` → Subject 與 `Symbol` 一致含 `-SWAP`
- `CLEAN.OKX.INDEX-CANDLE.*` → Subject 與 `Symbol` 一致含 `-INDEX`
- `Source.interval` 與 body.bar.interval 完全一致（包含大小寫與 `utc`）
- 僅發佈 `confirmed=true` 的 bar
- `Timestamps` 鏈完整，`event_ts_us` = bar `open_ts_us`

---

## 🧰 主要元件對照

### Collect Group

- `collect_group/Chief`
  - 以 **interval 粒度**啟動一條 Collector
  - OKX `business` ws endpoint：`wss://ws.okx.com:8443/ws/v5/business`
  - `okx.MakeChannel(baseFeed, interval)`：保留分鐘小寫 `m`，小時/日/週/月大寫；若屬 OKX 提供的 UTC 版本則附加 `utc`。

- `collect_group/exchanges/okx/Adapter`
  - `BuildSubscribeMsgs(channels, symbols)` 產生訂閱 JSON
  - `Handle(data)`：
    - 解析 `arg.channel` 與 `arg.instId`
    - 拆解 `family / interval`（例：`mark-price-candle1m` → `1m`）
    - 組成 **RAW（含市場後綴）**：`RAW.OKX.(MARK|INDEX)-CANDLE.<BASE>.<QUOTE>.<SUFFIX>.<INTERVAL>`
    - Body = `Any<RawBody>`（raw JSON bytes）

### Refine Group

- `refine_group/Chief`：將 `"MARK-CANDLE" / "INDEX-CANDLE"` 正規化為 `"KLINE.MARKPRICE" / "KLINE.INDEX"`，分別訂閱 `RAW.OKX.MARK-CANDLE.>` / `RAW.OKX.INDEX-CANDLE.>`（相容含後綴的新版）。
- `refine_group/exchanges/okx/adapter.go`：
  - 解析 `RawBody`
  - 只保留 `confirmed=1` 的列
  - **強制規範** Symbol 後綴（MARK→SWAP / INDEX→INDEX）
  - 組 `OKXMarkPriceKLineBody` / `OKXIndexKLineBody`
  - 計算 `message_id`，補 `timestamps`，發佈 `CLEAN...`（含市場後綴）。

---

## 🚨 常見問題（Troubleshooting）

1. **訂閱 RAW 看到亂碼？**
   正常：訊息是「protobuf Envelope bytes」+「raw JSON」混雜，CLI 直印會出現不可見字元。使用 `tests/sniff` 或寫解析器即可。

2. **偶發 Symbol 後綴不一致？**
   Refine 端強制規範化（MARK→SWAP / INDEX→INDEX）。若仍觀察到例外，檢查是否同時跑了舊版進程；建議在主程式印出 commit/build info。

3. **interval 大小寫與 `utc` 尾綴**
   外部傳入怎樣就儘量保留原樣；OKX 有提供 UTC 變體的才加 `utc`。分鐘一律 `1m/5m/15m/30m`（小寫 `m`）。

4. **沒有資料？**
   - 檢查 NATS_URL
   - 檢查 `symbols.Resolver` 是否載入成功
   - 檢查 WS 連線與訂閱是否成功（`[lf.kline] started interval=...` 日誌）
   - 檢查 refiner 是否啟動（`[lf/kol_chief] refiner started: exchange=okx feeds=[...]`）

---

## 🧭 設計原則回顧

- **單一職責**：Collect 只包裝、Refine 才標準化。
- **命名一致**：`RAW/CLEAN` 皆含市場後綴，語義直觀一致。
- **可觀測**：完整 `timestamps` 鏈路。
- **穩定性**：只發佈 `confirmed=true`，避免半成品 bar。
- **可擴充**：新增交易所只需實作對應 `Adapter` 與在 Chief 註冊。

---

## 🪜 後續擴充建議

- **Binance KLine**：複用相同 subject 與 Envelope schema。
- **Prometheus 指標**：msgs/s、confirmed 比例、端對端延遲分佈。
- **健全性驗證**：Refine 前置 schema/欄位範圍校驗。
- **Build Info**：在主程式印出 `commit`/`build time`，避免舊版進程混淆。

---

## 📘 範例指令速查

```bash
# 啟動
go run data_dpt/kols/lf_market_data_kol/kol_chief/main.go

# 觀察 CLEAN（Mark / Index）
SUBJECT=CLEAN.OKX.MARK-CANDLE.BTC.USDT.SWAP.1m   go run tests/sniff/main.go
SUBJECT=CLEAN.OKX.INDEX-CANDLE.BTC.USDT.INDEX.1m go run tests/sniff/main.go

# 觀察 RAW（會混合 bytes + JSON，僅供驗證）
nats sub "RAW.OKX.MARK-CANDLE.BTC.USDT.SWAP.1m"
nats sub "RAW.OKX.INDEX-CANDLE.BTC.USDT.INDEX.1m"
```
