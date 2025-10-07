# Market Stream Subjects Spec (OKX)

> 目的：統一「主題(subject) 命名規範」與「各 subject 的用途／原始載荷格式」，方便上游 Adapter 與下游清洗、儲存、監控互通。  
> 範圍：OKX 公共 WS（`trades` / `trades-all` / `bbo-tbt` / `books`），訊息以 **Proto Envelope (market_stream_v1.Envelope)** 包裹、Body 為 `RawBody.raw_data`（原樣 JSON）。

---

## 命名規範
RAW..<FEED_TYPE>[.]

- `RAW`：代表不做清洗的原始資料層。
- `<EXCHANGE>`：交易所代碼（全部大寫）。本文件採用 `OKX`。
- `<FEED_TYPE>`：資料型態（`TRADES` | `TRADES-ALL` | `BBO` | `BOOK`）。
- `<EXTRA>`：必要時的附加維度（預留）。

> 範例：`RAW.OKX.TRADES`、`RAW.OKX.TRADES-ALL`、`RAW.OKX.BBO`、`RAW.OKX.BOOK`

---

## 對照表

| Subject             | OKX WS Channel | Feed Enum                          | 說明                                                                 | 原始 JSON 位置關鍵 | 取用 symbol | 備註 |
|---------------------|----------------|------------------------------------|----------------------------------------------------------------------|--------------------|-------------|------|
| `RAW.OKX.TRADES`    | `trades`       | `FEED_OKX_TRADES`                  | 精選成交（OKX 的「trades」頻道）                                     | `arg.channel`, `arg.instId`, `data` | `arg.instId` | – |
| `RAW.OKX.TRADES-ALL`| `trades-all`   | `FEED_OKX_TRADES_ALL`              | 全量成交（含更細分撮合來源）                                         | 同上               | 同上        | – |
| `RAW.OKX.BBO`       | `bbo-tbt`      | `FEED_OKX_BBO`                     | 最優買/賣（tick-by-tick）                                            | 同上               | 同上        | – |
| `RAW.OKX.BOOK`      | `books`        | `FEED_OKX_BOOK`                    | 深度（Order Book，OKX 預設增量/快照依訂閱）                         | 同上               | 同上        | – |

> Router 的「淺層解析」**只讀** `arg.channel` 與 `arg.instId`，用來決定 subject 與 `Envelope.Symbol`，**不改動原始 payload**。

---

## Envelope（外包裝）規格

- `market_stream_v1.Envelope`（Proto3）
  - `version`: `1`
  - `source.exchange`: `EXCHANGE_OKX`
  - `source.feed`: 對應上表 `Feed Enum`
  - `source.vendor_channel`: 直接填 `arg.channel`（e.g., `trades-all`）
  - `symbol`: **填交易所 native symbol**（`arg.instId`）
  - `market_type`: 未指定時用 `MARKET_TYPE_UNSPECIFIED`
  - `body.raw.raw_data`: **完整原始 WS JSON（bytes）**

> 傳輸以 `proto.Marshal(envelope)` 序列化。

---

## 路由邏輯（Adapter.Handle）

1. 解析 head（僅取 `arg.channel`／`arg.instId`）  
2. `channel → (subject, feed)` 映射（見對照表）  
3. 建立 Envelope（把 `data` 原封不動放進 `body.raw.raw_data`）  
4. 送往 `subject = RAW.OKX.<FEED_TYPE>`  

> **不支援**的 channel：直接丟棄（`return "", nil, nil`）。

---

## 訂閱封包（Adapter.BuildSubscribeMsgs）

- 入參：`channels`（語義化別名，會經 `normalizeChannel` 轉成 OKX 實際 channel），`symbols`（OKX instId）
- 產出：多筆 JSON 字串（逐組 channel × symbol）

### normalizeChannel 對照
- `"trades"` → `"trades"`
- `"books"` / `"book"` → `"books"`
- `"bbo"` → `"bbo-tbt"`
- `"trades-all"` → `"trades-all"`
- 其他：原樣傳回

### 訂閱範例
```json
{"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT-SWAP"}]}
{"op":"subscribe","args":[{"channel":"trades-all","instId":"BTC-USDT-SWAP"}]}
{"op":"subscribe","args":[{"channel":"bbo-tbt","instId":"BTC-USDT-SWAP"}]}
{"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT-SWAP"}]}



清洗完後的 subject CLEAN.交易所.FEEDTYPE

CLEAN.OKX.BBO
CLEAN.OKX.TRADES-ALL
CLEAN.OKX.TRADES
CLEAN.OKX.BOOK.DEALTA
CLEAN.OKX.BOOK.FULL