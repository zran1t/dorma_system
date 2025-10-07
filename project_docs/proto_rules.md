# 📐 Proto 欄位設計規範

## 1. 整數類型

### `uint32`
- 用途：數值範圍明確不會太大，且只會是非負整數。
- 適合：
  - `seqId`（流水號，per-symbol / per-channel）
  - `order_count`（掛單筆數）
  - `version`（格式版本號）

### `uint64`
- 用途：支援大範圍正整數，或來源明確定義為 long integer。
- 適合：
  - `tradeId`（全域唯一成交 ID）
  - `message_id`（自算的去重 hash）
  - `timestamps`（Unix 時間戳 ms/us/ns）

### `int64`
- 用途：金融數值，需支援正負號，通常為定點小數。
- 適合：
  - `px_e9`（價格，放大 e9）
  - `qty_e9`（數量，放大 e9）
  - 淨倉位變化等可能為負的數值

---

## 2. 時間戳
- 一律 `uint64`。
- 命名規範：  
  - `*_ms` → 毫秒 (ms)  
  - `*_us` → 微秒 (us)  
- 範例：
  - `event_ms`（交易所原始事件時間）
  - `collect_recv_us`, `refiner_pub_us`（pipeline 打點）

---

## 3. 字串
- 交易所代號 / 商品代號 → `string`
  - e.g. `symbol`, `instId`, `vendor_symbol`
- 含字母或特殊符號的 ID → `string`
  - e.g. Bybit `execId` (UUID 格式)

---

## 4. 枚舉
- **Feed 類型**
  ```proto
  enum Feed {
    FEED_UNSPECIFIED = 0;
    FEED_TRADES      = 1;
    FEED_BOOK        = 2;
    FEED_BBO         = 3;
    FEED_TRADES_ALL  = 4;
  }