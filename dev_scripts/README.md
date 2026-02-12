## Redis 使用方法

---

### ✅ 開啟全部 Redis

```bash
./scripts/redis_start_all.sh
```

---

### 🛑 關閉單一 Redis 層

```bash
./scripts/stop_redis_layer.sh push
./scripts/stop_redis_layer.sh cache
./scripts/stop_redis_layer.sh delay
./scripts/stop_redis_layer.sh snapshot
./scripts/stop_redis_layer.sh failover
```

---

### 🛑 關閉全部 Redis

```bash
./scripts/redis_stop_all.sh
```
---

## 📊 Redis 連線狀態查詢

查看每一層 Redis 是否啟動 × 目前有多少連線：

```bash
./scripts/redis_status_check.sh
```

### ✅ 執行結果範例：

```
📊 Redis 連線狀態總覽：
✅ [push] (port: 6379) → connected_clients: 3
✅ [cache] (port: 6380) → connected_clients: 2
❌ [delay] (port: 6381) 無法連線或 Redis 未啟動
✅ [snapshot] (port: 6382) → connected_clients: 1
✅ [failover] (port: 6383) → connected_clients: 2
```

> 可快速確認所有 Redis 實例是否在線、是否已被模組連線
---

## Redis 常用指令

### 🔍 測試連線

```bash
redis-cli -p 6379 ping
# 輸出應為：PONG
```

---

### 📄 查看所有鍵

```bash
redis-cli -p 6379 keys "*"
```

---

### 📦 查看某個 key 的值

```bash
redis-cli -p 6380 get mykey
redis-cli -p 6380 hgetall account:BTC-USDT
redis-cli -p 6382 lrange snapshot:xxx 0 -1
redis-cli -p 6379 xread STREAMS stream:xxx 0
```

---

### 🧹 刪除某個 key

```bash
redis-cli -p 6380 del mykey
```

---

### 💣 清空整個資料庫（危險）

```bash
redis-cli -p 6380 flushall
```

---

### 📦 查看 AOF 或 RDB 是否啟用（確認持久化）

```bash
redis-cli -p 6380 config get appendonly
redis-cli -p 6380 config get save
```

---

### 📈 Redis 狀態監控

```bash
redis-cli -p 6380 info
```

---

## 🚀 FastAPI 外部控制接口使用方法

### ✅ 啟動伺服器

在 `injector_api/` 目錄下執行：

```bash
uvicorn injector_api.main:app --reload
```

- `main:app`：表示 main.py 裡的 `app = FastAPI()`
- `--reload`：開啟自動重載（開發用）
- 預設伺服器位置為：`http://127.0.0.1:8000`

---

### ✅ 測試介面（Swagger）

瀏覽器打開：

```
http://127.0.0.1:8000/docs
```

可使用 `/inject` 進行策略注入測試。貼入測試資料後按下 `Execute` 即可觸發寫入 Redis：

```json
{
  "symbols": ["BTC-USDT", "ETH-USDT"],
  "strategy_id": "sma_v2",
  "params": {
    "period": 20,
    "threshold": 0.01
  }
}
```

---

### ✅ 查 Redis 是否寫入成功

例如查 BTC-USDT 的策略設定：

```bash
redis-cli -p 6380 hgetall strategy:BTC-USDT
```

---

## 📡 NATS 使用方法

---

### ✅ 啟動 NATS 主通訊總線（module-bus）

背景腳本執行：

```bash
./scripts/nats_start_all.sh
```

> ℹ️ 注意：目前已啟用授權機制，所有 client 需提供帳號密碼：
> - 使用者名稱：`module`
> - 密碼：`dorma_core`


```bash
./scripts/nats_status_check.sh
```


---

### 🛑 關閉 NATS

使用下列指令關閉目前執行中的 NATS（預設 port: 4222）：

背景腳本執行：

```bash
./scripts/nats_stop_all.sh
```

---

### 📋 常用指令（需安裝 nats CLI）

```bash
nats server check connection -s nats://127.0.0.1:4222 --user module --password dorma_core        # 檢查連線狀態
nats pub subject.name "Hello" --user module --password dorma_core                                # 發送訊息
nats sub subject.name --user module --password dorma_core                                        # 訂閱訊息
nats stream add stream_id --subjects "foo.*" --user module --password dorma_core                 # 建立 Stream
nats stream ls --user module --password dorma_core                                               # 查看所有 Stream
nats stream info stream_id --user module --password dorma_core                                   # 查看指定 Stream
nats stream rm stream_id --user module --password dorma_core                                     # 刪除 Stream
nats consumer add stream_id --name consumer_id --user module --password dorma_core               # 建立 Consumer
nats consumer ls stream_id --user module --password dorma_core                                   # 查看 Consumers
```

---



source .venv/bin/activate








cd schemas

protoc -I proto \
  --go_out=paths=source_relative:gen/go \
  --go-grpc_out=paths=source_relative:gen/go \
  proto/market/stream/v1/market_stream.proto

protoc -I proto \
  --go_out=paths=source_relative:gen/go \
  --go-grpc_out=paths=source_relative:gen/go \
  proto/market/common/v1/market_common.proto

protoc -I proto \
  --go_out=paths=source_relative:gen/go \
  --go-grpc_out=paths=source_relative:gen/go \
  proto/market/kline/v1/market_kline.proto

  revive -config revive.toml -formatter stylish ./...



// FunctionName 函式功能一句話說明（動詞開頭）。
// 功能:
//   - （描述這個函式主要做什麼事，盡量簡短清楚）
// 參數:
//   - param1: 說明這個參數用途。
//   - param2: 說明這個參數用途。
// 回傳:
//   - result: 回傳值的意義與範圍。
//   - err: 錯誤情況下的行為（若適用）。
// 備註:
//   - 特殊邏輯或例外情況。
//   - 若有外部依賴、cache、I/O 等副作用可在此說明。
func FunctionName(param1 Type1, param2 Type2) (result Type3, err error) {
    // ...
}
