# `infra/pubsub` 套件說明

> **角色定位：**
> 提供系統內部統一的「訊息總線抽象層」，將 *NATS 等具體實作* 藏在底下，讓上層只依賴 `Publisher` / `Subscriber` / `Bus` 介面。

---

## 1. 套件職責（What）

這個 package 主要負責的事情：

1. **定義通用介面 / 型別**
   - `Publisher` / `Subscriber` / `Bus`
   - `Message` / `Handler`
   - `SubOptions` / `SubOpt` / `Subscription`
   - `BusOptions`
2. **提供 NATS Core 的具體實作**
   - `NATSCoreBus`：建立 NATS 連線、封裝 Publish / Subscribe / Close
   - `natsCoreSub`：包裝 `nats.Subscription`，對外提供 `Unsubscribe()` / `Drain()`

---

## 2. 設計動機與理由（Why）

### 2.1 為什麼要有這個 package？

- 讓上層服務不要直接依賴 NATS SDK（避免各處散落連線/訂閱/錯誤處理細節）。
- 讓「傳輸/基礎設施」與「業務邏輯」切開：上層只關心 subject 與 payload，不關心底層是 NATS 或未來其他 backend。

### 2.2 為什麼介面要這樣設計？

- 拆成 `Publisher` / `Subscriber`：上層元件只拿「最小能力」，避免依賴過大介面。
- `Bus` 只是組合：當某個元件真的同時需要收/發/關閉時才依賴 `Bus`。
- `Handler` 用 function type：讓 Subscribe 的使用方式更直覺（直接丟 callback），也方便在上層封裝 middleware。

### 2.3 為什麼資料結構長這樣？

- `Message` 只保留最核心的通用欄位：
  - `Subject`：對應 NATS subject
  - `Data`：payload（JSON / Protobuf…由上層決定）
  - `ReceivedAt`：訂閱端收到時間，方便做 latency/追查
  - `Header`：預留欄位，目前 `NATSCoreBus` 尚未映射 `nats.Msg.Header`，未使用時維持 `nil`
- `BusOptions` 是連線選項承載體：讓建構函式參數保持乾淨，預設值策略集中在 `NewNATSCoreBus`。

---

## 3. 目錄結構（Layout）

```text
infra/pubsub/
  ├── bus.go           // 抽象介面 + 通用型別 (Message / Handler / Options)
  ├── nats_core.go     // NATS Core 具體實作 (NATSCoreBus)
  ├── bus_test.go      // 介面相容/型別行為檢查（不連 NATS）
  ├── nats_core_test.go// 整合測試：真的連線本機 NATS
  └── README.md        // 本文件
```

---

## 4. 主要型別與介面（API Overview）

### 4.1 核心資料結構：`Message`

```go
type Message struct {
    Subject    string
    Data       []byte
    Header     map[string]string
    ReceivedAt time.Time
}
```

- `Subject` / `Data`：必填（實務上）
- `Header`：可為 `nil`（未使用時建議維持 `nil`）
- `ReceivedAt`：由實作填入（`NATSCoreBus` 於訂閱 callback 填 `time.Now()`）

### 4.2 介面：`Publisher` / `Subscriber` / `Bus`

```go
type Publisher interface {
    Publish(ctx context.Context, subject string, data []byte) error
}

type Subscriber interface {
    Subscribe(ctx context.Context, subject string, h Handler, opts ...SubOpt) (Subscription, error)
}

type Bus interface {
    Publisher
    Subscriber
    Close()
}
```

- `Publish`：把 payload 發到指定 subject
  - 目前 `NATSCoreBus.Publish` 不使用 `ctx` 控制 I/O（保留參數以維持介面一致性）
- `Subscribe`：建立訂閱並註冊 handler（實作會把底層訊息包裝成 `Message`）
- `Close`：釋放底層資源（`NATSCoreBus` 會關閉 NATS connection；可重複呼叫）

### 4.3 訂閱生命週期：`Subscription` / `SubOptions`

```go
type Subscription interface {
    Unsubscribe() error
    Drain() error
}

type SubOptions struct {
    QueueGroup  string
    MaxInflight int
}
```

- `Unsubscribe()`：停止接收後續訊息（不保證已排隊訊息一定處理完成）
- `Drain()`：優雅關閉該訂閱（等目前排隊訊息處理後再關閉，依 NATS 行為為準）
- `QueueGroup`：對應 NATS queue group
- `MaxInflight`：目前僅保留於 `SubOptions`，`NATSCoreBus` 尚未實作併發限制

### 4.4 Options：`BusOptions`

```go
type BusOptions struct {
    URL           string
    Name          string
    PingInterval  time.Duration
    ReconnectWait time.Duration
    Timeout       time.Duration
    MaxReconnects int

    HandlerTimeout time.Duration
    OnHandlerError  func(ctx context.Context, subject string, err error)
}
```

- 零值行為由 `NewNATSCoreBus` 補預設值（例如 PingInterval / Timeout / MaxReconnects…）
- `HandlerTimeout`：
  - 若 > 0，`Subscribe` 會為「每一筆訊息」派生 `context.WithTimeout`
  - 注意：`ctx` 只是訊號；handler 內部要自己尊重 `ctx.Done()` 才會真的中止阻塞工作
- `OnHandlerError`：
  - handler 回傳 error 時呼叫（因為 NATS callback 是非同步，無法從 `Subscribe()` return 往上拋）

---

## 5. 具體實作（Implementation）

### 5.1 主要實作型別：`NATSCoreBus`

```go
type NATSCoreBus struct {
    nc   *nats.Conn
    opts BusOptions
}

var _ Bus        = (*NATSCoreBus)(nil)
var _ Publisher  = (*NATSCoreBus)(nil)
var _ Subscriber = (*NATSCoreBus)(nil)
```

- `nc`：底層 NATS connection（goroutine-safe）
- `opts`：保存初始化用選項（用於 Subscribe callback 的行為，例如 HandlerTimeout / OnHandlerError）

### 5.2 建構函式：`NewNATSCoreBus`

```go
func NewNATSCoreBus(opts BusOptions) (*NATSCoreBus, error)
```

- 會在建構時做 NATS 連線（包含網路 I/O）
- 對零值欄位補預設值（以 `nats.Connect` 的 options 套用）
- 連線失敗直接回傳 error（不會回傳半套物件）

### 5.3 關鍵方法行為

- `Publish(ctx, subject, data)`：
  - 直接呼叫 `nc.Publish(subject, data)`
  - `ctx` 目前保留未使用
- `Subscribe(ctx, subject, handler, opts...)`：
  - 依 `QueueGroup` 決定 `Subscribe` 或 `QueueSubscribe`
  - callback 內把 `nats.Msg` 映射成 `Message`
  - 若 `HandlerTimeout > 0`，每筆訊息派生 `msgCtx`
  - handler 回傳 error 透過 `OnHandlerError` 回報
  - 建立訂閱後會 `nc.Flush()` 一次，提早暴露錯誤
- `Close()`：
  - 立即關閉底層連線（不等待 handler 完成）
  - 多次呼叫安全（idempotent）
  - 不會自動 `Drain()` 所有訂閱；若需要優雅關閉，請由上層持有 `Subscription` 並先 `Drain()`

---

## 6. 使用範例（Examples）

### 6.1 最小可用範例：Publish 一筆訊息

```go
bus, err := pubsub.NewNATSCoreBus(pubsub.BusOptions{
    URL:  "nats://127.0.0.1:4222",
    Name: "example-pub",
})
if err != nil { panic(err) }
defer bus.Close()

_ = bus.Publish(context.Background(), "demo.subject", []byte("hello"))
```

### 6.2 典型用法：Subscribe + QueueGroup

```go
bus, _ := pubsub.NewNATSCoreBus(pubsub.BusOptions{
    URL:  "nats://127.0.0.1:4222",
    Name: "example-sub",
})
defer bus.Close()

sub, err := bus.Subscribe(context.Background(), "demo.subject",
    func(ctx context.Context, m *pubsub.Message) error {
        // handle m.Data
        return nil
    },
    pubsub.WithQueue("demo-workers"),
)
if err != nil { panic(err) }
defer sub.Drain()
```

### 6.3 與其他 package 合作：注入最小介面

- 若某個元件只需要發佈能力，請依賴 `pubsub.Publisher`，不要依賴整個 `Bus`：

```go
func NewCollector(pub pubsub.Publisher) *Collector {
    return &Collector{pub: pub}
}
```

---

## 7. 行為與限制（Behavior & Constraints）

- Thread-safety：
  - `NATSCoreBus` 可併發呼叫 `Publish` / `Subscribe`（底層 `nats.Conn` goroutine-safe）
- Lifecycle：
  - `Close()` 可重複呼叫，不會 panic
  - `Close()` 是「立即關閉連線」，不保證等待 handler 收尾
  - 想要優雅關閉請由上層管理 `Subscription` 並先 `Drain()`
- Error handling：
  - `Subscribe` 的 handler error 不會回到 `Subscribe()` 的 return
  - 若需要統一處理 handler error，請提供 `BusOptions.OnHandlerError`
- 性能/阻塞：
  - handler 在 NATS callback 內執行；避免在 handler 內做長時間阻塞
  - 若啟用 `HandlerTimeout`，僅提供取消/逾時訊號，不會強制中止阻塞 I/O

---

## 8. 測試（Testing）

本 package 有兩類測試：

- **介面/型別檢查**（`bus_test.go`）：不連外，用來保護介面相容與基本型別行為
- **NATS 整合測試**（`nats_core_test.go`）：真的連線本機 `127.0.0.1:4222`；若偵測不到 NATS 會 `t.Skip`

### 8.1 跑測試

```bash
go test ./infra/pubsub -v
```

### 8.2 強制每次都跑、不要用 test cache

```bash
go test ./infra/pubsub -v -count=1
```

### 8.3 直接清掉 Go 的 test cache（需要時）

```bash
go clean -testcache
```

---

## 9. 未來擴充方向

- `MaxInflight`：在 `NATSCoreBus` 落實「單訂閱同時處理上限」
- `Header` 映射：將 `nats.Msg.Header` 映射到 `Message.Header`
- 更進階模式：
  - Request/Reply（可能會使用 `BusOptions.Timeout` 當預設逾時）
  - metrics / tracing（handler latency、queue depth 等）

---

## 10. 命名與使用規範（Guidelines）

- 檔名命名規則
  - 抽象層：`bus.go`
  - backend 實作：`nats_core.go`（未來新增其他 backend 以 `<backend>_*.go` 命名）
- interface 使用規則
  - 上層優先依賴最小介面（`Publisher` / `Subscriber`），非必要不要依賴 `Bus`
- options/config 規則
  - 初始化參數一律用 struct（`BusOptions`），避免建構函式參數爆炸
  - 零值行為由實作補預設值，但需在註解與 README 清楚寫出來
