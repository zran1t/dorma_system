# `infra/pubsub` 套件說明

> **角色定位：**
> 提供系統內部統一的「訊息總線抽象層」，把 *NATS 等具體實作* 隔離在底下，讓上層只依賴 `Publisher` / `Subscriber` / `Bus` 介面。

---

## 1. 套件職責（What）

這個 package 做的事情只有兩件：

1. **定義通用的訊息總線介面**
   - `Publisher`
   - `Subscriber`
   - `Bus`（`Publisher + Subscriber + Close`）
2. **提供一個以 NATS Core 為底層的實作**
   - `NATSCoreBus`

它 **不關心業務內容**，只提供：

- 如何發佈一則訊息（`Publish`）
- 如何訂閱某個 subject 並處理訊息（`Subscribe`）
- 如何關閉連線（`Close`）

所有實際「要發什麼資料、subject 命名規則、資料格式（JSON / Protobuf…）」都交給上層處理。

---

## 2. 設計動機與理由（Why）

### 2.1 為什麼要抽象成 `Publisher` / `Subscriber` / `Bus`？

原因很單純：**不同元件需要的能力不一樣。**

- 像 `WS Collector` 這種只會從交易所收資料，然後「往內部總線丟資料」，只需要：

  ```go
  type Publisher interface {
      Publish(ctx context.Context, subject string, data []byte) error
  }
  ```

  它根本不需要訂閱，也不需要知道要怎麼關閉 NATS。

- 另一種服務（例如 Match Engine / Risk Engine）只需要從總線「訂閱事件並處理」，就只需要：

  ```go
  type Subscriber interface {
      Subscribe(ctx context.Context, subject string, h Handler, opts ...SubOpt) (Subscription, error)
  }
  ```

  這種服務不一定會發佈任何東西。

只有少數「中樞服務」會需要同時具備 `Publish + Subscribe + Close` 三種能力，這時才會依賴：

```go
type Bus interface {
    Publisher
    Subscriber
    Close()
}
```

> **好處：** 上層元件只拿自己需要的最小介面，彼此之間的耦合更低，測試、替換實作、重構都更簡單。


### 2.2 為什麼要 `BusOptions`？

NATS 連線選項很多，如果全部塞在建構函式參數會變得很醜：

```go
NewNATSCoreBus(url string, pingInterval time.Duration, reconnectWait time.Duration, timeout time.Duration, maxReconnects int, ...)
```

改成：

```go
type BusOptions struct {
    URL           string
    Name          string
    PingInterval  time.Duration
    ReconnectWait time.Duration
    Timeout       time.Duration
    MaxReconnects int
}
```

- 呼叫端一眼就知道每個欄位的作用
- 之後如果要加 TLS / 認證等選項也不會爆炸
- 零值選項由實作（`NewNATSCoreBus`）負責補上合理預設值

### 2.3 為什麼 `Message` 長這樣？

```go
type Message struct {
    Subject    string
    Data       []byte
    Header     map[string]string
    ReceivedAt time.Time
}
```

設計重點：

- `Subject` + `Data`：所有實作都會有的最小集合
- `Header`：預留空間給像 HTTP / NATS header 做映射（目前 NATSCoreBus 先不使用，維持 `nil`）
- `ReceivedAt`：實作端決定要不要填，方便之後做延遲分析、監控等

這樣設計可以讓不同實作（NATS / Redis / Kafka…）共用一個 Handler 簽名，不會被某個實作綁死。

---

## 3. 目錄結構（Layout）

目前結構很單純：

```text
infra/pubsub/
  ├── bus.go        // 抽象介面 + 通用型別 (Message / Handler / Options)
  ├── nats_core.go  // NATS Core 具體實作 (NATSCoreBus)
  └── README.md     // 本文件
```

之後如果要新增其它實作（例如：

- `redis_streams.go`
- `kafka_bus.go`

也會放在同一層，照慣例命名。

---

## 4. 主要型別與介面（API Overview）

### 4.1 `Message`

```go
type Message struct {
    Subject    string
    Data       []byte
    Header     map[string]string
    ReceivedAt time.Time
}
```

用來承載經過總線的訊息內容。

- `Subject`：要處理的主題（等同於 NATS subject）
- `Data`：序列化好的 payload（一般會是 JSON / Protobuf）
- `Header`：額外中繼資訊，預留欄位
- `ReceivedAt`：訊息被客戶端接收到的時間


### 4.2 `Handler`

```go
type Handler func(ctx context.Context, m *Message) error
```

訂閱 callback 的簽名。

- `ctx`：傳遞 trace、逾時、取消等資訊
- `m`：收到的訊息
- 回傳 `error`：留給上層決定要不要記 log / retry，這個 package 不做策略判斷


### 4.3 訂閱相關：`Subscription`、`SubOptions`、`SubOpt`

```go
type Subscription interface {
    Unsubscribe() error
    Drain() error
}
```

- `Unsubscribe()`：取消訂閱（不保證所有在 flight 的訊息都處理完）
- `Drain()`：優雅關閉（等待已在隊列中的訊息處理完再關閉）

```go
type SubOptions struct {
    QueueGroup  string
    MaxInflight int
}

type SubOpt func(*SubOptions)
```

目前提供的選項：

```go
func WithQueue(group string) SubOpt
func WithMaxInflight(n int) SubOpt
```

- `WithQueue("group")`：指定 queue group 名稱（對應 NATS queue group）
- `WithMaxInflight(n)`：預留給未來實作「限制同時處理中訊息數量」的功能（目前 NATSCoreBus 尚未使用）


### 4.4 能力介面：`Publisher` / `Subscriber` / `Bus`

```go
type Publisher interface {
    Publish(ctx context.Context, subject string, data []byte) error
}
```

```go
type Subscriber interface {
    Subscribe(ctx context.Context, subject string, h Handler, opts ...SubOpt) (Subscription, error)
}
```

```go
type Bus interface {
    Publisher
    Subscriber
    Close()
}
```

- 上層元件只依賴自己需要的介面，例如：
  - `Collector` 只需要 `Publisher`
  - 撰寫事件處理服務時只需要 `Subscriber`
  - 只有真的要收發合一的服務才會依賴 `Bus`


### 4.5 `BusOptions`

```go
type BusOptions struct {
    URL           string
    Name          string
    PingInterval  time.Duration
    ReconnectWait time.Duration
    Timeout       time.Duration
    MaxReconnects int // -1 = 無限; 0 = 走 nats 預設
}
```

一些注意點：

- `URL`：NATS server endpoint，例如 `nats://127.0.0.1:4222`
- `Name`：client 名稱，方便監控與 debug
- `PingInterval` / `ReconnectWait` / `MaxReconnects`：直接對應 NATS 參數
- `Timeout`：在 `NATSCoreBus` 中會透過 `nats.Timeout(opts.Timeout)` 實際套用到 NATS 連線（例如握手逾時、重新連線等）。
  之後若實作像 `Request` 這類需要逾時控制的操作，也可以以此欄位作為預設逾時來源。

實際補預設值的邏輯在 `NewNATSCoreBus` 中實作。

---

## 5. NATSCoreBus 實作（Implementation）

### 5.1 型別定義

```go
type NATSCoreBus struct {
    nc   *nats.Conn
    opts BusOptions
}

// 編譯期介面保證
var _ Bus        = (*NATSCoreBus)(nil)
var _ Publisher  = (*NATSCoreBus)(nil)
var _ Subscriber = (*NATSCoreBus)(nil)
```

- `nc`：實際的 NATS 連線
- `opts`：建構時使用的選項（只保存，方便 debug 或之後擴充）
- `var _ Interface = (*Type)(nil)`：編譯期檢查，確保 `NATSCoreBus` 有完整實作這些介面


### 5.2 建構：`NewNATSCoreBus`

```go
func NewNATSCoreBus(opts BusOptions) (*NATSCoreBus, error)
```

**預設值策略：**

- `PingInterval == 0` → `10s`
- `ReconnectWait == 0` → `500ms`
- `Timeout == 0` → `5s`（目前僅傳給 NATS，用於 initial connect timeout）
- `MaxReconnects == 0` → `-1`（視為無限重連）

這些都是「合理的通用預設值」，上層可以視情況覆蓋。


### 5.3 發佈：`Publish`

```go
func (b *NATSCoreBus) Publish(ctx context.Context, subject string, data []byte) error {
    _ = ctx // 目前保留參數以利未來擴充
    return b.nc.Publish(subject, data)
}
```

目前 `ctx` 還沒真正綁到 NATS I/O（NATS Core 沒有 context-aware 版本的 `Publish`）。
之後如果要支援更嚴格的逾時行為，可再包一層（例如：channel + select + timeout）。


### 5.4 訂閱：`Subscribe`

```go
func (b *NATSCoreBus) Subscribe(ctx context.Context, subject string, h Handler, opts ...SubOpt) (Subscription, error)
```

流程：

1. 組裝 `SubOptions`（應用所有 `SubOpt`）
2. 包一層 `wrap` 把 `nats.Msg` 轉成 `Message`
3. 根據 `QueueGroup` 是不是空字串，決定要用 `Subscribe` 或 `QueueSubscribe`
4. `b.nc.Flush()` 一次讓訂閱盡快生效，並在這裡提早看出錯誤
5. 回傳包裝過的 `natsCoreSub`

值得注意：

- `Handler` 目前是用同一個 `ctx` 呼叫所有訊息
  - 如果某個 Handler 需要更細粒度的逾時或 cancel，可以自己 `ctx, cancel := context.WithTimeout(ctx, ...)`
- `Header` 暫時不從 `nats.Msg` 映射過來，維持 `nil`
- `ReceivedAt` 由這裡統一填 `time.Now()`（訂閱端收到的時間）


### 5.5 關閉：`Close`

```go
func (b *NATSCoreBus) Close() {
    if b.nc != nil && !b.nc.IsClosed() {
        b.nc.Close()
    }
}
```

- 冪等（idempotent）：多次呼叫不會 panic
- 若連線已經是 `nil` 或 `IsClosed == true`，什麼都不做

---

## 6. 使用範例（Examples）

### 6.1 建立一個 `NATSCoreBus` 並發佈一則訊息

```go
import (
    "context"
    "log"
    "time"

    "dorma_system/infra/pubsub"
)

func examplePublish() {
    bus, err := pubsub.NewNATSCoreBus(pubsub.BusOptions{
        URL:  "nats://127.0.0.1:4222",
        Name: "example-publisher",
    })
    if err != nil {
        log.Fatalf("failed to connect nats: %v", err)
    }
    defer bus.Close()

    ctx := context.Background()
    if err := bus.Publish(ctx, "tick.btcusdt", []byte(`{"p":123.45}`)); err != nil {
        log.Printf("publish error: %v", err)
    }
}
```


### 6.2 建立訂閱並處理訊息

```go
func exampleSubscribe() {
    bus, err := pubsub.NewNATSCoreBus(pubsub.BusOptions{
        URL:  "nats://127.0.0.1:4222",
        Name: "example-subscriber",
    })
    if err != nil {
        log.Fatalf("failed to connect nats: %v", err)
    }
    defer bus.Close()

    ctx := context.Background()

    sub, err := bus.Subscribe(ctx, "tick.btcusdt", func(ctx context.Context, m *pubsub.Message) error {
        log.Printf("recv tick: %s", string(m.Data))
        return nil
    }, pubsub.WithQueue("tick-workers"))
    if err != nil {
        log.Fatalf("subscribe error: %v", err)
    }
    defer sub.Drain()

    // block for demo
    time.Sleep(10 * time.Minute)
}
```


### 6.3 搭配 `ws.Collector` 使用（只依賴 Publisher）

在 `infra/transport/ws` 裡，Collector 的定義是：

```go
type Collector struct {
    ws   WSClient
    adj  Adapter
    pub  pubsub.Publisher
    conf CollectorConfig
}
```

上層初始化時可以傳入 `NATSCoreBus`：

```go
bus, _ := pubsub.NewNATSCoreBus(pubsub.BusOptions{
    URL:  "nats://127.0.0.1:4222",
    Name: "binance-collector",
})

collector := ws.NewCollector(
    ws.NewGorillaWS(),
    binanceKlineAdapter, // Adapter 實作
    bus,                 // 只用到 Publisher 能力
    ws.CollectorConfig{ /* ... */ },
)
```

Collector 完全不知道底下是 NATS，只知道自己可以呼叫 `Publish(ctx, subject, data)`。

---

## 7. 行為與限制（Behavior & Constraints）

### 7.1 Publish

- 不保證同步送達，只是把訊息交給 NATS client
- 若底層連線有問題，會回傳錯誤
- 目前不根據 `ctx` 做超時或取消處理（之後可以在外層封裝）


### 7.2 Subscribe

- `Handler` 目前在 NATS 的 callback goroutine 中執行，請避免在裡面做太重的阻塞操作
- `QueueGroup` 會對應 NATS queue group 機制，用於做水平擴展
- `MaxInflight` 目前僅存在於 `SubOptions` 中，尚未在 NATSCoreBus 中啟用控制邏輯


### 7.3 Close

- 多次呼叫是安全的
- 不會幫你 Unsubscribe / Drain 現有的訂閱
  - 如需完整 drain 行為，建議由上層先呼叫 `sub.Drain()`，再呼叫 `bus.Close()`


### 7.4 Thread-safety

- NATS 官方 `*nats.Conn` 是 goroutine-safe，`NATSCoreBus` 也沒有額外共享可變狀態
- 可以在多個 goroutine 中同時呼叫 `Publish` / `Subscribe`


---

## 8. 測試（Testing）

目前 `infra/pubsub` 底下的所有 NATSCoreBus 測試，都會透過測試工具函式（例如 `newTestBus(t)`）
**實際連線到一個 NATS server**。
因此本套件的測試屬於「輕量整合測試」，而非完全的單元測試。

---

### 8.1 執行測試

執行：

```bash
go test ./infra/pubsub -v
```

目前涵蓋：

- `SubOptions` / `WithQueue` / `WithMaxInflight` 行為
- `Handler` 簽名編譯期檢查
- `BusOptions` 零值接受度
- `NATSCoreBus` 是否完整實作 `Bus` / `Publisher` / `Subscriber`
- `NATSCoreBus` 在實際連線下的基本行為：
  - Publish → 訂閱端可以收到
  - Close → 可多次呼叫且不會 panic


### 8.2 連線 NATS 的整合測試

目前 `TestNATSCoreBus_PublishSubscribe` 需要：

- 本地或遠端有一個可用的 NATS server
- 測試時會實際建立連線 / 發佈 / 訂閱

如需在 CI 上執行：

- 可以用 docker 起一個 `nats:latest`
- 或把這類測試標記成 integration test，在 CI 中用 tag 控制是否執行

---

## 9. 未來擴充方向

- **Request/Reply 支援**
  - 在 `Bus` 上另外加一個 `Request` 介面，會用到 `BusOptions.Timeout`
- **Header 映射**
  - 將 `nats.Msg.Header` 映射到 `Message.Header`
- **MaxInflight 實作**
  - 實際在 NATS 訂閱流程中限制同時處理的訊息數量
- **更多實作**
  - 例如 `KafkaBus` / `RedisStreamsBus`，都可以用同一組 interface 來替換


---

## 10. 命名與使用規範（Guidelines）

- 新的實作建議命名為：`<backend>_bus.go`
  - 例如：`kafka_bus.go`、`redis_streams_bus.go`
- 所有實作都應該：
  - 提供建構函式（例如 `NewKafkaBus(...)`）
  - 做好零值預設值補齊（類似 `NewNATSCoreBus`）
  - 在檔案頂部加上：

    ```go
    var _ Bus = (*XxxBus)(nil)
    ```

- 上層服務在依賴這個 package 時，優先依賴：
  - `Publisher` / `Subscriber`，只有真的需要才依賴整個 `Bus`

這樣可以維持整個專案的訊息流設計保持一致、容易理解，也方便未來替換底層實作。
