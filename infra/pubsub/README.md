# `infra/pubsub` 套件說明

> **角色定位：**
> 提供系統內部統一的「訊息總線抽象層」，將 *NATS 等具體實作* 藏在底下，讓上層只依賴 `Publisher` / `Subscriber` / `Bus` 介面完成 Publish / Subscribe。

---

## 1. 套件職責（What）

這個 package 主要負責的事情：

1. **定義通用介面 / 型別**
   - `Publisher` / `Subscriber` / `Bus`
   - `Message` / `Handler`
   - `Subscription` / `SubOptions` / `SubOpt`
2. **提供一個或多個具體實作**
   - `NATSCoreBus`：以 NATS Core 為底層的 Bus 實作
   - `natsCoreSub`：包裝 `nats.Subscription`，提供 `Unsubscribe()` / `Drain()`

> 重點：這一節只講「做什麼」，不要塞設計理由（留到下一節）。

---

## 2. 設計動機與理由（Why）

### 2.1 為什麼要有這個 package？

- 讓上層服務不要直接依賴 NATS SDK，避免各處散落連線/訂閱/錯誤處理細節。
- 讓「傳輸/基礎設施」與「業務邏輯」切開：上層只關心 subject 與 payload，不關心底層是 NATS 或未來其他 backend。
- 提供更一致的測試方式：上層可依賴介面（或最小介面 `Publisher`/`Subscriber`）注入 stub/fake。

### 2.2 為什麼介面要這樣設計？

- 拆成 `Publisher` / `Subscriber`：上層只依賴「最小能力」，避免依賴過大的大一統介面。
- `Bus` 只是組合：只有需要同時收/發/關閉時才依賴 `Bus`。
- `Handler` 用 function type：使用端可直接傳 callback，亦便於上層封裝 middleware（例如 metrics / tracing / validation）。

### 2.3 為什麼資料結構長這樣？

- `Message` 只保留通用欄位：
  - `Subject`：底層 subject（對應 NATS subject）
  - `Data`：原始 payload（JSON / Protobuf…由上層決定）
  - `ReceivedAt`：訂閱端收到時間，便於 latency 追查
  - `Header`：預留欄位；目前 `NATSCoreBus` 未映射 `nats.Msg.Header`，未使用時維持 `nil`
- `SubOptions` 採 function options（`SubOpt`）：
  - 避免 `Subscribe(...)` 參數爆炸
  - 容易向後相容地擴充

---

## 3. 目錄結構（Layout）

```text
infra/pubsub/
  ├── bus.go            // 抽象介面 + 通用型別 (Message / Handler / Options)
  ├── nats_core.go      // NATS Core 具體實作 (NATSCoreBus)
  ├── bus_test.go       // 純邏輯/編譯期相容測試（不連外）
  ├── nats_core_test.go // 整合測試：真的連線到 NATS（可 skip）
  └── README.md         // 本文件
```

> 規則：
> - 抽象 / 介面類：放一起，命名偏通用，如 `bus.go`
> - 具體實作類：依 backend / 目的命名，如 `nats_core.go`

---

## 4. 主要型別與介面（API Overview）

> 這一節是給「只想知道怎麼用」的人看的，
> 想深入再去看下一節 Implementation。

### 4.1 核心資料結構（例如：`Message`）

```go
type Message struct {
    Subject    string
    Data       []byte
    Header     map[string]string
    ReceivedAt time.Time
}
```

- `Subject` / `Data`：實務上視為必填（由實作填入）
- `Header`：可為 `nil`；目前 NATS header 未映射
- `ReceivedAt`：由實作填入（`NATSCoreBus` 在 callback 內填 `time.Now()`）

### 4.2 介面（例如：`Publisher`, `Subscriber`, `Bus`）

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

- 是否允許併發呼叫：
  - `NATSCoreBus` 的 `Publish` / `Subscribe` 可併發使用（依賴底層 `nats.Conn` 的 goroutine-safety）
- 是否會 block / heavy：
  - `Publish`：包含網路 I/O（由底層 NATS client 決定），可能 block
  - `Subscribe`：建立訂閱時會做底層 I/O，且建立後會 `Flush()` 一次以提早暴露錯誤
- 常見錯誤情境：
  - NATS 連線失敗、DNS/網路不可達、URL 格式不合法、訂閱建立失敗等

### 4.3 Options / Config 類型

```go
type SubOptions struct {
    QueueGroup  string
    MaxInflight int
}

type SubOpt func(*SubOptions)
```

- `QueueGroup`：對應 NATS queue group（共享訂閱）
- `MaxInflight`：目前僅保留於 `SubOptions`；`NATSCoreBus` 尚未做「同時處理上限」的實作

> 補充：本 package 內部亦維持一份「連線/行為 options」快照（例如 timeout / handler 行為）。
> 目前建構函式採用系統預設值，不對外提供調整入口；未來若引入權威設定（例如 addressbook），再擴充配置來源。

---

## 5. 具體實作（Implementation）

> 這一節是給「要維護 / 擴充這個 package」的人看的。

### 5.1 主要實作型別

```go
type NATSCoreBus struct {
    nc   *nats.Conn
    opts BusOptions
}

var _ Bus        = (*NATSCoreBus)(nil)
var _ Publisher  = (*NATSCoreBus)(nil)
var _ Subscriber = (*NATSCoreBus)(nil)
```

- `nc`：底層 NATS connection
- `opts`：保存初始化用的行為設定快照（供 `Subscribe` callback 使用）
- Thread-safety / lifecycle：
  - `Close()` 可重複呼叫（idempotent）
  - `Close()` 會立即關閉連線，不等待 handler 收尾

### 5.2 建構函式

```go
func NewNATSCoreBus() (*NATSCoreBus, error)
```

- 是否會在裡面就打外部連線 / I/O：
  - 會。建構時會直接建立 NATS 連線（網路 I/O）
- 端點來源與預設策略：
  - 優先採用環境變數 `NATS_URL`（可選）
  - 未提供時使用系統預設：`nats://127.0.0.1:4222`
- 端點格式驗證（現行規則）：
  - 支援逗號分隔多個 server URL（例如 `"nats://a:4222,nats://b:4222"`）
  - scheme 僅允許 `nats` / `tls`
  - host 必須存在
- 失敗時可能回傳的錯誤：
  - URL 格式驗證失敗（空值、scheme 不支援、host 缺失等）
  - `nats.Connect` 連線失敗（網路/權限/服務未啟動等）
- Name（NATS client name）策略：
  - 目前不提供外部設定入口；若需要可在未來導入權威化配置（例如 addressbook）後再做映射

### 5.3 關鍵方法行為

- `Publish(ctx, subject, data)`：
  - 直接呼叫底層 `nc.Publish(subject, data)`
  - `ctx` 目前保留未用（維持介面一致性、為未來擴充預留）
- `Subscribe(ctx, subject, handler, opts...)`：
  - 依 `QueueGroup` 決定使用 `Subscribe` 或 `QueueSubscribe`
  - callback 內把 `nats.Msg` 映射成 `Message`
  - handler 回傳 error 時，若有設定 `OnHandlerError`（內部行為選項），會被回報（NATS callback 為非同步，無法從 `Subscribe()` return 往上拋）
  - 建立成功後會 `Flush()` 一次，以提早暴露錯誤
- `Close()`：
  - 立即關閉底層連線
  - 多次呼叫安全（不得 panic）

---

## 6. 使用範例（Examples）

> 寫給「我要抄一段範例趕快用」的未來同事。

### 6.1 最小可用範例：建立 Bus 並 Publish 一筆訊息

```go
bus, err := pubsub.NewNATSCoreBus()
if err != nil {
    panic(err)
}
defer bus.Close()

_ = bus.Publish(context.Background(), "demo.subject", []byte("hello"))
```

### 6.2 典型實戰用法：Subscribe + QueueGroup

```go
bus, _ := pubsub.NewNATSCoreBus()
defer bus.Close()

sub, err := bus.Subscribe(context.Background(), "demo.subject",
    func(ctx context.Context, m *pubsub.Message) error {
        // handle m.Data
        return nil
    },
    pubsub.WithQueue("demo-workers"),
)
if err != nil {
    panic(err)
}
defer sub.Drain()
```

### 6.3 與其他 package 合作：注入最小介面

若元件只需要發佈能力，建議依賴 `pubsub.Publisher` 而非整個 `Bus`：

```go
func NewCollector(pub pubsub.Publisher) *Collector {
    return &Collector{pub: pub}
}
```

---

## 7. 行為與限制（Behavior & Constraints）

- Thread-safety：
  - `NATSCoreBus` 可併發呼叫 `Publish` / `Subscribe`
- Lifecycle：
  - `Close()` 可重複呼叫，不會 panic
  - `Close()` 是「立即關閉連線」，不保證等待 handler 收尾
  - 想要優雅關閉訂閱，請由上層持有 `Subscription` 並呼叫 `Drain()`
- Error handling：
  - `Subscribe` 的 handler error 不會回到 `Subscribe()` 的 return（因為 NATS callback 為非同步）
  - 建議 handler 避免做長時間阻塞；若有阻塞，需自行尊重 `ctx.Done()`
- 效能相關限制：
  - handler 在 NATS callback 中執行；避免在 callback 中做 heavy work（建議丟到內部 queue / goroutine）

---

## 8. 測試（Testing）

本 package 有兩類測試：

- **純 unit test（不連外）**：`bus_test.go`
  - 驗證型別/介面相容性、options 套用等純邏輯行為
- **integration test（真的連外）**：`nats_core_test.go`
  - 會嘗試連線到 NATS
  - 測試前會做可用性探測；不可用則 `t.Skip`

執行指令：

```bash
go test ./infra/pubsub -v -count=1
```

補充：
- 若你透過 `NATS_URL` 覆蓋端點，integration test 亦會依同樣端點行為進行連線/探測（依測試檔實作為準）。

---

## 9. 未來擴充方向

- `MaxInflight`：落實「單訂閱同時處理上限」
- `Header` 映射：將 `nats.Msg.Header` 映射到 `Message.Header`
- 更進階模式：
  - Request/Reply
  - metrics / tracing（handler latency、錯誤率、queue depth 等）
- 權威化配置：
  - 由 addressbook 推導 NATS client name / subject prefix / actor_id，而非由各 entrypoint 手寫

---

## 10. 命名與使用規範（Guidelines）

- 檔名命名規則
  - 抽象層：`bus.go`
  - backend 實作：`nats_core.go`（未來新增其他 backend 以 `<backend>_*.go` 命名）
- interface 使用規則
  - 上層優先依賴最小介面（`Publisher` / `Subscriber`），非必要不要依賴整體 `Bus`
- options/config 規則
  - 訂閱選項採 function options（`SubOpt`），避免建構/方法參數膨脹
  - 若新增「對外可設定」的零值行為，需在註解與 README 同步寫清楚（避免使用端誤用）
