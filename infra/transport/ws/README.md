# `infra/transport/ws` 套件說明

> **角色定位：**
> 提供 WebSocket 傳輸層抽象與一個基於 `gorilla/websocket` 的具體實作，並提供 `Collector` 將「WS 訂閱 → 解析 → 發佈」工作流標準化，讓上層只依賴介面（`WSClient` / `Adapter` / `pubsub.Publisher`），不綁定具體連線與協定細節。

---

## 1. 套件職責（What）

這個 package 主要負責的事情：

1. **定義通用介面 / 型別**
   - `WSClient`：連線與 I/O（Connect / Recv / SendJSON / SendPing / Close）
   - `Adapter`：交易所協定（訂閱封包、心跳策略、訊息解析與路由）
   - `CollectorConfig`：Collector 靜態設定（URL/Headers/Channels/Symbols/心跳/重連 backoff）
   - `Collector`：把 WSClient + Adapter + Publisher 組裝成可跑的收集器

2. **提供一個或多個具體實作**
   - `GorillaWS`：基於 `gorilla/websocket` 的 `WSClient` 實作，包含：
     - `RWMutex` 保護 conn swap
     - `writeSem` 串行化寫入（避免 gorilla concurrent write）
     - deadline 策略（read/write/ping timeout）

3. **提供測試護欄**
   - `client_test.go`：驗證 GorillaWS 核心契約（並發寫入安全、Recv/Close 行為）
   - `collect_base_test.go`：驗證 Collector 狀態機（發佈、重連、心跳分支）

> 重點：本節只描述「做什麼」，設計理由與取捨在下一節。

---

## 2. 設計動機與理由（Why）

### 2.1 為什麼要有這個 package？

- **切開傳輸層與業務/協定層**：上層策略（跑幾條 WS、跑哪些 symbol/channel）不需要知道 gorilla 的併發限制、deadline、關閉語義。
- **方便測試**：Collector 以 `WSClient` / `Adapter` / `Publisher` 介面組裝，單元測試可用 fake 精準控制斷線與消息流，不依賴真實交易所。
- **可替換底層實作**：未來可把 gorilla 換成其他 WS 套件、或在測試中注入 stub，不動上層。

### 2.2 為什麼介面要這樣設計？

- `WSClient` 採用「最小能力集合」：Connect / Recv / SendJSON / SendPing / Close，避免把 adapter/publisher 等責任混進 client。
- `Adapter` 把「協定」責任集中：訂閱格式、心跳 payload 與頻率、解析與 subject 路由，讓 Collector 不理解業務。
- `pubsub.Publisher` 僅暴露 `Publish(ctx, subject, data)`：Collector 只負責送出，不要求 bus 支援訂閱/關閉，符合單向資料流（WS → Bus）。

### 2.3 為什麼資料結構長這樣？

- **`CollectorConfig`**
  - `URL` / `Headers`：連線資訊
  - `Channels` / `Symbols`：交給 Adapter 解讀（不同交易所語意不同）
  - `PingEvery`：Adapter 不提供 heartbeat 時的 fallback（為 0 表示不啟用）
  - `ReconnectBackoff`：重連節流（避免打爆對端）；零值/非正值會在建構時補預設 3s

- **`Collector`**
  - 把 WSClient、Adapter、Publisher 組合成可重連的狀態機。
  - 對上層而言，Collector 是「可跑的收集單元」，上層的擴展點是：Adapter、Publisher、以及你要開幾條 Collector。

---

## 3. 目錄結構（Layout）

```text
infra/transport/ws/
  ├── types.go              // 抽象介面：WSClient / Adapter
  ├── client.go             // GorillaWS：gorilla/websocket 實作（conn swap、write 序列化、deadline）
  ├── collect_base.go       // Collector：訂閱、心跳、收訊、解析、發佈、重連
  ├── client_test.go        // GorillaWS 契約測試（並發寫、ping、deadline、Close 打斷 Recv）
  ├── collect_base_test.go  // Collector 狀態機測試（publish、reconnect、heartbeat 分支）
  └── README.md             // 本文件
```

> 規則：
> - 抽象 / 介面類：`types.go`
> - 具體實作類：`client.go`（gorilla）
> - 流程組裝類：`collect_base.go`（Collector）

---

## 4. 主要型別與介面（API Overview）

### 4.1 核心資料結構（`CollectorConfig`）

```go
type CollectorConfig struct {
    URL              string
    Headers          http.Header
    Channels         []string
    Symbols          []string
    PingEvery        time.Duration
    ReconnectBackoff time.Duration
}
```

- `URL`：必填（合法 WS URL），否則 `Connect` 直接失敗。
- `Headers`：可為 nil。
- `Channels` / `Symbols`：交由 Adapter 解釋；允許為空（依各 adapter 規則）。
- `PingEvery`：
  - 若 Adapter.Heartbeat().every == 0 且 PingEvery > 0，Collector 會用 PingEvery 週期送 ping（或 heartbeat）。
  - 為 0 表示不啟用（除非 Adapter 有回 heartbeat）。
- `ReconnectBackoff`：
  - <= 0 時在 `NewCollector` 會補預設 3 秒。
  - 建議不要過小，避免頻繁重連。

### 4.2 介面（`WSClient`, `Adapter`）

```go
type WSClient interface {
    Connect(ctx context.Context, url string, hdr http.Header) error
    SendJSON(ctx context.Context, text string) error
    Recv(ctx context.Context) (msgType int, data []byte, err error)
    Close() error
    SendPing(ctx context.Context) error
}

type Adapter interface {
    BuildSubscribeMsgs(channels []string, symbols []string) ([]string, error)
    Heartbeat() (payload string, every time.Duration)
    Handle(raw []byte) (subject string, body []byte, err error)
}
```

- **並發允許性**
  - `WSClient.SendJSON` / `WSClient.SendPing`：允許多 goroutine 併發呼叫（GorillaWS 會串行化 write）。
  - `WSClient.Recv`：在 gorilla 的限制下必須確保同一條連線只有單一 reader；Collector 已以「每個連線生命週期一個 reader goroutine」落實。
- **阻塞/重工作為**
  - `Recv`：阻塞式讀取；GorillaWS 透過 ReadDeadline 避免永久阻塞，且可由 `Close()` 打斷。
  - `SendJSON` / `SendPing`：會做網路寫入；透過 WriteDeadline 限制阻塞時間。
- **常見錯誤情境**
  - 未連線：回 `ws 未連線`
  - deadline 到期：底層 `ReadMessage` / `WriteMessage` 回錯
  - Close 後再收發：回錯（conn 已 nil）

### 4.3 Options / Config 類型

- `CollectorConfig` 為初始化參數（靜態設定），Collector 內部不會在運行中修改此 struct（除了 `NewCollector` 補預設 backoff）。
- GorillaWS 本身不另外暴露 options；如需調整 dialer、timeout、read limit，建議在未來以 Options struct 擴充（見第 9 節）。

---

## 5. 具體實作（Implementation）

> 這一節是給「要維護 / 擴充這個 package」的人看的。

### 5.1 主要實作型別（`GorillaWS`, `Collector`）

```go
type GorillaWS struct {
    dialer *websocket.Dialer
    conn   *websocket.Conn
    mu     sync.RWMutex

    writeOnce sync.Once
    writeSem  chan struct{}
}

var _ WSClient = (*GorillaWS)(nil)

type Collector struct {
    ws   WSClient
    adj  Adapter
    pub  pubsub.Publisher
    conf CollectorConfig
}
```

- `GorillaWS.mu`：保護 `conn` 指標（含 swap / nil）。
- `GorillaWS.writeSem`：容量 1 的 semaphore，串行化所有寫入（`WriteMessage` / `WriteControl`），避免 gorilla concurrent write。
- `Collector`：
  - 以主循環 `select` 驅動（ctx.Done、heartbeat tick、errCh、msgCh）。
  - 使用一個 reader goroutine 把阻塞 `Recv` 轉成事件流，並在錯誤時結束，由主循環接手重連。

### 5.2 建構函式

```go
func NewGorillaWS() *GorillaWS

func NewCollector(ws WSClient, adj Adapter, pub pubsub.Publisher, conf CollectorConfig) *Collector
```

- `NewCollector` 會補 `ReconnectBackoff` 預設（<=0 時設定為 3s）。
- 建構函式不做外部 I/O；實際連線在 `Connect()`。

### 5.3 關鍵方法行為

- **`GorillaWS.Connect`**
  - DialContext 成功後設定 read limit、ReadDeadline、PongHandler。
  - 取得 `writeSem` 後 swap conn（確保 swap 期間沒有 writer 用舊 conn）。
  - swap 完成後釋放 `writeSem`，再 close 舊 conn（避免 I/O 占用臨界區）。

- **`GorillaWS.SendJSON` / `SendPing`**
  - 先 `lockWrite(ctx)`（等待 writeSem，ctx 可取消等待）。
  - 在 write lock 範圍內讀取 `conn`，避免寫到舊連線。
  - 設定 WriteDeadline（min(預設 timeout, ctx.Deadline)）。

- **`GorillaWS.Recv`**
  - 設定 ReadDeadline（min(預設 readTimeout, ctx.Deadline)）後呼叫 `ReadMessage`。
  - 注意：若啟用 PongHandler 且對端頻繁 pong，ReadDeadline 可能被刷新延後，因此 ctx.Deadline 並非硬上限；需要「立即中止」請靠上層 `Close()`。

- **`Collector.Run`**
  - heartbeat：優先採用 Adapter.Heartbeat()，若 every=0 才 fallback 到 conf.PingEvery。
  - reader：每個連線生命週期只啟一個 reader；讀取錯誤後 reader 結束、主循環進入重連 loop，重連成功後再啟新 reader。
  - publish：只處理 `websocket.TextMessage`；交由 Adapter.Handle(raw) 回傳 subject/body，再呼叫 Publisher.Publish。

---

## 6. 使用範例（Examples）

> 寫給「我要抄一段範例趕快用」的未來同事。

### 6.1 最小可用範例（單條 WS）

```go
wscli := ws.NewGorillaWS()
adj   := NewYourExchangeAdapter(...)      // 你自己的 Adapter 實作
pub   := NewYourPublisher(...)            // 例如 NATSCoreBus（實作 pubsub.Publisher）

c := ws.NewCollector(wscli, adj, pub, ws.CollectorConfig{
    URL:      "wss://example.com/ws",
    Headers:  nil,
    Channels: []string{"kline"},
    Symbols:  []string{"BTCUSDT"},
})

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// 建議：啟動前先 connect + subscribe
_ = c.Connect(ctx)
_ = c.Subscribe(ctx)

go func() { _ = c.Run(ctx) }()

// ...服務運行...

// 停止：cancel 後 Close（或交由上層統一 defer Close）
cancel()
_ = c.Close()
```

### 6.2 典型實戰用法（服務啟動注入）

- 服務啟動時建立 `Publisher`（例如 NATS）與多個 `Collector`（不同交易所/不同資料）。
- 上層依策略決定「開幾條 WS、每條訂閱哪些 symbols」，但每條都透過同一套 Collector 行為（心跳、重連、發佈）。

### 6.3 與其他 package 合作（WS → pubsub）

- `infra/transport/ws`：只負責把 WS stream 轉成（subject, body）並 publish。
- `infra/pubsub`：負責把 publish 轉送到 bus（例如 NATS / log / in-memory）。

---

## 7. 行為與限制（Behavior & Constraints）

條列這個 package 的重要特性與地雷：

- **Thread-safety**
  - `GorillaWS.SendJSON/SendPing`：goroutine-safe（寫入被 `writeSem` 串行化）。
  - `GorillaWS.Recv`：同一條連線不應被多 goroutine 同時呼叫；Collector 已確保「一條連線一個 reader」。
- **Lifecycle**
  - `Close()` 建議由上層在停服務時呼叫，用於打斷阻塞讀取並釋放連線。
  - `Close()` 可重複呼叫：conn 已 nil 時為 no-op（回 nil）。
- **Error handling**
  - `Collector.Run`：Recv/解析/Publish 出錯會記 log，並在 Recv error 時進入重連；不把錯誤向上 bubble（除非 ctx 被取消結束）。
  - `Connect` / `Subscribe`：以 error 回傳（由上層決定是否退出或重試）。
- **效能相關**
  - sendping/sendjson 都是輕量 I/O；主要資源占用在 `Recv` 阻塞讀取與解析/發佈。
  - `Subscribe` 目前以 `time.Sleep(20ms)` 做簡單節流；若需要更精細節流，建議引入 rate limiter（需求驅動再做）。

---

## 8. 測試（Testing）

簡述這個 package 的測試策略：

- **不連到真實外部服務**
  - `client_test.go`：使用 `httptest` + `websocket.Upgrader` 建立本地 WS server。
  - `collect_base_test.go`：使用 `FakeWSClient` 精準控制 Recv 行為與斷線注入；Publisher/Adapter 使用 stub。

- **有區分**
  - 純 unit test（mock / fake）：Collector 狀態機、心跳分支、重連行為
  - component test（本地 server）：gorilla 寫入/讀取/關閉語義驗證

- **跑測試的指令**
```bash
go test ./infra/transport/ws -v -count=1
```

- `-count=1`：可避免使用測試快取，利於你在迭代階段確認本次修改確實被執行。

---

## 9. 未來擴充方向

簡單列出幾個可能會做，但現在還沒做、也不一定會做的點：

- **GorillaWS Options 化**
  - dialer（proxy、TLS、handshake timeout）
  - read/write/ping timeout
  - read limit（maxMessageSize）
- **重連策略強化**
  - exponential backoff + jitter
  - 熔斷/降載（對端限流或封鎖時）
- **可觀測性**
  - metrics：重連次數、publish 失敗次數、消息延遲、佇列深度
  - tracing：把 subject 對應到上游消息來源
- **更完整的 shutdown 語義**
  - graceful shutdown deadline
  - publish flush / drain（若業務要求「停機前必發完」）

> 用來提醒未來的自己：當初是有想過這些方向的。

---

## 10. 命名與使用規範（Guidelines）

這一節放「團隊約定」，讓之後的人照著來，維持一致性，例如：

- **檔名命名規則**
  - 抽象層：`types.go`
  - backend 實作：`client.go`（若未來增加其他 backend，建議改成 `gorilla_ws.go` 並新增 `<backend>_ws.go`）
  - 流程組裝：`collect_base.go`
  - 測試：與原檔並列 `*_test.go`

- **interface 使用規則**
  - 上層優先依賴最小介面（`WSClient` / `Adapter` / `pubsub.Publisher`），避免直接依賴 gorilla。
  - 若只需要發佈能力，依賴 `pubsub.Publisher`，不要擴大到 bus 的其他能力。

- **options/config 規則**
  - 初始化參數一律用 struct（`CollectorConfig`），避免建構函式參數爆炸。
  - 零值行為要清楚寫出來：
    - `ReconnectBackoff <= 0`：`NewCollector` 補預設（3s）
    - `PingEvery == 0` 且 Adapter 不提供 heartbeat：Collector 不主動送 ping
