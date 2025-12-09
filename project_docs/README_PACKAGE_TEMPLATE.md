# `<PACKAGE_PATH>` 套件說明（Template）

> **角色定位：**
> 用一段簡短敘述說明這個 package 在系統中的角色，
> 例如：提供 XXX 抽象層，把具體實作藏在底下，讓上層只依賴介面。

---

## 1. 套件職責（What）

這個 package 主要負責的事情（請自行調整數量與內容）：

1. **定義通用介面 / 型別**
   - 例：`Foo` 介面、`Bar` 型別、錯誤型別等
2. **提供一個或多個具體實作**
   - 例：`NATSCoreBus`、`GorillaWS`、`RedisRepo` 等

> 重點：這一節只講「做什麼」，不要塞設計理由（留到下一節）。

---

## 2. 設計動機與理由（Why）

### 2.1 為什麼要有這個 package？

- 說明為什麼需要這層抽象 / 這個元件
- 例如：為了切開傳輸層與業務邏輯、為了方便測試、為了可以替換底層實作

### 2.2 為什麼介面要這樣設計？

- 解釋關鍵介面的設計取捨
  - 例：拆成 `Publisher` / `Subscriber`，而不是一個超大的 `Client`
  - 例：用 function type (`Handler`) 當 callback，而不是 interface

### 2.3 為什麼資料結構長這樣？

- 挑一兩個核心 struct 出來解釋
  - 例：`Message`、`Config`、`Options`…
- 說明欄位存在的理由、預期用途、預留欄位的未來打算

---

## 3. 目錄結構（Layout）

描述這個 package 底下的檔案切法，例如：

```text
<PACKAGE_PATH>/
  ├── foo.go        // 抽象介面 + 通用型別
  ├── bar_impl.go   // 某個具體實作
  ├── config.go     // 設定 / 選項相關型別
  └── README.md     // 本文件
```

> 規則：
> - 抽象 / 介面類：放一起，命名偏通用，如 `bus.go`、`client.go`
> - 具體實作類：依 backend / 目的命名，如 `nats_core.go`、`gorilla_ws.go`

---

## 4. 主要型別與介面（API Overview）

> 這一節是給「只想知道怎麼用」的人看的，
> 想深入再去看下一節 Implementation。

### 4.1 核心資料結構（例如：`Message` / `Config` 等）

```go
type Xxx struct {
    // 簡要欄位
}
```

- 說明欄位用途
- 哪些是必填，哪些可以是零值 / nil
- 有沒有預期會在未來擴充的欄位

### 4.2 介面（例如：`Foo`, `Bar`, `Client` 等）

```go
type Foo interface {
    DoSomething(ctx context.Context, in *Input) (*Output, error)
}
```

- 說明每個 method 的語意
- 特別註明：
  - 是否允許併發呼叫
  - 行為是否會 block / heavy
  - 回傳錯誤的常見情境

### 4.3 Options / Config 類型

```go
type XxxOptions struct {
    // 重要設定
}
```

- 說明每個欄位的功能
- 零值行為（由實作補預設？會直接錯？）
- 是否會被後續操作修改，還是當初始化參數用而已

---

## 5. 具體實作（Implementation）

> 這一節是給「要維護 / 擴充這個 package」的人看的。

### 5.1 主要實作型別

```go
type XxxImpl struct {
    // 欄位
}

// 編譯期介面保證（視需要加）
var _ Foo = (*XxxImpl)(nil)
```

- 每個欄位的職責（例如：連線物件、設定、鎖…）
- 是否有 thread-safety / lifecycle 相關的約束

### 5.2 建構函式

```go
func NewXxxImpl(opts XxxOptions) (*XxxImpl, error)
```

- 補預設值策略（零值會怎麼處理）
- 失敗時可能回哪幾種錯誤
- 是否會在裡面就打外部連線 / I/O

### 5.3 關鍵方法行為

挑幾個最重要的方法各自說明：

- 執行流程的大致順序
- 哪邊會呼叫外部 I/O（DB / 網路 / 磁碟）
- 哪邊吞錯、哪邊 bubble error 出去

---

## 6. 使用範例（Examples）

> 寫給「我要抄一段範例趕快用」的未來同事。

至少準備 2~3 段：

1. **最小可用範例**
   - 例：建立 client、呼叫一次 `Foo`、收結果
2. **典型實戰用法**
   - 例：在服務啟動時初始化，注入到 handler / collector 中
3. **與其他 package 合作**
   - 例：這個 package 怎麼跟 `infra/transport/ws` / `infra/repo` 等搭配

---

## 7. 行為與限制（Behavior & Constraints）

條列這個 package 的重要特性與地雷：

- Thread-safety：哪些型別是 goroutine-safe，哪些不是
- Lifecycle：
  - 需不需要呼叫 `Close()`？
  - 可以多次呼叫嗎？
- Error handling：
  - 哪些錯誤只會記 log 不往外丟？
  - 哪些錯誤一定會回傳？
- 效能相關限制：
  - 是否會阻塞？
  - 單次呼叫做了哪些重事（序列化、網路往返…）

---

## 8. 測試（Testing）

簡述這個 package 的測試策略：

- 是否會連到真實外部服務（例如 NATS / Redis / DB）
- 有沒有區分：
  - 純 unit test（mock / fake）
  - integration test（真的連外）
- 跑測試的指令（通常就是：）

```bash
go test ./<PACKAGE_PATH> -v
```

如果有額外需求（例如要先起 docker），在這邊註明。

---

## 9. 未來擴充方向

簡單列出幾個可能會做，但現在還沒做、也不一定會做的點：

- 新增其他 backend 實作
- 支援更多 pattern（例如 Request/Reply、Batch 等）
- 更完整的監控 / metrics / tracing

> 用來提醒未來的自己：當初是有想過這些方向的。

---

## 10. 命名與使用規範（Guidelines）

這一節放「團隊約定」，讓之後的人照著來，維持一致性，例如：

- 檔名命名規則
  - 抽象層：`bus.go`、`client.go`、`repo.go`
  - backend 實作：`<backend>_bus.go`、`gorilla_ws.go`
- interface 使用規則
  - 上層優先依賴最小介面（例如 `Publisher` / `Reader`），非必要不要依賴大一統介面
- options/config 規則
  - 一律用 struct 承載，不要讓建構函式超過 N 個參數
  - 零值行為要在註解和 README 清楚寫出來

寫完這份 README 之後，可以把其中「不特定於這個 package」的段落抽出來，變成整個專案共用的 README 模板。
