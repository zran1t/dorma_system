
# infra_py/nats 套件說明

## 角色定位（Role）

`infra_py.nats` 是 **系統控制面（control-plane）專用的 NATS / JetStream 基礎設施套件**，
負責將「宣告式 YAML 拓樸描述」轉換為實際的 JetStream 狀態，並提供：

- 拓樸建立與狀態收斂（bootstrap）
- 拓樸唯讀稽核（audit）
- 拓樸清理與環境回收（reset）
- channels YAML 設定讀取與最小驗證（config_loader）

本套件 **不承載任何業務資料流**，僅用於 system / infra 層級之控制與治理。

---

## 1. 套件職責（What）

本套件主要負責以下事項：

1. **JetStream 拓樸管理**
   - 建立 streams / consumers
   - 偵測差異並進行最小必要更新
   - 清理 consumers / streams 以回收環境

2. **宣告式設定治理**
   - 以 YAML 作為單一權威來源（source of truth）
   - 提供最小結構驗證，避免明顯錯誤進入控制面

3. **控制面可觀測性**
   - 稽核結果輸出為結構化 JSON
   - 所有操作具備 trace_id 與時間戳

---

## 2. 模組一覽與責任邊界（Modules & Responsibilities）

```text
infra_py/nats/
  ├── config_loader.py   # channels YAML 讀取與最小結構驗證
  ├── js_bootstrap.py    # JetStream 拓樸建立 / 更新（狀態收斂）
  ├── js_auditor.py      # JetStream 拓樸唯讀稽核與報告輸出
  ├── js_reset.py        # JetStream 拓樸清理（破壞性操作）
  ├── test_*.py          # 單元測試（使用 fake / mock，不連真實 NATS）
  └── README.md          # 本文件
```

### 2.1 config_loader.py
- 讀取 `configs/channels/*.yaml`
- 驗證 nats / subjects / jetstream 的最小結構
- **不做任何語意轉換或 JetStream 操作**

### 2.2 js_bootstrap.py
- 建立 NATS 連線並取得 JetStream context
- 依 YAML 宣告建立或更新 streams / consumers
- 僅在偵測到差異時才 update（避免控制面震盪）
- **不刪除任何既有資源**

### 2.3 js_auditor.py
- 以 YAML 為期望狀態，稽核實際 JetStream 狀態
- 只讀，不做任何修改
- 支援 ack_wait 容許誤差（±1 秒）
- 支援將稽核結果輸出為 JSON 檔案

### 2.4 js_reset.py
- 依 YAML 描述清理 consumers 與 streams
- 清理順序固定為：**Consumer → Stream**
- 為破壞性操作，僅用於初始化重置或環境回收

---

## 3. 使用方式（Usage Examples）

### 3.1 Bootstrap（建立 / 對齊拓樸）

```python
from infra_py.nats.config_loader import load_config
from infra_py.nats.js_bootstrap import bootstrap_nats_and_js

cfg = load_config("configs/channels/inter.yaml")
nc, js = await bootstrap_nats_and_js(cfg)
```

用途：
- 系統啟動時
- CI / deploy 時對齊 JetStream 狀態

---

### 3.2 Audit（唯讀稽核）

```python
from infra_py.nats.js_auditor import audit_and_dump

report = await audit_and_dump(cfg, nc)
```

用途：
- CI 檢查
- 人工比對環境狀態
- on-call 排查拓樸 drift

---

### 3.3 Reset（清理拓樸）⚠️

```python
from infra_py.nats.js_reset import reset_channels_topology

report = await reset_channels_topology(cfg, nc)
```

用途：
- 環境重建
- 測試環境回收

⚠️ **注意：此為破壞性操作，不可在系統運行中使用。**

---

## 4. 行為與限制（Behavior & Constraints）

- **bootstrap**
  - 僅 create / update
  - 不刪除任何資源
  - 僅在偵測差異時才更新

- **audit**
  - 純讀取
  - 不修改 JetStream 狀態
  - ack_wait 比對允許 ±1 秒誤差

- **reset**
  - 破壞性操作
  - 固定順序：consumer → stream
  - 資源不存在視為 skipped，不視為錯誤

- **duck typing**
  - auditor 與 bootstrap 測試皆使用 fake JetStream
  - 不強制綁定實際 NATS client 實作

---

## 5. 測試策略（Testing）

### 5.1 單元測試（Unit Tests）

- 測試檔案與模組同層（`test_*.py`）
- 使用 fake / mock JetStream
- **不連線真實 NATS**
- 覆蓋：
  - config_loader 驗證邏輯
  - bootstrap 差異判斷
  - audit 差異回報
  - reset 成功 / skipped / error 行為

執行方式：

```bash
pytest infra_py/nats -v
```

### 5.2 真實環境測試（Integration Tests）

- **不放在本套件內**
- 由 `system_initializer` 負責：
  - 啟動真實 NATS / JetStream
  - 驗證 bootstrap → audit → reset 的整體行為

---

## 6. 命名與設計約定（Guidelines）

- YAML 為唯一權威描述來源
- 本套件僅依賴 infra_py 內部模組
- 不得依賴 application / domain 層
- 所有錯誤訊息使用英文（便於 log / 監控）
- 註解與 README 使用中文（本 repo 規範）

---

## 7. 未來可能擴充方向

- 支援更多 JetStream 欄位之稽核（max_age / backoff 等）
- 提供 CLI 封裝（bootstrap / audit / reset）
- 導入 metrics / tracing（control-plane observability）

> 本 README 用於描述「控制面治理工具」，
> 若需系統初始化流程，請參考 `system_initializer` 套件。
