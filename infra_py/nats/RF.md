# infra_py/nats 重構（Refactor）備忘錄

> 本文件為 **未來重構指引**，不是立即行動清單。
> 目的在於保留設計直覺，避免日後重構時重新「想一次」。

---

## 一、目前狀態總結（2026-02）

### 已完成的事實
- JetStream **reset / bootstrap / audit 行為已穩定**
- audit 結果可重跑、可比對、可落地（JSON）
- logging contract 已收斂至 system_initializer
- infra_py/nats 已回歸 **library 角色**（不掌握流程、不決定 logging）

### 已知但接受的問題
- 模組數量偏多（心理噪音高）
- 抽象層級「過度完整」相對於實際複雜度
- bootstrap / auditor / reset 之間存在形式上的重複防衛

---

## 二、真正需要的分層（極簡版）

> 重構的核心不是「重寫行為」，而是 **減少心理與結構負擔**。

### 1️⃣ 唯一值得留下的抽象邊界

```
YAML
  ↓
[轉換層]  ← 重構重點
  ↓
JetStream API calls
```

### 2️⃣ 明確不需要的抽象
- ❌ expected / actual / diff 作為完整模型體系
- ❌ 為 bootstrap / audit 各自維護一套 loader
- ❌ 多檔案只為了「看起來乾淨」

---

## 三、建議的最終檔案結構（目標狀態）

```
infra_py/nats/
├── connection.py        # 建立並管理 NATS / JetStream 連線（可共用設定）
├── topology_convert.py  # YAML → JetStream Config 的「純轉換函式」
├── apply.py             # reset / bootstrap / audit 的實際 API 呼叫
├── audit.py             # 只做「讀 + 比對 + report」
├── types.py             # （選擇性）輕量 dataclass / TypedDict
└── README.md            # 說清楚設計意圖（給未來的自己）
```

> 檔案數量目標：**5 ±1**

---

## 四、重構優先順序（未來）

### Phase 1（最小價值）
- 抽出所有：
  - `_to_stream_config`
  - `_to_consumer_config`
  - duration / enum mapping
- 集中到 `topology_convert.py`
- bootstrap / audit / reset **只呼叫轉換後結果**

👉 這一步就能消除 50% 的不爽感

---

### Phase 2（結構收斂）
- bootstrap / audit 共用：
  - 單一「讀實際狀態」邏輯
- audit 不再依賴 bootstrap 的防衛性 fallback
- warning → info（只在「非 reset 模式」才重要）

---

### Phase 3（如果還想做）
- 用 TypedDict / dataclass 取代隱性 dict
- 不追求型別完備，只追求「可讀性」

---

## 五、什麼時候 *不該* 重構

- 情緒煩躁
- 只是因為「看不順眼」
- 沒有新需求、沒有新約束

> **這是一個「作者型重構」**
> 必須在你心情好的時候做，否則只會變成另一筆技術債。

---

## 六、給未來自己的備註

- 這套東西「行為是對的」
- audit = ok 不是偶然
- 不要因為它不漂亮就否定現在的自己

---

*最後更新：2026-02-08*
