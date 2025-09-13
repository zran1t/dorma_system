# 📡 通訊協定 v1.0

## 核心識別
- **trace_id**  
  UUIDv4，全鏈路唯一識別碼，貫穿訊息生命週期。  

- **timestamp**  
  每次送出更新，RFC3339 格式。  

- **version**  
  協定版本，目前固定 `"1.0.0"`。  

---

## 路由資訊
- **layer**  
  - `inter`: 部門 ↔ 部門  
  - `intra`: 部門 ↔ 科別  
  - `kol`:   科別內部  

- **direction**  
  - `down`: 部門 → 科別  
  - `up`:   科別 → 部門  
  - `lateral`: 部門 ↔ 部門  

- **source** / **destination**  
  - `type`: `"api" | "dpt" | "kol" | "sys"`  
  - `id`: 系統內唯一字串（建議全小寫、底線分隔）  

---

## 歷史紀錄
- **history** : 陣列，每一跳一筆紀錄  
  - `hop`: 跳數（1 起算）  
  - `timestamp`: 當下 hop 的時間  
  - `from`: 來源物件（type + id）  
  - `to`: 目的物件（type + id）  
  - `note`: 預設等於 `body.message`，可覆寫補充  

---

## 訊息主體
- **body.message**  
  指令名稱或事件代號，例如 `"INIT_START"`。  

- **body.data**  
  執行所需的資料，結構自由定義。  

- **body.meta**（可選）  
  - `priority`: `"low" | "normal" | "high" | "critical"`  
  - `tags`: 標籤清單，用於檢索與監控。  

---

## 範例 JSON

```json
{
  "trace_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-09-08T01:23:45.000Z",
  "version": "1.0.0",
  "layer": "inter",
  "direction": "down",
  "source": {
    "type": "api",
    "id": "api_gateway"
  },
  "destination": {
    "type": "dpt",
    "id": "initialization_dpt"
  },
  "history": [
    {
      "hop": 1,
      "timestamp": "2025-09-08T01:23:45.000Z",
      "from": { "type": "api", "id": "api_gateway" },
      "to": { "type": "dept", "id": "initialization_dpt" },
      "note": "INIT_START"
    }
  ],
  "body": {
    "message": "INIT_START",
    "data": {
      "usr_id": "U123",
      "strategies": []
    },
    "meta": {
      "priority": "normal",
      "tags": ["init", "kline"]
    }
  }
}