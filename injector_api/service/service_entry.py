# injector_api/service/service_entry.py
# [修改] run_init_service：不再要求 event/data；自動把 payload 轉成 dict 後「原封」送出

from typing import Any, Dict
from .nats_publisher import publish_to_initialization


async def run_init_service(payload: Any) -> Dict[str, Any]:
    """
    單一入口（極簡包裹）：把 API 傳來的資料「原封裝」送到初始化部門通訊處室。

    行為：
        1) 若是 Pydantic 物件則轉成 dict（v2: .model_dump / v1: .dict）
        2) 確認最後為 dict；否則報錯
        3) 呼叫 publish_to_initialization(body_payload=payload)

    參數:
        payload (Any): 由 FastAPI 端點接收到的物件（dict 或 Pydantic model）

    回傳:
        Dict[str, Any]: 統一成功訊息
    """
    # 允許 Pydantic v2/v1 物件
    if hasattr(payload, "model_dump") and callable(payload.model_dump):
        payload = payload.model_dump()   # Pydantic v2
    elif hasattr(payload, "dict") and callable(payload.dict):
        payload = payload.dict()         # Pydantic v1

    if not isinstance(payload, dict):
        raise ValueError("payload 必須為物件（dict），請確認路由型別定義")

    # Log（中文）
    print("準備送往初始化部門通訊處室；內容鍵數：", len(payload))

    # 直接把整包 payload 放進 body
    await publish_to_initialization(body_payload=payload)

    print("送往初始化部門通訊處室完成")
    return {"ok": True, "msg": "已送往初始化部門通訊處室"}