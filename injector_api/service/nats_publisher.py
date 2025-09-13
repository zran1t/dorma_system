# injector_api/service/nats_publisher.py
# [修正] 以「扁平協定 v1」發佈，保留舊介面 body_payload 以配合 service_entry

from __future__ import annotations

from typing import Dict, Optional, Any
from common.nats_comm import NatsComm

# 固定主題與身分
INIT_IN_SUBJECT = "inter.dpt.initialization.comm.in"
API_ACTOR = {"type": "api", "id": "api_gateway"}
INIT_DEPT_ACTOR = {"type": "dept", "id": "initialization_dpt"}

async def publish_to_initialization(
    body_payload: Dict[str, Any],
    nats_url: str = "nats://127.0.0.1:4222",
    *,
    message: str = "INIT_START",
    meta: Optional[Dict[str, Any]] = None,
) -> str:
    """
    發佈到初始化部門通訊處室（inter 層）。
    - 介面保持舊版：body_payload (dict)
    - 封包符合扁平協定 v1：message/data/meta + source/destination
    - 回傳 trace_id（呼叫端要不要用隨意）
    """
    if not isinstance(body_payload, dict):
        raise TypeError("body_payload 必須為 dict")

    comm = NatsComm()
    await comm.connect([nats_url], name="api_gateway")

    try:
        trace_id = await comm.publish_new(
            subject=INIT_IN_SUBJECT,
            message=message,
            data=body_payload,
            layer="inter",
            direction="down",
            source=API_ACTOR,
            destination=INIT_DEPT_ACTOR,
            meta=meta,
            note="api→dept inject",
        )
        return trace_id
    finally:
        await comm.close()