# injector_api/service/nats_publisher.py
from __future__ import annotations
from pathlib import Path
from typing import Optional

from nats.aio.client import Client as NATS

from common import ChannelConfig


# 只讀 inter.yaml 拿 routes+nats_url
ROOT = Path(__file__).resolve().parents[2]
INTER_YAML = ROOT / "configs" / "channels" / "inter.yaml"

_nc: Optional[NATS] = None
_routes = None
_nats_url: Optional[str] = None


async def _ensure_nats_and_routes() -> None:
    global _nc, _routes, _nats_url
    if _nc is not None and _nc.is_connected and _routes is not None:
        return
    cfg = ChannelConfig.load(INTER_YAML)
    _routes = cfg.routes
    _nats_url = cfg.nats_url
    _nc = NATS()
    await _nc.connect(servers=[_nats_url])


async def publish_strategy_bytes(payload_bytes: bytes, *, usr_id: str, version: int) -> str:
    """
    發佈到 inter 的 dept2kol.init.kline（publisher-only，不建 consumer）。
    使用 JetStream publish 以取得 PubAck，確保訊息已寫入 STREAM_INTER。
    
    參數:
        payload_bytes (bytes): 已序列化的策略注入資料
        usr_id (str): 使用者 ID（用於組冪等訊息 ID）
        version (int): 版本號（用於組冪等訊息 ID）
    
    回傳:
        str: 實際 subject（方便上層紀錄）
    """
    await _ensure_nats_and_routes()

    subject = f"{_routes.dept2kol_prefix}.init.kline"  # e.g. bus.inter.dept2kol.init.kline
    msg_id = f"{usr_id}-{version}"                     # 冪等訊息 ID（避免重送重複）

    # 偵錯列印：讓你在 API 視窗看到實際 subject 與 payload 大小
    print(f"📤 API 發佈 Inter → subject={subject}, usr_id={usr_id}, version={version}, size={len(payload_bytes)} bytes")

    # 用 JetStream 送，拿 PubAck 當「已寫入 Stream」的證據
    js = _nc.jetstream()
    ack = await js.publish(subject, payload_bytes, headers={"Nats-Msg-Id": msg_id})

    # 列印 PubAck（stream / seq / duplicate）
    # duplicate=True 表示在 duplicate_window 內撞到同一個 msg_id
    try:
        dup = getattr(ack, "duplicate", False)
        stream = getattr(ack, "stream", "?")
        seq = getattr(ack, "seq", "?")
        print(f"✅ PubAck：stream={stream}, seq={seq}, duplicate={dup}")
    except Exception as _:
        # 某些版本屬性名不同；保底印出 repr
        print(f"✅ PubAck：{ack!r}")

    return subject
