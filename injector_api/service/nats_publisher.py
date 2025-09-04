# injector_api/service/nats_publisher.py
from __future__ import annotations
from pathlib import Path
from typing import Optional

from nats.aio.client import Client as NATS

from common.bus_config import ChannelConfig

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
    回傳實際 subject 方便 log 。
    """
    await _ensure_nats_and_routes()
    subject = f"{_routes.dept2kol_prefix}.init.kline"  # e.g. bus.inter.dept2kol.init.kline
    await _nc.publish(subject, payload_bytes)
    return subject