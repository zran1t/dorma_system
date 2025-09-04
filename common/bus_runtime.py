# common/bus_runtime.py
from __future__ import annotations
import asyncio
from typing import Dict, Optional

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.errors import Error as NATSError

from .bus_config import BusConfig

class BusRuntime:
    """純通訊客戶端：吃 BusConfig，不讀 YAML。"""
    def __init__(self, nc: NATS, js, cfg: BusConfig):
        self.nc = nc
        self.js = js
        self.cfg = cfg
        self._queues: Dict[str, asyncio.Queue] = {}

    @classmethod
    async def connect(cls, cfg: BusConfig) -> "BusRuntime":
        nc = NATS()
        await nc.connect(servers=[cfg.nats_url])
        js = nc.jetstream()
        self = cls(nc, js, cfg)

        # Streams
        for sc in cfg.streams_js:
            try:    await js.add_stream(sc)
            except NATSError:  # 已存在
                pass

        # Consumers（push/_AUTO → 建 inbox + subscribe → 塞 queue）
        for c in cfg.consumers:
            deliver_subject = None
            if (str(c.get("mode")).lower() == "push") and (c.get("deliver_subject", "_AUTO") in (None, "_AUTO")):
                deliver_subject = await nc.new_inbox()

            cc = cfg.build_consumer_config(c, deliver_subject=deliver_subject)
            try:    await js.add_consumer(stream=c["stream"], config=cc)
            except NATSError:
                pass

            if deliver_subject:
                q: asyncio.Queue[Msg] = asyncio.Queue()
                async def _cb(msg: Msg, _q=q):
                    await _q.put(msg)
                await nc.subscribe(subject=deliver_subject, cb=_cb)
                self._queues[c["durable"]] = q

        return self

    def queue(self, durable: str) -> Optional[asyncio.Queue]:
        return self._queues.get(durable)

    async def publish(self, subject: str, data: bytes, *, msg_id: Optional[str] = None) -> None:
        await self.nc.publish(subject, data)

    async def close(self) -> None:
        try:
            await self.nc.drain()
        except Exception:
            try: await self.nc.close()
            except Exception: pass