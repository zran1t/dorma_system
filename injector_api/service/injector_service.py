# injector_api/service/injector_service.py
from __future__ import annotations

from .persist_file import snapshot_and_write
from .persist_redis import get_client, allocate_version, write_summaries
from .nats_publisher import publish_strategy_bytes
from ..schemas.strategy_pools import StrategySubmitRequest


async def process_strategy_injection(req: StrategySubmitRequest) -> None:
    """
    正式流程（發送端）：
      1) 向 Redis 拿版本號
      2) 產生快照並落檔（帶 hash）
      3) 寫 Redis 摘要（全域＋每 user）
      4) 發 NATS（inter.dept2kol.init.kline）
    """
    # 1) Redis 連線 & 版本號
    rds = await get_client(pool_name="strategy")
    version = await allocate_version(rds)

    # 2) 落檔（回來 content_hash、payload_bytes）
    fpath, content_hash, payload_bytes, _snapshot = snapshot_and_write(
        req, version=version, source="api"
    )

    # 3) 寫 Redis 摘要
    await write_summaries(
        rds,
        usr_id=req.usr_id,
        strategies_count=len(req.strategies or []),
        version=version,
        content_hash=content_hash,
        file_path=str(fpath),
        source="api",
    )

    # 4) 發 NATS
    subject = await publish_strategy_bytes(payload_bytes, usr_id=req.usr_id, version=version)
    print(f"📡 已廣播到 NATS: {subject} (v={version})")