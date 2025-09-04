# injector_api/service/persist_redis.py
from __future__ import annotations
import os
from pathlib import Path
from typing import Optional, Dict, Any

import yaml
import redis.asyncio as redis

# 專案根與設定檔
ROOT = Path(__file__).resolve().parents[2]
REDIS_YAML = ROOT / "configs" / "redis" / "redis_urls.yaml"

# 預設使用的 pool 名稱（你要換 capital/order/position/kline 就改呼叫時的參數）
DEFAULT_POOL = "strategy"

# key 命名
RKEY_LATEST = "strategy_pool:latest"
RKEY_VER = "strategy_pool:version"
RKEY_BY_USER_PREFIX = "strategy_pool:user:"


def _load_redis_url(pool_name: str = DEFAULT_POOL) -> str:
    """優先 {POOL}_REDIS_URL，其次 YAML，最後預設。"""
    env_key = f"{pool_name.upper()}_REDIS_URL"
    if env_key in os.environ:
        return os.environ[env_key]

    try:
        if REDIS_YAML.exists():
            data = yaml.safe_load(REDIS_YAML.read_text(encoding="utf-8")) or {}
            if isinstance(data, dict) and data.get(pool_name):
                return str(data[pool_name])
    except Exception:
        pass

    return "redis://127.0.0.1:6379/0"


async def get_client(pool_name: str = DEFAULT_POOL) -> redis.Redis:
    url = _load_redis_url(pool_name)
    return redis.from_url(url, encoding="utf-8", decode_responses=True)


async def allocate_version(rds: redis.Redis) -> int:
    """取得全域 version（簡單做法：遞增計數器）。"""
    return await rds.incr(RKEY_VER)


async def write_summaries(
    rds: redis.Redis,
    *,
    usr_id: str,
    strategies_count: int,
    version: int,
    content_hash: str,
    file_path: str,
    source: str = "api",
) -> None:
    """更新最新摘要（全域 + 每 user）。"""
    summary = {
        "version": version,
        "hash": content_hash,
        "usr_id": usr_id,
        "strategies": strategies_count,
        "file": file_path,
        "source": source,
    }
    import json
    s = json.dumps(summary, ensure_ascii=False)
    await rds.set(RKEY_LATEST, s)
    await rds.set(f"{RKEY_BY_USER_PREFIX}{usr_id}", s)