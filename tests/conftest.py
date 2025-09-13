# tests/conftest.py
# [修改] 將 async fixture 由 pytest.fixture → pytest_asyncio.fixture；並保留 yield NATS 實例

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Dict, AsyncGenerator

import sys
import pytest
import pytest_asyncio  # ★ 新增
from nats.aio.client import Client as NATS

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from common.channel_config_loader import load_channel_config  # noqa: E402


def cfg_path(rel: str) -> str:
    return str(PROJECT_ROOT / rel)


@pytest.fixture(scope="session")
def yaml_init_path() -> str:
    return cfg_path("configs/channels/initialization.yaml")


@pytest.fixture(scope="session")
def yaml_inter_path() -> str:
    return cfg_path("configs/channels/inter.yaml")


@pytest.fixture(scope="session")
def cfg_init(yaml_init_path: str) -> Dict:
    return load_channel_config(yaml_init_path)


@pytest.fixture(scope="session")
def cfg_inter(yaml_inter_path: str) -> Dict:
    return load_channel_config(yaml_inter_path)


async def _try_connect_nats(servers) -> NATS:
    nc = NATS()
    try:
        await asyncio.wait_for(nc.connect(servers=servers), timeout=2.0)
    except Exception:
        pytest.skip("⚠️ 測試跳過：無法連上 NATS（請確認本機 127.0.0.1:4222 已啟動）")
    return nc


# ★★★ 關鍵修改：使用 pytest_asyncio.fixture，並設定 function scope，確保得到真正的 NATS 物件
@pytest_asyncio.fixture(scope="function")
async def nc_conn(cfg_init) -> AsyncGenerator[NATS, None]:
    """
    提供一個已連線的 NATS client（function-scope）。
    測試結束後自動 drain/close。
    """
    servers = (cfg_init.get("nats") or {}).get("servers") or ["nats://127.0.0.1:4222"]
    nc = await _try_connect_nats(servers)
    try:
        yield nc  # ← 測試端拿到的是『NATS 實例』而非 async_generator
    finally:
        try:
            await nc.drain()
        except Exception:
            await nc.close()