# tests/test_js_bootstrap.py
# 測試 JetStream 統一初始化：建立/更新 Streams 與 Consumers，並逐一確認存在

from __future__ import annotations

import pytest
from nats.js.api import StreamConfig, ConsumerConfig

from common.nats_bootstrap import bootstrap_nats_and_js


@pytest.mark.asyncio
async def test_bootstrap_init_and_verify(cfg_init):
    """
    測試目標：
        1) 呼叫 bootstrap_nats_and_js 進行冪等初始化
        2) 逐一以 stream_info / consumer_info 驗證 YAML 內的資源都存在
    """
    nc, js = await bootstrap_nats_and_js(cfg_init)

    # 驗證 Streams
    streams_cfg = (cfg_init.get("jetstream") or {}).get("streams", [])
    for s in streams_cfg:
        name = s["name"]
        info = await js.stream_info(name)
        assert isinstance(info.config, StreamConfig)
        # 額外 sanity：subjects 至少包含 YAML 的第一個 subject
        assert s["subjects"][0] in (info.config.subjects or [])
        print(f"【Bootstrap 檢查】Stream OK：{name}")

    # 驗證 Consumers
    consumers_cfg = (cfg_init.get("jetstream") or {}).get("consumers", [])
    for c in consumers_cfg:
        stream = c["stream"]
        durable = c["durable_name"]
        ci = await js.consumer_info(stream, durable)
        assert isinstance(ci.config, ConsumerConfig)
        print(f"【Bootstrap 檢查】Consumer OK：{durable}@{stream}")

    # 關閉連線
    try:
        await nc.drain()
    except Exception:
        await nc.close()