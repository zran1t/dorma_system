"""
File: infra_py/nats/js_reset.py
Module: infra_py.nats.js_reset

職責 (Responsibility):
    提供 JetStream 拓樸清理能力（破壞性），以 expected-driven 方式刪除 consumers 與 streams。
    用於初始化重置與環境回收，不參與日常資料流。

注意事項 (Notes):
    - 清理順序為 Consumer → Stream，不可顛倒。
    - expected-driven：只刪除 YAML 宣告的資源，不列舉全量 inventory。
    - 本模組具破壞性操作，不得在運行中資料流使用。
    - 不得依賴 system_initializer 或 application/domain 層模組。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

# === 第三方套件 (Third-Party Libraries) ===
from nats.aio.client import Client as NATS

# === 系統內模組 (Internal Modules) ===
from infra_py.logging.logger import get_logger
from infra_py.nats.expected_loader import build_expected_topology


logger = get_logger(__name__)


async def reset_channels_topology(cfg: Dict[str, Any], nc: NATS) -> Dict[str, Any]:
    """
    reset_channels_topology 清理 JetStream 拓樸。

    功能:
        - 依 YAML 定義刪除 consumers。
        - 於 consumers 清理完成後刪除 streams。

    參數:
        - cfg: YAML mirror dict（描述 JetStream 拓樸）。
        - nc: 已連線之 NATS client。

    回傳:
        - result: 清理操作報告 dict。
        - error: 無（刪除失敗會記錄於 errors 欄位）。

    備註:
        - 若資源不存在則記錄為 skipped，不視為錯誤。
        - 任一刪除失敗將記錄於 errors 欄位，供上層治理判斷。
    """
    js = nc.jetstream()
    expected = build_expected_topology(cfg)

    report: Dict[str, Any] = {
        "trace_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dropped": {"streams": [], "consumers": []},
        "skipped": {"streams": [], "consumers": []},
        "errors": [],
    }

    # ------------------------------------------------------------------
    # Step 1: Consumers
    # ------------------------------------------------------------------
    for c in expected.consumers:
        try:
            await js.consumer_info(c.stream_name, c.durable_name)
        except Exception:
            logger.info("consumer not exists | %s", c.key)
            report["skipped"]["consumers"].append(c.key)
            continue

        try:
            await js.delete_consumer(c.stream_name, c.durable_name)
            logger.info("consumer deleted | %s", c.key)
            report["dropped"]["consumers"].append(c.key)
        except Exception as e:
            logger.error("consumer delete failed | %s | %r", c.key, e)
            report["errors"].append({"target": c.key, "error": repr(e)})

    # ------------------------------------------------------------------
    # Step 2: Streams
    # ------------------------------------------------------------------
    for s in expected.streams:
        try:
            await js.stream_info(s.stream_name)
        except Exception:
            logger.info("stream not exists | %s", s.stream_name)
            report["skipped"]["streams"].append(s.stream_name)
            continue

        try:
            await js.delete_stream(s.stream_name)
            logger.info("stream deleted | %s", s.stream_name)
            report["dropped"]["streams"].append(s.stream_name)
        except Exception as e:
            logger.error("stream delete failed | %s | %r", s.stream_name, e)
            report["errors"].append({"target": s.stream_name, "error": repr(e)})

    if report["errors"]:
        logger.warning("reset finished with errors | trace_id=%s", report["trace_id"])
    else:
        logger.info("reset finished | trace_id=%s", report["trace_id"])

    return report
