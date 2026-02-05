"""
File: infra_py/nats/js_reset.py
Module: infra_py.nats.js_reset

職責 (Responsibility):
    提供 JetStream 拓樸清理能力，
    依 YAML 解析後之 dict 描述，刪除對應的 consumers 與 streams。
    本模組用於初始化重置與環境回收，不參與日常資料流。

注意事項 (Notes):
    - 清理順序為 Consumer → Stream，不可顛倒。
    - 呼叫端需保證 NATS client 已完成連線。
    - 本模組具備破壞性操作，不得在運行中資料流使用。
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


logger = get_logger(__name__, subdir="jetstream")


async def reset_channels_topology(cfg: Dict[str, Any], nc: NATS) -> Dict[str, Any]:
    """
    reset_channels_topology 清理 JetStream 拓樸。

    功能:
        - 依 YAML 定義刪除 consumers。
        - 於 consumers 清理完成後刪除 streams。

    參數:
        - cfg: YAML mirror dict，描述 JetStream 拓樸。
        - nc: 已連線之 NATS client。

    回傳:
        - result: 清理操作報告。
        - error: 無。

    備註:
        - 若資源不存在則記錄為 skipped，不視為錯誤。
        - 任一刪除失敗將記錄於 errors 欄位。
    """
    if not isinstance(cfg, dict):
        raise ValueError("cfg must be dict")
    if not isinstance(nc, NATS):
        raise ValueError("nc must be NATS client")

    js = nc.jetstream()

    report: Dict[str, Any] = {
        "trace_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dropped": {"streams": [], "consumers": []},
        "skipped": {"streams": [], "consumers": []},
        "errors": [],
    }

    js_cfg = cfg.get("jetstream") or {}
    streams_cfg: List[Dict[str, Any]] = js_cfg.get("streams") or []
    consumers_cfg: List[Dict[str, Any]] = js_cfg.get("consumers") or []

    # ------------------------------------------------------------------
    # Step 1: Consumers
    # ------------------------------------------------------------------
    for c in consumers_cfg:
        stream = c.get("stream")
        durable = c.get("durable_name")

        if not stream or not durable:
            logger.warning("invalid consumer config | %s", c)
            report["skipped"]["consumers"].append(str(c))
            continue

        key = f"{durable}@{stream}"

        try:
            await js.stream_info(stream)
            await js.consumer_info(stream, durable)
        except Exception:
            logger.info("consumer not exists | %s", key)
            report["skipped"]["consumers"].append(key)
            continue

        try:
            await js.delete_consumer(stream, durable)
            logger.info("consumer deleted | %s", key)
            report["dropped"]["consumers"].append(key)
        except Exception as e:
            logger.error("consumer delete failed | %s | %r", key, e)
            report["errors"].append({"target": key, "error": repr(e)})

    # ------------------------------------------------------------------
    # Step 2: Streams
    # ------------------------------------------------------------------
    for s in streams_cfg:
        name = s.get("name")
        if not name:
            logger.warning("invalid stream config | %s", s)
            report["skipped"]["streams"].append(str(s))
            continue

        try:
            await js.stream_info(name)
        except Exception:
            logger.info("stream not exists | %s", name)
            report["skipped"]["streams"].append(name)
            continue

        try:
            await js.delete_stream(name)
            logger.info("stream deleted | %s", name)
            report["dropped"]["streams"].append(name)
        except Exception as e:
            logger.error("stream delete failed | %s | %r", name, e)
            report["errors"].append({"target": name, "error": repr(e)})

    if report["errors"]:
        logger.warning("reset finished with errors | trace_id=%s", report["trace_id"])
    else:
        logger.info("reset finished | trace_id=%s", report["trace_id"])

    return report
