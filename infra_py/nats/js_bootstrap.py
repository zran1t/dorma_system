"""
File: infra_py/nats/js_bootstrap.py
Module: infra_py.nats.js_bootstrap

職責 (Responsibility):
    提供 NATS 與 JetStream 拓樸之建立與對齊能力，
    依 YAML 解析後之 dict 建立或更新 streams 與 consumers。
    本模組負責「狀態收斂」，而非僅建立一次性資源。

注意事項 (Notes):
    - duplicate_window 與 ack_wait 一律以秒數(float)傳入 JetStream API（避免型別不相容）。
    - 若資源已存在，僅在設定差異時進行更新，避免不必要的控制面震盪。
    - 本模組不負責刪除既有資源；刪除行為由 reset 工具負責。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import re
from typing import Any, Dict, Tuple

# === 第三方套件 (Third-Party Libraries) ===
from nats.aio.client import Client as NATS
from nats.js.api import (
    StreamConfig,
    ConsumerConfig,
    StorageType,
    RetentionPolicy,
    DiscardPolicy,
    AckPolicy,
)

# === 系統內模組 (Internal Modules) ===
from infra_py.logging.logger import get_logger


logger = get_logger(__name__, subdir="jetstream/bootstrap")

# _DURATION_RE 解析 duration 字串用途說明。
#
# 功能:
#     - 提供 duration 字串之最小可行解析能力（value + unit）。
#     - 供 duplicate_window / ack_wait 轉換為秒數(float)使用。
#
# 契約 / 限制:
#     - 僅接受單一數值與單一單位尾綴（例如 30s / 1500ms / 2m / 1h / 7d）。
#     - 解析失敗時由上層回退至 default_seconds。
_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)?\s*$")


def _parse_duration_seconds(text: str | float | int, default_seconds: float) -> float:
    """
    _parse_duration_seconds 將多型時間表示轉換為秒數(float)。

    功能:
        - 支援秒數數值與字串型 duration（ms/s/m/h/d）。
        - 解析失敗時回退至 default_seconds，確保 bootstrap 流程可持續執行。

    參數:
        - text: 時間字串或數值；數值視為秒數。
        - default_seconds: 解析失敗時使用之預設秒數。

    回傳:
        - result: 秒數(float)。
        - error: 無。

    備註:
        - JetStream Python client 部分欄位以秒數(float)表達，需避免傳入 timedelta 或非相容型別。
    """
    if text is None:
        return float(default_seconds)
    if isinstance(text, (int, float)):
        return float(text)

    s = str(text).strip().lower()
    m = _DURATION_RE.match(s)
    if not m:
        return float(default_seconds)

    val = float(m.group(1))
    unit = (m.group(2) or "s").lower()

    if unit in ("s", "sec", "secs", "second", "seconds"):
        return val
    if unit in ("ms", "msec", "msecs", "millisecond", "milliseconds"):
        return val / 1000.0
    if unit in ("m", "min", "mins", "minute", "minutes"):
        return val * 60.0
    if unit in ("h", "hr", "hrs", "hour", "hours"):
        return val * 3600.0
    if unit in ("d", "day", "days"):
        return val * 86400.0

    return val


def _map_storage(v: str) -> StorageType:
    """
    _map_storage 將 YAML storage 字串映射為 StorageType。

    功能:
        - 將 "file" / "memory" 映射為 JetStream StorageType。

    參數:
        - v: YAML storage 值。

    回傳:
        - result: StorageType。
        - error: 無。

    備註:
        - 未識別值視為 MEMORY，以降低不可用配置造成的啟動中斷風險。
    """
    return StorageType.FILE if str(v).lower() == "file" else StorageType.MEMORY


def _map_retention(v: str) -> RetentionPolicy:
    """
    _map_retention 將 YAML retention 字串映射為 RetentionPolicy。

    功能:
        - 將常用 retention 值映射為 JetStream RetentionPolicy。

    參數:
        - v: YAML retention 值。

    回傳:
        - result: RetentionPolicy。
        - error: 無。

    備註:
        - 未識別值回退至 LIMITS 以保持行為可預期。
    """
    s = str(v).lower()
    if s == "interest":
        return RetentionPolicy.INTEREST
    if s == "workqueue":
        return RetentionPolicy.WORKQUE
    return RetentionPolicy.LIMITS


def _map_discard(v: str) -> DiscardPolicy:
    """
    _map_discard 將 YAML discard 字串映射為 DiscardPolicy。

    功能:
        - 將常用 discard 值映射為 JetStream DiscardPolicy。

    參數:
        - v: YAML discard 值。

    回傳:
        - result: DiscardPolicy。
        - error: 無。

    備註:
        - 未識別值回退至 NEW 以保持行為可預期。
    """
    return DiscardPolicy.OLD if str(v).lower() == "old" else DiscardPolicy.NEW


async def bootstrap_nats_and_js(cfg: Dict[str, Any]) -> Tuple[NATS, Any]:
    """
    bootstrap_nats_and_js 建立並對齊 NATS 與 JetStream 狀態。

    功能:
        - 建立 NATS 連線並取得 JetStream context。
        - 依 cfg 內容建立或更新 streams（必要時 update）。
        - 依 cfg 內容建立或更新 consumers（必要時 update）。

    參數:
        - cfg: YAML mirror dict（需包含 nats / jetstream 結構）。

    回傳:
        - result: (NATS client, JetStream context)。
        - error: 無。

    備註:
        - 本函式不刪除任何既有資源；僅做「建立或更新」以收斂狀態。
        - 更新行為僅在偵測到差異時執行，避免因重覆 update 造成控制面不穩定。
    """
    nats_cfg = cfg.get("nats") or {}
    servers = nats_cfg.get("servers")

    if not isinstance(servers, list) or not servers:
        raise ValueError("nats.servers must be non-empty list")

    # 建立連線以取得 JetStream context，作為後續拓樸收斂之唯一控制面入口。
    logger.info("connecting nats | servers=%s", servers)

    nc = NATS()
    await nc.connect(servers=servers)
    js = nc.jetstream()

    js_cfg = cfg.get("jetstream") or {}

    # ------------------------------------------------------------------
    # Streams
    # ------------------------------------------------------------------
    streams = js_cfg.get("streams") or []
    for s in streams:
        name = s.get("name")
        subjects = s.get("subjects") or []

        if not name or not subjects:
            raise ValueError(f"invalid stream config | {s}")

        sc = StreamConfig(
            name=name,
            subjects=subjects,
            storage=_map_storage(s.get("storage", "file")),
            retention=_map_retention(s.get("retention", "limits")),
            discard=_map_discard(s.get("discard", "old")),
            # 以秒數(float)提供 duplicate_window，避免型別不相容造成的建立/更新失敗。
            duplicate_window=_parse_duration_seconds(s.get("duplicates", "30s"), 30.0),
        )

        try:
            await js.add_stream(sc)
            logger.info("stream created | %s", name)
        except Exception:
            info = await js.stream_info(name)

            # 僅在核心欄位差異時更新，以維持控制面操作的最小化與可預期性。
            need_update = (
                set(info.config.subjects or []) != set(sc.subjects or [])
                or info.config.storage != sc.storage
                or info.config.retention != sc.retention
                or info.config.discard != sc.discard
                or float(info.config.duplicate_window or 0) != float(sc.duplicate_window or 0)
            )

            if need_update:
                await js.update_stream(sc)
                logger.info("stream updated | %s", name)
            else:
                logger.info("stream unchanged | %s", name)

    # ------------------------------------------------------------------
    # Consumers
    # ------------------------------------------------------------------
    consumers = js_cfg.get("consumers") or []
    for c in consumers:
        stream = c.get("stream")
        durable = c.get("durable_name")
        filter_subject = c.get("filter_subject")
        deliver_subject = c.get("deliver_subject")

        if not all([stream, durable, filter_subject, deliver_subject]):
            raise ValueError(f"invalid consumer config | {c}")

        ack_policy = (
            AckPolicy.EXPLICIT
            if str(c.get("ack_policy", "explicit")).lower() == "explicit"
            else AckPolicy.NONE
        )

        cc = ConsumerConfig(
            durable_name=durable,
            filter_subject=filter_subject,
            deliver_subject=deliver_subject,
            ack_policy=ack_policy,
            # 以秒數(float)提供 ack_wait，避免型別不相容造成的建立/更新失敗。
            ack_wait=_parse_duration_seconds(c.get("ack_wait", "30s"), 30.0),
        )

        try:
            await js.add_consumer(stream, cc)
            logger.info("consumer created | %s@%s", durable, stream)
        except Exception:
            info = await js.consumer_info(stream, durable)

            # 僅在核心欄位差異時更新，以維持 consumer delivery contract 的穩定性。
            need_update = (
                info.config.filter_subject != cc.filter_subject
                or info.config.deliver_subject != cc.deliver_subject
                or info.config.ack_policy != cc.ack_policy
                or float(info.config.ack_wait or 0) != float(cc.ack_wait or 0)
            )

            if need_update:
                await js.update_consumer(stream, cc)
                logger.info("consumer updated | %s@%s", durable, stream)
            else:
                logger.info("consumer unchanged | %s@%s", durable, stream)

    return nc, js
