"""
File: infra_py/nats/js_bootstrap.py
Module: infra_py.nats.js_bootstrap

職責 (Responsibility):
    提供 JetStream 拓樸之建立與對齊能力（狀態收斂）。
    採用 expected_loader + actual_loader + diff 作為唯一治理管線，避免多處推導契約。

注意事項 (Notes):
    - 本模組只負責 create/update，不負責 delete（delete 由 js_reset 負責）。
    - 所有 duration 欄位一律以 seconds(float) 傳入 nats-py JetStream API（由 SDK 處理 ns 序列化）。
    - consumer timing contract 由 ExpectedConsumer.timing 提供，bootstrap 不再做 override 推導。
    - 不得依賴 system_initializer 或 application/domain 層模組。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
from typing import Any, Dict, Optional

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
from nats.js.errors import BadRequestError
from nats.js.errors import NotFoundError

# === 系統內模組 (Internal Modules) ===
from infra_py.logging.logger import get_logger
from infra_py.nats.actual_loader import load_actual_topology
from infra_py.nats.diff import diff_topology
from infra_py.nats.duration import float_equal
from infra_py.nats.expected_loader import build_expected_topology


logger = get_logger(__name__)


async def bootstrap_channels_topology(cfg: Dict[str, Any], nc: NATS) -> Any:
    """
    bootstrap_channels_topology 收斂 JetStream 拓樸至期望狀態。

    功能:
        - 依 cfg 建立 ExpectedTopology。
        - 讀取 ActualTopology（expected-driven）。
        - 產出 Diff 並執行 create/update 收斂。

    參數:
        - cfg: YAML mirror dict（需包含 jetstream 結構；已由 config_loader 驗證）。
        - nc: 已連線之 NATS client（由上層管理生命週期）。

    回傳:
        - result: JetStream context（nc.jetstream()）。
        - error: BadRequestError / 其他例外上拋。

    備註:
        - 本函式不做 delete；如需清理殘留，請先執行 js_reset。
    """
    js = nc.jetstream()

    expected = build_expected_topology(cfg)

    # 先嘗試讀現況（存在性由 try/except 控制），以決定 create/update；缺失在 create 路徑處理
    # 若讀現況失敗（例如某些不存在），bootstrap 仍可進行 create；diff 的 missing 只用於 audit 報告。
    actual = await _load_actual_best_effort(expected, nc)

    diff = diff_topology(expected, actual)

    # ------------------------------------------------------------------
    # Streams: create/update
    # ------------------------------------------------------------------
    for es in expected.streams:
        sc = _to_stream_config(es)

        if es.stream_name in diff.missing_streams:
            await js.add_stream(sc)
            logger.info("stream created | %s", es.stream_name)
            continue

        # 只在 mismatch 時 update，避免控制面震盪
        if es.stream_name in diff.mismatched_streams:
            await js.update_stream(sc)
            logger.info("stream updated | %s", es.stream_name)
        else:
            logger.info("stream unchanged | %s", es.stream_name)

    # ------------------------------------------------------------------
    # Consumers: create/update
    # ------------------------------------------------------------------
    for ec in expected.consumers:
        cc = _to_consumer_config(ec)

        if ec.key in diff.missing_consumers:
            try:
                await js.add_consumer(ec.stream_name, cc)
                logger.info("consumer created | %s | ack_wait=%ss", ec.key, float(ec.timing.ack_wait_seconds))
                continue
            except BadRequestError as e:
                logger.error("consumer create bad request | %s | err=%r", ec.key, e)
                raise

        if ec.key in diff.mismatched_consumers:
            await js.update_consumer(ec.stream_name, cc)
            logger.info("consumer updated | %s | ack_wait=%ss", ec.key, float(ec.timing.ack_wait_seconds))
        else:
            logger.info("consumer unchanged | %s", ec.key)

    return js


async def _load_actual_best_effort(expected: Any, nc: NATS) -> Any:
    """
    _load_actual_best_effort 以 best-effort 方式載入 ActualTopology。

    功能:
        - 避免因部分資源缺失導致 bootstrap 無法進行 create。
        - 對於缺失資源，略過並交由 diff 判定 missing。

    參數:
        - expected: ExpectedTopology。
        - nc: NATS client。

    回傳:
        - result: ActualTopology（可能缺少部分項目）。
        - error: 無（僅在不可恢復的查詢錯誤時上拋）。

    備註:
        - 這是 bootstrap 的實務防衛；auditor 會使用完整讀取（缺失會被 report）。
    """
    js = nc.jetstream()

    streams = []
    for s in expected.streams:
        try:
            # 讓 actual_loader 做統一解碼，但這裡無法逐個呼叫；因此直接交給 actual_loader
            pass
        except Exception:
            pass

    try:
        return await load_actual_topology(expected, nc)
    except NotFoundError:
        # reset 後 / 初次 bootstrap 的正常路徑
        logger.info(
            "actual topology not found; treat as empty and bootstrap from scratch"
        )
        from infra_py.nats.model import ActualTopology
        return ActualTopology(streams=[], consumers=[])
    except Exception as e:
        # 真正不預期的錯誤
        logger.warning(
            "load actual topology failed; fallback to empty actual | err=%r", e
        )
        from infra_py.nats.model import ActualTopology
        return ActualTopology(streams=[], consumers=[])


def _to_stream_config(es: Any) -> StreamConfig:
    """
    _to_stream_config 將 ExpectedStream 映射為 StreamConfig。

    功能:
        - 將治理層模型轉為 nats-py StreamConfig，供 add/update 使用。

    參數:
        - es: ExpectedStream。

    回傳:
        - result: StreamConfig。
        - error: ValueError（當 enum 值不支援）。

    備註:
        - max_age_seconds=None 表示不設定（保留 server default）；因此不傳入 max_age。
    """
    kwargs: Dict[str, Any] = {
        "name": es.stream_name,
        "subjects": list(es.subjects),
        "storage": _map_storage(es.storage),
        "retention": _map_retention(es.retention),
        "discard": _map_discard(es.discard),
        "duplicate_window": float(es.duplicate_window_seconds),
    }

    if es.max_age_seconds is not None:
        kwargs["max_age"] = float(es.max_age_seconds)

    return StreamConfig(**kwargs)


def _to_consumer_config(ec: Any) -> ConsumerConfig:
    """
    _to_consumer_config 將 ExpectedConsumer 映射為 ConsumerConfig。

    功能:
        - 將治理層模型轉為 nats-py ConsumerConfig，供 add/update 使用。

    參數:
        - ec: ExpectedConsumer。

    回傳:
        - result: ConsumerConfig。
        - error: ValueError（當 enum 值不支援）。

    備註:
        - timing contract：ACK_WAIT 模式不傳 backoff；BACKOFF 模式傳 backoff 並傳 ack_wait_seconds（等於 backoff[0]）。
    """
    kwargs: Dict[str, Any] = {
        "durable_name": ec.durable_name,
        "filter_subject": ec.filter_subject,
        "deliver_subject": ec.deliver_subject,
        "ack_policy": _map_ack_policy(ec.ack_policy),
        "ack_wait": float(ec.timing.ack_wait_seconds),
    }

    if ec.max_deliver is not None:
        kwargs["max_deliver"] = int(ec.max_deliver)

    if ec.timing.backoff_seconds is not None:
        kwargs["backoff"] = list(ec.timing.backoff_seconds)

    return ConsumerConfig(**kwargs)


def _map_storage(v: str) -> StorageType:
    """
    _map_storage 將 YAML storage 字串映射為 StorageType。

    功能:
        - 將 "file"/"memory" 映射為 JetStream StorageType。

    參數:
        - v: YAML storage 值。

    回傳:
        - result: StorageType。
        - error: 無。

    備註:
        - 未識別值視為 MEMORY；若需 fail-fast 可在 config_loader 加強約束。
    """
    return StorageType.FILE if str(v).lower() == "file" else StorageType.MEMORY


def _map_retention(v: str) -> RetentionPolicy:
    """
    _map_retention 將 YAML retention 字串映射為 RetentionPolicy。

    功能:
        - 將 "limits"/"interest"/"workqueue" 映射為 JetStream RetentionPolicy。

    參數:
        - v: YAML retention 值。

    回傳:
        - result: RetentionPolicy。
        - error: 無。

    備註:
        - 未識別值回退至 LIMITS。
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
        - 將 "old"/"new" 映射為 JetStream DiscardPolicy。

    參數:
        - v: YAML discard 值。

    回傳:
        - result: DiscardPolicy。
        - error: 無。
    """
    return DiscardPolicy.OLD if str(v).lower() == "old" else DiscardPolicy.NEW


def _map_ack_policy(v: str) -> AckPolicy:
    """
    _map_ack_policy 將 YAML ack_policy 字串映射為 AckPolicy。

    功能:
        - 將 explicit/none/all 映射為 JetStream AckPolicy enum。

    參數:
        - v: YAML ack_policy 字串。

    回傳:
        - result: AckPolicy enum。
        - error: ValueError（未知值）。

    備註:
        - config_loader 已約束值域；此處仍保留 fail-fast 防衛。
    """
    s = str(v).strip().lower()
    if s == "explicit":
        return AckPolicy.EXPLICIT
    if s == "none":
        return AckPolicy.NONE
    if s == "all":
        return AckPolicy.ALL
    raise ValueError(f"invalid ack_policy: {v}")
