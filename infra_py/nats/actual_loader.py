"""
File: infra_py/nats/actual_loader.py
Module: infra_py.nats.actual_loader

職責 (Responsibility):
    從 JetStream 控制面查詢實際 stream/consumer 狀態並解碼為 ActualTopology。
    將 SDK/版本差異（例如 ack_wait 型別）集中在此層，避免 diff/boot/audit 重複處理。

注意事項 (Notes):
    - 本模組只做讀取，不做任何 JetStream 寫入。
    - ack_wait 解碼需處理 ns(int)/sec(float)/timedelta 等多型回傳，避免誤判。
    - 回傳範圍採 expected-driven：只查 expected 需要的資源，避免 inventory 權限耦合。
    - 不得依賴 system_initializer 或 application/domain 層模組。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
from datetime import timedelta
from typing import Any, Dict, List, Optional

# === 第三方套件 (Third-Party Libraries) ===
from nats.aio.client import Client as NATS
from nats.js.api import AckPolicy, StorageType, RetentionPolicy, DiscardPolicy

# === 系統內模組 (Internal Modules) ===
from infra_py.nats.model import (
    ActualConsumer,
    ActualStream,
    ActualTopology,
    ExpectedTopology,
)
from infra_py.logging.logger import get_logger


logger = get_logger(__name__)


async def load_actual_topology(expected: ExpectedTopology, nc: NATS) -> ActualTopology:
    """
    load_actual_topology 載入 expected-driven 的 ActualTopology。

    功能:
        - 依 expected streams/consumers 查詢 JetStream 實際狀態並解碼為治理層模型。
        - 將回讀的 enum/duration 正規化為可比對字串與 seconds(float)。

    參數:
        - expected: ExpectedTopology（定義要查哪些資源）。
        - nc: 已連線之 NATS client。

    回傳:
        - result: ActualTopology。
        - error: 例外上拋（由上層決定缺失如何呈現；diff 層將處理 missing/mismatch）。

    備註:
        - 本函式不捕捉缺失資源，缺失由 diff 層表達；此處專注「存在時的解碼一致性」。
    """
    js = nc.jetstream()

    streams: List[ActualStream] = []
    for s in expected.streams:
        info = await js.stream_info(s.stream_name)
        streams.append(
            ActualStream(
                stream_name=s.stream_name,
                subjects=sorted(list(info.config.subjects or [])),
                storage=_storage_to_str(info.config.storage),
                retention=_retention_to_str(getattr(info.config, "retention", None)),
                discard=_discard_to_str(getattr(info.config, "discard", None)),
                duplicate_window_seconds=float(getattr(info.config, "duplicate_window", 0.0) or 0.0),
                max_age_seconds=_opt_float(getattr(info.config, "max_age", None)),
            )
        )

    consumers: List[ActualConsumer] = []
    for c in expected.consumers:
        ci = await js.consumer_info(c.stream_name, c.durable_name)
        consumers.append(
            ActualConsumer(
                stream_name=c.stream_name,
                durable_name=c.durable_name,
                filter_subject=str(ci.config.filter_subject or ""),
                deliver_subject=str(ci.config.deliver_subject or ""),
                ack_policy=_ack_policy_to_str(ci.config.ack_policy),
                max_deliver=_opt_int(getattr(ci.config, "max_deliver", None)),
                ack_wait_seconds=_ack_wait_to_seconds(getattr(ci.config, "ack_wait", None)),
                backoff_seconds=_opt_backoff_seconds(getattr(ci.config, "backoff", None)),
            )
        )

    return ActualTopology(streams=streams, consumers=consumers)


def _opt_int(v: Any) -> Optional[int]:
    """
    _opt_int 將可能為 None 的值轉為 Optional[int]。

    功能:
        - 正規化 SDK 回讀欄位，避免 diff 層處理多型。

    參數:
        - v: 可能為 None/數值。

    回傳:
        - result: Optional[int]。
        - error: 無。

    備註:
        - 非 int 直接回傳 None，交由 diff 層視為未設定。
    """
    return int(v) if isinstance(v, int) else None


def _opt_float(v: Any) -> Optional[float]:
    """
    _opt_float 將可能為 None 的值轉為 Optional[float]。

    功能:
        - 正規化 SDK 回讀欄位，避免 diff 層處理多型。

    參數:
        - v: 可能為 None/float/int/timedelta。

    回傳:
        - result: Optional[float]。
        - error: 無。

    備註:
        - timedelta 轉 total_seconds。
    """
    if v is None:
        return None
    if isinstance(v, timedelta):
        return float(v.total_seconds())
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _opt_backoff_seconds(v: Any) -> Optional[List[float]]:
    """
    _opt_backoff_seconds 解碼 backoff 序列為 seconds list。

    功能:
        - 正規化 consumer_info 回傳的 backoff 型別，統一回傳 seconds(float) list。

    參數:
        - v: consumer_info.config.backoff（可能為 None/list/其他）。

    回傳:
        - result: Optional[List[float]]。
        - error: 無。

    備註:
        - 若 backoff 不存在則回傳 None。
        - 若元素為 timedelta 則轉 seconds；若為 int 則視為 ns（契約一致化）。
    """
    if v is None:
        return None
    if not isinstance(v, list):
        return None

    out: List[float] = []
    for item in v:
        if isinstance(item, timedelta):
            out.append(float(item.total_seconds()))
        elif isinstance(item, int):
            out.append(float(item) / 1_000_000_000.0)
        elif isinstance(item, float):
            out.append(float(item))
        else:
            # 不支援型別直接忽略，避免單一元素型別差異造成全體不可用
            continue

    return out


def _ack_wait_to_seconds(value: Any) -> float:
    """
    _ack_wait_to_seconds 解碼 JetStream consumer ack_wait 為 seconds(float)。

    功能:
        - 正確處理 consumer_info 回傳的 ack_wait 型別差異（ns/int、sec/float、timedelta）。
        - 統一回傳 seconds(float) 供 diff 使用。

    參數:
        - value: consumer_info 回傳的 ack_wait 欄位。

    回傳:
        - result: seconds(float)。
        - error: TypeError（不支援型別時）。

    備註:
        - 不使用 heuristic（例如 >1e6 視為 ns）以避免邊界值誤判。
    """
    if value is None:
        return 0.0

    if isinstance(value, timedelta):
        return float(value.total_seconds())

    if isinstance(value, int):
        return float(value) / 1_000_000_000.0

    if isinstance(value, float):
        return float(value)

    raise TypeError(f"unsupported ack_wait type: {type(value)}")


def _storage_to_str(storage: StorageType) -> str:
    """
    _storage_to_str 將 StorageType 轉換為可比對字串。

    功能:
        - 將 JetStream StorageType 映射為 YAML 字串表示。

    參數:
        - storage: JetStream StorageType。

    回傳:
        - result: "file" / "memory" / fallback。
        - error: 無。

    備註:
        - fallback 僅用於相容未知 enum 值之情境。
    """
    if storage == StorageType.FILE:
        return "file"
    if storage == StorageType.MEMORY:
        return "memory"
    return str(storage).lower()


def _retention_to_str(retention: Any) -> str:
    """
    _retention_to_str 將 RetentionPolicy 轉換為可比對字串。

    功能:
        - 將 JetStream RetentionPolicy 映射為 YAML 字串表示。

    參數:
        - retention: JetStream RetentionPolicy 或其他。

    回傳:
        - result: "limits" / "interest" / "workqueue" / fallback。
        - error: 無。

    備註:
        - 若回讀值為 None，回傳 fallback 字串以避免崩潰。
    """
    if retention == RetentionPolicy.LIMITS:
        return "limits"
    if retention == RetentionPolicy.INTEREST:
        return "interest"
    if retention == RetentionPolicy.WORKQUE:
        return "workqueue"
    return str(retention).lower()


def _discard_to_str(discard: Any) -> str:
    """
    _discard_to_str 將 DiscardPolicy 轉換為可比對字串。

    功能:
        - 將 JetStream DiscardPolicy 映射為 YAML 字串表示。

    參數:
        - discard: JetStream DiscardPolicy 或其他。

    回傳:
        - result: "old" / "new" / fallback。
        - error: 無。

    備註:
        - 若回讀值為 None，回傳 fallback 字串以避免崩潰。
    """
    if discard == DiscardPolicy.OLD:
        return "old"
    if discard == DiscardPolicy.NEW:
        return "new"
    return str(discard).lower()


def _ack_policy_to_str(policy: AckPolicy) -> str:
    """
    _ack_policy_to_str 將 AckPolicy 轉換為可比對字串。

    功能:
        - 將 AckPolicy 映射為 YAML 使用之字串表示。

    參數:
        - policy: JetStream AckPolicy。

    回傳:
        - result: "explicit" / "none" / "all" / fallback。
        - error: 無。

    備註:
        - fallback 僅用於相容未知 enum 值之情境。
    """
    if policy == AckPolicy.EXPLICIT:
        return "explicit"
    if policy == AckPolicy.NONE:
        return "none"
    if policy == AckPolicy.ALL:
        return "all"
    return str(policy).lower()
