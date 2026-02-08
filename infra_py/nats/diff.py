"""
File: infra_py/nats/diff.py
Module: infra_py.nats.diff

職責 (Responsibility):
    提供 ExpectedTopology 與 ActualTopology 的唯一比對邏輯（single source of truth）。
    產出 TopologyDiff，供 bootstrap 決策 create/update 與 auditor 產出 mismatch 報告。

注意事項 (Notes):
    - 本模組不做 I/O，不查 JetStream，不做任何寫入。
    - expected-driven：只比較 expected 要求的資源；extra 不列舉（避免 inventory 耦合）。
    - timing contract 已由 ExpectedConsumer.timing 收斂；diff 不再推導 backoff→ack_wait。
    - 不得依賴 system_initializer 或 application/domain 層模組。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# === 第三方套件 (Third-Party Libraries) ===
# 無

# === 系統內模組 (Internal Modules) ===
from infra_py.nats.duration import float_equal, format_seconds
from infra_py.nats.model import (
    ActualConsumer,
    ActualStream,
    ActualTopology,
    ExpectedConsumer,
    ExpectedStream,
    ExpectedTopology,
)


@dataclass(frozen=True)
class TopologyDiff:
    """
    TopologyDiff 拓樸差異結果。

    功能:
        - 表達 expected-driven 的 missing/mismatched 集合，供 audit 與 bootstrap 共用。
        - 將差異結構化，避免 bootstrap/audit 各自再組 report。

    欄位說明:
        - ok: 是否完全一致。
        - missing_streams: 缺失的 stream 名稱清單。
        - missing_consumers: 缺失的 consumer key 清單（durable@stream）。
        - mismatched_streams: stream 欄位差異 dict（name -> field -> {expected, actual}）。
        - mismatched_consumers: consumer 欄位差異 dict（key -> field -> {expected, actual}）。

    契約 / 限制:
        - 不列舉 extra（inventory 不在本治理工具契約內）。

    備註:
        - mismatch 的 expected/actual 以「治理層字串/秒數」輸出，避免 SDK 原生型別噪音。
    """

    ok: bool
    missing_streams: List[str]
    missing_consumers: List[str]
    mismatched_streams: Dict[str, Dict[str, Dict[str, Any]]]
    mismatched_consumers: Dict[str, Dict[str, Dict[str, Any]]]


def diff_topology(expected: ExpectedTopology, actual: ActualTopology) -> TopologyDiff:
    """
    diff_topology 比對 ExpectedTopology 與 ActualTopology。

    功能:
        - 產出 missing/mismatched，作為 bootstrap 與 audit 的共同判斷依據。

    參數:
        - expected: 期望拓樸。
        - actual: 實際拓樸（expected-driven 範圍）。

    回傳:
        - result: TopologyDiff。
        - error: 無。

    備註:
        - 時間欄位（duplicate_window/ack_wait）以 seconds(float) 比對，使用容許誤差避免反覆震盪。
    """
    actual_stream_map: Dict[str, ActualStream] = {s.stream_name: s for s in actual.streams}
    actual_consumer_map: Dict[str, ActualConsumer] = {c.key: c for c in actual.consumers}

    missing_streams: List[str] = []
    missing_consumers: List[str] = []
    mismatched_streams: Dict[str, Dict[str, Dict[str, Any]]] = {}
    mismatched_consumers: Dict[str, Dict[str, Dict[str, Any]]] = {}

    ok = True

    # ------------------------------------------------------------------
    # Streams
    # ------------------------------------------------------------------
    for es in expected.streams:
        a = actual_stream_map.get(es.stream_name)
        if a is None:
            ok = False
            missing_streams.append(es.stream_name)
            continue

        _cmp_stream_field(mismatched_streams, es.stream_name, "subjects", sorted(es.subjects), sorted(a.subjects))
        _cmp_stream_field(mismatched_streams, es.stream_name, "storage", es.storage, a.storage)
        _cmp_stream_field(mismatched_streams, es.stream_name, "retention", es.retention, a.retention)
        _cmp_stream_field(mismatched_streams, es.stream_name, "discard", es.discard, a.discard)

        if not float_equal(a.duplicate_window_seconds, es.duplicate_window_seconds, tolerance=0.001):
            _cmp_stream_field(
                mismatched_streams,
                es.stream_name,
                "duplicate_window",
                format_seconds(es.duplicate_window_seconds),
                format_seconds(a.duplicate_window_seconds),
            )

        # max_age_seconds=None 表示「不要求收斂」，因此不比較
        if es.max_age_seconds is not None:
            actual_max_age = float(a.max_age_seconds or 0.0)
            if not float_equal(actual_max_age, float(es.max_age_seconds), tolerance=0.001):
                _cmp_stream_field(
                    mismatched_streams,
                    es.stream_name,
                    "max_age",
                    format_seconds(float(es.max_age_seconds)),
                    format_seconds(actual_max_age),
                )

    # ------------------------------------------------------------------
    # Consumers
    # ------------------------------------------------------------------
    for ec in expected.consumers:
        a = actual_consumer_map.get(ec.key)
        if a is None:
            ok = False
            missing_consumers.append(ec.key)
            continue

        _cmp_consumer_field(mismatched_consumers, ec.key, "filter_subject", ec.filter_subject, a.filter_subject)
        _cmp_consumer_field(mismatched_consumers, ec.key, "deliver_subject", ec.deliver_subject, a.deliver_subject)
        _cmp_consumer_field(mismatched_consumers, ec.key, "ack_policy", ec.ack_policy, a.ack_policy)

        # max_deliver：None 表示不要求收斂（保留 server 現況）
        if ec.max_deliver is not None:
            _cmp_consumer_field(mismatched_consumers, ec.key, "max_deliver", ec.max_deliver, a.max_deliver)

        # timing：只比對最終有效 ack_wait_seconds；backoff 序列也比（若 expected 是 BACKOFF）
        if not float_equal(a.ack_wait_seconds, ec.timing.ack_wait_seconds, tolerance=0.5):
            _cmp_consumer_field(
                mismatched_consumers,
                ec.key,
                "ack_wait",
                format_seconds(ec.timing.ack_wait_seconds),
                format_seconds(a.ack_wait_seconds),
            )

        if ec.timing.mode.value == "backoff":
            exp_backoff = list(ec.timing.backoff_seconds or [])
            act_backoff = list(a.backoff_seconds or [])
            if exp_backoff != act_backoff:
                _cmp_consumer_field(
                    mismatched_consumers,
                    ec.key,
                    "backoff",
                    exp_backoff,
                    act_backoff,
                )

    if mismatched_streams or mismatched_consumers or missing_streams or missing_consumers:
        ok = False

    return TopologyDiff(
        ok=ok,
        missing_streams=missing_streams,
        missing_consumers=missing_consumers,
        mismatched_streams=mismatched_streams,
        mismatched_consumers=mismatched_consumers,
    )


def _cmp_stream_field(
    mismatched: Dict[str, Dict[str, Dict[str, Any]]],
    stream_name: str,
    field: str,
    expected: Any,
    actual: Any,
) -> None:
    """
    _cmp_stream_field 記錄 stream 欄位差異。

    功能:
        - 將差異寫入 mismatched_streams 結構，供上層產出報告與 bootstrap 決策。

    參數:
        - mismatched: stream mismatch dict（可變容器）。
        - stream_name: stream 名稱。
        - field: 欄位名稱。
        - expected: 期望值。
        - actual: 實際值。

    回傳:
        - result: 無。
        - error: 無。

    備註:
        - 此函式不做 logging，避免噪音；report 是唯一權威輸出。
    """
    if expected == actual:
        return
    mismatched.setdefault(stream_name, {})
    mismatched[stream_name][field] = {"expected": expected, "actual": actual}


def _cmp_consumer_field(
    mismatched: Dict[str, Dict[str, Dict[str, Any]]],
    key: str,
    field: str,
    expected: Any,
    actual: Any,
) -> None:
    """
    _cmp_consumer_field 記錄 consumer 欄位差異。

    功能:
        - 將差異寫入 mismatched_consumers 結構，供上層產出報告與 bootstrap 決策。

    參數:
        - mismatched: consumer mismatch dict（可變容器）。
        - key: consumer key（durable@stream）。
        - field: 欄位名稱。
        - expected: 期望值。
        - actual: 實際值。

    回傳:
        - result: 無。
        - error: 無。

    備註:
        - 只做資料結構收斂，不做格式化輸出。
    """
    if expected == actual:
        return
    mismatched.setdefault(key, {})
    mismatched[key][field] = {"expected": expected, "actual": actual}
