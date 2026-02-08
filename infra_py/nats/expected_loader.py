"""
File: infra_py/nats/expected_loader.py
Module: infra_py.nats.expected_loader

職責 (Responsibility):
    將 YAML mirror dict 轉換為治理層的 ExpectedTopology 模型。
    將預設值與 normalize 集中在此層，避免 bootstrap/audit 各自補洞造成 drift。

注意事項 (Notes):
    - 本模組不做 I/O，不讀檔、不查 JetStream；只做 mapping/normalize。
    - consumer timing contract 使用 model.resolve_consumer_timing()，禁止此處自行推導。
    - max_age 缺省時回傳 None，表示「不要求收斂」。
    - 不得依賴 system_initializer 或 application/domain 層模組。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
from typing import Any, Dict, List, Optional

# === 第三方套件 (Third-Party Libraries) ===
# 無

# === 系統內模組 (Internal Modules) ===
from infra_py.nats.duration import parse_duration_seconds
from infra_py.nats.model import (
    ExpectedConsumer,
    ExpectedStream,
    ExpectedTopology,
    resolve_consumer_timing,
)


def build_expected_topology(cfg: Dict[str, Any]) -> ExpectedTopology:
    """
    build_expected_topology 建立 ExpectedTopology。

    功能:
        - 將 YAML mirror dict 的 jetstream 區塊映射為 ExpectedTopology。
        - 補齊治理層需要的預設值與 seconds 表示。

    參數:
        - cfg: YAML mirror dict（已通過 config_loader 最小驗證）。

    回傳:
        - result: ExpectedTopology。
        - error: ValueError（當結構不符合預期或 duration 解析失敗導致不合理狀態）。

    備註:
        - stream: duplicates/duplicate_window 缺省 30s。
        - consumer: ack_wait 缺省 30s（僅在 backoff 不存在時）。
    """
    js = cfg.get("jetstream") or {}

    streams_cfg: List[Dict[str, Any]] = list(js.get("streams") or [])
    consumers_cfg: List[Dict[str, Any]] = list(js.get("consumers") or [])

    streams: List[ExpectedStream] = []
    for s in streams_cfg:
        stream_name = str(s.get("name")).strip()
        subjects = list(s.get("subjects") or [])

        storage = str(s.get("storage", "file")).strip().lower()
        retention = str(s.get("retention", "limits")).strip().lower()
        discard = str(s.get("discard", "old")).strip().lower()

        dup_raw = s.get("duplicate_window", s.get("duplicates", "30s"))
        duplicate_window_seconds = parse_duration_seconds(dup_raw, 30.0)

        max_age_seconds: Optional[float] = None
        if "max_age" in s and s.get("max_age") is not None:
            max_age_seconds = parse_duration_seconds(s.get("max_age"), 0.0)

        streams.append(
            ExpectedStream(
                stream_name=stream_name,
                subjects=subjects,
                storage=storage,
                retention=retention,
                discard=discard,
                duplicate_window_seconds=float(duplicate_window_seconds),
                max_age_seconds=max_age_seconds,
            )
        )

    consumers: List[ExpectedConsumer] = []
    for c in consumers_cfg:
        stream_name = str(c.get("stream")).strip()
        durable_name = str(c.get("durable_name")).strip()
        consumer_name = str(c.get("name")).strip()

        filter_subject = str(c.get("filter_subject")).strip()
        deliver_subject = str(c.get("deliver_subject")).strip()

        ack_policy = str(c.get("ack_policy", "explicit")).strip().lower()
        max_deliver = c.get("max_deliver", None)

        timing = resolve_consumer_timing(c)

        consumers.append(
            ExpectedConsumer(
                stream_name=stream_name,
                durable_name=durable_name,
                consumer_name=consumer_name,
                filter_subject=filter_subject,
                deliver_subject=deliver_subject,
                ack_policy=ack_policy,
                max_deliver=max_deliver if isinstance(max_deliver, int) else None,
                timing=timing,
            )
        )

    return ExpectedTopology(streams=streams, consumers=consumers)
