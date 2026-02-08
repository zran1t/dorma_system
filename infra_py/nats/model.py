"""
File: infra_py/nats/model.py
Module: infra_py.nats.model

職責 (Responsibility):
    定義 JetStream topology 治理所需的資料模型（Expected/Actual/Diff 基礎結構）。
    將 consumer timing contract（ack_wait/backoff 互斥與推導）集中在單一入口，避免多處推導造成 drift。

注意事項 (Notes):
    - 本模組不做 I/O（不讀 YAML、不查 JetStream），只提供資料結構與純函式 resolver。
    - consumer timing contract：
        * backoff 存在且非空 → mode=BACKOFF，ack_wait_seconds=backoff[0]
        * backoff 不存在 → mode=ACK_WAIT，ack_wait_seconds=ack_wait（缺省 30s）
    - 若 YAML 同時提供 ack_wait 與 backoff，應在 config_loader fail-fast，不允許走到此層。
    - 不得依賴 system_initializer 或 application/domain 層模組。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

# === 第三方套件 (Third-Party Libraries) ===
# 無

# === 系統內模組 (Internal Modules) ===
from infra_py.nats.duration import parse_duration_seconds


class ConsumerTimingMode(Enum):
    """
    ConsumerTimingMode Consumer timing 模式列舉。

    功能:
        - 表達 consumer 的 timing 設計策略：使用 ack_wait 或使用 backoff。

    契約 / 限制:
        - BACKOFF 模式下 ack_wait_seconds 必須等於 backoff_seconds[0]。
        - ACK_WAIT 模式下 backoff_seconds 必須為 None。

    備註:
        - 互斥檢查在 config_loader 先行 fail-fast；此層只做 contract 收斂。
    """

    ACK_WAIT = "ack_wait"  # 使用 ack_wait 作為 redelivery gating
    BACKOFF = "backoff"    # 使用 backoff 序列控制 redelivery 節奏


@dataclass(frozen=True)
class ConsumerTiming:
    """
    ConsumerTiming Consumer 時間行為設定。

    功能:
        - 將 YAML 的 ack_wait/backoff 轉成治理層使用的統一模型。
        - 提供 bootstrap/audit 的共同輸入，避免各自推導。

    欄位說明:
        - mode: timing 模式（ACK_WAIT/BACKOFF）。
        - ack_wait_seconds: 最終有效的 ack_wait 秒數（JetStream 實際生效值）。
        - backoff_seconds: backoff 序列秒數；ACK_WAIT 模式為 None。

    契約 / 限制:
        - mode=BACKOFF 時 backoff_seconds 必須非空且 ack_wait_seconds==backoff_seconds[0]。
        - mode=ACK_WAIT 時 backoff_seconds 必須為 None。

    備註:
        - ack_wait_seconds 一律是 seconds(float)，禁止在此層做 ns 轉換。
    """

    mode: ConsumerTimingMode
    ack_wait_seconds: float
    backoff_seconds: Optional[List[float]]


@dataclass(frozen=True)
class ExpectedStream:
    """
    ExpectedStream 期望 Stream 狀態模型。

    功能:
        - 表達 YAML 宣告的 stream 設定（治理層期望狀態）。

    欄位說明:
        - stream_name: stream 名稱。
        - subjects: subject 清單。
        - storage: "file" 或 "memory"（以 YAML 字串表示）。
        - retention: "limits"|"interest"|"workqueue"（以 YAML 字串表示）。
        - discard: "old"|"new"（以 YAML 字串表示）。
        - duplicate_window_seconds: duplicates window 秒數。
        - max_age_seconds: max_age 秒數（None 表示不要求收斂）。

    契約 / 限制:
        - subjects 必須非空。
        - max_age_seconds 為 None 表示不檢查、不覆蓋 server default。

    備註:
        - storage/retention/discard 的 enum 映射由 bootstrap 實作層負責。
    """

    stream_name: str
    subjects: List[str]
    storage: str
    retention: str
    discard: str
    duplicate_window_seconds: float
    max_age_seconds: Optional[float]


@dataclass(frozen=True)
class ExpectedConsumer:
    """
    ExpectedConsumer 期望 Consumer 狀態模型。

    功能:
        - 表達 YAML 宣告的 consumer 設定（治理層期望狀態）。

    欄位說明:
        - stream_name: 所屬 stream。
        - durable_name: durable 名稱（治理身份識別）。
        - consumer_name: human-readable 名稱（僅供檔案辨識與 log，不作 identity）。
        - filter_subject: filter subject。
        - deliver_subject: deliver subject。
        - ack_policy: "explicit"|"none"|"all"。
        - max_deliver: 最大投遞次數（None 表示不設定）。
        - timing: timing contract（ack_wait/backoff 統一模型）。

    契約 / 限制:
        - identity key 為 "{durable_name}@{stream_name}"。
        - timing contract 由 resolve_consumer_timing 統一產出。

    備註:
        - deliver_subject 不支援萬用字元屬於上層規範；此層不做語意驗證。
    """

    stream_name: str
    durable_name: str
    consumer_name: str
    filter_subject: str
    deliver_subject: str
    ack_policy: str
    max_deliver: Optional[int]
    timing: ConsumerTiming

    @property
    def key(self) -> str:
        """
        key 回傳 consumer identity key。

        功能:
            - 提供唯一識別鍵供 diff/audit/bootstrap 使用。

        參數:
            - param1: 無。
            - param2: 無。

        回傳:
            - result: "{durable}@{stream}" 字串。
            - error: 無。

        備註:
            - durable_name 與 stream_name 皆需為已驗證之非空字串。
        """
        return f"{self.durable_name}@{self.stream_name}"


@dataclass(frozen=True)
class ExpectedTopology:
    """
    ExpectedTopology 期望拓樸集合。

    功能:
        - 聚合 stream/consumer 的期望狀態，作為治理流程的唯一輸入。

    欄位說明:
        - streams: 期望 streams。
        - consumers: 期望 consumers。

    契約 / 限制:
        - 本集合是 expected-driven；治理只處理此集合內的資源。
        - 不列舉全量 JetStream inventory，避免權限與成本耦合。

    備註:
        - reset/bootstrap/audit 都應由此模型驅動。
    """

    streams: List[ExpectedStream]
    consumers: List[ExpectedConsumer]


@dataclass(frozen=True)
class ActualStream:
    """
    ActualStream 實際 Stream 狀態模型。

    功能:
        - 表達 JetStream stream_info 回讀的實際狀態（治理層觀測）。

    欄位說明:
        - stream_name: stream 名稱。
        - subjects: subjects。
        - storage: "file"|"memory"。
        - retention: "limits"|"interest"|"workqueue"。
        - discard: "old"|"new"。
        - duplicate_window_seconds: duplicates window 秒數。
        - max_age_seconds: max_age 秒數（0/None 視 server 回讀語意）。

    契約 / 限制:
        - 欄位皆為已解碼之治理層表示，不含 JetStream 原生型別。

    備註:
        - 由 actual_loader 統一解碼。
    """

    stream_name: str
    subjects: List[str]
    storage: str
    retention: str
    discard: str
    duplicate_window_seconds: float
    max_age_seconds: Optional[float]


@dataclass(frozen=True)
class ActualConsumer:
    """
    ActualConsumer 實際 Consumer 狀態模型。

    功能:
        - 表達 JetStream consumer_info 回讀的實際狀態（治理層觀測）。

    欄位說明:
        - stream_name: 所屬 stream。
        - durable_name: durable 名稱。
        - filter_subject: filter subject。
        - deliver_subject: deliver subject。
        - ack_policy: "explicit"|"none"|"all"。
        - max_deliver: 最大投遞次數（None 表示 server 未設）。
        - ack_wait_seconds: ack_wait 的實際秒數（已解碼）。
        - backoff_seconds: backoff 的實際序列秒數（若 server 回傳）。

    契約 / 限制:
        - identity key 為 "{durable_name}@{stream_name}"。

    備註:
        - ack_wait_seconds 的解碼必須在 actual_loader 完成，避免版本差異造成誤判。
    """

    stream_name: str
    durable_name: str
    filter_subject: str
    deliver_subject: str
    ack_policy: str
    max_deliver: Optional[int]
    ack_wait_seconds: float
    backoff_seconds: Optional[List[float]]

    @property
    def key(self) -> str:
        """
        key 回傳 consumer identity key。

        功能:
            - 提供唯一識別鍵供 diff/audit/bootstrap 使用。

        參數:
            - param1: 無。
            - param2: 無。

        回傳:
            - result: "{durable}@{stream}" 字串。
            - error: 無。

        備註:
            - durable_name 與 stream_name 皆需為已解碼之非空字串。
        """
        return f"{self.durable_name}@{self.stream_name}"


@dataclass(frozen=True)
class ActualTopology:
    """
    ActualTopology 實際拓樸集合。

    功能:
        - 聚合 stream/consumer 的實際狀態，作為 diff 的右值輸入。

    欄位說明:
        - streams: 觀測到的 streams（expected-driven 範圍）。
        - consumers: 觀測到的 consumers（expected-driven 範圍）。

    契約 / 限制:
        - 只包含 expected 要求的資源；不負責列舉全量。

    備註:
        - 由 actual_loader 建構。
    """

    streams: List[ActualStream]
    consumers: List[ActualConsumer]


def resolve_consumer_timing(consumer_cfg: Dict[str, Any]) -> ConsumerTiming:
    """
    resolve_consumer_timing 決定 consumer timing contract 並收斂成統一模型。

    功能:
        - 將 YAML consumer dict 的 ack_wait/backoff 解析為 ConsumerTiming。
        - 使 bootstrap/audit 只依賴 timing 模型，不再做二次推導。

    參數:
        - consumer_cfg: YAML consumer dict（已通過 config_loader 最小驗證）。

    回傳:
        - result: ConsumerTiming。
        - error: ValueError（當 backoff 為空或解析出非正秒數等不合理狀態）。

    備註:
        - ack_wait/backoff 的互斥保證由 config_loader 負責；此處仍做最小防衛式檢查以避免上層繞過。
    """
    backoff_raw = consumer_cfg.get("backoff", None)
    ack_wait_raw = consumer_cfg.get("ack_wait", None)

    if backoff_raw is not None:
        if ack_wait_raw is not None:
            raise ValueError("invalid consumer timing: ack_wait and backoff are mutually exclusive")

        if not isinstance(backoff_raw, list) or not backoff_raw:
            raise ValueError("invalid consumer timing: backoff must be non-empty list")

        backoff_seconds = [parse_duration_seconds(v, 0.0) for v in backoff_raw]
        backoff_seconds = [v for v in backoff_seconds if v > 0.0]
        if not backoff_seconds:
            raise ValueError("invalid consumer timing: backoff must contain positive durations")

        return ConsumerTiming(
            mode=ConsumerTimingMode.BACKOFF,
            ack_wait_seconds=float(backoff_seconds[0]),
            backoff_seconds=backoff_seconds,
        )

    ack_wait_seconds = parse_duration_seconds(ack_wait_raw if ack_wait_raw is not None else "30s", 30.0)
    if ack_wait_seconds <= 0.0:
        raise ValueError("invalid consumer timing: ack_wait must be positive")

    return ConsumerTiming(
        mode=ConsumerTimingMode.ACK_WAIT,
        ack_wait_seconds=float(ack_wait_seconds),
        backoff_seconds=None,
    )
