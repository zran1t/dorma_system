# common/nats_bootstrap.py
# [修改] 將時間解析改為「秒數 float」，符合 nats.js.api 需求

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from nats.aio.client import Client as NATS
from nats.js.api import (
    StreamConfig,
    ConsumerConfig,
    StorageType,
    RetentionPolicy,
    DiscardPolicy,
    AckPolicy,
)

# --------------------------
# 時間與列舉對應小工具
# --------------------------

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)?\s*$")

def _parse_duration_seconds(text: str | float | int, default_seconds: float) -> float:
    """
    將 YAML 內的 duration（如 '30s', '2m', '1500ms', 60）解析為『秒數（float）』。

    Args:
        text (str|float|int): 時間字串或數字；數字直接視為秒
        default_seconds (float): 解析失敗時使用的預設秒數

    Returns:
        float: 以秒為單位的浮點數
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
    if unit in ("ms", "msec", "millisecond", "milliseconds"):
        return val / 1000.0
    if unit in ("m", "min", "mins", "minute", "minutes"):
        return val * 60.0
    if unit in ("h", "hr", "hour", "hours"):
        return val * 3600.0

    # 未知單位時以秒處理
    return val


def _map_storage(v: str) -> StorageType:
    return StorageType.FILE if str(v).lower() == "file" else StorageType.MEMORY


def _map_retention(v: str) -> RetentionPolicy:
    s = str(v).lower()
    if s == "interest":
        return RetentionPolicy.INTEREST
    if s == "workqueue":
        return RetentionPolicy.WORKQUE
    return RetentionPolicy.LIMITS


def _map_discard(v: str) -> DiscardPolicy:
    return DiscardPolicy.OLD if str(v).lower() == "old" else DiscardPolicy.NEW


# --------------------------
# 主要入口
# --------------------------

async def bootstrap_nats_and_js(cfg: Dict[str, Any]) -> Tuple[NATS, Any]:
    """
    根據 channels YAML 解析後的 dict，完成「NATS 連線 + JetStream streams / consumers 的建立或更新」。
    重要修正：duplicate_window / ack_wait 一律以『秒數（float）』提供給 nats.js.api。
    """
    # 1) 連線
    nats_cfg = cfg.get("nats") or {}
    servers = nats_cfg.get("servers")
    if not isinstance(servers, list) or not servers:
        raise ValueError("nats.servers 必須為非空清單（list[str]）")
    print("【Bootstrap】連線到 NATS：", ", ".join(servers))

    nc = NATS()
    await nc.connect(servers=servers)
    js = nc.jetstream()

    # 2) Streams
    js_cfg = cfg.get("jetstream") or {}
    streams = js_cfg.get("streams") or []
    if not isinstance(streams, list):
        raise ValueError("jetstream.streams 必須為清單（list）")

    for s in streams:
        if not isinstance(s, dict):
            raise ValueError(f"stream 設定必須為物件（mapping）：{s}")

        name: str = s.get("name")
        subjects: List[str] = s.get("subjects") or []
        if not name or not subjects:
            raise ValueError(f"stream 缺少必要欄位 name/subjects：{s}")

        sc = StreamConfig(
            name=name,
            subjects=subjects,
            storage=_map_storage(s.get("storage", "file")),
            retention=_map_retention(s.get("retention", "limits")),
            discard=_map_discard(s.get("discard", "old")),
            # ★ 以秒（float）提供，符合 nats.js.api 期望
            duplicate_window=_parse_duration_seconds(s.get("duplicates", "30s"), 30.0),
        )

        try:
            await js.add_stream(sc)
            print(f"【Bootstrap】已建立 Stream：{name}")
        except Exception:
            # 已存在 → 嘗試更新（僅在有差異時）
            try:
                info = await js.stream_info(name)
                need_update = (
                    set(info.config.subjects or []) != set(sc.subjects or [])
                    or info.config.storage != sc.storage
                    or info.config.retention != sc.retention
                    or info.config.discard != sc.discard
                    or float(info.config.duplicate_window or 0) != float(sc.duplicate_window or 0)
                )
                if need_update:
                    await js.update_stream(sc)
                    print(f"【Bootstrap】已更新 Stream：{name}")
                else:
                    print(f"【Bootstrap】Stream 無需更新：{name}")
            except Exception as e:
                print(f"【Bootstrap】⚠️  讀取/更新 Stream 失敗：{name} → {e}")
                # 若要強斷，可 raise；此處沿用原行為不中斷其他資源

    # 3) Consumers
    consumers = js_cfg.get("consumers") or []
    if not isinstance(consumers, list):
        raise ValueError("jetstream.consumers 必須為清單（list）")

    for c in consumers:
        if not isinstance(c, dict):
            raise ValueError(f"consumer 設定必須為物件（mapping）：{c}")

        stream: str = c.get("stream")
        durable: str = c.get("durable_name")
        filter_subject: str = c.get("filter_subject")
        deliver_subject: str = c.get("deliver_subject")
        ack_policy_text: str = c.get("ack_policy", "explicit")
        ack_wait_text: str = c.get("ack_wait", "30s")

        if not all([stream, durable, filter_subject, deliver_subject]):
            raise ValueError(f"consumer 缺少必要欄位（stream/durable_name/filter_subject/deliver_subject）：{c}")

        ack_policy = AckPolicy.EXPLICIT if str(ack_policy_text).lower() == "explicit" else AckPolicy.NONE
        # ★ 以秒（float）提供 ack_wait
        ack_wait_seconds = _parse_duration_seconds(ack_wait_text, 30.0)

        cc = ConsumerConfig(
            durable_name=durable,
            filter_subject=filter_subject,
            deliver_subject=deliver_subject,
            ack_policy=ack_policy,
            ack_wait=ack_wait_seconds,
        )

        try:
            await js.add_consumer(stream=stream, config=cc)
            print(f"【Bootstrap】已建立 Consumer：{durable}@{stream}")
        except Exception:
            try:
                info = await js.consumer_info(stream, durable)
                need_update = (
                    info.config.filter_subject != cc.filter_subject
                    or info.config.deliver_subject != cc.deliver_subject
                    or info.config.ack_policy != cc.ack_policy
                    or float(info.config.ack_wait or 0) != float(cc.ack_wait or 0)
                )
                if need_update:
                    await js.update_consumer(stream=stream, config=cc)
                    print(f"【Bootstrap】已更新 Consumer：{durable}@{stream}")
                else:
                    print(f"【Bootstrap】Consumer 無需更新：{durable}@{stream}")
            except Exception as e:
                print(f"【Bootstrap】⚠️  讀取/更新 Consumer 失敗：{durable}@{stream} → {e}")

    return nc, js