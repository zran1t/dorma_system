# common/js_reset.py
# [精簡版] JetStream 清理工具：僅依 YAML dict 清除對應的 Consumers → Streams

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from nats.aio.client import Client as NATS


async def reset_channels_topology(cfg: Dict[str, Any], nc: NATS) -> Dict[str, Any]:
    """
    依據 channels 類 YAML 設定（dict），清理 JetStream 拓樸。
    行為（固定且唯一）：
      1) 先刪除 YAML 列出的 Consumers（避免 deliver 中的訂閱卡住）
      2) 再刪除 YAML 列出的 Streams

    Args:
        cfg (Dict[str, Any]): 由 channel_config_loader 載入的 YAML mirror dict
        nc (NATS): 已連線的 NATS client

    Returns:
        Dict[str, Any]: 操作報告
            {
              "trace_id": "<uuid>",
              "timestamp": "<RFC3339>",
              "dropped": {"streams": [...], "consumers": [...]},
              "skipped": {"streams": [...], "consumers": []},
              "errors":  [{"target": "...", "error": "..."}, ...]
            }

    Raises:
        ValueError: 當輸入參數或 cfg 結構非法
    """
    if not isinstance(cfg, dict):
        raise ValueError("cfg 必須為 dict")
    if not isinstance(nc, NATS):
        raise ValueError("nc 必須為已連線的 NATS 物件")

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

    # 1) 先刪 Consumers
    for c in consumers_cfg:
        stream = c.get("stream")
        durable = c.get("durable_name")
        if not stream or not durable:
            print(f"⚠️  [Reset] consumer 設定缺少必要欄位（stream/durable）：{c}")
            report["skipped"]["consumers"].append(str(c))
            continue

        target = f"{durable}@{stream}"
        # 確認存在
        try:
            await js.stream_info(stream)
            await js.consumer_info(stream, durable)  # 不存在會丟例外
        except Exception:
            print(f"🛈 [Reset] Consumer 不存在（略過）：{target}")
            report["skipped"]["consumers"].append(target)
            continue

        try:
            await js.delete_consumer(stream, durable)
            print(f"🗑️  [Reset] 已刪除 Consumer：{target}")
            report["dropped"]["consumers"].append(target)
        except Exception as e:
            print(f"❌ [Reset] 刪除 Consumer 失敗：{target} → {e}")
            report["errors"].append({"target": target, "error": repr(e)})

    # 2) 再刪 Streams
    for s in streams_cfg:
        name = s.get("name")
        if not name:
            print(f"⚠️  [Reset] stream 設定缺少 name：{s}")
            report["skipped"]["streams"].append(str(s))
            continue

        try:
            await js.stream_info(name)  # 不存在會丟例外
        except Exception:
            print(f"🛈 [Reset] Stream 不存在（略過）：{name}")
            report["skipped"]["streams"].append(name)
            continue

        try:
            await js.delete_stream(name)
            print(f"🗑️  [Reset] 已刪除 Stream：{name}")
            report["dropped"]["streams"].append(name)
        except Exception as e:
            print(f"❌ [Reset] 刪除 Stream 失敗：{name} → {e}")
            report["errors"].append({"target": name, "error": repr(e)})

    # 總結
    if report["errors"]:
        print(f"⚠️  [Reset 完成] 有錯誤項目，請檢視 report['errors'] | trace_id={report['trace_id']}")
    else:
        print(f"✅ [Reset 完成] 已清理 | trace_id={report['trace_id']}")

    return report