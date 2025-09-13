# common/js_auditor.py
# JetStream 稽核器（只讀）：以 YAML dict 為期望，比對實際 JS 狀態，並可將報告落地成檔。
# - audit_channels_topology(cfg, nc): 核心稽核（全量）
# - scope_cfg_for_kol(cfg, kol_id):   只保留指定 kol 相關拓樸
# - scope_cfg_for_inter_dept(cfg, dept): 只保留指定 inter 部門拓樸
# - audit_and_dump(...): 一行式呼叫（可選擇 full/kol/inter），自動寫檔（支援時間戳）

from __future__ import annotations

import json
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Literal

from nats.aio.client import Client as NATS
from nats.js.api import AckPolicy, StorageType


# ========== 對外：單一入口（可裁切 + 落地） ====================================

async def audit_and_dump(
    cfg: Dict[str, Any],
    nc: NATS,
    *,
    mode: Literal["full", "kol", "inter"] = "full",
    kol_id: Optional[str] = None,
    dept: Optional[str] = None,
    outfile: str | Path = "reports/js_audit",   # ← 預設改成前綴，會自動加時間戳與 .json
) -> Dict[str, Any]:
    """
    一行式稽核並落地成檔。

    outfile 使用規則：
      - 若 outfile 含 .json 副檔名 → 直接寫入該檔名（覆蓋/重寫）
      - 若 outfile 無副檔名       → 自動加時間戳與 .json，例如：
          reports/inter_audit  →  reports/inter_audit_2025-09-09T07-15-00Z.json
    """
    eff_cfg = cfg
    scope_desc = "full"
    if mode == "kol":
        if not kol_id:
            raise ValueError("mode='kol' 需要提供 kol_id")
        eff_cfg = scope_cfg_for_kol(cfg, kol_id=kol_id)
        scope_desc = f"kol:{kol_id}"
    elif mode == "inter":
        if not dept:
            raise ValueError("mode='inter' 需要提供 dept")
        eff_cfg = scope_cfg_for_inter_dept(cfg, dept=dept)
        scope_desc = f"inter:{dept}"

    print(f"→ 稽核（scope: {scope_desc}）")
    report = await audit_channels_topology(eff_cfg, nc)

    # 解析 outfile：若無 .json 副檔名則自動加時間戳與 .json
    outpath = _resolve_outfile_path(outfile)

    _dump_report(report, outpath)
    print(f"🗂️  稽核報告已輸出：{outpath}")
    return report


def _resolve_outfile_path(outfile: str | Path) -> Path:
    """
    將 outfile 轉為絕對路徑。
    - 若無副檔名：追加 _<UTC時間戳>.json
    - 若有 .json：直接使用
    """
    p = Path(outfile)
    if not p.is_absolute():
        p = Path.cwd() / p

    if p.suffix.lower() == ".json":
        return p

    # 無副檔名：自動加時間戳與 .json
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")  # 避免 ':' 影響檔名
    return p.with_name(f"{p.name}_{ts}.json")


def _dump_report(report: Dict[str, Any], outfile: str | Path) -> None:
    """將報告寫入 outfile（確保資料夾存在；失敗時印出原因）。"""
    p = Path(outfile)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"❌ 無法寫入稽核報告：{p} | 原因：{repr(e)}")
        raise


# ========== 核心：全量稽核（只讀） ============================================

async def audit_channels_topology(cfg: Dict[str, Any], nc: NATS) -> Dict[str, Any]:
    """
    稽核 JetStream 拓樸（只讀，無副作用）。

    比對項目（最小集合）：
      * Streams：name、subjects（集合相等）、storage（file/memory）
      * Consumers：stream、durable_name、filter_subject、deliver_subject、
                   ack_policy（explicit/none/all）、ack_wait（允許 ±1s 誤差）
    """
    if not isinstance(cfg, dict):
        raise ValueError("cfg 必須為 dict")
    if not isinstance(nc, NATS):
        raise ValueError("nc 必須為已連線的 NATS 物件")

    js = nc.jetstream()
    report: Dict[str, Any] = {
        "trace_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ok": True,
        "missing": {"streams": [], "consumers": []},
        "mismatched": {"streams": {}, "consumers": {}},
        "extra": {"streams": [], "consumers": []},  # 保留欄位（不同版本/權限可能無法列舉）
    }

    js_cfg = cfg.get("jetstream") or {}
    streams_cfg: List[Dict[str, Any]] = js_cfg.get("streams", [])
    consumers_cfg: List[Dict[str, Any]] = js_cfg.get("consumers", [])

    # ---- Streams ---------------------------------------------------------
    for s in streams_cfg:
        name = s.get("name")
        subjects_expected = list(s.get("subjects") or [])
        storage_expected = (s.get("storage") or "file").strip().lower()

        if not name or not subjects_expected:
            print(f"⚠️  [JS 稽核] stream 設定缺少必要欄位（name/subjects）：{s}")
            report["ok"] = False
            continue

        try:
            info = await js.stream_info(name)
        except Exception:
            print(f"❌ [JS 稽核] 缺少 Stream：{name}")
            report["missing"]["streams"].append(name)
            report["ok"] = False
            continue

        actual_subjects = sorted(list(info.config.subjects or []))
        expected_subjects = sorted(subjects_expected)
        if actual_subjects != expected_subjects:
            report["mismatched"]["streams"].setdefault(name, {})
            report["mismatched"]["streams"][name]["subjects"] = {
                "expected": expected_subjects,
                "actual": actual_subjects,
            }
            print(f"❌ [JS 稽核] Stream.subjects 不一致：{name}")
            print(f"   - 預期：{expected_subjects}")
            print(f"   - 實際：{actual_subjects}")
            report["ok"] = False

        actual_storage = _storage_to_str(info.config.storage)
        if actual_storage != storage_expected:
            report["mismatched"]["streams"].setdefault(name, {})
            report["mismatched"]["streams"][name]["storage"] = {
                "expected": storage_expected,
                "actual": actual_storage,
            }
            print(f"❌ [JS 稽核] Stream.storage 不一致：{name}（預期 {storage_expected}，實際 {actual_storage}）")
            report["ok"] = False

    # ---- Consumers -------------------------------------------------------
    for c in consumers_cfg:
        stream = c.get("stream")
        durable = c.get("durable_name")
        filter_subject_exp = c.get("filter_subject")
        deliver_subject_exp = c.get("deliver_subject")
        ack_policy_exp = (c.get("ack_policy") or "explicit").strip().lower()
        ack_wait_exp_raw = c.get("ack_wait", "30s")

        if not stream or not durable or not filter_subject_exp or not deliver_subject_exp:
            print(f"⚠️  [JS 稽核] consumer 設定缺少必要欄位（stream/durable/filter/deliver）：{c}")
            report["ok"] = False
            continue

        key = f"{durable}@{stream}"

        try:
            await js.stream_info(stream)
        except Exception:
            print(f"❌ [JS 稽核] 其所屬 Stream 不存在，無法檢查 Consumer：{key}")
            report["missing"]["consumers"].append(key)
            report["ok"] = False
            continue

        try:
            ci = await js.consumer_info(stream, durable)
        except Exception:
            print(f"❌ [JS 稽核] 缺少 Consumer：{key}")
            report["missing"]["consumers"].append(key)
            report["ok"] = False
            continue

        _cmp_consumer_field(report, key, "filter_subject",
                            expected=filter_subject_exp, actual=ci.config.filter_subject)
        _cmp_consumer_field(report, key, "deliver_subject",
                            expected=deliver_subject_exp, actual=ci.config.deliver_subject)
        _cmp_consumer_field(report, key, "ack_policy",
                            expected=ack_policy_exp, actual=_ack_policy_to_str(ci.config.ack_policy))

        if not _ack_wait_equal(ack_wait_exp_raw, ci.config.ack_wait, tolerance=timedelta(seconds=1)):
            report["mismatched"]["consumers"].setdefault(key, {})
            report["mismatched"]["consumers"][key]["ack_wait"] = {
                "expected": str(ack_wait_exp_raw),
                "actual": str(ci.config.ack_wait),
            }
            print(f"❌ [JS 稽核] Consumer.ack_wait 不一致：{key}（預期 {ack_wait_exp_raw}，實際 {ci.config.ack_wait}）")
            report["ok"] = False

    # ---- Summary ---------------------------------------------------------
    if report["ok"]:
        print(f"✅ [JS 稽核完成] 設定與實際狀態一致 | trace_id={report['trace_id']}")
    else:
        print(f"⚠️  [JS 稽核完成] 發現差異，請檢視報告 | trace_id={report['trace_id']}")

    return report


# ========== 便捷裁切（scoped audit） =========================================

def scope_cfg_for_kol(cfg: Dict[str, Any], *, kol_id: str) -> Dict[str, Any]:
    """
    依 initialization.yaml 模板，裁切出「只與指定 kol 有關」的 consumers/streams。
    保留規則：
      - Consumer：filter_subject ∈ {to_kol, from_kol} 或 deliver_subject == to_kol_deliver
      - Stream：凡被保留之 consumer 所屬 stream 一律保留
    """
    subjects = cfg.get("subjects", {})
    to_kol = (subjects.get("to_kol_tpl") or "").format(kol=kol_id)
    to_kol_deliver = (subjects.get("to_kol_deliver") or "").format(kol=kol_id)
    from_kol = (subjects.get("from_kol_tpl") or "").format(kol=kol_id)

    js_cfg = cfg.get("jetstream", {})
    consumers = js_cfg.get("consumers", [])
    streams = js_cfg.get("streams", [])

    keep_consumers: List[Dict[str, Any]] = []
    keep_stream_names: set[str] = set()

    for c in consumers:
        fs = c.get("filter_subject")
        ds = c.get("deliver_subject")
        if fs in (to_kol, from_kol) or ds == to_kol_deliver:
            keep_consumers.append(c)
            if c.get("stream"):
                keep_stream_names.add(c["stream"])

    keep_streams = [s for s in streams if s.get("name") in keep_stream_names]

    return {
        **cfg,
        "jetstream": {
            "streams": keep_streams,
            "consumers": keep_consumers,
        },
    }


def scope_cfg_for_inter_dept(cfg: Dict[str, Any], *, dept: str) -> Dict[str, Any]:
    """
    依 inter.yaml 模板，裁切出「只與指定部門有關」的 consumers/streams。
    保留規則：
      - Consumer：filter_subject == to_department_tpl(dept) 且 deliver_subject == to_department_deliver(dept)
      - Stream：凡被保留之 consumer 所屬 stream 一律保留
    """
    subjects = cfg.get("subjects", {})
    to_dept = (subjects.get("to_department_tpl") or "").format(dept=dept)
    to_dept_deliver = (subjects.get("to_department_deliver") or "").format(dept=dept)

    js_cfg = cfg.get("jetstream", {})
    consumers = js_cfg.get("consumers", [])
    streams = js_cfg.get("streams", [])

    keep_consumers: List[Dict[str, Any]] = []
    keep_stream_names: set[str] = set()

    for c in consumers:
        if c.get("filter_subject") == to_dept and c.get("deliver_subject") == to_dept_deliver:
            keep_consumers.append(c)
            if c.get("stream"):
                keep_stream_names.add(c["stream"])

    keep_streams = [s for s in streams if s.get("name") in keep_stream_names]

    return {
        **cfg,
        "jetstream": {
            "streams": keep_streams,
            "consumers": keep_consumers,
        },
    }


# ========== 內部工具：轉換/比較 =============================================

def _storage_to_str(storage: StorageType) -> str:
    if storage == StorageType.FILE:
        return "file"
    if storage == StorageType.MEMORY:
        return "memory"
    return str(storage).lower()


def _ack_policy_to_str(policy: AckPolicy) -> str:
    if policy == AckPolicy.EXPLICIT:
        return "explicit"
    if policy == AckPolicy.NONE:
        return "none"
    if policy == AckPolicy.ALL:
        return "all"
    return str(policy).lower()


def _ack_wait_equal(expected: Any, actual: Any, tolerance: timedelta) -> bool:
    exp_td = _to_timedelta(expected)
    act_td = _to_timedelta(actual)
    return abs(exp_td - act_td) <= tolerance


def _to_timedelta(value: Any) -> timedelta:
    if isinstance(value, timedelta):
        return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    if isinstance(value, str):
        txt = value.strip().lower()
        try:
            if txt.endswith("ms"):
                return timedelta(milliseconds=float(txt[:-2]))
            if txt.endswith("s"):
                return timedelta(seconds=float(txt[:-1] or 0))
            if txt.endswith("m"):
                return timedelta(minutes=float(txt[:-1] or 0))
            if txt.endswith("h"):
                return timedelta(hours=float(txt[:-1] or 0))
            if txt.endswith("d"):
                return timedelta(days=float(txt[:-1] or 0))
            return timedelta(seconds=float(txt))
        except Exception:
            return timedelta(seconds=30)
    return timedelta(seconds=30)


def _cmp_consumer_field(report: Dict[str, Any], key: str, field: str, expected: Any, actual: Any) -> None:
    if expected == actual:
        return
    report["mismatched"]["consumers"].setdefault(key, {})
    report["mismatched"]["consumers"][key][field] = {"expected": expected, "actual": actual}
    print(f"❌ [JS 稽核] Consumer.{field} 不一致：{key}（預期 {expected}，實際 {actual}）")
    report["ok"] = False