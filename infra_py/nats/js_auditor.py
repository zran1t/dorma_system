"""
File: infra_py/nats/js_auditor.py
Module: infra_py.nats.js_auditor

職責 (Responsibility):
    提供 JetStream 拓樸之唯讀稽核能力，
    以 YAML 解析後之 dict 作為期望狀態，比對實際 JetStream streams 與 consumers。
    本模組同時支援稽核結果之報告落地（JSON）。

注意事項 (Notes):
    - 本模組為純讀取行為，不得對 JetStream 狀態造成任何修改。
    - 呼叫端需保證 NATS client 已完成連線，且具備查詢 streams/consumers 之權限。
    - 不得依賴 system initializer 或 application 層模組；僅允許依賴 infra_py 內部共用模組。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import json
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

# === 第三方套件 (Third-Party Libraries) ===
from nats.aio.client import Client as NATS
from nats.js.api import AckPolicy, StorageType

# === 系統內模組 (Internal Modules) ===
from infra_py.logging.logger import get_logger


logger = get_logger(__name__, subdir="jetstream/audit")


async def audit_and_dump(
    cfg: Dict[str, Any],
    nc: NATS,
    *,
    outfile: str | Path = "logs/jetstream/audit/js_audit",
) -> Dict[str, Any]:
    """
    audit_and_dump 執行 JetStream 稽核並將結果輸出為檔案。

    功能:
        - 驗證期望拓樸設定與實際 JetStream 狀態之差異。
        - 產出結構化稽核報告並落地為 JSON 檔案。

    參數:
        - cfg: YAML 解析後之期望拓樸 dict（結構需包含 jetstream.streams / jetstream.consumers）。
        - nc: 已連線之 NATS client（需可取得 JetStream context 並執行查詢）。
        - outfile: 輸出檔案路徑或前綴；若無 .json 副檔名則自動附加 UTC 時間戳與 .json。

    回傳:
        - result: 稽核結果 dict（包含 trace_id / ok / missing / mismatched 等欄位）。
        - error: 無。

    備註:
        - 本函式不進行任何 JetStream 寫入行為。
        - 輸出路徑預設落在 logs/jetstream/audit/ 以便與 runtime log 分層。
    """
    # 執行稽核前置條件檢查，以避免將型別錯誤誤判為拓樸差異。
    report = await audit_channels_topology(cfg, nc)

    # 將輸出路徑收斂至可落地之最終檔名，以確保報告具備可追溯性與可比對性。
    outpath = _resolve_outfile_path(outfile)

    _dump_report(report, outpath)

    logger.info("audit report written | path=%s | trace_id=%s | ok=%s", outpath, report.get("trace_id"), report.get("ok"))
    return report


def _resolve_outfile_path(outfile: str | Path) -> Path:
    """
    _resolve_outfile_path 正規化稽核輸出檔案路徑。

    功能:
        - 將相對路徑轉換為絕對路徑。
        - 若未指定 .json 副檔名，自動附加 UTC 時間戳與 .json。

    參數:
        - outfile: 使用者提供之輸出路徑或前綴。

    回傳:
        - result: 最終輸出檔案 Path。
        - error: 無。

    備註:
        - 使用 UTC 時間戳避免跨時區造成排序與比對困難。
        - 以檔名層級附加時間戳，避免覆蓋歷史報告。
    """
    p = Path(outfile)
    if not p.is_absolute():
        p = Path.cwd() / p

    if p.suffix.lower() == ".json":
        return p

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    return p.with_name(f"{p.name}_{ts}.json")


def _dump_report(report: Dict[str, Any], outfile: str | Path) -> None:
    """
    _dump_report 將稽核結果寫入檔案。

    功能:
        - 確保輸出目錄存在。
        - 將報告內容以 JSON 格式序列化並寫入檔案。

    參數:
        - report: 稽核結果 dict。
        - outfile: 輸出檔案路徑。

    回傳:
        - result: 無。
        - error: I/O 失敗時拋出例外。

    備註:
        - 不進行錯誤吞噬；由呼叫端決定是否重試或降級處理。
    """
    p = Path(outfile)

    # 建立輸出目錄以確保報告落地具備原子性與可預期性。
    p.parent.mkdir(parents=True, exist_ok=True)

    # 以 UTF-8 與 pretty print 便於人工檢視與版本化比對。
    with p.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)


async def audit_channels_topology(cfg: Dict[str, Any], nc: NATS) -> Dict[str, Any]:
    """
    audit_channels_topology 執行 JetStream 拓樸一致性稽核。

    功能:
        - 驗證 streams 與 consumers 是否與期望設定一致。
        - 收集缺失（missing）與不一致（mismatched）項目，並輸出結構化報告。

    參數:
        - cfg: 期望 JetStream 拓樸設定 dict。
        - nc: 已連線之 NATS client。

    回傳:
        - result: 稽核報告 dict（ok 為布林彙總結果）。
        - error: 無。

    備註:
        - 僅檢查控制面最小必要集合（subjects/storage/ack_policy/ack_wait 等），避免與 JetStream 版本差異耦合。
        - ack_wait 允許在容許誤差內視為一致，以降低不同 runtime 表示法造成的誤判。
    """
    if not isinstance(cfg, dict):
        raise ValueError("cfg must be dict")
    if not isinstance(nc, NATS):
        raise ValueError("nc must be NATS client")

    js = nc.jetstream()

    report: Dict[str, Any] = {
        "trace_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ok": True,
        "missing": {"streams": [], "consumers": []},
        "mismatched": {"streams": {}, "consumers": {}},
        "extra": {"streams": [], "consumers": []},
    }

    js_cfg = cfg.get("jetstream") or {}
    streams_cfg: List[Dict[str, Any]] = js_cfg.get("streams", [])
    consumers_cfg: List[Dict[str, Any]] = js_cfg.get("consumers", [])

    # 以期望設定為權威基準進行對照，避免因列舉權限不足而誤判 extra。
    for s in streams_cfg:
        name = s.get("name")
        subjects_expected = list(s.get("subjects") or [])
        storage_expected = (s.get("storage") or "file").strip().lower()

        if not name or not subjects_expected:
            logger.warning("invalid stream config | %s", s)
            report["ok"] = False
            continue

        try:
            info = await js.stream_info(name)
        except Exception:
            logger.error("missing stream | %s", name)
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
            report["ok"] = False

        actual_storage = _storage_to_str(info.config.storage)
        if actual_storage != storage_expected:
            report["mismatched"]["streams"].setdefault(name, {})
            report["mismatched"]["streams"][name]["storage"] = {
                "expected": storage_expected,
                "actual": actual_storage,
            }
            report["ok"] = False

    for c in consumers_cfg:
        stream = c.get("stream")
        durable = c.get("durable_name")
        filter_subject_exp = c.get("filter_subject")
        deliver_subject_exp = c.get("deliver_subject")
        ack_policy_exp = (c.get("ack_policy") or "explicit").strip().lower()
        ack_wait_exp = c.get("ack_wait", "30s")

        if not all([stream, durable, filter_subject_exp, deliver_subject_exp]):
            logger.warning("invalid consumer config | %s", c)
            report["ok"] = False
            continue

        key = f"{durable}@{stream}"

        try:
            ci = await js.consumer_info(stream, durable)
        except Exception:
            logger.error("missing consumer | %s", key)
            report["missing"]["consumers"].append(key)
            report["ok"] = False
            continue

        _cmp_consumer_field(report, key, "filter_subject", expected=filter_subject_exp, actual=ci.config.filter_subject)
        _cmp_consumer_field(report, key, "deliver_subject", expected=deliver_subject_exp, actual=ci.config.deliver_subject)
        _cmp_consumer_field(report, key, "ack_policy", expected=ack_policy_exp, actual=_ack_policy_to_str(ci.config.ack_policy))

        if not _ack_wait_equal(ack_wait_exp, ci.config.ack_wait, tolerance=timedelta(seconds=1)):
            report["mismatched"]["consumers"].setdefault(key, {})
            report["mismatched"]["consumers"][key]["ack_wait"] = {
                "expected": str(ack_wait_exp),
                "actual": str(ci.config.ack_wait),
            }
            report["ok"] = False

    return report


def _storage_to_str(storage: StorageType) -> str:
    """
    _storage_to_str 將 StorageType 轉換為可比對字串。

    功能:
        - 將 JetStream StorageType 映射為 YAML 使用之字串表示。

    參數:
        - storage: JetStream StorageType。

    回傳:
        - result: "file" / "memory" / fallback 字串。
        - error: 無。

    備註:
        - fallback 僅用於相容未知 enum 值之情境。
    """
    if storage == StorageType.FILE:
        return "file"
    if storage == StorageType.MEMORY:
        return "memory"
    return str(storage).lower()


def _ack_policy_to_str(policy: AckPolicy) -> str:
    """
    _ack_policy_to_str 將 AckPolicy 轉換為可比對字串。

    功能:
        - 將 JetStream AckPolicy 映射為 YAML 使用之字串表示。

    參數:
        - policy: JetStream AckPolicy。

    回傳:
        - result: "explicit" / "none" / "all" / fallback 字串。
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


def _ack_wait_equal(expected: Any, actual: Any, tolerance: timedelta) -> bool:
    """
    _ack_wait_equal 判斷 ack_wait 是否在容許誤差範圍內。

    功能:
        - 將 expected/actual 轉換為 timedelta 後，以容許誤差判斷一致性。

    參數:
        - expected: 期望值（支援 str / int / float / timedelta）。
        - actual: 實際值（支援 JetStream 回傳型別或可轉換表示）。
        - tolerance: 容許誤差。

    回傳:
        - result: 一致性判斷結果。
        - error: 無。

    備註:
        - 容許誤差用於降低不同 runtime 內部表現造成的誤判。
    """
    exp_td = _to_timedelta(expected)
    act_td = _to_timedelta(actual)
    return abs(exp_td - act_td) <= tolerance


def _to_timedelta(value: Any) -> timedelta:
    """
    _to_timedelta 將多型時間表示轉換為 timedelta。

    功能:
        - 支援 timedelta / int / float（秒） / 字串（ms/s/m/h/d）。
        - 解析失敗時回退至 30 秒，以避免稽核流程中斷。

    參數:
        - value: 多型時間表示。

    回傳:
        - result: timedelta。
        - error: 無。

    備註:
        - 回退值僅用於容錯；若需嚴格模式，應由上層 validator 先行保證格式正確。
    """
    if isinstance(value, timedelta):
        return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    if isinstance(value, str):
        txt = value.strip().lower()
        try:
            if txt.endswith("ms"):
                return timedelta(milliseconds=float(txt[:-2] or 0))
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
    """
    _cmp_consumer_field 比對單一 consumer 欄位並記錄差異。

    功能:
        - 將差異寫入 report["mismatched"]["consumers"] 並更新 report["ok"]。

    參數:
        - report: 稽核報告 dict。
        - key: consumer 識別鍵（durable@stream）。
        - field: 欄位名稱。
        - expected: 期望值。
        - actual: 實際值。

    回傳:
        - result: 無。
        - error: 無。

    備註:
        - 僅記錄差異，不負責 logger 輸出，以降低噪音並維持報告為權威來源。
    """
    if expected == actual:
        return

    report["mismatched"]["consumers"].setdefault(key, {})
    report["mismatched"]["consumers"][key][field] = {"expected": expected, "actual": actual}
    report["ok"] = False
