"""
File: infra_py/nats/js_auditor.py
Module: infra_py.nats.js_auditor

職責 (Responsibility):
    提供 JetStream 拓樸之唯讀稽核能力，以 expected-driven 方式比對 YAML 與實際狀態。
    將稽核結果輸出為結構化報告（dict/JSON），供 CI/治理工具使用。

注意事項 (Notes):
    - 本模組為純讀取行為，不得對 JetStream 狀態造成任何修改。
    - 比對邏輯唯一來源為 infra_py.nats.diff；不得在此層自寫 compare。
    - extra 不列舉（避免 inventory 權限/成本耦合）。
    - 不得依賴 system_initializer 或 application/domain 層模組。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import json
import uuid
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict

# === 第三方套件 (Third-Party Libraries) ===
from nats.aio.client import Client as NATS

# === 系統內模組 (Internal Modules) ===
from infra_py.logging.logger import get_logger
from infra_py.nats.actual_loader import load_actual_topology
from infra_py.nats.diff import diff_topology
from infra_py.nats.expected_loader import build_expected_topology


logger = get_logger(__name__)


async def audit_channels_topology(cfg: Dict[str, Any], nc: NATS) -> Dict[str, Any]:
    """
    audit_channels_topology 執行 JetStream 拓樸一致性稽核。

    功能:
        - 將 YAML cfg 轉為 ExpectedTopology。
        - 查詢 ActualTopology（expected-driven）。
        - 產出 TopologyDiff 並轉換為 report dict。

    參數:
        - cfg: YAML mirror dict。
        - nc: 已連線之 NATS client。

    回傳:
        - result: 稽核報告 dict（含 ok/missing/mismatched）。
        - error: 例外上拋。

    備註:
        - 報告格式與你之前一致：missing/mismatched/extra（extra 維持空）。
    """
    expected = build_expected_topology(cfg)
    actual = await load_actual_topology(expected, nc)
    d = diff_topology(expected, actual)

    report: Dict[str, Any] = {
        "trace_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ok": bool(d.ok),
        "missing": {
            "streams": list(d.missing_streams),
            "consumers": list(d.missing_consumers),
        },
        "mismatched": {
            "streams": dict(d.mismatched_streams),
            "consumers": dict(d.mismatched_consumers),
        },
        "extra": {
            "streams": [],
            "consumers": [],
        },
    }

    return report


async def audit_and_dump(
    cfg: Dict[str, Any],
    nc: NATS,
    *,
    outfile: str | Path = "reports/js_auditor/js_audit",
) -> Dict[str, Any]:
    """
    audit_and_dump 執行稽核並落地輸出 JSON 報告。

    功能:
        - 呼叫 audit_channels_topology() 取得報告。
        - 正規化輸出路徑，並寫入 JSON 檔案。

    參數:
        - cfg: YAML mirror dict。
        - nc: 已連線之 NATS client。
        - outfile: 輸出檔案路徑或前綴（若無 .json 副檔名則附加 UTC timestamp）。

    回傳:
        - result: 稽核報告 dict。
        - error: I/O 失敗或稽核失敗之例外上拋。

    備註:
        - outfile 為治理 artifact（report），不等同於 logging。
    """
    report = await audit_channels_topology(cfg, nc)
    outpath = _resolve_outfile_path(outfile)
    _dump_report(report, outpath)

    logger.info(
        "audit report written | path=%s | trace_id=%s | ok=%s",
        str(outpath),
        report.get("trace_id"),
        report.get("ok"),
    )
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
    p.parent.mkdir(parents=True, exist_ok=True)

    with p.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
