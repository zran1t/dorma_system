"""
File: system_initializer/steps/nats_bootstrap.py
Module: system_initializer.steps.nats_bootstrap

職責 (Responsibility):
    串接 infra_py.nats 之 reset / bootstrap / audit 三步驟，
    作為 system_initializer 啟動流程中的 NATS JetStream 控制面拓樸收斂入口。

注意事項 (Notes):
    - 預設流程：reset → bootstrap → audit。
    - reset 具破壞性：只應用於初始化/測試環境回收，不得在運行中資料流使用。
    - 本模組統一管理 NATS 連線生命週期：connect 一次、共用同一條 nc、最後 close。
    - 本模組是 system_initializer step；不得被 infra_py 依賴。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import argparse
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# === 第三方套件 (Third-Party Libraries) ===
from nats.aio.client import Client as NATS

# === 系統內模組 (Internal Modules) ===
from infra_py.logging.logger import get_logger
from infra_py.nats.config_loader import load_config
from infra_py.nats.js_auditor import audit_and_dump, audit_channels_topology
from infra_py.nats.js_bootstrap import bootstrap_channels_topology
from infra_py.nats.js_reset import reset_channels_topology


logger = get_logger(__name__)


@dataclass(frozen=True)
class NatsBootstrapOptions:
    """
    NatsBootstrapOptions 啟動步驟選項集合。

    功能:
        - 定義本步驟要跑哪些子流程與輸出位置。

    欄位說明:
        - config_path: channels YAML 路徑。
        - do_reset: 是否執行 reset（破壞性）。
        - do_bootstrap: 是否執行 bootstrap（建立/更新 streams/consumers）。
        - do_audit: 是否執行 audit（比對 YAML 與實際 JetStream）。
        - audit_outfile: audit 報告輸出路徑/前綴；None 表示不落地，只回傳 dict。

    契約 / 限制:
        - do_audit=True 且 audit_outfile 非 None 時，會寫檔（audit report artifact），不等同於 logging。

    備註:
        - audit_outfile 是治理輸出（artifact），與 log 檔分離。
    """
    config_path: str | Path
    do_reset: bool = True
    do_bootstrap: bool = True
    do_audit: bool = True
    audit_outfile: Optional[str | Path] = "reports/js_auditor/js_audit"


async def run_nats_bootstrap(opts: NatsBootstrapOptions) -> Dict[str, Any]:
    """
    run_nats_bootstrap 執行 NATS JetStream 拓樸治理流程。

    功能:
        - 讀取 YAML 設定。
        - 建立單一 NATS 連線。
        - 依選項執行 reset / bootstrap / audit（共用同一條 nc）。
        - 彙總各步驟 report 回傳。

    參數:
        - opts: NatsBootstrapOptions。

    回傳:
        - result: 總報告 dict（包含每步驟子報告）。
        - error: 任一步驟拋出例外將直接向上拋出。

    備註:
        - bootstrap 失敗通常不應繼續 audit（避免把控制面錯誤包成 mismatch）。
    """
    trace_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    cfg = load_config(opts.config_path)

    report: Dict[str, Any] = {
        "trace_id": trace_id,
        "timestamp": started_at,
        "ok": True,
        "steps": {"reset": None, "bootstrap": None, "audit": None},
        "errors": [],
    }

    logger.info(
        "nats bootstrap start | trace_id=%s | reset=%s | bootstrap=%s | audit=%s | config=%s",
        trace_id,
        opts.do_reset,
        opts.do_bootstrap,
        opts.do_audit,
        str(opts.config_path),
    )

    servers = (cfg.get("nats") or {}).get("servers")
    if not isinstance(servers, list) or not servers:
        raise ValueError("invalid config: nats.servers must be non-empty list")

    nc: Optional[NATS] = None
    try:
        nc = NATS()
        await nc.connect(servers=servers)

        if opts.do_reset:
            logger.info("reset start | trace_id=%s", trace_id)
            reset_report = await reset_channels_topology(cfg, nc)
            report["steps"]["reset"] = reset_report
            if reset_report.get("errors"):
                report["ok"] = False
                report["errors"].append(
                    {"step": "reset", "error": "reset finished with errors", "detail": reset_report.get("errors")}
                )
                logger.warning("reset finished with errors | trace_id=%s", trace_id)
            else:
                logger.info("reset finished | trace_id=%s", trace_id)

        if opts.do_bootstrap:
            logger.info("bootstrap start | trace_id=%s", trace_id)
            try:
                await bootstrap_channels_topology(cfg, nc)
                report["steps"]["bootstrap"] = {"ok": True}
                logger.info("bootstrap finished | trace_id=%s", trace_id)
            except Exception as e:
                report["ok"] = False
                report["steps"]["bootstrap"] = {"ok": False, "error": repr(e)}
                report["errors"].append({"step": "bootstrap", "error": repr(e)})
                logger.error("bootstrap failed | trace_id=%s | err=%r", trace_id, e)
                raise

        if opts.do_audit:
            logger.info("audit start | trace_id=%s", trace_id)
            try:
                if opts.audit_outfile is None:
                    audit_report = await audit_channels_topology(cfg, nc)
                else:
                    audit_report = await audit_and_dump(cfg, nc, outfile=opts.audit_outfile)

                report["steps"]["audit"] = audit_report

                if not audit_report.get("ok", False):
                    report["ok"] = False
                    report["errors"].append({"step": "audit", "error": "topology mismatch", "detail": audit_report})
                    logger.warning("audit mismatch | trace_id=%s", trace_id)
                else:
                    logger.info("audit ok | trace_id=%s", trace_id)

            except Exception as e:
                report["ok"] = False
                report["steps"]["audit"] = {"ok": False, "error": repr(e)}
                report["errors"].append({"step": "audit", "error": repr(e)})
                logger.error("audit failed | trace_id=%s | err=%r", trace_id, e)
                raise

        logger.info("nats bootstrap done | trace_id=%s | ok=%s", trace_id, report["ok"])
        return report

    finally:
        if nc is not None:
            try:
                await nc.close()
            except Exception as e:
                logger.warning("failed to close nc | trace_id=%s | err=%r", trace_id, e)


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """
    _parse_args 解析 CLI 參數（供手動執行或 CI script 使用）。

    功能:
        - 提供 reset/bootstrap/audit 的開關與 audit 輸出設定。
        - 允許手動指定 inter/intra config。

    參數:
        - argv: CLI 參數清單；若無則讀 sys.argv。

    回傳:
        - result: argparse Namespace。
        - error: 無。

    備註:
        - 若 system_initializer 以程式方式呼叫 run_nats_bootstrap()，可不使用 CLI。
    """
    p = argparse.ArgumentParser(prog="nats_bootstrap", description="Run JetStream reset/bootstrap/audit sequence.")
    p.add_argument("--config", required=True, help="Path to configs/channels/*.yaml")
    p.add_argument("--no-reset", action="store_true", help="Skip reset step")
    p.add_argument("--no-bootstrap", action="store_true", help="Skip bootstrap step")
    p.add_argument("--no-audit", action="store_true", help="Skip audit step")
    p.add_argument("--audit-out", default="reports/js_auditor/js_audit", help="Audit report outfile prefix (.json optional)")
    p.add_argument("--audit-no-dump", action="store_true", help="Do not dump audit report to file (return dict only)")
    return p.parse_args(argv)


async def main(argv: Optional[list[str]] = None) -> int:
    """
    main CLI 入口。

    功能:
        - 解析參數並執行 run_nats_bootstrap()。
        - 將 ok 結果轉換為 process exit code。

    參數:
        - argv: CLI 參數清單；若無則讀 sys.argv。

    回傳:
        - result: 0 表示成功；1 表示 mismatch 或流程失敗。
        - error: 無。

    備註:
        - 此入口只用於 CLI；真正系統啟動請用 system_initializer/main.py。
    """
    args = _parse_args(argv)

    opts = NatsBootstrapOptions(
        config_path=args.config,
        do_reset=not args.no_reset,
        do_bootstrap=not args.no_bootstrap,
        do_audit=not args.no_audit,
        audit_outfile=None if args.audit_no_dump else args.audit_out,
    )

    report = await run_nats_bootstrap(opts)
    return 0 if report.get("ok") else 1


if __name__ == "__main__":
    import asyncio
    raise SystemExit(asyncio.run(main()))
