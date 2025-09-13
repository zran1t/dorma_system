# tools/runtime_bootstrap.py
from __future__ import annotations
from typing import Dict, Any, Tuple
from pathlib import Path
from datetime import datetime

from common.channel_config_loader import load_channel_config
from common.nats_bootstrap import bootstrap_nats_and_js
from common.js_reset import reset_channels_topology
from common.js_auditor import audit_and_dump

def _project_root() -> Path:
    # 以本檔案所在位置往上一層當專案根（tools/ 的父層）
    return Path(__file__).resolve().parents[1]

async def centralized_reset_bootstrap(
    *,
    do_audit: bool = True,
    inter_outfile: str = "reports/inter_audit.json",
    init_outfile: str = "reports/init_audit.json",
    timestamped_reports: bool = False,    # ← 新增：是否時間戳留存
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    cfg_inter = load_channel_config("configs/channels/inter.yaml")
    cfg_init  = load_channel_config("configs/channels/initialization.yaml")

    # 統一路徑到專案根
    root = _project_root()
    reports_dir = (root / "reports")
    reports_dir.mkdir(parents=True, exist_ok=True)

    if timestamped_reports:
        # e.g. inter_audit_2025-09-09_11-32-10.json
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        inter_path = reports_dir / f"inter_audit_{ts}.json"
        init_path  = reports_dir / f"init_audit_{ts}.json"
    else:
        inter_path = (root / inter_outfile)
        init_path  = (root / init_outfile)

    # 先連一次 NATS 給 reset
    print("【Bootstrap】連線到 NATS：", ", ".join(cfg_inter["nats"]["servers"]))
    nc_for_reset, _ = await bootstrap_nats_and_js({
        "nats": cfg_inter["nats"],
        "jetstream": {"streams": [], "consumers": []}
    })
    try:
        await reset_channels_topology(cfg_inter, nc_for_reset)
        await reset_channels_topology(cfg_init,  nc_for_reset)
    finally:
        try:
            await nc_for_reset.drain()
        except Exception:
            await nc_for_reset.close()

    # inter → bootstrap + (audit→寫檔)
    nc_inter, _ = await bootstrap_nats_and_js(cfg_inter)
    try:
        if do_audit:
            await audit_and_dump(cfg_inter, nc_inter, mode="full", outfile=inter_path)
    finally:
        try:
            await nc_inter.drain()
        except Exception:
            await nc_inter.close()

    # init → bootstrap + (audit→寫檔)
    nc_init, _ = await bootstrap_nats_and_js(cfg_init)
    try:
        if do_audit:
            await audit_and_dump(cfg_init, nc_init, mode="full", outfile=init_path)
    finally:
        try:
            await nc_init.drain()
        except Exception:
            await nc_init.close()

    return cfg_inter, cfg_init