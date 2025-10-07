# initialization_dpt/init_kline_kol/kol_comm_room/communicate_core.py
# 使用 NatsComm（扁平協定 v1）+ subject_resolver（動態主題）

from __future__ import annotations

import sys, asyncio, json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict

# ── 專案根偵測（往上找含 configs/channels 的資料夾） ─────────────────
def _project_root() -> Path:
    here = Path(__file__).resolve()
    for p in list(here.parents)[:8]:
        if (p / "configs" / "channels").exists():
            return p
    return here.parents[3]

ROOT = _project_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── imports ────────────────────────────────────────────────────────────
from common.nats_comm import NatsComm
from common.channel_config_loader import load_channel_config
from common.subject_resolver import resolve_init_subjects

DEPT_ID = "initialization_dpt"   # 扁平協定 Actor.id

async def run_communicate(nats_url: str = "nats://127.0.0.1:4222") -> None:
    # 讀 YAML（只需要 initialization.yaml）
    init_cfg  = load_channel_config(str(ROOT / "configs/channels/initialization.yaml"))

    # 取第一個 kol id（或之後你要改成從環境變數/參數傳入也行）
    kols = init_cfg["subjects"].get("kols") or []
    if not kols:
        raise RuntimeError("initialization.yaml 的 subjects.kols 不可為空")
    kol_id = kols[0]

    init_subj = resolve_init_subjects(init_cfg, kol_id=kol_id)
    KOL_IN_DELIVER = init_subj["to_kol_deliver"]
    KOL_OUT        = init_subj["from_kol"]

    comm = NatsComm()
    await comm.connect([nats_url], name=f"kol_comm_room:{kol_id}")

    print(f"【KOL 通訊處室（{kol_id}）】監聽 deliver(DPT→KOL)：", KOL_IN_DELIVER)

    async def _handle_down(env: Dict[str, any]) -> None:
        trace_id = env.get("trace_id")
        body_in = env.get("body", {})
        print(f"【{kol_id}】收到下行：", json.dumps(body_in, ensure_ascii=False))

        # 模擬處理
        processed = {
            "status": "processed",
            "kol_room": kol_id,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "note": "kol 已處理完成並上行回報（不 echo 原文）",
        }

        # 上行回報（更新 direction/source/destination；覆蓋 body.message/data）
        await comm.publish_forward(
            subject=KOL_OUT,
            envelope=env,
            from_actor={"type": "kol", "id": kol_id},
            to_actor={"type": "dept", "id": DEPT_ID},
            direction="up",
            message="kol_processed",
            data=processed,
            note="kol→dept processed",
        )
        print(f"【{kol_id}】已上報部門：{KOL_OUT} trace_id={trace_id}")

    await comm.subscribe(KOL_IN_DELIVER, _handle_down, js_ack=True)

    try:
        await asyncio.Future()
    finally:
        await comm.close()