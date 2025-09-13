# initialization_dpt/dpt_comm_room/communicate_core.py
# 使用 NatsComm（扁平協定 v1）+ subject_resolver（動態主題）

from __future__ import annotations

import asyncio, json, sys
from pathlib import Path
from typing import Dict

# ── 專案根偵測（往上找含 configs/channels 的資料夾） ─────────────────
def _project_root() -> Path:
    here = Path(__file__).resolve()
    for p in list(here.parents)[:8]:
        if (p / "configs" / "channels").exists():
            return p
    # 退而求其次：使用此檔兩層上（搭配 dispatcher 已注入 PYTHONPATH 通常足夠）
    return here.parents[2]

ROOT = _project_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── imports ────────────────────────────────────────────────────────────
from common.nats_comm import NatsComm
from common.channel_config_loader import load_channel_config
from common.subject_resolver import resolve_inter_subjects, resolve_init_subjects

DEPT_ID = "initialization_dpt"   # 扁平協定 Actor.id

async def run_communicate(nats_url: str = "nats://127.0.0.1:4222") -> None:
    # 讀 YAML
    inter_cfg = load_channel_config(str(ROOT / "configs/channels/inter.yaml"))
    init_cfg  = load_channel_config(str(ROOT / "configs/channels/initialization.yaml"))

    # inter：API→部門 deliver
    inter_subj = resolve_inter_subjects(inter_cfg, dept="initialization")
    API_DELIVER = inter_subj["to_department_deliver"]

    # init：部門↔KOL
    kols = init_cfg["subjects"].get("kols") or []
    if not kols:
        raise RuntimeError("initialization.yaml 的 subjects.kols 不可為空")
    kol_id = kols[0]  # 取第一個
    init_subj = resolve_init_subjects(init_cfg, kol_id=kol_id)
    KOL_UP_DELIVER = init_subj["dept_comm_in_deliver"]
    TO_KOL_IN      = init_subj["to_kol"]

    comm = NatsComm()
    await comm.connect([nats_url], name="init_dpt_comm_room")

    print("【初始化部門】監聽 deliver(API→DPT)：", API_DELIVER)
    print("【初始化部門】監聽 deliver(KOL→DPT)：", KOL_UP_DELIVER)

    # 收 API → DPT：沿用 trace_id 直送到 KOL 的 .comm.in（不改 body）
    async def _handle_from_api(env: Dict[str, any]) -> None:
        print("【DPT ← API】", json.dumps(env.get("body") or {}, ensure_ascii=False))
        await comm.publish_forward(
            subject=TO_KOL_IN,
            envelope=env,
            from_actor={"type": "dept", "id": DEPT_ID},
            to_actor={"type": "kol", "id": kol_id},
            direction="down",
            note="forward: api→kol",
        )
        print("  ↘ 已轉發至 KOL：", TO_KOL_IN, "| trace_id:", env.get("trace_id"))

    # 收 KOL → DPT：純列印確認（避免回路）
    async def _handle_from_kol(env: Dict[str, any]) -> None:
        body = env.get("body") or {}
        print("【DPT ← KOL】", json.dumps(body, ensure_ascii=False))

    await comm.subscribe(API_DELIVER, _handle_from_api, js_ack=True)
    await comm.subscribe(KOL_UP_DELIVER, _handle_from_kol, js_ack=True)

    try:
        await asyncio.Future()
    finally:
        await comm.close()