# initialization_dpt/dpt_comm_room/communicator.py
from __future__ import annotations

import asyncio
from pathlib import Path
from common.bus_runtime import BusRuntime
from common.bus_config import BusConfig
from .communicator_core import consume_inter, consume_intra_kol

ROOT = Path(__file__).resolve().parents[2]
INTER_YAML = ROOT / "configs" / "channels" / "inter.yaml"
INTRA_INIT_YAML = ROOT / "configs" / "channels" / "initialization.yaml"

async def run() -> None:
    """
    主流程：讀 inter.yaml / initialization.yaml → 連 NATS → 起兩個消費迴圈
    """
    inter_cfg = BusConfig.load(INTER_YAML)
    intra_cfg = BusConfig.load(INTRA_INIT_YAML)

    # 由 routes 自動查 durable 名（不寫死）
    durable_inter_main = inter_cfg.durable_for(inter_cfg.routes.dept2kol_prefix)      # INTER: dept→kol
    durable_intra_from_kol = intra_cfg.durable_for(intra_cfg.routes.kol2dept_prefix)  # INTRA: kol→dept

    inter_rt = await BusRuntime.connect(inter_cfg)
    intra_rt = await BusRuntime.connect(intra_cfg)

    print("✅ 部門通訊處室（Dept Communication Room）已就緒")
    print(f"   外部通道 : {INTER_YAML}")
    print(f"   內部通道 : {INTRA_INIT_YAML}")
    print(f"   監聽 Inter durable（dept→kol）: {durable_inter_main}")
    print(f"   監聽 Intra durable（kol→dept）: {durable_intra_from_kol}")

    task_inter = asyncio.create_task(consume_inter(inter_rt, intra_rt, durable_inter_main))
    task_kol = asyncio.create_task(consume_intra_kol(intra_rt, inter_rt, durable_intra_from_kol))

    try:
        await asyncio.gather(task_inter, task_kol)
    except asyncio.CancelledError:
        print("🔻 收到停止訊號，準備關閉...")
    finally:
        for t in (task_inter, task_kol):
            if not t.done():
                t.cancel()
        await inter_rt.close()
        await intra_rt.close()
        print("👋 已關閉連線")

if __name__ == "__main__":
    asyncio.run(run())
