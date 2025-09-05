# initialization_dpt/init_kline_kol/kol_comm_room/communicator.py
from __future__ import annotations

import asyncio
from pathlib import Path
from common.bus_config import BusConfig
from common.bus_runtime import BusRuntime
from .communicator_core import consume_intra_dept

def _root() -> Path:
    return Path(__file__).resolve().parents[3]

ROOT = _root()
INTRA_INIT_YAML = ROOT / "configs" / "channels" / "initialization.yaml"
if not INTRA_INIT_YAML.exists():
    raise FileNotFoundError(f"找不到 initialization.yaml：{INTRA_INIT_YAML}\n請確認執行位置或調整 parents 層級。")

async def main() -> None:
    """
    主流程：讀 initialization.yaml → 連 NATS → 起 Dept→Kol 消費迴圈。
    """
    intra_cfg = BusConfig.load(INTRA_INIT_YAML)
    durable_intra_from_dept = intra_cfg.durable_for(intra_cfg.routes.dept2kol_prefix)

    intra_rt = await BusRuntime.connect(intra_cfg)

    print("✅ 科別通訊處室（Kol Communication Room）已就緒")
    print(f"   內部通道 : {INTRA_INIT_YAML}")
    print(f"   監聽 Intra durable（dept→kol）: {durable_intra_from_dept}")

    task = asyncio.create_task(consume_intra_dept(intra_rt, durable_intra_from_dept))
    try:
        await task
    except asyncio.CancelledError:
        print("🔻 收到停止訊號，準備關閉...")
    finally:
        if not task.done():
            task.cancel()
        await intra_rt.close()
        print("👋 已關閉連線")

if __name__ == "__main__":
    asyncio.run(main())
