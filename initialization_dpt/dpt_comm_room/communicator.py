# initialization_dpt/dpt_comm_room/communicator.py
import asyncio
from pathlib import Path

from common.bus_runtime import ChannelRuntime
from common.bus_config import ChannelConfig
from .communicator_core import consume_inter, consume_intra_kol

ROOT = Path(__file__).resolve().parents[2]
INTER_YAML = ROOT / "configs" / "channels" / "inter.yaml"
INTRA_INIT_YAML = ROOT / "configs" / "channels" / "initialization.yaml"

async def run():
    # 讀 YAML，找 durable（用你 comm_runtime 提供的工具）
    inter_cfg = ChannelConfig.load(INTER_YAML)
    intra_cfg = ChannelConfig.load(INTRA_INIT_YAML)

    durable_inter_main = ChannelRuntime.find_durable_by_filter(inter_cfg, filter_prefix="bus.inter.")
    durable_intra_from_kol = ChannelRuntime.find_durable_by_filter(
        intra_cfg, filter_prefix="bus.intra.initialization.kol2dept."
    )

    # 建 runtime
    inter_rt = await ChannelRuntime.from_yaml(INTER_YAML)
    intra_rt = await ChannelRuntime.from_yaml(INTRA_INIT_YAML)

    print("✅ 初始化部門通訊處室 已就緒.")
    print(f"   外部通道 : {INTER_YAML}")
    print(f"   內部通道 : {INTRA_INIT_YAML}")

    # 啟監聽循環
    task_inter = asyncio.create_task(consume_inter(inter_rt, intra_rt, durable_inter_main))
    task_kol   = asyncio.create_task(consume_intra_kol(intra_rt, inter_rt, durable_intra_from_kol))

    try:
        await asyncio.gather(task_inter, task_kol)
    except asyncio.CancelledError:
        print("🔻 收到停止訊號，準備關閉...")
    finally:
        for t in (task_inter, task_kol):
            if not t.done():
                t.cancel()
        await inter_rt.drain()
        await intra_rt.drain()
        print("👋 已關閉連線")

if __name__ == "__main__":
    asyncio.run(run())