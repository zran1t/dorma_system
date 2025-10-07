# initialization_dpt/kol_comm_room/communicate.py
# [新增] kol 通訊處室主流程：呼叫 core

import asyncio
from communicate_core import run_communicate

async def main(nats_url: str = "nats://127.0.0.1:4222") -> None:
    """主流程入口：啟動 kol 通訊處室接收器。"""
    await run_communicate(nats_url=nats_url)

if __name__ == "__main__":
    asyncio.run(main())