# initialization_dpt/dpt_comm_room/communicate.py
# [新增] 主流程：簡單呼叫 core；可作為程式入口或由上層服務調用

import asyncio
from communicate_core import run_communicate


async def main(nats_url: str = "nats://127.0.0.1:4222") -> None:
    """
    主流程入口：啟動初始化部門通訊處室接收器。

    Args:
        nats_url (str): NATS 伺服器位址（預設本機 4222）
    """
    # 單純轉呼核心實作，保持主流程乾淨
    await run_communicate(nats_url=nats_url)


if __name__ == "__main__":
    # 允許直接以「python communicate.py」啟動
    asyncio.run(main())