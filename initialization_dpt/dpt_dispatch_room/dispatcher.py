# initialization_dpt/dpt_dispatch_room/dispatcher.py
from __future__ import annotations
"""
部門調度室（Department Dispatcher）— 主流程
- 呼叫 dispatcher_core.launch_submodules() 啟動子模組
- 撐住事件循環，保留視窗
"""

import asyncio
from .dispatcher_core import launch_submodules


async def main() -> None:
    launch_submodules()
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
