# initialization_dpt/init_kline_kol/kol_dispatch_room/dispatcher.py
import asyncio
from .dispatcher_core import launch_submodules

async def main():
    """
    主流程：啟動科別調度室底下的子模組，並常駐。
    """
    launch_submodules()
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())
