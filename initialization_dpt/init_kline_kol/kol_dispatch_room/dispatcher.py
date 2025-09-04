# initialization_dpt/init_kline_kol/kol_dispatch_room/dispatcher.py
import asyncio
from .dispatcher_core import launch_submodules

async def main():
    launch_submodules()
    # 撐住這個視窗，不讓腳本跑完就結束
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())