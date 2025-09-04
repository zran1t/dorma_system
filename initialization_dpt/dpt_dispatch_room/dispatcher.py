# initialization_dpt/dpt_dispatch_room/dispatcher.py
import asyncio
from .dispatcher_core import launch_submodules

async def main():
    launch_submodules()
    await asyncio.sleep(999999)

if __name__ == "__main__":
    asyncio.run(main())