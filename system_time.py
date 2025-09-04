import os
import sys
import subprocess
import asyncio
import aiohttp
from datetime import datetime, timezone, timedelta

url = "https://www.okx.com/api/v5/public/time"

async def fetch_time(session):
    try:
        async with session.get(url, timeout=5) as resp:
            data = await resp.json()
            if data.get("code") == "0":
                ts_ms = int(data["data"][0]["ts"])  # 毫秒
                ts_s = ts_ms / 1000
                iso_utc = datetime.fromtimestamp(ts_s, tz=timezone.utc).isoformat()
                iso_taiwan = datetime.fromtimestamp(ts_s, tz=timezone(timedelta(hours=8))).isoformat()

                print(f"[UTC] {iso_utc}   [台灣] {iso_taiwan}")
            else:
                print(f"API 回應錯誤: {data}")
    except Exception as e:
        print(f"請求錯誤: {e}")

async def main():
    async with aiohttp.ClientSession() as session:
        while True:
            await fetch_time(session)
            await asyncio.sleep(1)  # 每秒抓取一次

if __name__ == "__main__":
    # 透過命令列旗標避免無限開視窗：第一次執行會開新視窗並附加 --popup
    if "--popup" in sys.argv:
        asyncio.run(main())
    else:
        if sys.platform == "darwin":  # macOS
            cmd = f'cd {os.getcwd()} && {sys.executable} {os.path.abspath(__file__)} --popup'
            subprocess.Popen(["osascript", "-e", f'tell app "Terminal" to do script "{cmd}"'])
