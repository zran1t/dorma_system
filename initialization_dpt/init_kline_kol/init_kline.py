import asyncio
import aiohttp
import redis.asyncio as redis
import json
from datetime import datetime, timedelta, timezone
from nats.aio.client import Client as NATS
from itertools import islice

# === Redis 初始化 ===
redis_client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
count = 200

# === 時間對齊工具 ===  (取得對應時間框架最新收完一根的時間點)
def align_to_interval(now_utc: datetime, interval_str: str) -> datetime:
    if interval_str.endswith("m"):
        minutes = int(interval_str[:-1])
        aligned = now_utc.replace(second=0, microsecond=0)
        return aligned.replace(minute=(aligned.minute // minutes) * minutes)
    elif interval_str.endswith("H"):
        hours = int(interval_str[:-1])
        aligned = now_utc.replace(minute=0, second=0, microsecond=0)
        return aligned.replace(hour=(aligned.hour // hours) * hours)
    else:
        raise ValueError(f"不支援的 interval 格式：{interval_str}")

# === 將 interval 轉為秒數，用於排序
def interval_to_seconds(interval: str) -> int:
    unit = interval[-1]
    value = int(interval[:-1])
    factor = {"m": 60, "H": 3600, "d": 86400, "w": 604800}
    return value * factor.get(unit, 0)


# === 分組工具 === 可以指定一次取多少個 iterable 物件 size 一次取幾個
def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk


# === 抓取 K 線 ===
async def fetch_okx_kline(symbol: str, interval: str, limit: int = 300):
    url = "https://www.okx.com/api/v5/market/candles"
    params = {"instId": symbol, "bar": interval, "limit": limit}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            res = await resp.json()
            return res.get("data", [])

# === 初始化快取 ===
async def initialize_kline_cache(symbol: str, interval: str, count: int):
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    floor_time = align_to_interval(now, interval) # 取得對應時間框架最新收完一根的時間點

    unit_seconds = interval_to_seconds(interval) # 把時間框架轉成秒數
    start_time = floor_time - timedelta(seconds=unit_seconds * count) # 依照時間框架取得所需資料長度最早的時間點

    raw_klines = list(reversed(await fetch_okx_kline(symbol, interval, count)))

    result = []
    for k in raw_klines:
        ts = int(k[0])
        dt = datetime.utcfromtimestamp(ts / 1000).replace(tzinfo=timezone.utc)
        if start_time <= dt <= floor_time:
            result.append({
                "timestamp": k[0], "open": k[1], "high": k[2],
                "low": k[3], "close": k[4], "volume": k[5]
            })

    key = f"kline:{symbol}:{interval}"
    await redis_client.delete(key)
    for kline in result:
        await redis_client.rpush(key, json.dumps(kline))

    print(f"✅ 初始化完成：{symbol} {interval}，共 {len(result)} 根")




ready_flag = asyncio.Event() # 非同步事件旗標 用來卡部門的

async def wait_start_signal(nc):
    async def handle_start(msg):
        print("🟢 收到啟動訊號：start.kline")
        ready_flag.set() # 設定事件旗標 → 解除等待
    await nc.subscribe("start.kline", cb=handle_start)

# === asyncio.Event vs asyncio.Future 差異說明 ===
#
# 1. asyncio.Event：用來「控制是否可以啟動」，像是「允許啟動的旗標」
#    - 適合卡住流程、等訊號解鎖，例如：
#         → 等外部通知模組可以開始工作（例：start.kline）
#    - 只能表示「狀態是否放行」，不關心執行結果
#    - 可重複使用（set → clear → 再 wait）
#
# 2. asyncio.Future：用來「等待某個任務的完成結果」，像是「你做完再叫我」
#    - 適合等一段任務做完再往下，例如：
#         → 等所有初始化資料抓完後，主程式才能結束
#    - 可傳遞值（set_result(value)），但只能使用一次
#    - 適合代表「一個具體任務」的完成狀態
#
#   總結：
#     - 用 Event：「你可以動了沒？」
#     - 用 Future：「你做完了沒？」


# === NATS 接收初始化指令 ===
async def listen_and_initialize():
    nc = NATS() 
    await nc.connect(
        servers=["nats://127.0.0.1:4222"],
        user="An",
        password="0318"
    )

    await wait_start_signal(nc) # 等待接收到結束空轉 開始的訊號 就會放行 繼續往下
    print("🕓 等待啟動訊號 start.kline ...")
    await ready_flag.wait()

    future = asyncio.Future() # 建立future物件 當阻塞點


    async def message_handler(msg): # 收到廣播後要執行的func 先寫好
        await nc.publish("status.kol.market", b"INIT")
        data = json.loads(msg.data.decode()) # 解析傳入資料成dict
        strategies = data["strategies"] 
        for i, s in enumerate(strategies):
            print(f"[第{i}筆策略] symbol =", s["symbol"], type(s["symbol"]))

        symbols = list({sym for s in strategies for sym in s["symbol"]}) 
        intervals = list({s["interval"] for s in strategies})


        print(f"🟡 收到初始化指令：symbols={symbols}, intervals={intervals}, count={count}")

        # 排序 時間級別 由小到大
        sorted_intervals = sorted(intervals, key=interval_to_seconds)

        for interval in sorted_intervals:
            all_tasks = [initialize_kline_cache(symbol, interval, count) for symbol in symbols] # 建立已經排序過的(依照時間級別)執行初始化清單
            batches = list(chunked_iterable(all_tasks, 10))

            for i, task_batch in enumerate(batches): #enumerate 打上編號 等同於迴圈 一直 i+=1 
                await asyncio.gather(*task_batch)
                is_last = (i == len(batches) - 1) # 判斷是否是最後一輪
                if not is_last: # 非最後一輪
                    print(f"⏸ 批次 {i+1} 完成，等 2 秒") # 因為從0開始 所以+1 人易讀
                    await asyncio.sleep(2)
        else:
            print(f"✅ 最後一批（{len(task_batch)} 個）已完成，略過等待")

        print("🎉 全部初始化完成，模組即將退出")
        await nc.publish("status.kol.market", b"READY")
        future.set_result(True) # True 讓他可以繼續走 不被future卡住

    await nc.subscribe("init.kline.data", cb=message_handler) # 放行後 就可以訂閱會給予 初始化指令的頻道 cb 接收到訊息後 要執行的func message_handler
    print("📡 已啟動 init_kline 模組，等待初始化資料...")

    await future # 還沒收到初始化指令 就會卡在這裡
    await nc.drain()

# === 啟動 ===
if __name__ == "__main__":
    asyncio.run(listen_and_initialize())