# receiver_hard.py
import asyncio
import json
from nats.aio.client import Client as NATS

# === 硬編設定：依你的環境改一下就好 ===
NATS_URL = "nats://127.0.0.1:4222"
NATS_USER = None         # 若無帳密，設為 None
NATS_PASS = None       # 若無帳密，設為 None

# 你的 broadcaster 目前會發到：
#   subject = f"{routes.dept2kol_prefix}.init.kline"
# 若你的 YAML 第一個 subjects 是 "bus.inter.>"，推導的 prefix 會是 "bus.inter.dept2kol"
# → 最終 publish 在 "bus.inter.dept2kol.init.kline"
# 所以這裡我們硬編訂閱 "bus.inter.>"，保證吃得到
SUBJECT = "bus.inter.>"

async def main():
    nc = NATS()

    # 連線參數
    conn_kwargs = {"servers": [NATS_URL]}
    if NATS_USER is not None and NATS_PASS is not None:
        conn_kwargs.update({"user": NATS_USER, "password": NATS_PASS})

    await nc.connect(**conn_kwargs)
    print(f"✅ Connected to {NATS_URL}, subscribing: {SUBJECT}")

    async def handler(msg):
        payload = msg.data
        try:
            obj = json.loads(payload.decode("utf-8"))
        except Exception:
            obj = {"raw": payload}
        print("\n==== 收到一筆訊息 ====")
        print("Subject:", msg.subject)
        print("Headers:", dict(msg.header or {}))
        print("Payload:", obj)

    # 核心 NATS 訂閱（不是 JetStream consumer，單純吃即時訊息）
    await nc.subscribe(SUBJECT, cb=handler)

    try:
        # 一直掛著收
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await nc.drain()
        print("👋 closed")

if __name__ == "__main__":
    asyncio.run(main())