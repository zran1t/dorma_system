# common/bus_runtime.py
from __future__ import annotations

import asyncio
from typing import Dict, Optional

from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.errors import Error as NATSError
from nats.js import JetStreamContext  # 型別註記用

from .bus_config import BusConfig

# NATS 官方去重用的訊息 ID Header（支援冪等、重送去重）
MSG_ID_HEADER = "Nats-Msg-Id"


class BusRuntime:
    """
    純通訊客戶端：吃 BusConfig，不讀 YAML。

    此類別負責：
      1) 依據已解析的 BusConfig 與 NATS 建立連線與 JetStream context
      2) 建立/確保 Streams 與 Consumers 存在（若已存在則忽略錯誤）
      3) 對於 push consumer：
         - deliver_subject 為 _AUTO/未填 → 自動建立 inbox 並訂閱
         - deliver_subject 為固定字串 → 直接以該 subject 訂閱
         並將訊息放入對應 durable 的 asyncio.Queue
      4) 提供 publish() 發送訊息（支援 msg_id Header），以及 get_queue() 取得 durable 對應的消息佇列
      5) 提供 close() 釋放連線資源

    屬性:
        nc (NATS): NATS 連線實例
        js (JetStreamContext): JetStream context
        cfg (BusConfig): 已解析的通訊配置
        _queues (Dict[str, asyncio.Queue[Msg]]): durable -> asyncio.Queue
    """

    def __init__(self, nc: NATS, js: JetStreamContext, cfg: BusConfig):
        self.nc = nc
        self.js = js
        self.cfg = cfg
        self._queues: Dict[str, asyncio.Queue[Msg]] = {}

    @classmethod
    async def connect(cls, cfg: BusConfig) -> "BusRuntime":
        """
        建立 NATS 連線與 JetStream context，並依照 cfg 準備好 Streams 與 Consumers。

        參數:
            cfg (BusConfig): 已載入的通訊配置

        回傳:
            BusRuntime: 已就緒的運行時物件（含自動訂閱的 push 佇列）
        """
        if not cfg or not isinstance(cfg, BusConfig):
            raise RuntimeError("配置錯誤：cfg 必須為 BusConfig 實例")
        if not cfg.nats_url or not isinstance(cfg.nats_url, str):
            raise RuntimeError("配置錯誤：cfg.nats_url 不可為空")

        nc = NATS()
        # 連線到 NATS
        await nc.connect(servers=[cfg.nats_url])

        js: JetStreamContext = nc.jetstream()
        self = cls(nc, js, cfg)

        # 1) 確保 Streams 存在（已存在則忽略）
        for sc in cfg.streams_js:
            try:
                await js.add_stream(sc)
                print(f"🧱 已建立 stream: name={sc.name}, subjects={list(sc.subjects or [])}")
            except NATSError:
                # 一般為「已存在」等情況，容錯即可
                try:
                    sinfo = await js.stream_info(sc.name)
                    print(f"🧱 stream 已存在且一致: name={sinfo.config.name}, subjects={list(sinfo.config.subjects or [])}")
                except Exception:
                    print(f"⚠️ 取得/更新 stream 失敗: name={sc.name}")

        # 2) 確保 Consumers 存在，並處理 push 訂閱（含 _AUTO 與固定 deliver_subject）
        for c in cfg.consumers:
            if "stream" not in c or "durable" not in c:
                raise RuntimeError("消費者配置缺少必要欄位：stream / durable")

            stream = c["stream"]
            durable = c["durable"]
            mode = str(c.get("mode", "pull")).lower()

            deliver_subject_cfg = c.get("deliver_subject")
            deliver_subject_to_subscribe: Optional[str] = None

            # 只有 push 才需要 deliver_subject 與訂閱
            if mode == "push":
                if deliver_subject_cfg in (None, "_AUTO"):
                    # _AUTO：由客戶端產生 inbox，並寫入 config
                    deliver_subject_to_subscribe = nc.new_inbox()  # 注意：nats-py 的 new_inbox() 是同步方法
                else:
                    # 固定 deliver_subject：直接採用
                    deliver_subject_to_subscribe = str(deliver_subject_cfg)

            # 用 cfg 工具生成 ConsumerConfig；若我們有 deliver_subject，覆寫之
            cc = cfg.build_consumer_config(c, deliver_subject=deliver_subject_to_subscribe)

            # 建立 consumer（若已存在則忽略錯誤）
            try:
                await js.add_consumer(stream=stream, config=cc)
                print(f"🧩 已新增 consumer: stream={stream}, durable={durable}, filter={c.get('filter')}")
            except NATSError:
                print(f"🧩 consumer 已存在: stream={stream}, durable={durable}")

            # push 模式下一定要訂閱
            if mode == "push":
                # 若 deliver_subject 是 _AUTO，但伺服器最後決定不同 subject，可讀取 consumer_info 校正
                if not deliver_subject_to_subscribe:
                    try:
                        ci = await js.consumer_info(stream, durable)
                        ds = getattr(ci, "config", None) and getattr(ci.config, "deliver_subject", None)
                        if isinstance(ds, str) and ds:
                            deliver_subject_to_subscribe = ds
                    except Exception:
                        deliver_subject_to_subscribe = None

                if not deliver_subject_to_subscribe:
                    print(f"⚠️ 無法訂閱 push consumer：stream={stream}, durable={durable}，查無 deliver_subject")
                else:
                    q: asyncio.Queue[Msg] = asyncio.Queue()

                    async def _cb(msg: Msg, _q=q):
                        # 推送訊息入 durable 對應 queue
                        await _q.put(msg)

                    await nc.subscribe(subject=deliver_subject_to_subscribe, cb=_cb)
                    self._queues[durable] = q
                    print(f"🪢 已訂閱 push consumer：stream={stream}, durable={durable}, deliver_subject={deliver_subject_to_subscribe}")

        return self

    def get_queue(self, durable: str) -> Optional[asyncio.Queue]:
        """
        取得指定 durable 的 asyncio.Queue（僅限 push/_AUTO 或固定 deliver_subject 已訂閱者）。

        參數:
            durable (str): consumer 的 durable 名稱

        回傳:
            Optional[asyncio.Queue]: 若該 durable 有建立 queue 則回傳，否則為 None。
        """
        return self._queues.get(durable)

    async def publish(self, subject: str, data: bytes, *, msg_id: Optional[str] = None) -> None:
        """
        發佈訊息到指定 subject。若提供 msg_id，則會使用 NATS Header（Nats-Msg-Id）以支援冪等去重。

        參數:
            subject (str): 發佈目標主題
            data (bytes): 負載資料（已序列化）
            msg_id (Optional[str]): 訊息唯一 ID（用於去重與追蹤）

        回傳:
            None
        """
        if msg_id:
            await self.nc.publish(subject, data, headers={MSG_ID_HEADER: msg_id})
        else:
            await self.nc.publish(subject, data)

    async def close(self) -> None:
        """
        優雅關閉 NATS 連線：
          1) 嘗試 drain（等待未完成訊息處理）
          2) 若失敗則強制 close
          3) 兩者過程中的例外都吞掉（避免關閉階段阻斷程式）
        """
        try:
            await self.nc.drain()
        except Exception:
            try:
                await self.nc.close()
            except Exception:
                pass
