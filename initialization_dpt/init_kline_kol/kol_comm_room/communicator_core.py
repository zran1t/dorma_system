# initialization_dpt/dpt_comm_room/communicator_core.py
from __future__ import annotations

import asyncio
import json
from nats.aio.msg import Msg
from injector_api.schemas.strategy_pools import StrategySubmitRequest
from common.bus_runtime import BusRuntime

# ========= Inter → Dept → Intra(dept→kol) =========
async def handle_from_inter(req: StrategySubmitRequest, inter_msg: Msg, intra_runtime: BusRuntime) -> None:
    """
    接收端只做轉送：Inter → Intra(dept→kol)

    參數:
        req (StrategySubmitRequest): 已通過驗證的請求模型
        inter_msg (Msg): 來源 NATS 訊息（用於 ack/nak）
        intra_runtime (BusRuntime): 內部通道 runtime（用來 publish）
    回傳:
        None
    """
    # Pydantic v2：先轉 dict，再用 json.dumps(ensure_ascii=False) 確保中文不轉義
    payload_dict = req.model_dump(exclude_none=True)
    payload_bytes = json.dumps(payload_dict, ensure_ascii=False).encode("utf-8")

    subject = f"{intra_runtime.cfg.routes.dept2kol_prefix}.init.kline"
    await intra_runtime.publish(subject, payload_bytes)
    print(f"📤 已轉發到內部通道: {subject}")
    await inter_msg.ack()
    print("✅ 已向 NATS 確認：inter→dept 的訊息處理完成")

# ========= Intra(kol→dept) → Dept → Inter =========
async def handle_from_kol(req: StrategySubmitRequest, intra_msg: Msg, inter_runtime: BusRuntime) -> None:
    """
    kol → dept 回來的資料只轉送到 Inter（kol→dept）

    參數:
        req (StrategySubmitRequest): 已通過驗證的請求模型
        intra_msg (Msg): 來源 NATS 訊息（用於 ack/nak）
        inter_runtime (BusRuntime): 外部通道 runtime（用來 publish）
    回傳:
        None
    """
    payload_dict = req.model_dump(exclude_none=True)
    payload_bytes = json.dumps(payload_dict, ensure_ascii=False).encode("utf-8")

    subject = f"{inter_runtime.cfg.routes.kol2dept_prefix}.init.kline"
    await inter_runtime.publish(subject, payload_bytes)
    print(f"📤 已轉發到對外通道: {subject}")
    await intra_msg.ack()
    print("✅ 已向 NATS 確認：kol→dept 的訊息處理完成")

# ========= Consumers（loops） =========
async def consume_inter(inter_rt: BusRuntime, intra_rt: BusRuntime, durable_inter_main: str) -> None:
    """
    監聽 Inter（dept→kol）並轉送到 Intra（dept→kol）

    參數:
        inter_rt (BusRuntime): Inter runtime
        intra_rt (BusRuntime): Intra runtime
        durable_inter_main (str): 由 inter.yaml 對應 dept→kol 的 durable 名稱
    """
    q = inter_rt.get_queue(durable_inter_main)
    if q is None:
        raise RuntimeError(
            f"找不到 inter durable={durable_inter_main} 的 push queue；請檢查 inter.yaml 是否為 mode=push，且 deliver_subject 有值（_AUTO 亦可）"
        )
    print(f"🟢 正在監聽 Inter 通道，durable={durable_inter_main}")

    while True:
        msg: Msg = await q.get()
        try:
            req = StrategySubmitRequest.model_validate_json(msg.data)
        except Exception as e:
            print(f"❌ Inter→Dept schema 驗證失敗：{e}")
            await msg.ack()
            continue

        try:
            await handle_from_inter(req, msg, intra_rt)
        except Exception as e:
            print(f"💥 Inter handler 例外：{e}")
            try:
                await msg.nak()
            except Exception:
                pass

async def consume_intra_kol(intra_rt: BusRuntime, inter_rt: BusRuntime, durable_intra_from_kol: str) -> None:
    """
    監聽 Intra（kol→dept）並轉送到 Inter（kol→dept）

    參數:
        intra_rt (BusRuntime): Intra runtime
        inter_rt (BusRuntime): Inter runtime
        durable_intra_from_kol (str): 由 initialization.yaml 對應 kol→dept 的 durable 名稱
    """
    q = intra_rt.get_queue(durable_intra_from_kol)
    if q is None:
        raise RuntimeError(
            f"找不到 intra durable={durable_intra_from_kol} 的 push queue；請檢查 initialization.yaml 是否為 mode=push，且 deliver_subject 有值（_AUTO 亦可）"
        )
    print(f"🟢 正在監聽 Intra 通道，durable={durable_intra_from_kol}")

    while True:
        msg: Msg = await q.get()
        try:
            req = StrategySubmitRequest.model_validate_json(msg.data)
        except Exception as e:
            print(f"❌ Kol→Dept schema 驗證失敗：{e}")
            await msg.ack()
            continue

        try:
            await handle_from_kol(req, msg, inter_rt)
        except Exception as e:
            print(f"💥 Kol handler 例外：{e}")
            try:
                await msg.nak()
            except Exception:
                pass
