# initialization_dpt/dpt_comm_room/communicator_core.py
import json
from nats.aio.msg import Msg
from injector_api.schemas.strategy_pools import StrategySubmitRequest
from common.bus_runtime import ChannelRuntime

# ========= Inter → Dept → Intra(dept→kol) =========
async def handle_from_inter(req: StrategySubmitRequest, inter_msg: Msg, intra_runtime: ChannelRuntime) -> None:
    """接收端只做轉送：Inter → Intra(dept→kol)"""
    # 把物件轉成json字串 可控的轉換過程 沒有的欄位不會輸出 中文會保持原樣
    payload_bytes = req.model_dump_json(exclude_none=True, ensure_ascii=False).encode("utf-8")
    subject = f"{intra_runtime.cfg.routes.dept2kol_prefix}.init.kline"
    await intra_runtime.publish(subject, payload_bytes)
    print(f"📤 已轉發到內部通道: {subject}")
    await inter_msg.ack()
    print("✅ 已向 NATS 確認：inter→dept 的訊息處理完成")

# ========= Intra(kol→dept) → Dept → Inter =========
async def handle_from_kol(req: StrategySubmitRequest, intra_msg: Msg, inter_runtime: ChannelRuntime) -> None:
    """kol → dept 回來的資料只轉送到 Inter（kol→dept）"""
    payload_bytes = req.model_dump_json(exclude_none=True, ensure_ascii=False).encode("utf-8")
    subject = f"{inter_runtime.cfg.routes.kol2dept_prefix}.init.kline"
    await inter_runtime.publish(subject, payload_bytes)
    print(f"📤 已轉發到對外通道: {subject}")
    await intra_msg.ack()
    print("✅ 已向 NATS 確認：kol→dept 的訊息處理完成")

# ========= Consumers =========
async def consume_inter(inter_rt: ChannelRuntime, intra_rt: ChannelRuntime, durable_inter_main: str):
    """Inter → Intra（dept→kol）"""
    q = inter_rt.get_push_queue(durable_inter_main)
    if q is None:
        raise RuntimeError(f"找不到 inter durable={durable_inter_main} 的 push queue；請檢查 inter.yaml 是否為 mode=push / deliver_subject=_AUTO")
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
            try: await msg.nak()
            except Exception: pass

async def consume_intra_kol(intra_rt: ChannelRuntime, inter_rt: ChannelRuntime, durable_intra_from_kol: str):
    """Intra(科別→部門) → Inter(kol→dept)"""
    q = intra_rt.get_push_queue(durable_intra_from_kol)
    if q is None:
        raise RuntimeError(f"找不到 intra durable={durable_intra_from_kol} 的 push queue；請檢查 initialization.yaml 是否為 mode=push / deliver_subject=_AUTO")
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
            try: await msg.nak()
            except Exception: pass