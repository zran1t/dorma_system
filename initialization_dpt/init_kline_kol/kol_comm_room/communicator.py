# initialization_dpt/init_kline_kol/kol_comm_room/communicator.py
import asyncio
import json
from pathlib import Path
from nats.aio.msg import Msg

from common.bus_runtime import ChannelRuntime
from injector_api.schemas.strategy_pools import StrategySubmitRequest

# === 路徑設定 ===
ROOT = Path(__file__).resolve().parents[3]
INTRA_INIT_YAML = ROOT / "configs" / "channels" / "initialization.yaml"
if not INTRA_INIT_YAML.exists():
    raise FileNotFoundError(f"找不到 initialization.yaml：{INTRA_INIT_YAML}\n請確認是從專案根目錄執行，或調整 parents 層級。")

# === Durable 名稱（對應 YAML）===
DURABLE_INTRA_FROM_DEPT = "INIT_KOL_RX"  # initialization.yaml（dept → kol）

# ========= Handlers =========

async def _handle_from_dept(req: StrategySubmitRequest, msg: Msg) -> None:
    """科別通訊處室：收到部門傳來的初始化策略池訊息"""
    payload = req.model_dump(exclude_none=True)
    print("📥 科別通訊處室 收到部門的訊息：")
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    await msg.ack()
    print("✅ 已回覆 NATS：dept→kol 的訊息處理完成")

# ========= Consumer =========

async def _consume_intra_dept(intra_rt: ChannelRuntime):
    """Intra (dept→kol) → Kol"""
    q = intra_rt.get_push_queue(DURABLE_INTRA_FROM_DEPT)
    if q is None:
        raise RuntimeError(
            f"找不到 intra durable={DURABLE_INTRA_FROM_DEPT} 的 push queue；"
            f"請檢查 {INTRA_INIT_YAML} 是否有正確設定"
        )
    print(f"🟢 科別通訊處室正在監聽 Intra 通道，durable={DURABLE_INTRA_FROM_DEPT}")
    while True:
        msg: Msg = await q.get()
        try:
            req = StrategySubmitRequest.model_validate_json(msg.data)
        except Exception as e:
            print(f"❌ Dept→Kol schema 驗證失敗：{e}")
            await msg.ack()
            continue
        try:
            await _handle_from_dept(req, msg)
        except Exception as e:
            print(f"💥 Kol handler 例外：{e}")
            try:
                await msg.nak()
            except Exception:
                pass

# ========= main =========

async def main():
    intra_rt = await ChannelRuntime.from_yaml(INTRA_INIT_YAML)
    print("✅ 科別通訊處室 已就緒.")
    print(f"   內部通道 : {INTRA_INIT_YAML}")

    task = asyncio.create_task(_consume_intra_dept(intra_rt))
    try:
        await task
    except asyncio.CancelledError:
        print("🔻 收到停止訊號，準備關閉...")
    finally:
        if not task.done():
            task.cancel()
        await intra_rt.drain()
        print("👋 已關閉連線")

if __name__ == "__main__":
    asyncio.run(main())