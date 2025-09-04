from fastapi import APIRouter
from ..service.injector_service import process_strategy_injection    # ← 兩個點：上一層
from ..schemas.strategy_pools import StrategySubmitRequest          # ← 兩個點：上一層

router = APIRouter()

@router.post("/inject")
async def inject_entry(strategy: StrategySubmitRequest):
    await process_strategy_injection(strategy)
    return {"status": "ok"}