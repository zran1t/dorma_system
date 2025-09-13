from fastapi import APIRouter
from ..service.service_entry import run_init_service   
from ..schemas.strategy_pools import StrategySubmitRequest          

router = APIRouter()

@router.post("/inject")
async def inject_entry(strategy: StrategySubmitRequest):
    await run_init_service(strategy)
    return {"status": "ok"}