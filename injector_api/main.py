from fastapi import FastAPI
from .routes.strategy_pools import router as strategy_router  # ← 相對匯入（同 package）

app = FastAPI()
app.include_router(strategy_router, prefix="/strategy", tags=["strategy"])