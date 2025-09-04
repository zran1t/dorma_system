from pydantic import BaseModel
from typing import List, Dict, Any

class StrategyPools(BaseModel):
    strategy_id: str
    symbol: List[str]


class StrategySubmitRequest(BaseModel):
    usr_id: str
    strategies: List[StrategyPools]
