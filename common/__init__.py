# common/__init__.py
"""
Common 模組對外匯出入口。
主推介使用：
- BusConfig / BusRuntime
- RedisConfig / RedisConfigError

為了向下相容，提供：
- ChannelConfig = BusConfig
- ChannelRuntime = BusRuntime
"""

from .bus_config import BusConfig as BusConfig
from .bus_runtime import BusRuntime as BusRuntime
from .redis_cfg import RedisConfig, RedisConfigError

# ---- Backward compatibility (舊名別名，不建議新碼再用) ----
ChannelConfig = BusConfig
ChannelRuntime = BusRuntime

__all__ = [
    "BusConfig",
    "BusRuntime",
    "RedisConfig",
    "RedisConfigError",
    # 舊名（保留相容）
    "ChannelConfig",
    "ChannelRuntime",
]
