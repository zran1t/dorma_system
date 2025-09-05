# common/redis_cfg.py
from __future__ import annotations

import yaml
from pathlib import Path
from typing import Dict, Any


class RedisConfigError(RuntimeError):
    """用於回報 Redis 配置相關錯誤的自訂例外。"""


class RedisConfig:
    """
    Redis pool URL 映射配置。

    功能：
      1) 從 YAML 載入 pool name -> Redis URL 映射
      2) 提供 get_url() 依名稱取 URL
      3) 可透過 load() 或 load_from_text() 建立

    YAML 格式範例：
    ```yaml
    default: "redis://localhost:6379/0"
    cache: "redis://localhost:6380/0"
    ```
    """

    def __init__(self, mapping: Dict[str, str]):
        if not isinstance(mapping, dict):
            raise RedisConfigError(f"配置錯誤：mapping 必須是 dict[str, str]，取得 {type(mapping)}")
        # 確認所有值都是字串
        for k, v in mapping.items():
            if not isinstance(v, str):
                raise RedisConfigError(f"配置錯誤：pool '{k}' 的值必須為字串 URL，取得 {type(v)}")
        self.mapping: Dict[str, str] = mapping

    # ----- Loaders -------------------------------------------------------------

    @classmethod
    def load(cls, path: Path) -> "RedisConfig":
        """
        從 YAML 檔案載入 Redis pool 配置。

        參數:
            path (Path): YAML 檔案路徑

        回傳:
            RedisConfig: 配置物件
        """
        try:
            text = Path(path).read_text(encoding="utf-8")
        except Exception as e:
            raise RedisConfigError(f"讀取 YAML 失敗：{path} ({e})") from e
        return cls.load_from_text(text)

    @classmethod
    def load_from_text(cls, text: str) -> "RedisConfig":
        """
        從 YAML 字串建立 RedisConfig（便於測試）。
        """
        try:
            data: Any = yaml.safe_load(text) or {}
        except Exception as e:
            raise RedisConfigError(f"YAML 解析失敗：{e}") from e
        if not isinstance(data, dict):
            raise RedisConfigError(f"配置錯誤：YAML 根必須是 dict，取得 {type(data)}")
        return cls(data)

    # ----- Accessors -----------------------------------------------------------

    def get_url(self, name: str) -> str:
        """
        取得指定 pool name 的 Redis URL。

        參數:
            name (str): pool 名稱

        回傳:
            str: Redis URL

        例外:
            RedisConfigError: 若 pool 不存在
        """
        if name not in self.mapping:
            raise RedisConfigError(f"Redis pool '{name}' 不存在於配置中")
        return self.mapping[name]
