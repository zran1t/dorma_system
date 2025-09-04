import yaml
from pathlib import Path
from typing import Dict

class RedisConfig:
    def __init__(self, mapping: Dict[str, str]):
        self.mapping = mapping

    @classmethod
    def load(cls, path: Path) -> "RedisConfig":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return cls(data)

    def get_url(self, name: str) -> str:
        if name not in self.mapping:
            raise KeyError(f"Redis pool '{name}' not found in config")
        return self.mapping[name]