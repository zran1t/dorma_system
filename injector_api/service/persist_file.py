# injector_api/service/persist_file.py
from __future__ import annotations
import os
import json
import hashlib
from pathlib import Path
from typing import Any, Dict, Tuple

from ..schemas.strategy_pools import StrategySubmitRequest  # pydantic v2 model


# 專案根 / 預設資料夾
ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "pool" / "strategy"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _stable_bytes(obj: Any) -> bytes:
    """排序 key、去多餘空白後轉 bytes（用於 hash / publish）"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    """原子寫檔：.tmp → fsync → replace"""
    tmp = path.with_suffix(path.suffix + ".tmp")
    b = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    with open(tmp, "wb") as f:
        f.write(b)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def snapshot_and_write(
    strategy: StrategySubmitRequest,
    version: int,
    source: str = "api",
    data_dir: Path = DATA_DIR,
) -> Tuple[Path, str, bytes, Dict[str, Any]]:
    """
    依照傳入的 version 產生快照並落地成檔。
    回傳：(file_path, content_hash, payload_bytes, snapshot_dict)
    """
    payload_obj = strategy.model_dump()                 
    payload_bytes = _stable_bytes(payload_obj)
    content_hash = hashlib.sha256(payload_bytes).hexdigest()

    snapshot = {
        "version": version,
        "hash": content_hash,
        "payload": payload_obj,
        "source": source,
    }

    fname = f"strategy_pool_{strategy.usr_id}_v{version}.json"
    fpath = data_dir / fname
    _atomic_write_json(fpath, snapshot)

    return fpath, content_hash, payload_bytes, snapshot