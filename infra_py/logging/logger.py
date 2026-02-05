"""
File: infra_py/logger.py
Module: infra_py.logger

職責 (Responsibility):
    提供 infra 層級統一之 logging 建立工具，
    確保所有基礎設施模組具備一致的輸出格式與落地策略。

注意事項 (Notes):
    - 本模組僅負責 logger 初始化，不承擔任何業務語意。
    - logger 為 process-local singleton，避免重複建立 handler。
    - 不得依賴 application 或 domain 層模組。
"""

# === 標準函式庫 (Standard Library) ===
import logging
from pathlib import Path
from typing import Optional

# === 第三方套件 (Third-Party Libraries) ===
# 無

# === 系統內模組 (Internal Modules) ===
# 無


def get_logger(name: str, *, subdir: str) -> logging.Logger:
    """
    get_logger 建立或取得指定名稱之 logger。

    功能:
        - 建立具備檔案輸出能力之 logger。
        - 統一 infra 層 log 格式與輸出位置。

    參數:
        - name: logger 名稱，通常使用 __name__。
        - subdir: logs 目錄下的子目錄名稱。

    回傳:
        - result: 已初始化完成之 Logger。
        - error: 無。

    備註:
        - 同名 logger 僅會初始化一次。
        - log 等級預設為 INFO。
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    base_dir = Path.cwd() / "logs" / subdir
    base_dir.mkdir(parents=True, exist_ok=True)

    log_file = base_dir / "infra.log"

    handler = logging.FileHandler(log_file, encoding="utf-8")
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False

    return logger
