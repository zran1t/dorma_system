"""
File: infra_py/logging/logger.py
Module: infra_py.logging.logger

職責 (Responsibility):
    提供 infra 層級統一之 logger 取得工具（library-safe），
    不在此處決定 handler / log path（由 entrypoint/system_initializer 負責）。

注意事項 (Notes):
    - 本模組僅負責 get_logger，不做 logging.configure。
    - 預設加上 NullHandler，避免 library 使用者未配置 logging 時產生警告。
    - 不得依賴 application 或 domain 層模組。
"""

from __future__ import annotations

import logging


def get_logger(name: str) -> logging.Logger:
    """
    get_logger 取得指定名稱之 logger（不做 handler/path 決策）。

    功能:
        - 回傳 logger instance。
        - 若 logger 尚未配置任何 handler，會加上 NullHandler 避免警告。
        - 不設定 level；由 root/entrypoint 決定。

    參數:
        - name: logger 名稱，通常使用 __name__。

    回傳:
        - result: Logger。
    """
    logger = logging.getLogger(name)

    # 若已經有 handler（例如 entrypoint 配置了），直接返回
    if logger.handlers:
        return logger

    # library-safe：避免 "No handler could be found..." 類警告
    logger.addHandler(logging.NullHandler())

    # 讓 root 的 handler 能接到（若 entrypoint 有配置 propagate/root handler）
    logger.propagate = True

    return logger
