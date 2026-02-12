"""
File: system_initializer/main.py
Module: system_initializer.main

職責 (Responsibility):
    作為 system_initializer 之唯一啟動入口。
    負責初始化 logging、控制整體初始化順序，並串接各個 bootstrap step。

注意事項 (Notes):
    - 本模組只做流程 orchestration，不包含任何 infra 細節。
    - logging 必須在任何 step 執行前初始化完成。
    - 任一步驟失敗將中止整體初始化流程。
    - 不得被 infra 層依賴。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import asyncio
import sys
from typing import Optional

# === 系統內模組 (Internal Modules) ===
from system_initializer.logger import bootstrap_logger, get_logger
from system_initializer.steps.nats_bootstrap import (
    NatsBootstrapOptions,
    run_nats_bootstrap,
)
# from system_initializer.steps.redis_bootstrap import run_redis_bootstrap
# from system_initializer.steps.xxx_bootstrap import run_xxx_bootstrap


async def run_initializer() -> int:
    """
    run_initializer 執行 system 初始化流程。

    功能:
        - 依固定順序執行各 bootstrap step。
        - 若任一步驟失敗則中止並回傳非零 exit code。

    參數:
        - 無。

    回傳:
        - result: 0 表示成功；1 表示初始化失敗。
        - error: 無（例外於此層攔截並轉換為 exit code）。

    備註:
        - 初始化順序應保持穩定，避免隱性依賴反轉。
        - 若日後新增 step，應明確標示其相依順序。
    """
    logger = get_logger(__name__)

    try:
        logger.info("event=initializer_start")

        # ------------------------------------------------------------------
        # Step 1: NATS JetStream Topology (inter/intra)
        # ------------------------------------------------------------------
        nats_configs = [
            ("inter", "configs/channels/inter.yaml"),
            ("intra", "configs/channels/intra.yaml"),
        ]

        for scope, config_path in nats_configs:
            logger.info("event=nats_scope_start | scope=%s | config=%s", scope, config_path)

            nats_opts = NatsBootstrapOptions(
                config_path=config_path,
                do_reset=True,
                do_bootstrap=True,
                do_audit=True,
                # trace_id 不用傳：會自動用 logger context 的 trace_id
            )

            nats_report = await run_nats_bootstrap(nats_opts)

            if not nats_report.get("ok", False):
                logger.error("event=nats_scope_failed | scope=%s", scope)
                return 1

            logger.info("event=nats_scope_done | scope=%s", scope)

        logger.info("event=nats_all_done")

        # ------------------------------------------------------------------
        # Step 2: Redis (預留)
        # ------------------------------------------------------------------
        # redis_report = await run_redis_bootstrap(...)
        # if not redis_report.get("ok", False):
        #     logger.error("event=redis_failed")
        #     return 1

        # ------------------------------------------------------------------
        # Step 3: Other Initialization (預留)
        # ------------------------------------------------------------------
        # await run_xxx_bootstrap(...)

        logger.info("event=initializer_done")
        return 0

    except Exception as e:
        logger.exception("event=initializer_crash | err=%r", e)
        return 1


def main(argv: Optional[list[str]] = None) -> int:
    """
    main 同步入口函式。

    功能:
        - 初始化 logging。
        - 啟動 asyncio event loop 執行 run_initializer。

    參數:
        - argv: CLI 參數（保留未來擴充）。

    回傳:
        - result: process exit code。
        - error: 無。

    備註:
        - logger 必須在此層 bootstrap，避免 step 自行建立 handler。
    """
    _ = argv  # [保留參數介面以利未來擴充，避免破壞呼叫契約。]

    bootstrap_logger()
    return asyncio.run(run_initializer())


if __name__ == "__main__":
    sys.exit(main())
