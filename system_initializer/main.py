"""
File: system_initializer/main.py
Module: system_initializer.main

職責 (Responsibility):
    作為 system_initializer 之唯一啟動入口。
    負責初始化 logging、控制整體初始化順序，並串接各個 bootstrap step。

注意事項 (Notes):
    - 本模組只做流程 orchestration，不包含任何 infra 細節。
    - logging 必須在任何 step 執行前初始化完成。
    - 初始化流程策略=B：會先 stop 舊 nats-server 再重新啟動（避免舊狀態污染）。
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
from system_initializer.steps.nats_bootstrap import NatsBootstrapOptions, run_nats_bootstrap
from system_initializer.steps.nats_server_runtime import NatsServerRuntimeOptions, restart_nats_server

# from system_initializer.steps.redis_bootstrap import run_redis_bootstrap
# from system_initializer.steps.redis_server_runtime import restart_redis_server


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
        - NATS server runtime 與 JetStream topology bootstrap 必須分離（錯誤語意不同）。
    """
    logger = get_logger(__name__)

    try:
        logger.info("system initializer start")

        # ------------------------------------------------------------------
        # Step 0: NATS Server Runtime (stop → start → ready)
        # ------------------------------------------------------------------
        restart_nats_server(
            NatsServerRuntimeOptions(
                conf_path="configs/nats/server.conf",
                # enable_http_probe=True  # [若 conf 有 http: <port>，會補做 /varz probe。]
            )
        )
        logger.info("nats server runtime ready")

        # ------------------------------------------------------------------
        # Step 1: NATS JetStream Topology (inter)
        # ------------------------------------------------------------------
        inter_report = await run_nats_bootstrap(
            NatsBootstrapOptions(
                config_path="configs/channels/inter.yaml",
                do_reset=True,
                do_bootstrap=True,
                do_audit=True,
            )
        )
        if not inter_report.get("ok", False):
            logger.error("nats bootstrap failed | scope=inter")
            return 1
        logger.info("nats bootstrap finished | scope=inter")

        # ------------------------------------------------------------------
        # Step 2: NATS JetStream Topology (intra)
        # ------------------------------------------------------------------
        intra_report = await run_nats_bootstrap(
            NatsBootstrapOptions(
                config_path="configs/channels/intra.yaml",
                do_reset=True,
                do_bootstrap=True,
                do_audit=True,
            )
        )
        if not intra_report.get("ok", False):
            logger.error("nats bootstrap failed | scope=intra")
            return 1
        logger.info("nats bootstrap finished | scope=intra")

        # ------------------------------------------------------------------
        # Step 3: Redis (預留)
        # ------------------------------------------------------------------
        # restart_redis_server(...)
        # redis_report = await run_redis_bootstrap(...)
        # if not redis_report.get("ok", False):
        #     logger.error("redis bootstrap failed")
        #     return 1

        logger.info("system initializer completed")
        return 0

    except Exception as e:
        logger.exception("initializer crashed | err=%r", e)
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

    # [在任何 step 執行前建立唯一的 per-run log 檔案，確保可觀測性收斂。]
    bootstrap_logger()

    return asyncio.run(run_initializer())


if __name__ == "__main__":
    sys.exit(main())
