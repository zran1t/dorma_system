"""
File: system_initializer/main.py
Module: system_initializer.main

職責 (Responsibility):
    system_initializer 程式入口。
    統一配置 logging，並依序執行 inter / intra 的 JetStream 控制面拓樸治理。

注意事項 (Notes):
    - 本檔案是 application entry；不得被 infra_py 依賴。
    - steps/* 為可重用步驟模組，本 entry 只負責流程編排。
    - 若要支援更多 channel，應在此處擴充流程，而非把入口邏輯塞回 steps。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import asyncio

# === 第三方套件 (Third-Party Libraries) ===
# 無

# === 系統內模組 (Internal Modules) ===
from system_initializer.steps.logging_setup import configure_logging, LoggingOptions
from system_initializer.steps.nats_bootstrap import run_nats_bootstrap, NatsBootstrapOptions


async def main() -> None:
    """
    main 執行 system_initializer 啟動流程。

    功能:
        - 配置 logging（一次性）。
        - 依序執行 inter / intra 的 reset/bootstrap/audit。

    參數:
        - param1: 無。
        - param2: 無。

    回傳:
        - result: 無。
        - error: 例外上拋（由 process runner 決定 exit code 策略）。

    備註:
        - 若要在 CI 分段跑，可直接呼叫 steps/nats_bootstrap.py 的 CLI。
    """
    configure_logging(
        LoggingOptions(
            log_dir=None,
            level=20,
            to_stdout=False,
        )
    )

    await run_nats_bootstrap(NatsBootstrapOptions(config_path="configs/channels/inter.yaml"))
    await run_nats_bootstrap(NatsBootstrapOptions(config_path="configs/channels/intra.yaml"))


if __name__ == "__main__":
    asyncio.run(main())
