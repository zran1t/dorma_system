"""
File: system_initializer/logger.py
Module: system_initializer.logger

職責 (Responsibility):
    提供 system_initializer 專用 logging bootstrap。
    以「單次啟動(trace) → 單一 log 檔案」作為唯一輸出模型。

注意事項 (Notes):
    - log 目錄固定為 logs/system_initializer/runs/<timestamp>_<trace_short>/
    - 每次 bootstrap 建立單一 run 資料夾
    - 所有 runtime artifacts (pid/log) 皆落於該 run_dir 底下
    - 使用 UTC 時間戳，避免跨時區混亂
    - 僅允許 bootstrap 一次；重複呼叫回傳相同 log_file
    - 不提供通用 logging 抽象；此模組只服務 system_initializer
    - system_initializer 為一次性 orchestrator；所有 runtime artifacts 僅對該 run 有效。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ------------------------------------------------------------------------------
# 全域狀態：trace-scoped logging context
# ------------------------------------------------------------------------------

_BOOTSTRAPPED = False
_CTX: Optional["LoggerContext"] = None


@dataclass(frozen=True)
class LoggerContext:
    """
    LoggerContext 初始化流程的 logging context。

    功能:
        - 保存本次 initializer 的 trace_id 與 log_file，供重入時取回。
        - 作為 system_initializer 內部「單次啟動」的識別資訊。

    欄位說明:
        - trace_id: 本次啟動識別碼，用於 log record 注入與檔名。
        - log_file: 本次啟動 log 檔完整路徑。
        - run_dir: 本次初始化 artifacts 根目錄（所有 runtime process 皆落於此）

    契約 / 限制:
        - 一個 process 只允許存在一個 active context。

    備註:
        - trace_id 不應依賴外部狀態（env/hostname），以避免 drift。
    """
    trace_id: str
    log_file: Path
    run_dir: Path


class _TraceIdFilter(logging.Filter):
    """
    _TraceIdFilter logging filter。

    功能:
        - 確保所有 log record 都具備 trace_id 欄位。
        - 以 root handler filter 方式套用，確保跨模組一致。
    """

    def __init__(self, trace_id: str) -> None:
        super().__init__()
        self._trace_id = trace_id

    def filter(self, record: logging.LogRecord) -> bool:
        # 確保 formatter 一定可取得 trace_id 欄位，避免 KeyError。
        if not hasattr(record, "trace_id"):
            record.trace_id = self._trace_id
        return True


def bootstrap_logger(
    *,
    level: int = logging.INFO,
    to_stdout: bool = False,
    trace_id: Optional[str] = None,
) -> LoggerContext:
    """
    bootstrap_logger 初始化 system_initializer logging。

    功能:
        - 建立本次 trace 的 trace_id 與 per-run log 檔案。
        - 設定 root logger（file handler + optional stdout handler）。
        - 使所有子模組 logger 自動落到同一份 log 檔案。

    參數:
        - level: logging level（INFO/DEBUG...）。
        - to_stdout: 是否同時輸出至 stdout（開發/CI 可開）。
        - trace_id: 外部指定的 trace_id；None 則自動產生。

    回傳:
        - result: LoggerContext（含 trace_id 與 log_file）。
        - error: 無；若已 bootstrap，回傳既有 context。

    備註:
        - 僅允許 bootstrap 一次；重複呼叫不會改變 handler 與格式。
        - trace_id 用於「全域單次啟動」關聯；此專案不區分 run_id/request_id。
    """
    global _BOOTSTRAPPED, _CTX

    if _BOOTSTRAPPED and _CTX is not None:
        return _CTX

    # [以純隨機識別碼確保每次啟動可獨立關聯，避免環境差異造成 drift。]
    resolved_trace_id = (trace_id or os.urandom(8).hex()).strip()

    # [固定輸出位置以降低決策分散；system_initializer 為唯一 log owner。]
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    short_id = resolved_trace_id[:8]

    run_folder_name = f"{ts}_{short_id}"
    base_dir = Path("logs/system_initializer/runs") / run_folder_name
    base_dir.mkdir(parents=True, exist_ok=True)

    log_file = base_dir / "system_initializer.log"

    # [root logger 為唯一收斂點；所有模組 logger 透過 propagate 匯入單一輸出。]
    root = logging.getLogger()
    root.setLevel(level)

    # [避免同一 process 重跑造成 handler 疊加，維持「單次啟動單一輸出」不變條件。]
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = "%(asctime)s | %(levelname)s | %(name)s | trace_id=%(trace_id)s | %(message)s"
    formatter = logging.Formatter(fmt=fmt, datefmt="%Y-%m-%dT%H:%M:%SZ")
    formatter.converter = time.gmtime  # 強制 UTC

    trace_filter = _TraceIdFilter(resolved_trace_id)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.addFilter(trace_filter)
    root.addHandler(fh)

    if to_stdout:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        sh.addFilter(trace_filter)
        root.addHandler(sh)

    _CTX = LoggerContext(
        trace_id=resolved_trace_id,
        log_file=log_file,
        run_dir=base_dir,
    )
    _BOOTSTRAPPED = True

    logging.getLogger(__name__).info(
        "logger bootstrapped | path=%s | level=%s",
        str(log_file),
        level,
    )
    return _CTX


def get_logger(name: str) -> logging.Logger:
    """
    get_logger 取得 logger。

    功能:
        - 回傳指定 name 的 logger，並依賴 root handlers 收斂至單一 log 檔案。

    參數:
        - name: logger name（建議用 __name__）。

    回傳:
        - result: logging.Logger。
        - error: 無。

    備註:
        - 本函式不負責 bootstrap；bootstrap 必須由 main() 在最前面呼叫。
    """
    return logging.getLogger(name)


def current_context() -> Optional[LoggerContext]:
    """
    current_context 取得目前 logging context。

    功能:
        - 讓 main() 或上層流程可取得 log_file/trace_id 用於回報或 debug。

    參數:
        - 無。

    回傳:
        - result: LoggerContext 或 None（尚未 bootstrap）。
        - error: 無。
    """
    return _CTX

def run_dir() -> Path:
    """
    功能:
        - run_dir 回傳本次啟動(run)的 artifacts 根目錄。
        - e.g. logs/system_initializer/runs/2026-02-13T15-04-21Z_ab12cd34/

    備註:
        - 此目錄由 bootstrap_logger 建立，為所有 runtime artifacts 的根目錄。
    """
    ctx = current_context()
    if ctx is None:
        raise RuntimeError("logger not bootstrapped: run_dir unavailable")
    return ctx.run_dir
