"""
File: system_initializer/steps/logging_setup.py
Module: system_initializer.steps.logging_setup

職責 (Responsibility):
    system_initializer 入口統一配置 logging（stdout + 檔案），並注入 run_id。
    提供「一次性」的 process-level logging 初始化，避免 infra_py 自行決定 log path 或重複掛載 handler。

注意事項 (Notes):
    - 本模組只負責 logging handler/formatter/run_id 注入，不負責任何業務流程。
    - 只允許 one-shot configure；若需重新配置，必須顯式呼叫 reset_logging()。
    - formatter 使用 UTC（time.gmtime）確保跨時區一致性。
    - 不得依賴 infra_py 或 application/domain 層模組，避免依賴方向反轉。
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


@dataclass(frozen=True)
class LoggingOptions:
    """
    LoggingOptions system_initializer logging 設定。

    功能:
        - 定義一次性 logging 初始化所需的參數集合。

    欄位說明:
        - log_dir: log 根目錄；None 表示走 env LOG_DIR 或預設策略。
        - level: logging level（INFO/DEBUG...）。
        - to_stdout: 是否輸出到 stdout（CI/開發用）。
        - filename: 指定檔名；None 表示使用預設 per-run 命名。
        - run_id: 注入到 log record 的 run_id；None 則自動產生。

    契約 / 限制:
        - configure_logging() 預設只能呼叫一次；重複呼叫會回傳既有 context，不會重掛 handler。

    備註:
        - 若 runner 會在同一 process 反覆跑（IDE/REPL），請在外部顯式呼叫 reset_logging() 再 configure。
    """
    log_dir: Optional[str | Path] = None
    level: int = logging.INFO
    to_stdout: bool = True
    filename: Optional[str] = None
    run_id: Optional[str] = None


@dataclass(frozen=True)
class LoggingContext:
    """
    LoggingContext logging 初始化結果。

    功能:
        - 回傳本次 logging 初始化的決策結果，供上層紀錄或跨模組傳遞 run_id。

    欄位說明:
        - run_id: 注入到 log record 的 run_id。
        - log_file: 實際 log file path。

    契約 / 限制:
        - run_id 應視為本次 initializer run 的唯一識別（不跨 process 保證）。

    備註:
        - 若未啟用檔案 handler（目前不支援），仍應維持 log_file 的決策一致性。
    """
    run_id: str
    log_file: Path


class _RunIdFilter(logging.Filter):
    """
    _RunIdFilter logging filter（注入 run_id）。

    功能:
        - 確保所有 log record 都帶有 run_id 欄位，使 formatter 可一致輸出。

    欄位說明:
        - _run_id: 本次 run 的識別。

    契約 / 限制:
        - 只做欄位注入，不做任何 routing 或 level 控制。

    備註:
        - 若 record 已有 run_id（例如上層自行注入），本 filter 不覆蓋。
    """

    def __init__(self, run_id: str) -> None:
        super().__init__()
        self._run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        # 確保 formatter 取得到 run_id 欄位（不覆蓋已存在值）
        if not hasattr(record, "run_id"):
            record.run_id = self._run_id
        return True


_CONFIGURED: bool = False
_CONTEXT: Optional[LoggingContext] = None


def configure_logging(opts: LoggingOptions) -> LoggingContext:
    """
    configure_logging 配置 root logging（一次性）。

    功能:
        - 依 opts 決定 log_dir/filename/run_id。
        - 建立 FileHandler（必要時建立 StreamHandler）。
        - 套用 UTC formatter 與 run_id filter。

    參數:
        - opts: LoggingOptions。

    回傳:
        - result: LoggingContext（run_id + log_file）。
        - error: OSError（建立目錄/檔案 handler 失敗時）。

    備註:
        - 本函式預設只允許一次性初始化；重複呼叫直接回傳既有 context，不做 reset。
        - 若需要重配，請先呼叫 reset_logging()。
    """
    global _CONFIGURED, _CONTEXT

    if _CONFIGURED and _CONTEXT is not None:
        return _CONTEXT

    run_id = (opts.run_id or os.urandom(8).hex()).strip()
    log_dir = Path(opts.log_dir).expanduser() if opts.log_dir else _default_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)

    filename = opts.filename or _default_filename(run_id)
    log_file = log_dir / filename

    root = logging.getLogger()
    root.setLevel(opts.level)

    # 清掉既有 handler，避免 REPL/pytest/IDE 重複掛載造成 log 重複
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = "%(asctime)s | %(levelname)s | %(name)s | run=%(run_id)s | %(message)s"
    formatter = logging.Formatter(fmt=fmt, datefmt="%Y-%m-%dT%H:%M:%SZ")
    formatter.converter = time.gmtime

    run_filter = _RunIdFilter(run_id)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.addFilter(run_filter)
    root.addHandler(fh)

    if opts.to_stdout:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        sh.addFilter(run_filter)
        root.addHandler(sh)

    _CONTEXT = LoggingContext(run_id=run_id, log_file=log_file)
    _CONFIGURED = True

    logging.getLogger(__name__).info("logging configured | path=%s | level=%s", str(log_file), opts.level)
    return _CONTEXT


def reset_logging() -> None:
    """
    reset_logging 清理 process 內已存在的 logging handlers 與狀態。

    功能:
        - 移除 root handlers。
        - 清理命名 logger handlers（避免舊版 get_logger(FileHandler...) 殘留）。
        - 重置 configure_logging() 的 one-shot 狀態，使其可再次初始化。

    參數:
        - param1: 無。
        - param2: 無。

    回傳:
        - result: 無。
        - error: 無。

    備註:
        - 僅建議在 REPL/IDE 反覆執行同 process 時使用；正式啟動流程不應呼叫。
    """
    global _CONFIGURED, _CONTEXT

    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    for obj in logging.root.manager.loggerDict.values():
        if isinstance(obj, logging.Logger):
            for h in list(obj.handlers):
                obj.removeHandler(h)
            obj.propagate = True
            obj.setLevel(logging.NOTSET)

    _CONFIGURED = False
    _CONTEXT = None


def _default_log_dir() -> Path:
    """
    _default_log_dir 決策預設 log dir。

    功能:
        - 依序決策：LOG_DIR env > repo-root/logs/system_initializer > cwd/logs/system_initializer。

    參數:
        - param1: 無。
        - param2: 無。

    回傳:
        - result: 預設 log dir Path。
        - error: 無。

    備註:
        - repo root 以 go.mod 作為 anchor。
    """
    env = os.getenv("LOG_DIR")
    if env and env.strip():
        return Path(env.strip()).expanduser()

    repo_root = _find_repo_root()
    if repo_root is not None:
        return repo_root / "logs" / "system_initializer"

    return Path.cwd() / "logs" / "system_initializer"


def _find_repo_root() -> Optional[Path]:
    """
    _find_repo_root 從目前檔案位置向上尋找 repo root。

    功能:
        - 以 go.mod 作為 repo root anchor。

    參數:
        - param1: 無。
        - param2: 無。

    回傳:
        - result: repo root Path 或 None。
        - error: 無。

    備註:
        - 若 repo 結構調整，需同步更新 anchor。
    """
    here = Path(__file__).resolve()
    for p in [here.parent, *here.parents]:
        if (p / "go.mod").exists():
            return p
    return None


def _default_filename(run_id: str) -> str:
    """
    _default_filename 生成 per-run 預設 log 檔名。

    功能:
        - 使用 UTC timestamp + run_id 前 8 碼，提供可排序與可追查性。

    參數:
        - run_id: 本次 run 的識別字串。

    回傳:
        - result: 檔名字串。
        - error: 無。

    備註:
        - timestamp 使用 UTC，避免跨時區排序混亂。
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"system_initializer_{ts}_{run_id[:8]}.log"
