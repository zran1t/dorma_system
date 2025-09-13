# start_system.py
# 清單驅動啟動器：
# 1) 集中 Reset + Bootstrap + Audit（由 runtime_bootstrap.py 執行，並將報表落地）
# 2) 依清單逐一在新 Terminal 視窗啟動各 dispatcher/communicate

from __future__ import annotations

import asyncio
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List

from tools.runtime_bootstrap import centralized_reset_bootstrap

# ───────────────────────── 使用者可調區 ─────────────────────────

LAUNCH_ITEMS: List[dict] = [
    {
        "title": "初始化部門調度室（會再自行啟 kol 調度室）",
        "script": "initialization_dpt/dpt_dispatch_room/dispatcher.py",
        "auto_close": False,
        "delay_sec": 0,
    },
]

PY_EXE = sys.executable
EXTRA_ENV = {
    # "NATS_URL": "nats://127.0.0.1:4222",
}

# ───────────────────────── 內部實作區 ─────────────────────────

@dataclass
class LaunchItem:
    title: str
    script: str
    auto_close: bool = False
    delay_sec: int = 3


def _as_items(raw: List[dict]) -> List[LaunchItem]:
    return [LaunchItem(**r) for r in raw]


def _project_root() -> Path:
    # 以此檔所在路徑作為專案根
    return Path(__file__).resolve().parent


def _osascript_run(cmd: str) -> None:
    """在 macOS 開新 Terminal 視窗執行 cmd。"""
    osa = f'''tell application "Terminal"
        do script "{cmd}"
        activate
    end tell'''
    subprocess.run(["/usr/bin/osascript", "-e", osa], check=True)


def _compose_shell_cmd(project_root: Path, script_relpath: str, auto_close: bool, delay_sec: int) -> str:
    """
    在新視窗中執行單一 script，並注入 PYTHONPATH=PROJECT_ROOT 以確保跨資料夾 import 正常。
    """
    root_str = str(project_root)
    script = shlex.quote(script_relpath)
    py = shlex.quote(PY_EXE)
    export_py_path = f'export PYTHONPATH={shlex.quote(root_str)}:$PYTHONPATH'
    base = f"clear; cd {shlex.quote(root_str)} && {export_py_path} && {py} {script}"
    if auto_close:
        base = f"{base}; sleep {delay_sec}; exit"
    return base

# ───────────────────────── 主入口 ─────────────────────────

async def main() -> None:
    # 1) 集中 Reset + Bootstrap + Audit（一次性呼叫；也會把稽核報表落地）
    await centralized_reset_bootstrap(
        do_audit=True,
        inter_outfile="reports/inter_audit.json",
        init_outfile="reports/init_audit.json",
        timestamped_reports=True,
    )

    # 2) 逐一啟動清單內模組
    items = _as_items(LAUNCH_ITEMS)
    project_root = _project_root()

    if EXTRA_ENV:
        os.environ.update(EXTRA_ENV)

    print("====== 啟動各模組 ======")
    for item in items:
        cmd = _compose_shell_cmd(project_root, item.script, item.auto_close, item.delay_sec)
        print(f"▶ {item.title}  →  {item.script}")
        _osascript_run(cmd)

    print("✅ 全部啟動指令已送出（各視窗內自行常駐/退出）")


if __name__ == "__main__":
    asyncio.run(main())