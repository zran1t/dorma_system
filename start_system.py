# start_system.py
from __future__ import annotations
"""
啟動系統腳本：
- 以「模組模式（python -m）」啟動各部門的「部門調度室（dispatcher）」
- 每個子模組在新終端視窗啟動（macOS: Terminal, Windows: cmd, 其他：當前視窗前景/背景）
- 路徑固定到專案根，避免相對路徑飄移

輸出全部為繁體中文，便於閱讀與排障。
"""

import subprocess
import pathlib
import sys
import shlex
import platform

BASE_DIR = pathlib.Path(__file__).parent.resolve()

# === 以「模組路徑」列出要啟動的部門調度室 ===
MODULES = [
    "initialization_dpt.dpt_dispatch_room.dispatcher",
    # "risk_dpt.dispatch_room.dispatcher",
    # "strategy_dpt.dispatch_room.dispatcher",
    # "data_dpt.dispatch_room.dispatcher",
    # "capital_dpt.dispatch_room.dispatcher",
]


def _resolve_python(base_dir: pathlib.Path) -> str:
    """
    解析可執行的 Python 路徑：
    - 優先使用專案根下 .venv
    - 否則 macOS/Linux 使用 'python3'，Windows 使用 'python'
    """
    if platform.system() == "Windows":
        cand = base_dir / ".venv" / "Scripts" / "python.exe"
        return str(cand) if cand.exists() else "python"
    else:
        cand = base_dir / ".venv" / "bin" / "python"
        return str(cand) if cand.exists() else "python3"


def _launch_module_in_new_terminal(py: str, module: str) -> None:
    """
    依作業系統在新終端視窗啟動指定模組。
    """
    system = platform.system()

    if system == "Darwin":
        # macOS：使用 AppleScript 打開新的 Terminal 視窗
        inner_cmd = f'clear; cd "{BASE_DIR}" && {shlex.quote(py)} -m {module}'
        inner_cmd_escaped = inner_cmd.replace("\\", "\\\\").replace('"', '\\"')
        osa = (
            'osascript -e '
            f'\'tell application "Terminal" to do script "{inner_cmd_escaped}"\''
        )
        subprocess.Popen(osa, shell=True)

    elif system == "Windows":
        # Windows：在新 cmd 視窗執行，保留視窗（/k）
        cmd = (
            f'start "" cmd /k '
            f'cd /d "{BASE_DIR}" && {shlex.quote(py)} -m {module}'
        )
        subprocess.Popen(cmd, shell=True)

    else:
        # 其他 *nix：於當前終端啟動（如需新視窗可改用 gnome-terminal / xterm 等）
        subprocess.Popen([py, "-m", module], cwd=BASE_DIR)


def launch_all() -> None:
    """
    逐一啟動 MODULES 內的部門調度室。
    """
    py = _resolve_python(BASE_DIR)

    if not MODULES:
        print("⚠️  沒有待啟動的模組（MODULES 為空）。")
        sys.exit(1)

    for module in MODULES:
        try:
            _launch_module_in_new_terminal(py, module)
            print(f"✅ 已啟動：{module}")
        except Exception as e:
            print(f"❌ 啟動失敗：{module}（{e}）")

    print("✅ 所有模組已嘗試啟動（模組模式）")
    sys.exit(0)


if __name__ == "__main__":
    launch_all()
