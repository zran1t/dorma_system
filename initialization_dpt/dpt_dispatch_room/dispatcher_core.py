# initialization_dpt/init_kline_kol/kol_dispatch_room/dispatcher_core.py
from __future__ import annotations

import pathlib
import subprocess
import platform
import shlex

__all__ = ["launch_submodules"]

# 這個檔在 .../initialization_dpt/init_kline_kol/kol_dispatch_room/
# 回到專案根要往上 3 級（parents[3]）
BASE_DIR = pathlib.Path(__file__).resolve().parents[3]

# 這裡列出要由「科別調度室」啟動的子模組（用 -m 的模組路徑）
SUB_MODULES = [
    "initialization_dpt.init_kline_kol.kol_comm_room.communicator",  # 科別通訊處室
    # 之後要加其他 kol 子模組（例如 watcher/consensus）就往下加
]

def _resolve_python(base_dir: pathlib.Path) -> str:
    """
    解析虛擬環境中的 python 執行路徑；找不到則退回系統 python。
    """
    if platform.system() == "Windows":
        cand = base_dir / ".venv" / "Scripts" / "python.exe"
        return str(cand) if cand.exists() else "python"
    else:
        cand = base_dir / ".venv" / "bin" / "python"
        return str(cand) if cand.exists() else "python3"

def launch_submodules() -> None:
    """
    在新終端/新程序中啟動 SUB_MODULES 列出的模組（使用 -m）。
    """
    py = _resolve_python(BASE_DIR)
    system = platform.system()

    for module in SUB_MODULES:
        if system == "Darwin":
            # 在 macOS 的 Terminal 開新視窗
            inner_cmd = f'clear; cd "{BASE_DIR}" && {shlex.quote(py)} -m {module}'
            inner_cmd_escaped = inner_cmd.replace("\\", "\\\\").replace('"', '\\"')
            osa = (
                'osascript -e '
                f'\'tell application "Terminal" to do script "{inner_cmd_escaped}"\''
            )
            subprocess.Popen(osa, shell=True)

        elif system == "Windows":
            # 在新 CMD 視窗執行，保留視窗
            cmd = (
                f'start "" cmd /k '
                f'cd /d "{BASE_DIR}" && {shlex.quote(py)} -m {module}'
            )
            subprocess.Popen(cmd, shell=True)

        else:
            # 其他 *nix：同視窗背景執行
            subprocess.Popen([py, "-m", module], cwd=BASE_DIR)

        print(f"✅ 科別調度室：子模組已在新終端啟動 → {module}")
