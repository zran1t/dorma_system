# start_system.py
import subprocess
import pathlib
import sys
import shlex
import platform

BASE_DIR = pathlib.Path(__file__).parent.resolve()

# 用「模組路徑」而不是檔案路徑
MODULES = [
    "initialization_dpt.dpt_dispatch_room.dispatcher",
    # "risk_dpt.dispatch_room.dispatcher",
    # "strategy_dpt.dispatch_room.dispatcher",
    # "data_dpt.dispatch_room.dispatcher",
    # "capital_dpt.dispatch_room.dispatcher",
]

def _resolve_python(base_dir: pathlib.Path) -> str:
    if platform.system() == "Windows":
        cand = base_dir / ".venv" / "Scripts" / "python.exe"
        return str(cand) if cand.exists() else "python"
    else:
        cand = base_dir / ".venv" / "bin" / "python"
        return str(cand) if cand.exists() else "python3"

def launch_all():
    py = _resolve_python(BASE_DIR)
    system = platform.system()

    for module in MODULES:
        if system == "Darwin":
            inner_cmd = f'clear; cd "{BASE_DIR}" && {shlex.quote(py)} -m {module}'
            inner_cmd_escaped = inner_cmd.replace("\\", "\\\\").replace('"', '\\"')
            osa = (
                'osascript -e '
                f'\'tell application "Terminal" to do script "{inner_cmd_escaped}"\''
            )
            subprocess.Popen(osa, shell=True)

        elif system == "Windows":
            cmd = (
                f'start "" cmd /k '
                f'cd /d "{BASE_DIR}" && {shlex.quote(py)} -m {module}'
            )
            subprocess.Popen(cmd, shell=True)

        else:  # Linux / 其他
            # 想開新視窗可改用：gnome-terminal --  / x-terminal-emulator -e 等
            subprocess.Popen([py, "-m", module], cwd=BASE_DIR)

        print(f"✅ 啟動：{module}")

    print("✅ 所有模組已啟動（模組模式）")
    sys.exit(0)

if __name__ == "__main__":
    launch_all()