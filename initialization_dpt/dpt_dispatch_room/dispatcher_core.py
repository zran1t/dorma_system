# initialization_dpt/dpt_dispatch_room/dispatcher_core.py
import pathlib
import subprocess
import platform
import shlex

BASE_DIR = pathlib.Path(__file__).parent.parent.parent.resolve()

SUB_MODULES = [
    "initialization_dpt.dpt_comm_room.communicator",
    "initialization_dpt.init_kline_kol.kol_dispatch_room.dispatcher",
]

def _resolve_python(base_dir: pathlib.Path) -> str:
    if platform.system() == "Windows":
        cand = base_dir / ".venv" / "Scripts" / "python.exe"
        return str(cand) if cand.exists() else "python"
    else:
        cand = base_dir / ".venv" / "bin" / "python"
        return str(cand) if cand.exists() else "python3"

def launch_submodules():
    py = _resolve_python(BASE_DIR)
    system = platform.system()

    for module in SUB_MODULES:
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

        else:
            subprocess.Popen([py, "-m", module], cwd=BASE_DIR)

        print(f"✅ 子模組已在新終端啟動：{module}")