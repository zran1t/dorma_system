# initialization_dpt/init_kline_kol/kol_dispatch_room/dispatcher.py
#!/usr/bin/env python3
import sys, shlex, subprocess
from pathlib import Path

FIXED_PY = "/Users/tingan/.pyenv/versions/quant-trading-py311/bin/python"

def _launch(project_root: Path, script_relpath: str, auto_close=False, delay_sec=3):
    if sys.platform != "darwin":
        raise RuntimeError("只支援 macOS")
    cmd = (
        f"clear; cd {shlex.quote(str(project_root))} && "
        f"export PYTHONPATH={shlex.quote(str(project_root))}:$PYTHONPATH && "
        f"{FIXED_PY} {shlex.quote(script_relpath)}"
    )
    if auto_close:
        cmd = f"{cmd}; sleep {delay_sec}; exit"
    osa = f'''tell application "Terminal"
        do script "{cmd}"
        activate
    end tell'''
    subprocess.run(["/usr/bin/osascript", "-e", osa], check=True)

if __name__ == "__main__":
    # 專案根 = dorma_system
    PROJECT_ROOT = Path(__file__).resolve().parents[3]  # ✅ 修正：3 而不是 4

    # 1) kol 通訊處室（常駐）
    KOL_COMM = "initialization_dpt/init_kline_kol/kol_comm_room/communicate.py"
    print("【KOL Dispatcher】啟動 kol_comm_room 通訊處室")
    _launch(PROJECT_ROOT, KOL_COMM, auto_close=False)