# initialization_dpt/dpt_dispatch_room/dispatcher.py
# 修正：在子終端啟動命令中注入 PYTHONPATH，指向專案根，確保可匯入 common/*

#!/usr/bin/env python3
import sys, shlex, subprocess
from pathlib import Path

FIXED_PY = "/Users/tingan/.pyenv/versions/quant-trading-py311/bin/python"

def _launch(project_root: Path, script_relpath: str, auto_close=False, delay_sec=3):
    """
    在新的 macOS Terminal 視窗中啟動指定腳本。

    Args:
        project_root (Path): 專案根目錄（將寫入 PYTHONPATH 與 cd）
        script_relpath (str): 以專案根為基準的相對路徑（例如 "initialization_dpt/dpt_comm_room/communicate.py"）
        auto_close (bool): 是否在結束後自動關閉終端
        delay_sec (int): 自動關閉前等待秒數
    """
    if sys.platform != "darwin":
        raise RuntimeError("只支援 macOS")

    # 將專案根加入 PYTHONPATH，讓子程序能夠 import common.*
    export_py_path = f'export PYTHONPATH={shlex.quote(str(project_root))}:$PYTHONPATH;'

    # 清畫面 -> 設定環境變數 -> 進入專案根 -> 執行指定腳本
    base_cmd = (
        f"clear; "
        f"{export_py_path} "
        f"cd {shlex.quote(str(project_root))} && "
        f"{FIXED_PY} {shlex.quote(script_relpath)}"
    )

    cmd = base_cmd
    if auto_close:
        cmd = f"{cmd}; sleep {delay_sec}; exit"

    osa = f'''tell application "Terminal"
        do script "{cmd}"
        activate
    end tell'''
    subprocess.run(["/usr/bin/osascript", "-e", osa], check=True)

if __name__ == "__main__":
    # 這支檔案位於：<ROOT>/initialization_dpt/dpt_dispatch_room/dispatcher.py
    # 專案根（ROOT）= parents[2]
    PROJECT_ROOT = Path(__file__).resolve().parents[2]   # ✅ 保持你先前修正

    # 1) 部門通訊處室（常駐）
    DPT_COMM = "initialization_dpt/dpt_comm_room/communicate.py"
    print("【DPT Dispatcher】啟動部門通訊處室")
    _launch(PROJECT_ROOT, DPT_COMM, auto_close=False)

    # 2) kol 調度室（延遲關閉）
    KOL_DISPATCHER = "initialization_dpt/init_kline_kol/kol_dispatch_room/dispatcher.py"
    print("【DPT Dispatcher】啟動 kol 調度室")
    _launch(PROJECT_ROOT, KOL_DISPATCHER, auto_close=True, delay_sec=3)