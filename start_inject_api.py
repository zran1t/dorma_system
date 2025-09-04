# run_uvicorn.py
import os
import platform
import shlex
import pathlib
import subprocess

# === 專案根目錄（以本檔所在目錄為準）===
BASE_DIR = pathlib.Path(__file__).parent.resolve()

# === 服務設定 ===
APP_PATH = "injector_api.main:app"
RELOAD = True

# === 可選：若你想強制用 pyenv 的特定環境，改為 True 並設定 ENV_NAME ===
USE_PYENV_FALLBACK = False
ENV_NAME = "quant-trading-py311"  # 只有在 USE_PYENV_FALLBACK=True 才會用到

def _resolve_python(base_dir: pathlib.Path) -> str:
    """
    優先使用專案內 .venv 的 Python；找不到再回退系統 python/python3。
    這樣可以避免新視窗沒有載入 pyenv 而找不到套件的問題。
    """
    if platform.system() == "Windows":
        cand = base_dir / ".venv" / "Scripts" / "python.exe"
        return str(cand) if cand.exists() else "python"
    else:
        cand = base_dir / ".venv" / "bin" / "python"
        return str(cand) if cand.exists() else "python3"

def _build_uvicorn_cmd(py_exe: str) -> str:
    reload_flag = "--reload" if RELOAD else ""
    # 一律用 python -m 方式，繞開 console script 的 PATH 依賴
    return f'{shlex.quote(py_exe)} -m uvicorn {APP_PATH} {reload_flag}'.strip()

def run_uvicorn():
    system = platform.system()
    py_exe = _resolve_python(BASE_DIR)

    # 預設路徑：.venv → 系統 Python
    base_cmd = _build_uvicorn_cmd(py_exe)

    # （可選）回退方案：強制用 pyenv 指定環境
    # 注意：這段會依賴 shell 有 pyenv 指令（Windows 需安裝 pyenv-win）。
    if USE_PYENV_FALLBACK:
        if system == "Windows":
            base_cmd = f'pyenv exec python -m uvicorn {APP_PATH} {"--reload" if RELOAD else ""}'.strip()
        else:
            # macOS/Linux：用 login shell 初始化 pyenv，再切環境執行
            base_cmd = (
                'zsh -lc '
                + shlex.quote(
                    f'eval "$(pyenv init -)"; '
                    f'eval "$(pyenv virtualenv-init -)"; '
                    f'pyenv shell {ENV_NAME}; '
                    f'python -m uvicorn {APP_PATH} {"--reload" if RELOAD else ""}'
                )
            )

    if system == "Darwin":
        # macOS：AppleScript 新開 Terminal，先清畫面、切目錄，再執行
        inner_cmd = f'clear; cd "{BASE_DIR}" && {base_cmd}'
        # 轉義給 AppleScript
        inner_cmd_escaped = inner_cmd.replace("\\", "\\\\").replace('"', '\\"')
        osa = f'osascript -e \'tell application "Terminal" to do script "{inner_cmd_escaped}"\''
        subprocess.Popen(osa, shell=True)
        print("✅ 已在新的 macOS Terminal 視窗啟動 uvicorn")

    elif system == "Windows":
        # Windows：新開 cmd 視窗（/k 保留視窗，方便看 log）
        cmd = f'start "" cmd /k cd /d "{BASE_DIR}" && {base_cmd}'
        subprocess.Popen(cmd, shell=True)
        print("✅ 已在新的 Windows cmd 視窗啟動 uvicorn")

    else:
        # Linux（若有需要可自行改為 gnome-terminal 等）
        subprocess.Popen(base_cmd, cwd=BASE_DIR, shell=True)
        print("✅ 已在目前終端機背景啟動 uvicorn（Linux）")

if __name__ == "__main__":
    run_uvicorn()