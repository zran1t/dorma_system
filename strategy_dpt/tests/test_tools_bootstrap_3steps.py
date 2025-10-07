# tests/test_tools_bootstrap_3steps.py
# [更新] 3 步驟外殼測試：reset → bootstrap → audit（已移除 --reset-mode）
from __future__ import annotations

import subprocess
import sys
from pathlib import Path
import pytest


@pytest.mark.parametrize("mode", ["init"])
def test_shell_reset_init_audit(mode):
    """
    子程序呼叫：
        python tools/bootstrap_channels.py --mode <mode> --reset --audit
    期望：
        - returncode == 0
    """
    project_root = Path(__file__).resolve().parents[1]
    tool = project_root / "tools" / "bootstrap_channels.py"
    assert tool.exists(), f"找不到外殼工具：{tool}"

    cmd = [
        sys.executable,
        str(tool),
        "--mode",
        mode,
        "--reset",  # 只保留新版參數
        "--audit",  # 稽核通過即 return 0
    ]
    print("執行命令：", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(project_root))
    assert proc.returncode == 0, f"外殼 3 合 1 失敗（return={proc.returncode}）"