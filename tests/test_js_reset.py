# tests/test_js_reset.py
# 測試 JetStream 清理工具：真的刪除（drop）YAML 所列的 streams/consumers

from __future__ import annotations

import pytest

from common.js_reset import reset_channels_topology


# tests/test_js_reset.py

@pytest.mark.asyncio
async def test_reset_drop_init(cfg_init, nc_conn):
    """
    測試目標：
        - 對 initialization.yaml 進行清理（刪除 YAML 內列出的 consumers & streams）
        - 驗證回報 report 結構齊全、無例外
    """
    report = await reset_channels_topology(cfg_init, nc_conn)

    # 基本欄位檢查
    assert "trace_id" in report and "timestamp" in report
    assert {"dropped", "skipped", "errors"} <= set(report.keys())

    print("【Reset】已刪除：", report["dropped"])
    print("【Reset】略過：", report["skipped"])
    print("【Reset】錯誤：", report["errors"])