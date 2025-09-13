# tests/test_channel_loader.py
# [改為 pytest 版] 驗證 channel_config_loader：
#   - 以 pytest 參數化測試 init / inter 兩份 YAML
#   - 列印固定路由、模板展開、streams / consumers，便於人工對照
#   - 加入基本斷言，確保關鍵欄位存在且型別正確

from __future__ import annotations

from typing import Any, Dict, List

import pytest

# 由 tests/conftest.py 提供以下 fixtures：
# - cfg_init / cfg_inter：分別載入 initialization.yaml / inter.yaml 的 dict


def _print_header(title: str) -> None:
    """印出分隔標題，提升輸出可讀性。"""
    print("\n" + title)
    print("=" * 60)


def _print_init(cfg: Dict[str, Any]) -> None:
    """列印 initialization.yaml 展開內容（純列印，配合下方斷言驗證鍵值存在與型別正確）。"""
    subjects = cfg.get("subjects", {})

    _print_header("[固定路由]")
    for key in ("department_broadcast", "department_comm_in", "department_comm_out"):
        print(f"{key:25s} = {subjects.get(key)}")

    _print_header("[模板路由展開（kols）]")
    kols: List[str] = subjects.get("kols", []) or []
    if not kols:
        print("（無 kols 清單）")
    else:
        for kol in kols:
            to_kol = subjects["to_kol_tpl"].format(kol=kol)
            to_kol_deliver = subjects["to_kol_deliver"].format(kol=kol)
            from_kol = subjects["from_kol_tpl"].format(kol=kol)
            print(f"to_kol ({kol:12s})        = {to_kol}")
            print(f"to_kol_deliver ({kol:12s})= {to_kol_deliver}")
            print(f"from_kol ({kol:12s})      = {from_kol}")

    _print_header("[JetStream Streams]")
    for s in cfg.get("jetstream", {}).get("streams", []):
        print(f"- {s['name']}: {s['subjects']}")

    _print_header("[JetStream Consumers]")
    for c in cfg.get("jetstream", {}).get("consumers", []):
        print(
            f"- {c['name']} (stream={c['stream']}, "
            f"filter={c['filter_subject']}, deliver={c['deliver_subject']}, "
            f"ack={c['ack_policy']})"
        )


def _print_inter(cfg: Dict[str, Any]) -> None:
    """列印 inter.yaml 展開內容（純列印，配合下方斷言驗證鍵值存在與型別正確）。"""
    subjects = cfg.get("subjects", {})
    depts: List[str] = subjects.get("departments", []) or []

    _print_header("[模板（Publisher / Subscriber）]")
    print(f"to_department_tpl     = {subjects.get('to_department_tpl')}")
    print(f"to_department_deliver = {subjects.get('to_department_deliver')}")
    print(f"from_department_tpl   = {subjects.get('from_department_tpl')}")

    _print_header("[部門清單]")
    print(", ".join(depts) if depts else "（無部門清單）")

    _print_header("[模板路由展開（每個部門）]")
    if not depts:
        print("（無可展開部門）")
    else:
        for dept in depts:
            to_dept = subjects["to_department_tpl"].format(dept=dept)
            to_dept_deliver = subjects["to_department_deliver"].format(dept=dept)
            from_dept = subjects["from_department_tpl"].format(dept=dept)
            print(f"[{dept}]")
            print(f"  to_department       = {to_dept}")
            print(f"  to_department_deliv = {to_dept_deliver}")
            print(f"  from_department     = {from_dept}")

    _print_header("[JetStream Streams]")
    for s in cfg.get("jetstream", {}).get("streams", []):
        print(f"- {s['name']}: {s['subjects']}")

    _print_header("[JetStream Consumers]")
    for c in cfg.get("jetstream", {}).get("consumers", []):
        print(
            f"- {c['name']} (stream={c['stream']}, "
            f"filter={c['filter_subject']}, deliver={c['deliver_subject']}, "
            f"ack={c['ack_policy']})"
        )


@pytest.mark.parametrize("mode", ["init", "inter"])
def test_channel_loader_print_and_basic_asserts(mode: str, cfg_init, cfg_inter) -> None:
    """
    以 pytest 參數化驗證兩個 channels 設定：
      - 列印固定路由 / 模板展開 / streams / consumers，供人工對照
      - 基本斷言：關鍵鍵值存在、型別合理
    """
    cfg = cfg_init if mode == "init" else cfg_inter

    assert isinstance(cfg, dict), "YAML 載入結果應為 dict"

    # subjects 區塊存在
    subjects = cfg.get("subjects")
    assert isinstance(subjects, dict), "subjects 區塊缺失或型別錯誤"

    # jetstream.streams / consumers 應為 list
    js = cfg.get("jetstream") or {}
    assert isinstance(js.get("streams", []), list), "jetstream.streams 應為 list"
    assert isinstance(js.get("consumers", []), list), "jetstream.consumers 應為 list"

    # 逐項列印（輔助人工檢視）
    if mode == "init":
        _print_init(cfg)
        # 初始化場景的最小關鍵斷言
        for key in ("department_broadcast", "department_comm_in", "department_comm_out"):
            assert isinstance(subjects.get(key), str) and subjects.get(key), f"{key} 必須為非空字串"
        # 模板存在性
        for key in ("to_kol_tpl", "to_kol_deliver", "from_kol_tpl"):
            assert isinstance(subjects.get(key), str) and subjects.get(key), f"{key} 模板缺失"
    else:
        _print_inter(cfg)
        # inter 場景的最小關鍵斷言
        for key in ("to_department_tpl", "to_department_deliver", "from_department_tpl"):
            assert isinstance(subjects.get(key), str) and subjects.get(key), f"{key} 模板缺失"
        assert isinstance(subjects.get("departments", []), list), "departments 應為 list"