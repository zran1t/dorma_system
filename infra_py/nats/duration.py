"""
File: infra_py/nats/duration.py
Module: infra_py.nats.duration

職責 (Responsibility):
    提供 infra_py.nats 內部統一使用的 duration 解析與數值比較工具。
    確保 YAML duration / JetStream duration 的秒數表示一致，避免多處各寫一套造成 drift。

注意事項 (Notes):
    - 本模組只做純函式工具，不涉及 I/O 與 JetStream 操作。
    - duration 統一輸出為 seconds(float)，由上層決定容許誤差與序列化格式。
    - 不得依賴 system_initializer 或 application/domain 層模組。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import re
from typing import Any

# === 第三方套件 (Third-Party Libraries) ===
# 無

# === 系統內模組 (Internal Modules) ===
# 無


_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([a-zA-Z]+)?\s*$")


def parse_duration_seconds(value: Any, default_seconds: float) -> float:
    """
    parse_duration_seconds 將多型時間表示轉換為秒數(float)。

    功能:
        - 支援數值型（視為秒）與字串型（ms/s/m/h/d）duration。
        - 解析失敗時回退至 default_seconds，避免上層治理流程因設定瑕疵中斷。

    參數:
        - value: 時間表示（str/int/float/None）。
        - default_seconds: 解析失敗或缺省時使用的預設秒數。

    回傳:
        - result: seconds(float)。
        - error: 無。

    備註:
        - 本函式只負責 YAML/人類輸入的 parse；JetStream 回讀值解碼在 actual_loader 處理。
    """
    if value is None:
        return float(default_seconds)

    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip().lower()
    m = _DURATION_RE.match(s)
    if not m:
        return float(default_seconds)

    val = float(m.group(1))
    unit = (m.group(2) or "s").lower()

    if unit in ("s", "sec", "secs", "second", "seconds"):
        return val
    if unit in ("ms", "msec", "msecs", "millisecond", "milliseconds"):
        return val / 1000.0
    if unit in ("m", "min", "mins", "minute", "minutes"):
        return val * 60.0
    if unit in ("h", "hr", "hrs", "hour", "hours"):
        return val * 3600.0
    if unit in ("d", "day", "days"):
        return val * 86400.0

    return float(default_seconds)


def float_equal(a: Any, b: Any, tolerance: float) -> bool:
    """
    float_equal 判斷兩個數值是否在容許誤差內一致。

    功能:
        - 將輸入轉為 float 後以 tolerance 比對，避免浮點誤差造成重複 update 或誤判 mismatch。

    參數:
        - a: 左值（可為 None 或數值）。
        - b: 右值（可為 None 或數值）。
        - tolerance: 容許誤差（秒）。

    回傳:
        - result: 一致性判斷結果。
        - error: 無。

    備註:
        - None 以 0.0 視之，維持比對行為可預期。
    """
    fa = float(a or 0.0)
    fb = float(b or 0.0)
    return abs(fa - fb) <= float(tolerance)


def format_seconds(seconds: float) -> str:
    """
    format_seconds 將秒數格式化為可讀字串。

    功能:
        - 用於報告輸出與 log 觀測，提供一致的秒數表示格式。

    參數:
        - seconds: 秒數(float)。

    回傳:
        - result: 格式化後字串（例如 "30.0s"）。
        - error: 無。

    備註:
        - 此函式不做單位換算，只固定輸出 seconds。
    """
    return f"{float(seconds)}s"
