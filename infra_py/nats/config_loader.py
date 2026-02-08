"""
File: infra_py/nats/config_loader.py
Module: infra_py.nats.config_loader

職責 (Responsibility):
    提供 configs/channels/*.yaml 之讀取與最小結構驗證能力，回傳 YAML mirror dict。
    在最靠近輸入的地方做 fail-fast，避免治理流程在控制面才爆炸並造成反覆試錯。

注意事項 (Notes):
    - 本模組僅負責讀取與最小驗證，不做任何 JetStream 操作與語意轉換。
    - consumer timing contract（互斥）在此層強制：
        * backoff 存在 → 禁止 ack_wait
        * backoff 不存在 → 允許 ack_wait（可缺省）
    - 錯誤訊息使用英文，便於 log/監控與跨團隊協作。
    - 不得依賴 system_initializer 或 application/domain 層模組。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
from pathlib import Path
from typing import Any, Dict, List

# === 第三方套件 (Third-Party Libraries) ===
import yaml

# === 系統內模組 (Internal Modules) ===
from infra_py.logging.logger import get_logger


logger = get_logger(__name__)


def load_config(path: str | Path) -> Dict[str, Any]:
    """
    load_config 載入 channels YAML 並進行最小結構驗證。

    功能:
        - 檢查檔案存在且可讀。
        - 使用 yaml.safe_load 載入 YAML。
        - 驗證必要欄位與型別（nats/subjects/jetstream 的最小集合）。
        - 驗證 consumer timing 互斥契約（ack_wait vs backoff）。

    參數:
        - path: 設定檔路徑（絕對或相對）。

    回傳:
        - result: YAML mirror dict（不做語意轉換）。
        - error: FileNotFoundError / ValueError。

    備註:
        - 本函式不關心 inter/intra 語意，只保證治理工具能安全運作所需的結構與互斥契約。
    """
    p = _normalize_path(path)

    try:
        raw = p.read_text(encoding="utf-8")
    except OSError as e:
        logger.error("failed to read config | path=%s | err=%r", str(p), e)
        raise FileNotFoundError(f"failed to read config file: {p}") from e

    try:
        cfg = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        logger.error("failed to parse yaml | path=%s | err=%r", str(p), e)
        raise ValueError(f"failed to parse yaml: {p}") from e

    if not isinstance(cfg, dict):
        raise ValueError("invalid yaml root: must be mapping/object")

    _validate_top_level(cfg)
    _validate_nats_block(cfg["nats"])
    _validate_subjects_block(cfg["subjects"])
    _validate_jetstream_block(cfg["jetstream"])

    logger.info("config loaded | path=%s", str(p))
    return cfg


def _normalize_path(path: str | Path) -> Path:
    """
    _normalize_path 正規化輸入路徑並做存在性檢查。

    功能:
        - 將 str / Path 統一轉為 Path。
        - 允許相對路徑，並以 cwd 為基準。
        - 確認檔案存在且為一般檔案。

    參數:
        - path: 輸入路徑。

    回傳:
        - result: 正規化後的 Path。
        - error: FileNotFoundError / ValueError。

    備註:
        - 路徑策略只在此層決策，避免下游模組自行推測工作目錄。
    """
    if isinstance(path, Path):
        p = path
    elif isinstance(path, str) and path.strip():
        p = Path(path.strip())
    else:
        raise ValueError("config path must be a non-empty string or Path")

    if not p.is_absolute():
        p = Path.cwd() / p

    if not p.exists():
        raise FileNotFoundError(f"config file not found: {p}")
    if not p.is_file():
        raise FileNotFoundError(f"config path is not a file: {p}")

    return p


def _validate_top_level(cfg: Dict[str, Any]) -> None:
    """
    _validate_top_level 驗證頂層必要區塊存在。

    功能:
        - 驗證 nats/subjects/jetstream 三個區塊必須存在且為 mapping。

    參數:
        - cfg: YAML root dict。

    回傳:
        - result: 無。
        - error: ValueError。

    備註:
        - 此處為最小前置條件，避免後續模組對 None/非 mapping 做不安全存取。
    """
    for key in ("nats", "subjects", "jetstream"):
        if key not in cfg:
            raise ValueError(f"missing required top-level block: {key}")

    if not isinstance(cfg["nats"], dict):
        raise ValueError("invalid 'nats' block: must be mapping/object")
    if not isinstance(cfg["subjects"], dict):
        raise ValueError("invalid 'subjects' block: must be mapping/object")
    if not isinstance(cfg["jetstream"], dict):
        raise ValueError("invalid 'jetstream' block: must be mapping/object")


def _validate_nats_block(nats: Dict[str, Any]) -> None:
    """
    _validate_nats_block 驗證 NATS 連線設定最小集合。

    功能:
        - 驗證 servers 為非空 list[str]。

    參數:
        - nats: nats 區塊 dict。

    回傳:
        - result: 無。
        - error: ValueError。

    備註:
        - TLS/auth/timeout 不在此處定義；未來擴充時再納入 schema。
    """
    servers = nats.get("servers")
    if not isinstance(servers, list) or not servers:
        raise ValueError("invalid 'nats.servers': must be a non-empty list")
    _ensure_list_of_str(servers, "nats.servers")


def _validate_subjects_block(subjects: Dict[str, Any]) -> None:
    """
    _validate_subjects_block 驗證 subjects 區塊最小集合。

    功能:
        - 驗證 namespace 存在且為非空字串。
        - departments/kols 若存在則做最小型別檢查。

    參數:
        - subjects: subjects 區塊 dict。

    回傳:
        - result: 無。
        - error: ValueError。

    備註:
        - subjects schema 會隨 inter/intra 演進，此處只做最低限度檢查。
    """
    namespace = subjects.get("namespace")
    if not isinstance(namespace, str) or not namespace.strip():
        raise ValueError("invalid 'subjects.namespace': must be non-empty string")

    depts = subjects.get("departments")
    if depts is not None:
        if not isinstance(depts, list):
            raise ValueError("invalid 'subjects.departments': must be list if present")
        _ensure_list_of_str(depts, "subjects.departments")

    kols = subjects.get("kols")
    if kols is not None:
        if not isinstance(kols, dict):
            raise ValueError("invalid 'subjects.kols': must be mapping if present")
        for dept, lst in kols.items():
            if not isinstance(dept, str) or not dept.strip():
                raise ValueError("invalid 'subjects.kols' key: must be non-empty string")
            if lst is None or not isinstance(lst, list):
                raise ValueError(f"invalid 'subjects.kols.{dept}': must be list (can be empty)")
            _ensure_list_of_str(lst, f"subjects.kols.{dept}")


def _validate_jetstream_block(js: Dict[str, Any]) -> None:
    """
    _validate_jetstream_block 驗證 jetstream 區塊最小集合。

    功能:
        - streams/consumers 必須存在（可為空 list）。
        - 每個 stream/consumer 項目做最小必要欄位檢查。
        - 強制 consumer timing 互斥契約：ack_wait vs backoff。

    參數:
        - js: jetstream 區塊 dict。

    回傳:
        - result: 無。
        - error: ValueError。

    備註:
        - 此處的「允許」必須與 expected_loader/model 的「實作支援」對齊，避免 silent drift。
    """
    streams = js.get("streams")
    consumers = js.get("consumers")

    if streams is None:
        raise ValueError("missing required field: jetstream.streams (can be empty list)")
    if consumers is None:
        raise ValueError("missing required field: jetstream.consumers (can be empty list)")

    if not isinstance(streams, list):
        raise ValueError("invalid 'jetstream.streams': must be list")
    if not isinstance(consumers, list):
        raise ValueError("invalid 'jetstream.consumers': must be list")

    for i, s in enumerate(streams):
        _validate_stream_item(s, i)

    for i, c in enumerate(consumers):
        _validate_consumer_item(c, i)


def _validate_stream_item(s: Any, idx: int) -> None:
    """
    _validate_stream_item 驗證單一 stream 宣告之最小集合。

    功能:
        - 驗證 name/subjects 必填。
        - 驗證可選欄位的最小型別，以避免進入控制面後才失敗。

    參數:
        - s: 單一 stream 設定（應為 dict）。
        - idx: streams 索引，用於錯誤訊息定位。

    回傳:
        - result: 無。
        - error: ValueError。

    備註:
        - duplicates 舊名與 duplicate_window 新名同時允許，以降低重構成本。
    """
    if not isinstance(s, dict):
        raise ValueError(f"invalid jetstream.streams[{idx}]: must be mapping/object")

    name = s.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"missing/invalid jetstream.streams[{idx}].name: must be non-empty string")

    subjects = s.get("subjects")
    if not isinstance(subjects, list) or not subjects:
        raise ValueError(f"missing/invalid jetstream.streams[{idx}].subjects: must be non-empty list")
    _ensure_list_of_str(subjects, f"jetstream.streams[{idx}].subjects")

    opt_keys = ("storage", "retention", "discard", "duplicate_window", "duplicates", "max_age")
    for opt_key in opt_keys:
        if opt_key in s and s[opt_key] is not None and not isinstance(s[opt_key], (str, int, float)):
            raise ValueError(f"invalid jetstream.streams[{idx}].{opt_key}: must be str|number if present")


def _validate_consumer_item(c: Any, idx: int) -> None:
    """
    _validate_consumer_item 驗證單一 consumer 宣告之最小集合。

    功能:
        - 驗證治理核心欄位存在（stream/durable/name/filter/deliver/ack_policy）。
        - 約束 ack_policy 值域，避免 silent downgrade。
        - 強制 timing 互斥：backoff 與 ack_wait 不可同時出現。

    參數:
        - c: 單一 consumer 設定（應為 dict）。
        - idx: consumers 索引，用於錯誤訊息定位。

    回傳:
        - result: 無。
        - error: ValueError。

    備註:
        - name 欄位維持必填，避免檔案辨識與治理報告缺少人類可讀識別。
    """
    if not isinstance(c, dict):
        raise ValueError(f"invalid jetstream.consumers[{idx}]: must be mapping/object")

    for key in ("stream", "durable_name", "filter_subject", "deliver_subject", "ack_policy", "name"):
        v = c.get(key)
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"missing/invalid jetstream.consumers[{idx}].{key}: must be non-empty string")

    ack_policy = str(c.get("ack_policy")).strip().lower()
    if ack_policy not in ("explicit", "none", "all"):
        raise ValueError(f"invalid jetstream.consumers[{idx}].ack_policy: must be one of explicit|none|all")

    backoff = c.get("backoff")
    ack_wait = c.get("ack_wait")

    # timing 互斥契約
    if backoff is not None and ack_wait is not None:
        raise ValueError(f"invalid jetstream.consumers[{idx}]: ack_wait and backoff are mutually exclusive")

    if backoff is not None:
        if not isinstance(backoff, list) or not backoff:
            raise ValueError(f"invalid jetstream.consumers[{idx}].backoff: must be non-empty list if present")
        for j, v in enumerate(backoff):
            if not isinstance(v, (str, int, float)):
                raise ValueError(f"invalid jetstream.consumers[{idx}].backoff[{j}]: must be str|number")

    if ack_wait is not None and not isinstance(ack_wait, (str, int, float)):
        raise ValueError(f"invalid jetstream.consumers[{idx}].ack_wait: must be str|number if present")

    max_deliver = c.get("max_deliver")
    if max_deliver is not None and not isinstance(max_deliver, int):
        raise ValueError(f"invalid jetstream.consumers[{idx}].max_deliver: must be int if present")


def _ensure_list_of_str(values: List[Any], label: str) -> None:
    """
    _ensure_list_of_str 檢查清單中是否全為字串。

    功能:
        - 防止 YAML 解析後混入非字串型別，造成 subject/name 比對異常。

    參數:
        - values: 待檢查的 list。
        - label: 錯誤訊息用標籤。

    回傳:
        - result: 無。
        - error: ValueError。

    備註:
        - 錯誤訊息使用英文，以統一 infra log 與可觀測性介面。
    """
    for i, v in enumerate(values):
        if not isinstance(v, str):
            raise ValueError(f"invalid {label}[{i}]: must be string")
