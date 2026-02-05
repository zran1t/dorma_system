"""
File: infra_py/nats/config_loader.py
Module: infra_py.nats.config_loader

職責 (Responsibility):
    提供 configs/channels/*.yaml 之讀取與最小結構驗證能力，
    回傳 YAML mirror dict，供 JetStream bootstrap / audit / reset 使用。

注意事項 (Notes):
    - 本模組僅負責讀取與最小驗證，不做任何語意轉換或 JetStream 寫入。
    - 驗證目標是「避免明顯結構錯誤」，不是完整 schema validation。
    - 錯誤訊息使用英文，便於 log/監控與跨團隊協作；註解維持中文以符合本 repo 規範。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
from pathlib import Path
from typing import Any, Dict, List

# === 第三方套件 (Third-Party Libraries) ===
import yaml

# === 系統內模組 (Internal Modules) ===
from infra_py.logging.logger import get_logger


logger = get_logger(__name__, subdir="jetstream/config")


def load_config(path: str | Path) -> Dict[str, Any]:
    """
    load_config 載入 channels 類 YAML 設定檔並做最小結構驗證。

    功能:
        - 檢查檔案存在且可讀。
        - 使用 yaml.safe_load 載入 YAML。
        - 驗證必要欄位與型別（nats/subjects/jetstream 的最小集合）。
        - 回傳 YAML mirror dict（不做轉換/包裝）。

    參數:
        - path: 設定檔路徑（絕對或相對）。

    回傳:
        - result: 與 YAML 結構一致之 dict。
        - error: 無。

    備註:
        - 本函式不關心 inter/intra 具體語意，只檢查 bootstrap/audit 所需的結構存在性。
    """
    p = _normalize_path(path)

    try:
        raw = p.read_text(encoding="utf-8")
    except OSError as e:
        logger.error("failed to read config | path=%s | err=%r", str(p), e)
        raise FileNotFoundError(f"failed to read config file: {p}") from e

    try:
        config = yaml.safe_load(raw)
    except yaml.YAMLError as e:
        logger.error("failed to parse yaml | path=%s | err=%r", str(p), e)
        raise ValueError(f"failed to parse yaml: {p}") from e

    if not isinstance(config, dict):
        raise ValueError("invalid yaml root: must be mapping/object")

    _validate_top_level(config)
    _validate_nats_block(config["nats"])
    _validate_subjects_block(config["subjects"])
    _validate_jetstream_block(config["jetstream"])

    logger.info("config loaded | path=%s", str(p))
    return config


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
        - error: 不存在或不是檔案時拋出例外。
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
    """
    servers = nats.get("servers")
    if not isinstance(servers, list) or not servers:
        raise ValueError("invalid 'nats.servers': must be a non-empty list")

    _ensure_list_of_str(servers, "nats.servers")


def _validate_subjects_block(subjects: Dict[str, Any]) -> None:
    """
    _validate_subjects_block 驗證 subjects 區塊最小集合。

    備註:
        - subjects 的 schema 會隨 inter/intra 演進，因此此處僅做最低限度檢查。
    """
    namespace = subjects.get("namespace")
    if not isinstance(namespace, str) or not namespace.strip():
        raise ValueError("invalid 'subjects.namespace': must be non-empty string")

    # 你目前 inter/intra 都是 departments list；允許缺省，但若存在則必須為 list[str]
    depts = subjects.get("departments")
    if depts is not None:
        if not isinstance(depts, list):
            raise ValueError("invalid 'subjects.departments': must be list if present")
        _ensure_list_of_str(depts, "subjects.departments")

    # intra: subjects.kols 是 mapping dept -> list[kol]; 允許空清單
    kols = subjects.get("kols")
    if kols is not None:
        if not isinstance(kols, dict):
            raise ValueError("invalid 'subjects.kols': must be mapping if present")
        for dept, lst in kols.items():
            if not isinstance(dept, str) or not dept.strip():
                raise ValueError("invalid 'subjects.kols' key: must be non-empty string")
            if lst is None:
                raise ValueError(f"invalid 'subjects.kols.{dept}': must be list (can be empty)")
            if not isinstance(lst, list):
                raise ValueError(f"invalid 'subjects.kols.{dept}': must be list")
            _ensure_list_of_str(lst, f"subjects.kols.{dept}")


def _validate_jetstream_block(js: Dict[str, Any]) -> None:
    """
    _validate_jetstream_block 驗證 jetstream 區塊最小集合。
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

    # 可選欄位：storage/retention/discard/duplicates/max_age 等，只做型別弱檢查以避免拋錯太早
    for opt_key in ("storage", "retention", "discard", "duplicates", "max_age"):
        if opt_key in s and s[opt_key] is not None and not isinstance(s[opt_key], (str, int, float)):
            raise ValueError(f"invalid jetstream.streams[{idx}].{opt_key}: must be str|number if present")


def _validate_consumer_item(c: Any, idx: int) -> None:
    """
    _validate_consumer_item 驗證單一 consumer 宣告之最小集合。

    備註:
        - bootstrap/audit 以 stream + durable_name + filter_subject + deliver_subject 為核心契約。
        - 其餘欄位（ack_wait/max_deliver/backoff）可選，但若提供則做型別檢查。
    """
    if not isinstance(c, dict):
        raise ValueError(f"invalid jetstream.consumers[{idx}]: must be mapping/object")

    for key in ("stream", "durable_name", "filter_subject", "deliver_subject", "ack_policy"):
        v = c.get(key)
        if not isinstance(v, str) or not v.strip():
            raise ValueError(f"missing/invalid jetstream.consumers[{idx}].{key}: must be non-empty string")

    # name：你 YAML 目前都有，用來 human-readable；保留必填，避免 drift
    name = c.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"missing/invalid jetstream.consumers[{idx}].name: must be non-empty string")

    ack_wait = c.get("ack_wait")
    if ack_wait is not None and not isinstance(ack_wait, (str, int, float)):
        raise ValueError(f"invalid jetstream.consumers[{idx}].ack_wait: must be str|number if present")

    max_deliver = c.get("max_deliver")
    if max_deliver is not None and not isinstance(max_deliver, int):
        raise ValueError(f"invalid jetstream.consumers[{idx}].max_deliver: must be int if present")

    backoff = c.get("backoff")
    if backoff is not None:
        if not isinstance(backoff, list) or not backoff:
            raise ValueError(f"invalid jetstream.consumers[{idx}].backoff: must be non-empty list if present")
        for j, v in enumerate(backoff):
            if not isinstance(v, (str, int, float)):
                raise ValueError(f"invalid jetstream.consumers[{idx}].backoff[{j}]: must be str|number")


def _ensure_list_of_str(values: List[Any], label: str) -> None:
    """
    _ensure_list_of_str 檢查清單中是否全為字串。

    備註:
        - 錯誤訊息使用英文，以統一 infra log 語言與可觀測性介面。
    """
    for idx, v in enumerate(values):
        if not isinstance(v, str):
            raise ValueError(f"invalid {label}[{idx}]: must be string")
