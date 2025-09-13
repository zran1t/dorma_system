# common/channel_config_loader.py
# [新檔] YAML 讀取器：專門讀取 configs/channels/*.yaml 設定檔
#  - 回傳 dict，結構與 YAML 完全一致
#  - 執行最小驗證：必要欄位、型別、非空清單
#  - 中文錯誤訊息，方便排錯

from __future__ import annotations

import os
from typing import Any, Dict, List

import yaml


def load_channel_config(path: str) -> Dict[str, Any]:
    """
    載入 channels 類 YAML 設定檔，並做最小結構驗證。

    功能：
        1. 檢查檔案是否存在且可讀。
        2. 使用 yaml.safe_load 載入。
        3. 驗證必要欄位：
           - 頂層必須包含：nats, subjects, jetstream
           - nats.servers 必須為非空清單（list[str]）
           - jetstream.streams / consumers 必須存在，且為 list
        4. 回傳與 YAML 一一對應的 dict（不做轉換/包裝）。

    Args:
        path (str): 設定檔路徑（絕對或相對）

    Returns:
        Dict[str, Any]: 與 YAML 結構一致的設定內容

    Raises:
        FileNotFoundError: 檔案不存在或不可讀
        ValueError: YAML 格式錯誤或缺少必要欄位
    """
    if not isinstance(path, str) or not path.strip():
        raise ValueError("設定檔路徑不可為空字串")
    if not os.path.exists(path):
        raise FileNotFoundError(f"找不到設定檔：{path}")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"路徑不是檔案：{path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"YAML 解析失敗：{e}") from e
    except OSError as e:
        raise FileNotFoundError(f"讀取設定檔失敗：{e}") from e

    if not isinstance(config, dict):
        raise ValueError("設定檔內容必須為物件（mapping）")

    # 驗證必要頂層鍵
    for key in ("nats", "subjects", "jetstream"):
        if key not in config:
            raise ValueError(f"缺少必要區塊：{key}")

    nats = config["nats"]
    subjects = config["subjects"]
    js = config["jetstream"]

    if not isinstance(nats, dict):
        raise ValueError("nats 區塊必須為物件（mapping）")
    if not isinstance(subjects, dict):
        raise ValueError("subjects 區塊必須為物件（mapping）")
    if not isinstance(js, dict):
        raise ValueError("jetstream 區塊必須為物件（mapping）")

    # nats.servers 檢查
    servers = nats.get("servers")
    if not isinstance(servers, list) or not servers:
        raise ValueError("nats.servers 必須為非空清單（list）")
    _ensure_list_of_str(servers, "nats.servers")

    # jetstream.streams / consumers 檢查
    streams = js.get("streams")
    consumers = js.get("consumers")
    if streams is None:
        raise ValueError("jetstream.streams 不可缺少（可為空清單）")
    if consumers is None:
        raise ValueError("jetstream.consumers 不可缺少（可為空清單）")
    if not isinstance(streams, list):
        raise ValueError("jetstream.streams 必須為清單（list）")
    if not isinstance(consumers, list):
        raise ValueError("jetstream.consumers 必須為清單（list）")

    for i, s in enumerate(streams):
        if not isinstance(s, dict):
            raise ValueError(f"jetstream.streams[{i}] 必須為物件（mapping）")
        if "name" not in s:
            raise ValueError(f"jetstream.streams[{i}] 缺少必要欄位：name")
        if "subjects" not in s:
            raise ValueError(f"jetstream.streams[{i}] 缺少必要欄位：subjects")
        if not isinstance(s["subjects"], list) or not s["subjects"]:
            raise ValueError(f"jetstream.streams[{i}].subjects 必須為非空清單")
        _ensure_list_of_str(s["subjects"], f"jetstream.streams[{i}].subjects")

    for i, c in enumerate(consumers):
        if not isinstance(c, dict):
            raise ValueError(f"jetstream.consumers[{i}] 必須為物件（mapping）")
        for key in ("stream", "name", "durable_name", "filter_subject", "deliver_subject", "ack_policy"):
            if key not in c:
                raise ValueError(f"jetstream.consumers[{i}] 缺少必要欄位：{key}")
        if not isinstance(c["ack_policy"], str):
            raise ValueError(f"jetstream.consumers[{i}].ack_policy 必須為字串")

    return config


def _ensure_list_of_str(values: List[Any], label: str) -> None:
    """
    檢查清單中是否全為字串；否則丟出 ValueError（繁中訊息）。
    """
    for idx, v in enumerate(values):
        if not isinstance(v, str):
            raise ValueError(f"{label}[{idx}] 必須為字串")