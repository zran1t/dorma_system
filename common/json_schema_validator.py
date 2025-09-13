# common/json_schema_validator.py
# Python 3.11+
# JSON Schema 驗證工具：依名稱與版本載入 schemas 檔案，快取編譯後的 validator，拋出中文錯誤。

from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Dict, Optional

from jsonschema import Draft202012Validator, exceptions  


class SchemaValidationError(ValueError):
    """
    JSON Schema 驗證失敗時拋出的錯誤（中文訊息）。

    Attributes:
        schema_name (str): schema 名稱（包含 domain/name）
        version (str): schema 版本（例如 "v1"）
        message (str): 具體錯誤描述（中文）
        path (str): 錯誤欄位路徑（以 JSON Pointer 近似格式表示）
    """
    def __init__(self, schema_name: str, version: str, message: str, path: str = "") -> None:
        super().__init__(message)
        self.schema_name = schema_name
        self.version = version
        self.path = path


def _default_base_dir() -> str:
    """
    取得預設的 schema 根目錄路徑：專案根目錄下的 'schemas'。
    以本檔案所在位置往上推一層做為專案根。
    """
    here = os.path.abspath(os.path.dirname(__file__))
    project_root = os.path.abspath(os.path.join(here, ".."))
    return os.path.join(project_root, "schemas")


def _resolve_schema_path(
    schema_name: str,
    version: str,
    base_dir: Optional[str] = None,
) -> str:
    """
    將 schema 名稱與版本轉為檔案路徑。

    約定：
        - schema_name 可為 "domain/name" 或 "name" 形式
        - schema 檔案路徑：<base_dir>/<schema_name>/<version>.json
          例如：
            - schemas/envelope/v1.json
            - schemas/api/strategy_submit/v1.json
    """
    base = base_dir or _default_base_dir()
    path = os.path.join(base, schema_name, f"{version}.json")
    return os.path.abspath(path)


@lru_cache(maxsize=128)
def _load_and_compile(schema_path: str) -> Draft202012Validator:
    """
    載入並編譯指定路徑的 JSON Schema，結果以 LRU 快取。
    """
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"找不到 schema 檔案：{schema_path}")

    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
    except Exception as e:
        raise ValueError(f"schema 檔案不是合法 JSON：{schema_path}，原因：{e!r}")

    try:
        validator = Draft202012Validator(schema)
    except Exception as e:
        raise ValueError(f"schema 不是合法的 JSON Schema：{schema_path}，原因：{e!r}")

    return validator


def validate_with_schema(
    payload: Dict[str, Any],
    *,
    schema_name: str,
    version: str = "v1",
    base_dir: Optional[str] = None,
) -> None:
    """
    使用指定名稱與版本的 JSON Schema 驗證 payload（通過則無回傳；失敗拋 SchemaValidationError）。

    Args:
        payload (Dict[str, Any]): 要驗證的資料（通常是 dict）
        schema_name (str): schema 名稱（可含子路徑，如 "envelope" 或 "api/xxx"）
        version (str): schema 版本，預設 "v1"
        base_dir (Optional[str]): schema 根目錄，預設為專案根下的 'schemas/'

    Raises:
        TypeError: payload 必須為 dict
        FileNotFoundError: 找不到 schema 檔
        ValueError: schema 檔或內容非法
        SchemaValidationError: 驗證失敗（中文錯誤訊息）
    """
    if not isinstance(payload, dict):
        raise TypeError("payload 必須為 dict")

    schema_path = _resolve_schema_path(schema_name, version, base_dir)
    validator = _load_and_compile(schema_path)

    try:
        validator.validate(payload)
    except exceptions.ValidationError as e:
        # 組裝錯誤路徑（像 /routing/source/type）
        path_parts = [str(p) for p in e.path]
        pointer = "/" + "/".join(path_parts) if path_parts else ""
        msg = f"JSON Schema 驗證失敗：{e.message}"
        raise SchemaValidationError(schema_name, version, msg, pointer) from None