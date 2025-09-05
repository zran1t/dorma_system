# common/bus_config.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import inspect
import yaml

from nats.js.api import (
    StreamConfig as JSStreamConfig,
    ConsumerConfig as JSConsumerConfig,
)
from nats.js.api import (
    StorageType,
    RetentionPolicy,
    DiscardPolicy,
    DeliverPolicy,
    ReplayPolicy,
)

# === Constants & Errors =======================================================

DEFAULT_NATS_URL = "nats://127.0.0.1:4222"


class ConfigError(RuntimeError):
    """用於回報 YAML/配置相關錯誤的自訂例外。"""


# === Helpers ==================================================================

def _secs(x: Any) -> Optional[int]:
    """
    將時間字串或數值轉換成秒數的整數表示。

    支援的格式包括：
    - 數字（int 或 float）：直接轉換為整數秒數
    - 字串形式的時間，支援單位：ms（毫秒）、s（秒）、m（分鐘）、h（小時）、d（天）

    參數:
        x (Any): 輸入的時間，可以是數字或字串

    回傳:
        Optional[int]: 轉換後的秒數整數，若輸入為 None 则回傳 None
    """
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return int(x)
    s = str(x).strip().lower()
    if s.endswith("ms"):
        return max(1, int(float(s[:-2]) / 1000))
    if s.endswith("s"):
        return int(float(s[:-1]))
    if s.endswith("m"):
        return int(float(s[:-1]) * 60)
    if s.endswith("h"):
        return int(float(s[:-1]) * 3600)
    if s.endswith("d"):
        return int(float(s[:-1]) * 86400)
    return int(float(s))


def _secs_list(xs: Any) -> Optional[List[int]]:
    """
    將一個時間字串或數字的列表轉換成秒數整數列表。

    參數:
        xs (Any): 輸入的時間列表，元素可以是字串或數字

    回傳:
        Optional[List[int]]: 轉換後的秒數整數列表，若輸入為空或 None 則回傳 None
    """
    if not xs:
        return None
    return [_secs(v) for v in xs if _secs(v) is not None]


def _as_str_list(val: Any, *, field: str, ctx: str) -> List[str]:
    """
    將輸入正規化為字串清單。若為單一字串則包裝成 list；若為 list 會逐一轉字串。
    任何非字串/清單型態將丟出 ConfigError。

    參數:
        val (Any): 原始資料
        field (str): 欄位名稱（用於錯誤訊息）
        ctx (str): 上下文描述（如 "stream[0]"）

    回傳:
        List[str]: 正規化後的字串清單（會過濾掉空白字串）
    """
    if val is None:
        raise ConfigError(f"配置錯誤：{ctx}.{field} 不可為空")

    if isinstance(val, str):
        items = [val]
    elif isinstance(val, list):
        items = [str(x) for x in val]
    else:
        raise ConfigError(f"配置錯誤：{ctx}.{field} 類型必須為 str 或 list[str]，取得 {type(val)!r}")

    # 清理空白、過濾空字串
    out = [s.strip() for s in items if str(s).strip()]
    if not out:
        raise ConfigError(f"配置錯誤：{ctx}.{field} 不可為空清單")
    return out


# === Data Models ==============================================================

@dataclass
class Routes:
    """
    儲存訊息路由相關的主題字串前綴設定。

    屬性:
        dept2kol_prefix (str): 從部門到 KOL 的主題字串前綴
        kol2dept_prefix (str): 從 KOL 到部門的主題字串前綴
    """
    dept2kol_prefix: str
    kol2dept_prefix: str


@dataclass
class BusConfig:
    """
    NATS Bus 的整體配置類別，包含連線資訊、路由設定、Stream 與 Consumer 配置等。

    屬性:
        nats_url (str): NATS 伺服器連線 URL
        routes (Routes): 路由相關的主題字串前綴設定
        streams_js (List[JSStreamConfig]): NATS JetStream 的 Stream 配置列表
        consumers (List[Dict[str, Any]]): Consumer 配置的原始字典列表
        raw (Dict[str, Any]): YAML 原始配置資料

    方法:
        load(path: str | Path) -> BusConfig:
            從 YAML 檔案載入並解析配置，回傳 BusConfig 實例。

        load_from_text(text: str) -> BusConfig:
            由 YAML 字串載入（便於測試）。

        durable_for(filter_prefix: str) -> str:
            依據 filter_subject 前綴尋找對應的 durable 名稱。

        build_consumer_config(c: Dict[str, Any], *, deliver_subject: Optional[str]) -> JSConsumerConfig:
            根據字典配置建立並回傳 JSConsumerConfig 實例。
    """
    nats_url: str
    routes: Routes
    streams_js: List[JSStreamConfig]
    consumers: List[Dict[str, Any]]
    raw: Dict[str, Any]

    # ----- Loaders -------------------------------------------------------------

    @staticmethod
    def load(path: str | Path) -> "BusConfig":
        """
        從指定的 YAML 配置檔案載入 NATS Bus 配置，並轉換成 BusConfig 物件。

        會解析 nats URL、streams、consumers、routes 等設定，並將 stream 配置轉換為 JSStreamConfig 物件列表。
        routes 若未指定，會自動從第一個 stream 的 subject 推斷出預設前綴。

        參數:
            path (str | Path): YAML 配置檔案路徑

        回傳:
            BusConfig: 解析後的 BusConfig 實例

        例外:
            ConfigError: YAML 壞格式或必要欄位缺漏
        """
        p = Path(path)
        try:
            text = p.read_text(encoding="utf-8")
        except Exception as e:
            raise ConfigError(f"讀取 YAML 失敗：{p}（{e}）") from e
        return BusConfig.load_from_text(text)

    @staticmethod
    def load_from_text(text: str) -> "BusConfig":
        """
        由 YAML 字串載入 BusConfig（便於單元測試）。

        參數:
            text (str): YAML 內容

        回傳:
            BusConfig
        """
        try:
            data: Dict[str, Any] = yaml.safe_load(text) or {}
        except Exception as e:
            raise ConfigError(f"YAML 解析失敗：{e}") from e

        # -- nats url
        nats_url = (data.get("nats") or {}).get("url", DEFAULT_NATS_URL) or DEFAULT_NATS_URL

        # -- collections basic checks
        streams = data.get("streams", []) or []
        consumers = data.get("consumers", []) or []
        if not isinstance(streams, list):
            raise ConfigError("配置錯誤：streams 應為 list")
        if not isinstance(consumers, list):
            raise ConfigError("配置錯誤：consumers 應為 list")

        # -- routes（允許省略，則自動推斷）
        rnode = (data.get("routes") or {})
        d2k = rnode.get("dept_to_kol_prefix")
        k2d = rnode.get("kol_to_dept_prefix")
        if not (d2k and k2d):
            base = "bus"
            if streams and isinstance(streams[0], dict) and streams[0].get("subjects"):
                subj0_raw = streams[0]["subjects"]
                subj0 = _as_str_list(subj0_raw, field="subjects", ctx="streams[0]")[0]
                # 去除尾端 '>' 與 '.'，取得更乾淨的 base
                base = subj0.rstrip(">").rstrip(".")
            d2k = d2k or f"{base}.dept2kol"
            k2d = k2d or f"{base}.kol2dept"
        if not (str(d2k).strip() and str(k2d).strip()):
            raise ConfigError("配置錯誤：routes 前綴不可為空")
        routes = Routes(dept2kol_prefix=str(d2k).strip(), kol2dept_prefix=str(k2d).strip())

        # -- stream dict -> JSStreamConfig
        streams_js: List[JSStreamConfig] = []
        for idx, s in enumerate(streams):
            if not isinstance(s, dict):
                raise ConfigError(f"配置錯誤：streams[{idx}] 應為 dict")
            ctx = f"streams[{idx}]"

            # 必要欄位：name、subjects
            name = s.get("name")
            if not (isinstance(name, str) and name.strip()):
                raise ConfigError(f"配置錯誤：{ctx}.name 不可為空")

            subjects = _as_str_list(s.get("subjects"), field="subjects", ctx=ctx)

            sc = JSStreamConfig(
                name=name.strip(),
                subjects=subjects,
                storage={"file": StorageType.FILE, "memory": StorageType.MEMORY}.get(
                    str(s.get("storage", "file")).lower(), StorageType.FILE
                ),
                retention={
                    "limits": RetentionPolicy.LIMITS,
                    "interest": RetentionPolicy.INTEREST,
                    "workqueue": RetentionPolicy.WORK_QUEUE,
                }.get(str(s.get("retention", "limits")).lower(), RetentionPolicy.LIMITS),
                discard={
                    "old": DiscardPolicy.OLD,
                    "new": DiscardPolicy.NEW,
                }.get(str(s.get("discard", "old")).lower(), DiscardPolicy.OLD),
                max_msgs=s.get("max_msgs", -1),
                max_msgs_per_subject=s.get("max_msgs_per_subject", -1),
                max_bytes=s.get("max_bytes", -1),
                max_age=_secs(s.get("max_age", 0)),
                duplicate_window=_secs(s.get("duplicate_window", s.get("duplicates", 0))),
                num_replicas=s.get("num_replicas", 1),
                allow_rollup_hdrs=s.get("allow_rollup_hdrs", False),
                allow_direct=s.get("allow_direct", False),
                deny_delete=s.get("deny_delete", False),
                deny_purge=s.get("deny_purge", False),
            )
            streams_js.append(sc)

        return BusConfig(
            nats_url=nats_url,
            routes=routes,
            streams_js=streams_js,
            consumers=consumers,
            raw=data,
        )

    # ----- Utilities -----------------------------------------------------------

    def durable_for(self, filter_prefix: str) -> str:
        """
        根據 filter_subject 的前綴字串尋找對應的 durable 名稱。

        用於方便依照主題前綴快速取得 durable consumer 名稱。

        參數:
            filter_prefix (str): filter_subject 的前綴字串

        回傳:
            str: 對應的 durable 名稱

        例外:
            ConfigError: 找不到符合條件的 consumer
        """
        for c in self.consumers:
            f = c.get("filter")
            if f and str(f).startswith(filter_prefix):
                return c["durable"]
        raise ConfigError(f"YAML 內找不到 filter 以「{filter_prefix}」開頭的 consumer")

    def build_consumer_config(self, c: Dict[str, Any], *, deliver_subject: Optional[str]) -> JSConsumerConfig:
        """
        根據 consumer 的字典配置建立並回傳 JSConsumerConfig 物件。

        會處理常見的 consumer 設定參數，並根據 JSConsumerConfig 的建構子參數決定要帶入哪些參數。
        可指定 deliver_subject 以覆寫交付主題。

        參數:
            c (Dict[str, Any]): consumer 配置字典
            deliver_subject (Optional[str]): 指定的 deliver_subject 主題

        回傳:
            JSConsumerConfig: 建立好的 ConsumerConfig 物件
        """
        # 動態檢查參數名（對齊 nats-py 版本）
        params = set(inspect.signature(JSConsumerConfig).parameters.keys())

        # 通用 mapping（含安全預設）
        kwargs: Dict[str, Any] = {
            "durable_name": c["durable"],
            "filter_subject": c.get("filter"),
            "ack_wait": _secs(c.get("ack_wait")),
            "max_deliver": c.get("max_deliver"),
            "max_ack_pending": c.get("max_ack_pending"),
            "deliver_policy": {
                "all": DeliverPolicy.ALL,
                "new": DeliverPolicy.NEW,
                "last": DeliverPolicy.LAST,
                "last_per_subject": DeliverPolicy.LAST_PER_SUBJECT,
            }.get(str(c.get("deliver_policy", "all")).lower(), DeliverPolicy.ALL),
            "replay_policy": {
                "instant": ReplayPolicy.INSTANT,
                "original": ReplayPolicy.ORIGINAL,
            }.get(str(c.get("replay_policy", "instant")).lower(), ReplayPolicy.INSTANT),
            "idle_heartbeat": _secs(c.get("idle_heartbeat")),
            "flow_control": c.get("flow_control", False),
            "headers_only": c.get("headers_only", False),
            "sample_freq": c.get("sample_freq"),
            "deliver_subject": deliver_subject,
        }

        # 版本差異處理：backoff / rate_limit*
        if "backoff" in params and c.get("backoff"):
            kwargs["backoff"] = _secs_list(c.get("backoff"))

        rl = c.get("rate_limit_bps", c.get("rate_limit"))
        if rl is not None:
            if "rate_limit_bps" in params:
                kwargs["rate_limit_bps"] = rl
            elif "rate_limit" in params:
                kwargs["rate_limit"] = rl

        # 僅保留 JSConsumerConfig 接受的參數且非 None
        allowed = {k: v for k, v in kwargs.items() if k in params and v is not None}
        return JSConsumerConfig(**allowed)
