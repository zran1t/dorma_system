"""
File: system_initializer/steps/nats_bootstrap.py
Module: system_initializer.steps.nats_bootstrap

職責 (Responsibility):
    提供 system_initializer 的 NATS JetStream 拓樸治理 step（reset / bootstrap / audit）。
    本模組為「流程實作層」：讀取 configs/channels/*.yaml，建立單一 NATS 連線並完成拓樸收斂。

注意事項 (Notes):
    - 本模組只服務 system_initializer；不提供通用 infra 抽象。
    - 使用單一 trace_id（由 system_initializer.logger 注入）作為「單次啟動」的唯一關聯鍵。
    - reset 具破壞性：只應用於初始化/測試環境回收，不得在運行中資料流使用。
    - 本模組統一管理 NATS 連線生命週期：connect 一次 → 共用 → close。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import json
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# === 第三方套件 (Third-Party Libraries) ===
import yaml
from nats.aio.client import Client as NATS
from nats.js.api import (
    StreamConfig,
    ConsumerConfig,
    StorageType,
    RetentionPolicy,
    DiscardPolicy,
    AckPolicy,
)
from nats.js.errors import NotFoundError

# === 系統內模組 (Internal Modules) ===
from system_initializer.logger import current_context, get_logger, run_dir


logger = get_logger(__name__)


# ------------------------------------------------------------------------------
# Options
# ------------------------------------------------------------------------------

@dataclass(frozen=True)
class NatsBootstrapOptions:
    """
    NatsBootstrapOptions NATS JetStream 拓樸治理選項。

    功能:
        - 定義本 step 要跑哪些子流程與治理 artifact 輸出位置。
        - 承接 system_initializer 的 trace_id，確保單次啟動全域一致。

    欄位說明:
        - config_path: channels YAML 路徑（inter/intra 任一均可）。
        - do_reset: 是否執行 reset（破壞性）。
        - do_bootstrap: 是否執行 bootstrap（建立/更新 streams/consumers）。
        - do_audit: 是否執行 audit（比對 YAML 與實際 JetStream）。
        - audit_outfile: audit 報告輸出路徑/前綴；None 表示不落地，只回傳 dict。
        - trace_id: 本次啟動 trace_id；None 則自動取用 system_initializer.logger context。

    契約 / 限制:
        - do_audit=True 且 audit_outfile 非 None 時，會寫檔（治理 artifact），不等同於 logging。
        - trace_id 必須是「單次啟動」全域唯一；不得在本 step 自行生成另一套 trace。

    備註:
        - audit_outfile 建議固定落在 reports/，便於版本化比對與留存。
    """
    config_path: str | Path
    do_reset: bool = True
    do_bootstrap: bool = True
    do_audit: bool = True
    audit_outfile: Optional[str | Path] = "reports/js_auditor/js_audit"
    trace_id: Optional[str] = None


# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------

async def run_nats_bootstrap(opts: NatsBootstrapOptions) -> Dict[str, Any]:
    """
    run_nats_bootstrap 執行 NATS JetStream 拓樸治理流程。

    功能:
        - 讀取 YAML 設定並做最小結構驗證。
        - 建立單一 NATS 連線（一次 connect）。
        - 依選項執行 reset / bootstrap / audit（共用同一條 nc）。
        - 彙總各步驟 report，回傳單一總報告 dict 供上層紀錄或測試斷言。

    參數:
        - opts: NatsBootstrapOptions。

    回傳:
        - result: 總報告 dict（包含每步驟子報告）。
        - error: 任一步驟拋出例外將直接向上拋出（由 system_initializer 決定 fail-fast 或降級）。

    備註:
        - trace_id 由 system_initializer.logger 注入到每一行 log；本函式不再自產 trace。
        - bootstrap 失敗通常不應繼續 audit（避免把控制面錯誤包成 mismatch）。
    """
    trace_id = _resolve_trace_id(opts.trace_id)
    started_at = datetime.now(timezone.utc).isoformat()

    cfg = _load_and_validate_config(opts.config_path)

    report: Dict[str, Any] = {
        "trace_id": trace_id,
        "timestamp": started_at,
        "ok": True,
        "steps": {
            "reset": None,
            "bootstrap": None,
            "audit": None,
        },
        "errors": [],
    }

    logger.info(
        "event=nats_bootstrap_start | reset=%s | bootstrap=%s | audit=%s | config=%s",
        opts.do_reset,
        opts.do_bootstrap,
        opts.do_audit,
        str(opts.config_path),
    )

    servers = (cfg.get("nats") or {}).get("servers")
    if not isinstance(servers, list) or not servers:
        raise ValueError("invalid config: nats.servers must be non-empty list[str]")

    expected = _build_expected_topology(cfg)

    nc: Optional[NATS] = None
    try:
        # [使用單一連線確保控制面操作具一致性，避免多連線造成觀測偏差。]
        nc = NATS()
        await nc.connect(servers=servers)

        if opts.do_reset:
            logger.info("event=step_start | step=reset")
            reset_report = await _reset_topology(nc, expected)
            report["steps"]["reset"] = reset_report

            if reset_report.get("errors"):
                report["ok"] = False
                report["errors"].append({"step": "reset", "error": "reset finished with errors", "detail": reset_report.get("errors")})
                logger.warning("event=step_done | step=reset | ok=false")
            else:
                logger.info("event=step_done | step=reset | ok=true")

        if opts.do_bootstrap:
            logger.info("event=step_start | step=bootstrap")
            try:
                await _bootstrap_topology(nc, expected)
                report["steps"]["bootstrap"] = {"ok": True}
                logger.info("event=step_done | step=bootstrap | ok=true")
            except Exception as e:
                report["ok"] = False
                report["steps"]["bootstrap"] = {"ok": False, "error": repr(e)}
                report["errors"].append({"step": "bootstrap", "error": repr(e)})
                logger.error("event=step_done | step=bootstrap | ok=false | err=%r", e)
                raise

        if opts.do_audit:
            logger.info("event=step_start | step=audit")
            try:
                audit_report = await _audit_topology(nc, expected, outfile=opts.audit_outfile, trace_id=trace_id)
                report["steps"]["audit"] = audit_report

                if not bool(audit_report.get("ok", False)):
                    report["ok"] = False
                    report["errors"].append({"step": "audit", "error": "topology mismatch", "detail": audit_report})
                    logger.warning("event=step_done | step=audit | ok=false")
                else:
                    logger.info("event=step_done | step=audit | ok=true")

            except Exception as e:
                report["ok"] = False
                report["steps"]["audit"] = {"ok": False, "error": repr(e)}
                report["errors"].append({"step": "audit", "error": repr(e)})
                logger.error("event=step_done | step=audit | ok=false | err=%r", e)
                raise

        logger.info("event=nats_bootstrap_done | ok=%s", bool(report["ok"]))
        return report

    finally:
        # [統一由本層關閉連線，確保生命週期邊界清楚且不滲漏。]
        if nc is not None:
            try:
                await nc.close()
            except Exception as e:
                logger.warning("event=nats_close_failed | err=%r", e)


# ------------------------------------------------------------------------------
# Config Loader (minimal validation)
# ------------------------------------------------------------------------------

def _load_and_validate_config(path: str | Path) -> Dict[str, Any]:
    """
    _load_and_validate_config 載入 channels YAML 並進行最小結構驗證。

    功能:
        - 檢查檔案存在且可讀。
        - 使用 yaml.safe_load 載入 YAML。
        - 驗證必要區塊存在（nats/jetstream）。

    參數:
        - path: 設定檔路徑（絕對或相對）。

    回傳:
        - result: YAML mirror dict。
        - error: FileNotFoundError / ValueError。

    備註:
        - 本層只保證治理流程所需的最低結構；語意細節由 mapping/SDK 的失敗來 fail-fast。
    """
    p = Path(path)
    if not p.is_absolute():
        p = Path.cwd() / p

    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"config file not found: {p}")

    raw = p.read_text(encoding="utf-8")
    cfg = yaml.safe_load(raw)
    if not isinstance(cfg, dict):
        raise ValueError("invalid yaml root: must be mapping/object")

    if "nats" not in cfg or "jetstream" not in cfg:
        raise ValueError("invalid config: missing required top-level blocks: nats, jetstream")

    logger.info("event=config_loaded | path=%s", str(p))
    return cfg


# ------------------------------------------------------------------------------
# Topology model (lightweight)
# ------------------------------------------------------------------------------

@dataclass(frozen=True)
class _ExpectedStream:
    """
    _ExpectedStream 期望 stream 宣告。

    功能:
        - 提供 bootstrap/audit/reset 共用的最小欄位集合。

    欄位說明:
        - name: stream 名稱。
        - subjects: subjects 清單。
        - storage: "file"/"memory"。
        - retention: "limits"/"interest"/"workqueue"。
        - discard: "old"/"new"。
        - duplicate_window_seconds: duplicates window 秒數。
        - max_age_seconds: max_age 秒數；None 表示不要求收斂。

    契約 / 限制:
        - subjects 必須非空。

    備註:
        - 這是 system_initializer 內部模型，不追求跨專案通用性。
    """
    name: str
    subjects: List[str]
    storage: str
    retention: str
    discard: str
    duplicate_window_seconds: float
    max_age_seconds: Optional[float]


@dataclass(frozen=True)
class _ExpectedConsumer:
    """
    _ExpectedConsumer 期望 consumer 宣告。

    功能:
        - 提供 bootstrap/audit/reset 共用的最小欄位集合。

    欄位說明:
        - stream: 所屬 stream。
        - durable_name: durable 名稱。
        - filter_subject: filter subject。
        - deliver_subject: deliver subject。
        - ack_policy: "explicit"/"none"/"all"。
        - ack_wait_seconds: ack_wait 秒數（最終生效值）。
        - backoff_seconds: backoff 序列（若有）。
        - max_deliver: 最大投遞次數；None 表示不設定。

    契約 / 限制:
        - identity key 為 "{durable}@{stream}"。

    備註:
        - timing contract（ack_wait vs backoff）在此模組內收斂，避免散落多處。
    """
    stream: str
    durable_name: str
    filter_subject: str
    deliver_subject: str
    ack_policy: str
    ack_wait_seconds: float
    backoff_seconds: Optional[List[float]]
    max_deliver: Optional[int]

    @property
    def key(self) -> str:
        """
        key 回傳 consumer identity key。

        功能:
            - 提供唯一識別鍵供治理流程輸出與觀測。

        參數:
            - 無。

        回傳:
            - result: "{durable}@{stream}" 字串。
            - error: 無。

        備註:
            - durable_name 與 stream 必須是非空字串。
        """
        return f"{self.durable_name}@{self.stream}"


@dataclass(frozen=True)
class _ExpectedTopology:
    """
    _ExpectedTopology 期望拓樸集合。

    功能:
        - 聚合 stream/consumer 的期望狀態，作為治理流程的唯一輸入。

    欄位說明:
        - streams: 期望 streams。
        - consumers: 期望 consumers。

    契約 / 限制:
        - expected-driven：治理只處理此集合內的資源。

    備註:
        - 本模組刻意不做 inventory 列舉，避免權限與成本耦合。
    """
    streams: List[_ExpectedStream]
    consumers: List[_ExpectedConsumer]


def _build_expected_topology(cfg: Dict[str, Any]) -> _ExpectedTopology:
    """
    _build_expected_topology 建立期望拓樸。

    功能:
        - 將 YAML mirror dict 的 jetstream 區塊映射為內部期望模型。
        - 將 duration 類欄位統一轉為 seconds(float)。

    參數:
        - cfg: YAML mirror dict。

    回傳:
        - result: _ExpectedTopology。
        - error: ValueError（缺欄位或解析出不合理狀態）。

    備註:
        - stream duplicate_window 缺省 30s。
        - consumer ack_wait 缺省 30s（僅在 backoff 不存在時）。
    """
    js = cfg.get("jetstream") or {}
    streams_cfg = list(js.get("streams") or [])
    consumers_cfg = list(js.get("consumers") or [])

    streams: List[_ExpectedStream] = []
    for s in streams_cfg:
        if not isinstance(s, dict):
            raise ValueError("invalid jetstream.streams item: must be mapping/object")

        name = str(s.get("name") or "").strip()
        subjects = list(s.get("subjects") or [])
        if not name or not subjects:
            raise ValueError("invalid stream: name and subjects are required")

        dup_raw = s.get("duplicate_window", s.get("duplicates", "30s"))
        duplicate_window_seconds = _parse_duration_seconds(dup_raw, 30.0)

        max_age_seconds: Optional[float] = None
        if "max_age" in s and s.get("max_age") is not None:
            max_age_seconds = _parse_duration_seconds(s.get("max_age"), 0.0)

        streams.append(
            _ExpectedStream(
                name=name,
                subjects=[str(x) for x in subjects],
                storage=str(s.get("storage", "file")).strip().lower(),
                retention=str(s.get("retention", "limits")).strip().lower(),
                discard=str(s.get("discard", "old")).strip().lower(),
                duplicate_window_seconds=float(duplicate_window_seconds),
                max_age_seconds=max_age_seconds,
            )
        )

    consumers: List[_ExpectedConsumer] = []
    for c in consumers_cfg:
        if not isinstance(c, dict):
            raise ValueError("invalid jetstream.consumers item: must be mapping/object")

        stream = str(c.get("stream") or "").strip()
        durable = str(c.get("durable_name") or "").strip()
        filter_subject = str(c.get("filter_subject") or "").strip()
        deliver_subject = str(c.get("deliver_subject") or "").strip()
        ack_policy = str(c.get("ack_policy", "explicit")).strip().lower()

        if not stream or not durable or not filter_subject or not deliver_subject:
            raise ValueError("invalid consumer: stream/durable/filter_subject/deliver_subject are required")

        backoff_raw = c.get("backoff", None)
        ack_wait_raw = c.get("ack_wait", None)

        if backoff_raw is not None and ack_wait_raw is not None:
            raise ValueError("invalid consumer timing: ack_wait and backoff are mutually exclusive")

        backoff_seconds: Optional[List[float]] = None
        ack_wait_seconds: float

        if backoff_raw is not None:
            if not isinstance(backoff_raw, list) or not backoff_raw:
                raise ValueError("invalid consumer timing: backoff must be non-empty list")
            backoff_seconds = [_parse_duration_seconds(v, 0.0) for v in backoff_raw]
            backoff_seconds = [float(v) for v in backoff_seconds if float(v) > 0.0]
            if not backoff_seconds:
                raise ValueError("invalid consumer timing: backoff must contain positive durations")
            ack_wait_seconds = float(backoff_seconds[0])
        else:
            ack_wait_seconds = float(_parse_duration_seconds(ack_wait_raw if ack_wait_raw is not None else "30s", 30.0))
            if ack_wait_seconds <= 0.0:
                raise ValueError("invalid consumer timing: ack_wait must be positive")

        max_deliver = c.get("max_deliver", None)
        max_deliver_int = int(max_deliver) if isinstance(max_deliver, int) else None

        consumers.append(
            _ExpectedConsumer(
                stream=stream,
                durable_name=durable,
                filter_subject=filter_subject,
                deliver_subject=deliver_subject,
                ack_policy=ack_policy,
                ack_wait_seconds=ack_wait_seconds,
                backoff_seconds=backoff_seconds,
                max_deliver=max_deliver_int,
            )
        )

    return _ExpectedTopology(streams=streams, consumers=consumers)


# ------------------------------------------------------------------------------
# reset / bootstrap / audit
# ------------------------------------------------------------------------------

async def _reset_topology(nc: NATS, expected: _ExpectedTopology) -> Dict[str, Any]:
    """
    _reset_topology 破壞性清理 JetStream 拓樸。

    功能:
        - 依 expected-driven 刪除 consumers。
        - 於 consumers 清理完成後刪除 streams。

    參數:
        - nc: 已連線之 NATS client。
        - expected: 期望拓樸。

    回傳:
        - result: 清理操作報告 dict。
        - error: 無（刪除失敗會記錄於 errors 欄位）。

    備註:
        - 若資源不存在則記錄為 skipped，不視為錯誤。
        - 清理順序不可顛倒（Consumer → Stream）。
    """
    js = nc.jetstream()

    report: Dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dropped": {"streams": [], "consumers": []},
        "skipped": {"streams": [], "consumers": []},
        "errors": [],
    }

    # [先刪 consumer，避免 stream 刪除後留下不可預期的控制面錯誤與噪音。]
    for c in expected.consumers:
        try:
            await js.consumer_info(c.stream, c.durable_name)
        except Exception:
            logger.info("event=reset_skip | target=consumer | key=%s", c.key)
            report["skipped"]["consumers"].append(c.key)
            continue

        try:
            await js.delete_consumer(c.stream, c.durable_name)
            logger.info("event=reset_drop | target=consumer | key=%s", c.key)
            report["dropped"]["consumers"].append(c.key)
        except Exception as e:
            logger.error("event=reset_error | target=consumer | key=%s | err=%r", c.key, e)
            report["errors"].append({"target": c.key, "error": repr(e)})

    # [再刪 stream，確保拓樸可以回到乾淨基線。]
    for s in expected.streams:
        try:
            await js.stream_info(s.name)
        except Exception:
            logger.info("event=reset_skip | target=stream | name=%s", s.name)
            report["skipped"]["streams"].append(s.name)
            continue

        try:
            await js.delete_stream(s.name)
            logger.info("event=reset_drop | target=stream | name=%s", s.name)
            report["dropped"]["streams"].append(s.name)
        except Exception as e:
            logger.error("event=reset_error | target=stream | name=%s | err=%r", s.name, e)
            report["errors"].append({"target": s.name, "error": repr(e)})

    return report


async def _bootstrap_topology(nc: NATS, expected: _ExpectedTopology) -> None:
    """
    _bootstrap_topology 收斂 JetStream 拓樸至期望狀態。

    功能:
        - 嘗試讀取現況；不存在視為 missing，走 create。
        - stream：create 或 update。
        - consumer：create 或 update。
        - 以「mismatch 才 update」避免控制面震盪。

    參數:
        - nc: 已連線之 NATS client。
        - expected: 期望拓樸。

    回傳:
        - result: 無。
        - error: JetStream API 例外上拋。

    備註:
        - 本函式不做 delete；delete 由 reset 負責。
    """
    js = nc.jetstream()

    # ------------------------------------------------------------------
    # Streams
    # ------------------------------------------------------------------
    for es in expected.streams:
        sc = _to_stream_config(es)

        exists = True
        try:
            _ = await js.stream_info(es.name)
        except Exception:
            exists = False

        if not exists:
            await js.add_stream(sc)
            logger.info("event=bootstrap_create | target=stream | name=%s", es.name)
            continue

        # [保守策略：直接 update 收斂（因 stream 欄位較少且更新可預期）。]
        await js.update_stream(sc)
        logger.info("event=bootstrap_update | target=stream | name=%s", es.name)

    # ------------------------------------------------------------------
    # Consumers
    # ------------------------------------------------------------------
    for ec in expected.consumers:
        cc = _to_consumer_config(ec)

        exists = True
        try:
            _ = await js.consumer_info(ec.stream, ec.durable_name)
        except Exception:
            exists = False

        if not exists:
            await js.add_consumer(ec.stream, cc)
            logger.info(
                "event=bootstrap_create | target=consumer | key=%s | ack_wait=%ss",
                ec.key,
                float(ec.ack_wait_seconds),
            )
            continue

        await js.update_consumer(ec.stream, cc)
        logger.info(
            "event=bootstrap_update | target=consumer | key=%s | ack_wait=%ss",
            ec.key,
            float(ec.ack_wait_seconds),
        )


async def _audit_topology(
    nc: NATS,
    expected: _ExpectedTopology,
    *,
    outfile: Optional[str | Path],
    trace_id: str,
) -> Dict[str, Any]:
    """
    _audit_topology 稽核 JetStream 拓樸一致性並可選擇落地 JSON 報告。

    功能:
        - expected-driven 查詢 streams/consumers 的實際狀態。
        - 比對 expected vs actual，輸出 missing/mismatched/extra（extra 固定空）。
        - 若 outfile 非 None，將報告寫入 JSON 檔案。

    參數:
        - nc: 已連線之 NATS client。
        - expected: 期望拓樸。
        - outfile: 輸出檔案路徑或前綴；None 表示不落地。
        - trace_id: 單次啟動 trace_id（用於報告關聯）。

    回傳:
        - result: 稽核報告 dict。
        - error: JetStream 查詢或 I/O 例外上拋。

    備註:
        - 本 audit 報告的 trace_id 必須與 system_initializer 全域一致。
    """
    js = nc.jetstream()

    missing_streams: List[str] = []
    missing_consumers: List[str] = []
    mismatched_streams: Dict[str, Dict[str, Dict[str, Any]]] = {}
    mismatched_consumers: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # Streams
    for es in expected.streams:
        try:
            info = await js.stream_info(es.name)
        except Exception:
            missing_streams.append(es.name)
            continue

        actual_subjects = sorted(list(info.config.subjects or []))
        if sorted(es.subjects) != actual_subjects:
            _put_mismatch(mismatched_streams, es.name, "subjects", sorted(es.subjects), actual_subjects)

        actual_storage = _storage_to_str(info.config.storage)
        if es.storage != actual_storage:
            _put_mismatch(mismatched_streams, es.name, "storage", es.storage, actual_storage)

        actual_retention = _retention_to_str(getattr(info.config, "retention", None))
        if es.retention != actual_retention:
            _put_mismatch(mismatched_streams, es.name, "retention", es.retention, actual_retention)

        actual_discard = _discard_to_str(getattr(info.config, "discard", None))
        if es.discard != actual_discard:
            _put_mismatch(mismatched_streams, es.name, "discard", es.discard, actual_discard)

        actual_dup = float(getattr(info.config, "duplicate_window", 0.0) or 0.0)
        if not _float_equal(actual_dup, es.duplicate_window_seconds, tolerance=0.001):
            _put_mismatch(mismatched_streams, es.name, "duplicate_window", es.duplicate_window_seconds, actual_dup)

        if es.max_age_seconds is not None:
            actual_max_age = _opt_float(getattr(info.config, "max_age", None)) or 0.0
            if not _float_equal(float(actual_max_age), float(es.max_age_seconds), tolerance=0.001):
                _put_mismatch(mismatched_streams, es.name, "max_age", es.max_age_seconds, actual_max_age)

    # Consumers
    for ec in expected.consumers:
        try:
            ci = await js.consumer_info(ec.stream, ec.durable_name)
        except Exception:
            missing_consumers.append(ec.key)
            continue

        actual_filter = str(ci.config.filter_subject or "")
        if ec.filter_subject != actual_filter:
            _put_mismatch(mismatched_consumers, ec.key, "filter_subject", ec.filter_subject, actual_filter)

        actual_deliver = str(ci.config.deliver_subject or "")
        if ec.deliver_subject != actual_deliver:
            _put_mismatch(mismatched_consumers, ec.key, "deliver_subject", ec.deliver_subject, actual_deliver)

        actual_ack_policy = _ack_policy_to_str(ci.config.ack_policy)
        if ec.ack_policy != actual_ack_policy:
            _put_mismatch(mismatched_consumers, ec.key, "ack_policy", ec.ack_policy, actual_ack_policy)

        if ec.max_deliver is not None:
            actual_max_deliver = _opt_int(getattr(ci.config, "max_deliver", None))
            if ec.max_deliver != actual_max_deliver:
                _put_mismatch(mismatched_consumers, ec.key, "max_deliver", ec.max_deliver, actual_max_deliver)

        actual_ack_wait = _ack_wait_to_seconds(getattr(ci.config, "ack_wait", None))
        if not _float_equal(actual_ack_wait, ec.ack_wait_seconds, tolerance=0.5):
            _put_mismatch(mismatched_consumers, ec.key, "ack_wait", ec.ack_wait_seconds, actual_ack_wait)

        if ec.backoff_seconds is not None:
            actual_backoff = _opt_backoff_seconds(getattr(ci.config, "backoff", None)) or []
            if list(ec.backoff_seconds) != list(actual_backoff):
                _put_mismatch(mismatched_consumers, ec.key, "backoff", list(ec.backoff_seconds), list(actual_backoff))

    ok = not (missing_streams or missing_consumers or mismatched_streams or mismatched_consumers)

    report: Dict[str, Any] = {
        "trace_id": trace_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ok": bool(ok),
        "missing": {"streams": missing_streams, "consumers": missing_consumers},
        "mismatched": {"streams": mismatched_streams, "consumers": mismatched_consumers},
        "extra": {"streams": [], "consumers": []},
    }

    if outfile is not None:
        outpath = _resolve_outfile_path(outfile)
        _dump_json(report, outpath)
        logger.info("event=audit_report_written | path=%s | ok=%s", str(outpath), bool(ok))

    return report


# ------------------------------------------------------------------------------
# Mapping helpers (YAML -> JetStream configs)
# ------------------------------------------------------------------------------

def _to_stream_config(es: _ExpectedStream) -> StreamConfig:
    """
    _to_stream_config 將期望 stream 映射為 nats-py StreamConfig。

    功能:
        - 將內部模型轉換為 JetStream API 所需 config 物件。

    參數:
        - es: _ExpectedStream。

    回傳:
        - result: StreamConfig。
        - error: ValueError（當 enum 值不支援）。

    備註:
        - max_age_seconds=None 表示不設定（保留 server default）；因此不傳入 max_age。
    """
    kwargs: Dict[str, Any] = {
        "name": es.name,
        "subjects": list(es.subjects),
        "storage": _map_storage(es.storage),
        "retention": _map_retention(es.retention),
        "discard": _map_discard(es.discard),
        "duplicate_window": float(es.duplicate_window_seconds),
    }

    if es.max_age_seconds is not None:
        kwargs["max_age"] = float(es.max_age_seconds)

    return StreamConfig(**kwargs)


def _to_consumer_config(ec: _ExpectedConsumer) -> ConsumerConfig:
    """
    _to_consumer_config 將期望 consumer 映射為 nats-py ConsumerConfig。

    功能:
        - 將內部模型轉換為 JetStream API 所需 config 物件。

    參數:
        - ec: _ExpectedConsumer。

    回傳:
        - result: ConsumerConfig。
        - error: ValueError（當 enum 值不支援）。

    備註:
        - BACKOFF 模式會傳 backoff；ack_wait 仍必傳且等於 backoff[0]，以符合 SDK 序列化契約。
    """
    kwargs: Dict[str, Any] = {
        "durable_name": ec.durable_name,
        "filter_subject": ec.filter_subject,
        "deliver_subject": ec.deliver_subject,
        "ack_policy": _map_ack_policy(ec.ack_policy),
        "ack_wait": float(ec.ack_wait_seconds),
    }

    if ec.max_deliver is not None:
        kwargs["max_deliver"] = int(ec.max_deliver)

    if ec.backoff_seconds is not None:
        kwargs["backoff"] = list(ec.backoff_seconds)

    return ConsumerConfig(**kwargs)


def _map_storage(v: str) -> StorageType:
    """
    _map_storage 將 storage 字串映射為 StorageType。

    功能:
        - 將 "file"/"memory" 映射為 JetStream StorageType。

    參數:
        - v: storage 值。

    回傳:
        - result: StorageType。
        - error: 無。

    備註:
        - 未識別值回退至 MEMORY，以降低初始化阻塞風險。
    """
    return StorageType.FILE if str(v).lower() == "file" else StorageType.MEMORY


def _map_retention(v: str) -> RetentionPolicy:
    """
    _map_retention 將 retention 字串映射為 RetentionPolicy。

    功能:
        - 將 "limits"/"interest"/"workqueue" 映射為 JetStream RetentionPolicy。

    參數:
        - v: retention 值。

    回傳:
        - result: RetentionPolicy。
        - error: 無。

    備註:
        - 未識別值回退至 LIMITS。
    """
    s = str(v).lower()
    if s == "interest":
        return RetentionPolicy.INTEREST
    if s == "workqueue":
        return RetentionPolicy.WORKQUE
    return RetentionPolicy.LIMITS


def _map_discard(v: str) -> DiscardPolicy:
    """
    _map_discard 將 discard 字串映射為 DiscardPolicy。

    功能:
        - 將 "old"/"new" 映射為 JetStream DiscardPolicy。

    參數:
        - v: discard 值。

    回傳:
        - result: DiscardPolicy。
        - error: 無。
    """
    return DiscardPolicy.OLD if str(v).lower() == "old" else DiscardPolicy.NEW


def _map_ack_policy(v: str) -> AckPolicy:
    """
    _map_ack_policy 將 ack_policy 字串映射為 AckPolicy。

    功能:
        - 將 explicit/none/all 映射為 JetStream AckPolicy enum。

    參數:
        - v: ack_policy 字串。

    回傳:
        - result: AckPolicy enum。
        - error: ValueError（未知值）。
    """
    s = str(v).strip().lower()
    if s == "explicit":
        return AckPolicy.EXPLICIT
    if s == "none":
        return AckPolicy.NONE
    if s == "all":
        return AckPolicy.ALL
    raise ValueError(f"invalid ack_policy: {v}")


# ------------------------------------------------------------------------------
# Audit decode helpers (JetStream -> comparable values)
# ------------------------------------------------------------------------------

def _opt_int(v: Any) -> Optional[int]:
    """
    _opt_int 將可能為 None 的值轉為 Optional[int]。

    功能:
        - 正規化 SDK 回讀欄位，避免比對時混入多型。

    參數:
        - v: 可能為 None/數值。

    回傳:
        - result: Optional[int]。
        - error: 無。
    """
    return int(v) if isinstance(v, int) else None


def _opt_float(v: Any) -> Optional[float]:
    """
    _opt_float 將可能為 None 的值轉為 Optional[float]。

    功能:
        - 正規化 SDK 回讀欄位，避免比對時混入多型。

    參數:
        - v: 可能為 None/float/int/timedelta。

    回傳:
        - result: Optional[float]。
        - error: 無。
    """
    if v is None:
        return None
    if isinstance(v, timedelta):
        return float(v.total_seconds())
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _opt_backoff_seconds(v: Any) -> Optional[List[float]]:
    """
    _opt_backoff_seconds 解碼 backoff 序列為 seconds list。

    功能:
        - 正規化 consumer_info 回傳的 backoff 型別，統一回傳 seconds(float) list。

    參數:
        - v: consumer_info.config.backoff（可能為 None/list/其他）。

    回傳:
        - result: Optional[List[float]]。
        - error: 無。

    備註:
        - 元素為 timedelta 則轉 seconds；元素為 int 則視為 ns。
    """
    if v is None or not isinstance(v, list):
        return None

    out: List[float] = []
    for item in v:
        if isinstance(item, timedelta):
            out.append(float(item.total_seconds()))
        elif isinstance(item, int):
            out.append(float(item) / 1_000_000_000.0)
        elif isinstance(item, float):
            out.append(float(item))
        else:
            continue
    return out


def _ack_wait_to_seconds(value: Any) -> float:
    """
    _ack_wait_to_seconds 解碼 JetStream consumer ack_wait 為 seconds(float)。

    功能:
        - 正確處理 consumer_info 回傳的 ack_wait 型別差異（ns/int、sec/float、timedelta）。
        - 統一回傳 seconds(float) 供稽核比對使用。

    參數:
        - value: consumer_info 回傳的 ack_wait 欄位。

    回傳:
        - result: seconds(float)。
        - error: TypeError（不支援型別時）。
    """
    if value is None:
        return 0.0

    if isinstance(value, timedelta):
        return float(value.total_seconds())

    if isinstance(value, int):
        return float(value) / 1_000_000_000.0

    if isinstance(value, float):
        return float(value)

    raise TypeError(f"unsupported ack_wait type: {type(value)}")


def _storage_to_str(storage: Any) -> str:
    """
    _storage_to_str 將 StorageType 轉換為可比對字串。

    功能:
        - 將 JetStream StorageType 映射為 YAML 字串表示。

    參數:
        - storage: JetStream StorageType。

    回傳:
        - result: "file" / "memory" / fallback。
        - error: 無。
    """
    if storage == StorageType.FILE:
        return "file"
    if storage == StorageType.MEMORY:
        return "memory"
    return str(storage).lower()


def _retention_to_str(retention: Any) -> str:
    """
    _retention_to_str 將 RetentionPolicy 轉換為可比對字串。

    功能:
        - 將 JetStream RetentionPolicy 映射為 YAML 字串表示。

    參數:
        - retention: JetStream RetentionPolicy 或其他。

    回傳:
        - result: "limits" / "interest" / "workqueue" / fallback。
        - error: 無。
    """
    if retention == RetentionPolicy.LIMITS:
        return "limits"
    if retention == RetentionPolicy.INTEREST:
        return "interest"
    if retention == RetentionPolicy.WORKQUE:
        return "workqueue"
    return str(retention).lower()


def _discard_to_str(discard: Any) -> str:
    """
    _discard_to_str 將 DiscardPolicy 轉換為可比對字串。

    功能:
        - 將 JetStream DiscardPolicy 映射為 YAML 字串表示。

    參數:
        - discard: JetStream DiscardPolicy 或其他。

    回傳:
        - result: "old" / "new" / fallback。
        - error: 無。
    """
    if discard == DiscardPolicy.OLD:
        return "old"
    if discard == DiscardPolicy.NEW:
        return "new"
    return str(discard).lower()


def _ack_policy_to_str(policy: Any) -> str:
    """
    _ack_policy_to_str 將 AckPolicy 轉換為可比對字串。

    功能:
        - 將 AckPolicy 映射為 YAML 使用之字串表示。

    參數:
        - policy: JetStream AckPolicy。

    回傳:
        - result: "explicit" / "none" / "all" / fallback。
        - error: 無。
    """
    if policy == AckPolicy.EXPLICIT:
        return "explicit"
    if policy == AckPolicy.NONE:
        return "none"
    if policy == AckPolicy.ALL:
        return "all"
    return str(policy).lower()


# ------------------------------------------------------------------------------
# Generic helpers
# ------------------------------------------------------------------------------

def _resolve_trace_id(explicit_trace_id: Optional[str]) -> str:
    """
    _resolve_trace_id 決定本次 step 使用的 trace_id。

    功能:
        - 優先使用 opts.trace_id。
        - 否則取用 system_initializer.logger current_context。
        - 若仍無，代表 caller 未 bootstrap logger，視為流程錯誤而 fail-fast。

    參數:
        - explicit_trace_id: 外部顯式指定 trace_id（可為 None）。

    回傳:
        - result: trace_id 字串。
        - error: RuntimeError（未 bootstrap logger）。

    備註:
        - trace_id 必須是單次啟動全域一致；此處不自行生成第二套識別碼。
    """
    if explicit_trace_id and str(explicit_trace_id).strip():
        return str(explicit_trace_id).strip()

    ctx = current_context()
    if ctx is None:
        raise RuntimeError("logger not bootstrapped: trace_id unavailable")
    return str(ctx.trace_id)


def _parse_duration_seconds(value: Any, default_seconds: float) -> float:
    """
    _parse_duration_seconds 將多型時間表示轉換為秒數(float)。

    功能:
        - 支援數值型（視為秒）與字串型（ms/s/m/h/d）duration。
        - 解析失敗時回退至 default_seconds，避免初始化卡死在非關鍵格式瑕疵。

    參數:
        - value: 時間表示（str/int/float/None）。
        - default_seconds: 解析失敗或缺省時使用的預設秒數。

    回傳:
        - result: seconds(float)。
        - error: 無。
    """
    if value is None:
        return float(default_seconds)

    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip().lower()
    if not s:
        return float(default_seconds)

    # 最小可用 parser（避免引入額外依賴）
    units = {
        "ms": 1 / 1000.0,
        "s": 1.0,
        "m": 60.0,
        "h": 3600.0,
        "d": 86400.0,
    }

    num = ""
    unit = ""
    for ch in s:
        if ch.isdigit() or ch == ".":
            num += ch
        else:
            unit += ch

    try:
        val = float(num) if num else float(default_seconds)
    except Exception:
        return float(default_seconds)

    unit = unit.strip() or "s"
    factor = units.get(unit, None)
    if factor is None:
        return float(default_seconds)

    return float(val * factor)


def _float_equal(a: Any, b: Any, tolerance: float) -> bool:
    """
    _float_equal 判斷兩個數值是否在容許誤差內一致。

    功能:
        - 將輸入轉為 float 後以 tolerance 比對，避免浮點/序列化差異造成誤判。

    參數:
        - a: 左值（可為 None 或數值）。
        - b: 右值（可為 None 或數值）。
        - tolerance: 容許誤差（秒）。

    回傳:
        - result: 一致性判斷結果。
        - error: 無。
    """
    fa = float(a or 0.0)
    fb = float(b or 0.0)
    return abs(fa - fb) <= float(tolerance)


def _put_mismatch(
    bucket: Dict[str, Dict[str, Dict[str, Any]]],
    key: str,
    field: str,
    expected: Any,
    actual: Any,
) -> None:
    """
    _put_mismatch 記錄欄位差異。

    功能:
        - 將差異寫入 mismatched_* 結構，供上層輸出報告。

    參數:
        - bucket: mismatch dict（可變容器）。
        - key: 資源識別鍵（stream name 或 consumer key）。
        - field: 欄位名稱。
        - expected: 期望值。
        - actual: 實際值。

    回傳:
        - result: 無。
        - error: 無。

    備註:
        - 不做 logging；report 是唯一權威輸出。
    """
    bucket.setdefault(key, {})
    bucket[key][field] = {"expected": expected, "actual": actual}


def _resolve_outfile_path(outfile: str | Path) -> Path:
    """
    將 audit report 固定落在本次 run_dir()/reports/ 之下。
    不附加 timestamp，不附加 trace_id。
    """

    p = Path(outfile)

    # 若是相對路徑 → 自動放到 run_dir()/reports/
    if not p.is_absolute():
        base = run_dir() / "reports"
        p = base / p

    # 強制 .json
    if p.suffix.lower() != ".json":
        p = p.with_suffix(".json")

    return p


def _dump_json(obj: Dict[str, Any], outpath: Path) -> None:
    """
    _dump_json 將 dict 以 JSON 格式寫入檔案。

    功能:
        - 確保輸出目錄存在。
        - 將內容以 JSON pretty 格式落地，便於 diff 與人讀。

    參數:
        - obj: 要輸出的 dict。
        - outpath: 輸出檔案路徑。

    回傳:
        - result: 無。
        - error: I/O 失敗時拋出例外。
    """
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with outpath.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
