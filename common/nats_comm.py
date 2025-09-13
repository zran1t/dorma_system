# common/nats_comm.py
# Python 3.11+
# 抽象化 NATS/JetStream（扁平協定 v1）+ 統一日誌輸出

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Literal, List

from nats.aio.client import Client as NATS

Direction = Literal["down", "up", "lateral"]
Layer = Literal["inter", "intra", "device"]
Actor = Dict[str, str]  # {"type": "...", "id": "..."}


class NatsComm:
    """
    NATS/JetStream 輕量封裝（不負責建立/更新 Streams/Consumers）：

    - connect() / close()
    - publish_new() / publish_forward()
    - subscribe()（deliver + 顯式 ACK/NAK）
    - 內建統一日誌輸出（可關閉/調整樣式）

    Args:
        default_headers: 發佈時預設 headers
        schema_version: 目前協定版本（預設 "1.0.0"）
        enable_validation: 發佈前最小驗證開關
        component: 本元件識別（顯示在 log），例："init_dpt_comm_room"
        log_enabled: 是否輸出日誌
        log_style: "compact"（單行摘要）或 "pretty"（多行漂亮輸出）
        redact_keys: 需要遮蔽的 body.data keys（例：["token","secret"]）
        preview_keys: 在摘要中預覽的 body keys（預設僅 "message"）
        max_value_len: 預覽文字截斷長度
    """

    def __init__(
        self,
        *,
        default_headers: Optional[Dict[str, str]] = None,
        schema_version: str = "1.0.0",
        enable_validation: bool = True,
        component: str = "",
        log_enabled: bool = True,
        log_style: Literal["compact", "pretty"] = "compact",
        redact_keys: Optional[List[str]] = None,
        preview_keys: Optional[List[str]] = None,
        max_value_len: int = 120,
    ) -> None:
        self._nc: Optional[NATS] = None
        self._default_headers: Dict[str, str] = dict(default_headers or {})
        self._schema_version = schema_version
        self._enable_validation = enable_validation

        # logging
        self._component = component
        self._log_enabled = log_enabled
        self._log_style = log_style
        self._redact_keys = set(redact_keys or [])
        self._preview_keys = list(preview_keys or ["message"])
        self._max_value_len = max_value_len

    # ───────────────────────── 基礎連線 / 關閉 ─────────────────────────

    async def connect(self, servers: list[str], *, name: Optional[str] = None) -> None:
        if not isinstance(servers, list) or not servers:
            raise ValueError("servers 必須為非空清單（list[str]）")
        self._nc = NATS()
        self._log_info(f"連線到 NATS 伺服器：{', '.join(servers)}")
        await self._nc.connect(servers=servers, name=name)

    async def close(self) -> None:
        if not self._nc:
            return
        try:
            await self._nc.drain()
        except Exception:
            await self._nc.close()
        finally:
            self._nc = None

    # ────────────────────────── 工具 ──────────────────────────

    @staticmethod
    def _now_rfc3339() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _uuid() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def _next_msg_id(trace_id: str, hop: int) -> str:
        # 避免不同 hop 在 duplicates window 內被去重
        return f"{trace_id}:{hop}"

    # ────────────────────────── 最小驗證（MVP） ──────────────────────────

    def _validate_envelope(self, env: Dict[str, Any]) -> None:
        if env.get("version") != self._schema_version:
            raise ValueError(f"協定版本不相容（got={env.get('version')}, need={self._schema_version}）")
        if not env.get("trace_id") or not isinstance(env["trace_id"], str):
            raise ValueError("trace_id 非法")
        if env.get("direction") not in ("down", "up", "lateral"):
            raise ValueError("direction 非法")
        if env.get("layer") not in ("inter", "intra", "device"):
            raise ValueError("layer 非法")
        for k in ("source", "destination"):
            actor = env.get(k)
            if not isinstance(actor, dict) or not actor.get("type") or not actor.get("id"):
                raise ValueError(f"{k} 必須為 {{type,id}}")
        body = env.get("body") or {}
        if not isinstance(body, dict):
            raise ValueError("body 必須為物件")
        if not body.get("message") or not isinstance(body["message"], str):
            raise ValueError("body.message 必須為非空字串")
        if not isinstance(body.get("data", {}), dict):
            raise ValueError("body.data 必須為 dict")
        if "meta" in body and not isinstance(body["meta"], dict):
            raise ValueError("body.meta 必須為 dict")
        hist = env.get("history", [])
        if not isinstance(hist, list) or not hist:
            raise ValueError("history 必須為非空清單")
        for h in hist:
            if not isinstance(h, dict) or "hop" not in h or "from" not in h or "to" not in h or "timestamp" not in h:
                raise ValueError("history 項目缺少必要欄位（hop/from/to/timestamp）")

    # ────────────────────────── 發佈（新建 / 轉發） ──────────────────────────

    async def publish_new(
        self,
        *,
        subject: str,
        message: str,
        data: Dict[str, Any],
        layer: Layer,
        direction: Direction,
        source: Actor,
        destination: Actor,
        meta: Optional[Dict[str, Any]] = None,
        note: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> str:
        if not self._nc:
            raise RuntimeError("尚未連線 NATS，請先呼叫 connect()")

        trace_id = self._uuid()
        now = self._now_rfc3339()
        env: Dict[str, Any] = {
            "trace_id": trace_id,
            "timestamp": now,
            "version": self._schema_version,
            "layer": layer,
            "direction": direction,
            "source": dict(source),
            "destination": dict(destination),
            "history": [
                {
                    "hop": 1,
                    "timestamp": now,
                    "from": dict(source),
                    "to": dict(destination),
                    **({"note": note} if note else {}),
                }
            ],
            "body": {
                "message": message,
                "data": dict(data),
                **({"meta": dict(meta)} if meta else {}),
            },
        }

        if self._enable_validation:
            self._validate_envelope(env)

        payload = json.dumps(env, ensure_ascii=False).encode("utf-8")
        hop = 1

        final_headers: Dict[str, str] = dict(self._default_headers)
        final_headers.setdefault("Nats-Msg-Id", self._next_msg_id(trace_id, hop))
        final_headers.setdefault("X-Origin-Subject", subject)
        if headers:
            final_headers.update(headers)

        await self._nc.publish(subject, payload=payload, headers=final_headers)
        await self._nc.flush()

        # 統一日誌
        self._log_send(subject, env, hop)

        return trace_id

    async def publish_forward(
        self,
        *,
        subject: str,
        envelope: Dict[str, Any],
        from_actor: Actor,
        to_actor: Actor,
        direction: Optional[Direction] = None,
        message: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        meta: Optional[Dict[str, Any]] = None,
        note: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> str:
        if not self._nc:
            raise RuntimeError("尚未連線 NATS，請先呼叫 connect()")
        if not isinstance(envelope, dict):
            raise TypeError("envelope 必須為 dict")

        now = self._now_rfc3339()
        envelope["timestamp"] = now
        if direction:
            envelope["direction"] = direction
        envelope["source"] = dict(from_actor)
        envelope["destination"] = dict(to_actor)

        if message is not None or data is not None or meta is not None:
            body = dict(envelope.get("body") or {})
            if message is not None:
                body["message"] = message
            if data is not None:
                body["data"] = dict(data)
            if meta is not None:
                body["meta"] = dict(meta)
            envelope["body"] = body

        history = envelope.setdefault("history", [])
        hop_no = (history[-1]["hop"] + 1) if history else 1
        history.append({
            "hop": hop_no,
            "timestamp": now,
            "from": dict(from_actor),
            "to": dict(to_actor),
            **({"note": note} if note else {}),
        })

        if self._enable_validation:
            self._validate_envelope(envelope)

        payload = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        trace_id = envelope["trace_id"]

        final_headers: Dict[str, str] = dict(self._default_headers)
        final_headers.setdefault("Nats-Msg-Id", self._next_msg_id(trace_id, hop_no))
        final_headers.setdefault("X-Origin-Subject", subject)
        if headers:
            final_headers.update(headers)

        await self._nc.publish(subject, payload=payload, headers=final_headers)
        await self._nc.flush()

        # 統一日誌
        self._log_send(subject, envelope, hop_no)

        return trace_id

    # ────────────────────────── 訂閱（deliver + ACK/NAK） ──────────────────────────

    async def subscribe(
        self,
        subject: str,
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
        *,
        js_ack: bool = True,
    ) -> None:
        if not self._nc:
            raise RuntimeError("尚未連線 NATS，請先呼叫 connect()")

        async def _cb(msg) -> None:
            try:
                raw = msg.data.decode("utf-8")
                env: Dict[str, Any] = json.loads(raw)
            except Exception as e:
                self._log_error(f"無法解析訊息：{e!r}")
                if js_ack:
                    try:
                        await msg.nak()
                    except Exception:
                        pass
                return

            # 統一日誌（收到）
            self._log_recv(subject, env)

            try:
                await handler(env)
                if js_ack:
                    await msg.ack()
            except Exception as e:
                self._log_error(f"handler 失敗：{e!r}")
                if js_ack:
                    try:
                        await msg.nak()
                    except Exception:
                        pass

        self._log_info(f"開始監聽 deliver：{subject}")
        await self._nc.subscribe(subject, cb=_cb)

    # ────────────────────────── 日誌：統一格式 ──────────────────────────

    def _log_info(self, text: str) -> None:
        if not self._log_enabled:
            return
        prefix = f"[{self._component}] " if self._component else ""
        print(prefix + text)

    def _log_error(self, text: str) -> None:
        if not self._log_enabled:
            return
        prefix = f"[{self._component}] " if self._component else ""
        print(prefix + "❌ " + text)

    def _log_send(self, subject: str, env: Dict[str, Any], hop: int) -> None:
        if not self._log_enabled:
            return
        if self._log_style == "pretty":
            print(self._fmt_pretty("SEND", subject, env, hop))
        else:
            print(self._fmt_compact("SEND", subject, env, hop))

    def _log_recv(self, subject: str, env: Dict[str, Any]) -> None:
        if not self._log_enabled:
            return
        hop = self._last_hop(env)
        if self._log_style == "pretty":
            print(self._fmt_pretty("RECV", subject, env, hop))
        else:
            print(self._fmt_compact("RECV", subject, env, hop))

    # ===== 格式化 =====

    def _fmt_compact(self, kind: str, subject: str, env: Dict[str, Any], hop: int) -> str:
        trace = env.get("trace_id")
        layer = env.get("layer")
        direc = env.get("direction")
        src = self._fmt_actor(env.get("source"))
        dst = self._fmt_actor(env.get("destination"))
        body = env.get("body") or {}
        previews = self._preview_items(body)
        prefix = f"[{self._component}] " if self._component else ""
        return (
            f"{prefix}{'⬆︎' if kind=='SEND' else '⬇︎'} {kind} | subj={subject} | "
            f"trace={trace} | hop={hop} | {layer}/{direc} | "
            f"{src} → {dst} | {previews}"
        )

    def _fmt_pretty(self, kind: str, subject: str, env: Dict[str, Any], hop: int) -> str:
        trace = env.get("trace_id")
        ts = env.get("timestamp")
        layer = env.get("layer")
        direc = env.get("direction")
        src = json.dumps(env.get("source"), ensure_ascii=False)
        dst = json.dumps(env.get("destination"), ensure_ascii=False)
        body = env.get("body") or {}
        previews = self._preview_items(body)
        prefix = f"[{self._component}] " if self._component else ""
        lines = [
            f"{prefix}{'⬆︎' if kind=='SEND' else '⬇︎'} {kind}",
            f"  subject     : {subject}",
            f"  trace_id    : {trace}",
            f"  timestamp   : {ts}",
            f"  hop         : {hop}",
            f"  layer/dir   : {layer}/{direc}",
            f"  from → to   : {src} → {dst}",
            f"  body.preview: {previews}",
        ]
        return "\n".join(lines)

    def _fmt_actor(self, actor: Any) -> str:
        if not isinstance(actor, dict):
            return "<?>"
        t = actor.get("type", "?")
        i = actor.get("id", "?")
        return f"{t}:{i}"

    def _preview_items(self, body: Dict[str, Any]) -> str:
        items = []
        for k in self._preview_keys:
            if k in body:
                v = body.get(k)
                if isinstance(v, (dict, list)):
                    s = json.dumps(self._maybe_redact(v), ensure_ascii=False)
                else:
                    s = str(self._maybe_redact(v))
                if len(s) > self._max_value_len:
                    s = s[: self._max_value_len] + "…"
                items.append(f"{k}={s}")
        # 另外附上 data.keys 數量，便於觀察負載
        data = body.get("data", {})
        if isinstance(data, dict):
            items.append(f"data_keys={len(data.keys())}")
        return "; ".join(items)

    def _maybe_redact(self, v: Any) -> Any:
        if not self._redact_keys:
            return v
        try:
            # 深度遮蔽 dict
            if isinstance(v, dict):
                return {k: ("***" if k in self._redact_keys else self._maybe_redact(val)) for k, val in v.items()}
            if isinstance(v, list):
                return [self._maybe_redact(x) for x in v]
        except Exception:
            return v
        return v

    def _last_hop(self, env: Dict[str, Any]) -> int:
        hist = env.get("history") or []
        if not hist:
            return 0
        try:
            return int(hist[-1].get("hop", 0))
        except Exception:
            return 0