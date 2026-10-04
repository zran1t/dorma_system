"""
File: system_initializer/steps/redis_server_runtime.py
Module: system_initializer.steps.redis_server_runtime

職責 (Responsibility):
    提供 system_initializer 專用的 Redis Server runtime 管理（stop → start → ready check）。
    支援多 instance（多 conf），以「初始化流程」為前提，確保舊進程被清乾淨並完成就緒檢查。

注意事項 (Notes):
    - 本模組只管理 redis-server process（OS/process 層）；不做 key/value bootstrap。
    - stop 具破壞性：會終止占用指定 port 的 redis-server（或 pidfile 指向的進程）。
    - readiness gate 以 TCP probe 為主；可選擇補充 redis-cli PING（若環境存在 redis-cli）。
    - 以 pidfile 固化識別，避免環境缺少 lsof/ss/netstat 時無法 best-effort stop。
    - 所有 pid/log 皆隔離於本次 run_dir 之下。
    - 不再支援自訂 pid/log 路徑（避免分散 artifact）。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import os
import re
import signal
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

# === 系統內模組 (Internal Modules) ===
from system_initializer.logger import get_logger, run_dir


logger = get_logger(__name__)


# ------------------------------------------------------------------------------
# Options
# ------------------------------------------------------------------------------

@dataclass(frozen=True)
class RedisServerInstance:
    """
    RedisServerInstance 單一 redis-server instance 定義。

    欄位說明:
        - name: instance 名稱（用於 log 與 pidfile 命名）。
        - conf_path: redis.conf 路徑。
        - host: 探測 host（預設 127.0.0.1）。
        - port: 覆蓋 conf 的 port；None 表示以 conf 解析為準。
        - pid_file: 不再由外部指定；固定落於
            <run_dir>/runtime/redis/<name>/redis.pid
        - log_file: 固定落於
            <run_dir>/runtime/redis/<name>/redis.out
        - enable_cli_ping: 若有 redis-cli，是否補做 PING readiness。
    備註：
        - runtime artifacts 僅存在於單次 initializer run 範圍內，不做跨 run 重用。
    """
    name: str
    conf_path: str | Path
    host: str = "127.0.0.1"
    port: Optional[int] = None
    pid_file: Optional[str | Path] = None
    log_file: Optional[str | Path] = None
    enable_cli_ping: bool = False


@dataclass(frozen=True)
class RedisServerRuntimeOptions:
    """
    RedisServerRuntimeOptions 多 instance runtime 選項。

    欄位說明:
        - instances: RedisServerInstance 清單（至少一個）。
        - startup_timeout_sec: 啟動就緒等待秒數。
        - shutdown_timeout_sec: 停止等待秒數（逾時則 SIGKILL）。
    """
    instances: list[RedisServerInstance]
    startup_timeout_sec: float = 6.0
    shutdown_timeout_sec: float = 6.0


# ------------------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------------------

def restart_redis_servers(opts: RedisServerRuntimeOptions) -> dict[str, int]:
    """
    restart_redis_servers 依序重啟多個 redis-server instance（stop → start → ready）。

    回傳:
        - dict[name] = pid
    """
    if not opts.instances:
        raise ValueError("instances must be non-empty")

    results: dict[str, int] = {}

    for inst in opts.instances:
        pid = restart_redis_server(
            inst,
            startup_timeout_sec=float(opts.startup_timeout_sec),
            shutdown_timeout_sec=float(opts.shutdown_timeout_sec),
        )
        results[inst.name] = pid

    return results


def restart_redis_server(
    inst: RedisServerInstance,
    *,
    startup_timeout_sec: float,
    shutdown_timeout_sec: float,
) -> int:
    """
    restart_redis_server 重啟單一 redis-server（stop → start → ready）。

    回傳:
        - pid
    """
    name = (inst.name or "").strip()
    if not name:
        raise ValueError("instance.name must be non-empty")

    conf_path = _normalize_existing_file(inst.conf_path)
    conf_port = _parse_port_from_conf(conf_path)
    port = int(inst.port) if inst.port is not None else conf_port

    runtime_dir = run_dir() / "runtime" / "redis" / name
    runtime_dir.mkdir(parents=True, exist_ok=True)

    pid_file = runtime_dir / "redis.pid"
    log_file = runtime_dir / "redis.out"

    pid_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "redis server restart begin | name=%s | conf=%s | host=%s | port=%s | pidfile=%s | logfile=%s",
        name, str(conf_path), inst.host, port, str(pid_file), str(log_file)
    )

    stop_redis_server(
        name=name,
        host=inst.host,
        port=port,
        pid_file=pid_file,
        shutdown_timeout_sec=float(shutdown_timeout_sec),
    )

    pid = start_redis_server(
        name=name,
        conf_path=conf_path,
        log_file=log_file,
        pid_file=pid_file,
    )

    wait_redis_ready(
        name=name,
        host=inst.host,
        port=port,
        startup_timeout_sec=float(startup_timeout_sec),
        enable_cli_ping=bool(inst.enable_cli_ping),
    )

    logger.info("redis server restart done | name=%s | pid=%s | tcp=%s:%s", name, pid, inst.host, port)
    return pid


def stop_redis_server(
    *,
    name: str,
    host: str,
    port: int,
    pid_file: Path,
    shutdown_timeout_sec: float,
) -> None:
    """
    stop_redis_server 停止既有 redis-server（best-effort）。

    策略:
        - 先 pidfile
        - 再 port-based PID 探測（lsof/ss/netstat best-effort）
        - SIGTERM → 等待 → SIGKILL
        - 最終以 tcp probe 判斷 port down
    """
    pid_candidates: list[int] = []

    pid_from_file = _read_pidfile(pid_file)
    if pid_from_file is not None:
        pid_candidates.append(pid_from_file)

    pid_candidates.extend(_find_listen_pids_by_port(port))

    # 去重保持順序
    seen: set[int] = set()
    unique_pids: list[int] = []
    for p in pid_candidates:
        if p not in seen:
            seen.add(p)
            unique_pids.append(p)

    if not unique_pids and not _tcp_probe(host, port, timeout_sec=0.25):
        logger.info("redis server not running | name=%s | tcp=%s:%s", name, host, port)
        _delete_pidfile(pid_file)
        return

    if unique_pids:
        logger.info("redis server stop begin | name=%s | pids=%s | tcp=%s:%s", name, unique_pids, host, port)
    else:
        logger.info("redis server stop begin | name=%s | pid=unknown | tcp=%s:%s", name, host, port)

    for pid in unique_pids:
        _terminate_pid(pid, timeout_sec=float(shutdown_timeout_sec))

    _wait_port_down(host, port, timeout_sec=float(shutdown_timeout_sec))
    _delete_pidfile(pid_file)

    logger.info("redis server stop done | name=%s | tcp=%s:%s", name, host, port)


def start_redis_server(*, name: str, conf_path: Path, log_file: Path, pid_file: Path) -> int:
    """
    start_redis_server 啟動 redis-server <conf> 並寫 pidfile。
    """
    if not _which("redis-server"):
        raise RuntimeError("redis-server binary not found in PATH")

    with log_file.open("ab", buffering=0) as f:
        try:
            proc = subprocess.Popen(
                ["redis-server", str(conf_path)],
                stdout=f,
                stderr=subprocess.STDOUT,
                close_fds=True,
            )
        except Exception as e:
            raise RuntimeError(f"failed to start redis-server: {e!r}") from e

    _write_pidfile(pid_file, proc.pid)
    logger.info("redis server started | name=%s | pid=%s | conf=%s | log=%s", name, proc.pid, str(conf_path), str(log_file))
    return int(proc.pid)


def wait_redis_ready(
    *,
    name: str,
    host: str,
    port: int,
    startup_timeout_sec: float,
    enable_cli_ping: bool,
) -> None:
    """
    wait_redis_ready 等待 redis-server 就緒。

    主 gate: TCP probe
    可選：redis-cli PING（若 enable_cli_ping 且 redis-cli 存在）
    """
    deadline = time.time() + float(startup_timeout_sec)

    while time.time() < deadline:
        if _tcp_probe(host, port, timeout_sec=0.25):
            break
        time.sleep(0.2)
    else:
        raise RuntimeError(f"redis-server not ready (tcp) within {startup_timeout_sec}s: {name} {host}:{port}")

    if enable_cli_ping and _which("redis-cli"):
        deadline = time.time() + min(float(startup_timeout_sec), 4.0)
        while time.time() < deadline:
            if _redis_cli_ping(host, port):
                logger.info("redis server ready | name=%s | tcp=%s:%s | ping=ok", name, host, port)
                return
            time.sleep(0.2)

        # PING 失敗不視為 fatal（避免 cli/權限問題干擾初始化）
        logger.info("redis server ready | name=%s | tcp=%s:%s | ping=skipped_or_failed", name, host, port)
        return

    logger.info("redis server ready | name=%s | tcp=%s:%s", name, host, port)


# ------------------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------------------

_PORT_RE = re.compile(r"^\s*port\s+(\d+)\s*$", re.IGNORECASE)


def _parse_port_from_conf(conf_path: Path) -> int:
    """
    _parse_port_from_conf 解析 redis.conf 的 port。

    - 若未設定 port，回退 6379
    """
    try:
        lines = conf_path.read_text(encoding="utf-8").splitlines()
    except OSError as e:
        raise ValueError(f"failed to read redis conf: {conf_path} | err={e!r}") from e

    for line in lines:
        # 忽略註解行
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        m = _PORT_RE.match(s)
        if m:
            return int(m.group(1))

    return 6379


def _normalize_existing_file(path: str | Path) -> Path:
    if isinstance(path, Path):
        p = path
    elif isinstance(path, str) and path.strip():
        p = Path(path.strip())
    else:
        raise ValueError("conf_path must be non-empty")

    if not p.is_absolute():
        p = Path.cwd() / p

    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"redis conf not found: {p}")

    return p.resolve()


def _resolve_path(path: str | Path) -> Path:
    p = Path(path) if not isinstance(path, Path) else path
    if not p.is_absolute():
        p = Path.cwd() / p
    return p


def _tcp_probe(host: str, port: int, timeout_sec: float) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=float(timeout_sec)):
            return True
    except OSError:
        return False


def _wait_port_down(host: str, port: int, timeout_sec: float) -> None:
    deadline = time.time() + float(timeout_sec)
    while time.time() < deadline:
        if not _tcp_probe(host, port, timeout_sec=0.25):
            return
        time.sleep(0.2)


def _terminate_pid(pid: int, timeout_sec: float) -> None:
    if pid <= 0:
        return
    if not _is_pid_alive(pid):
        return

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except PermissionError:
        logger.warning("no permission to terminate pid=%s", pid)
        return

    deadline = time.time() + float(timeout_sec)
    while time.time() < deadline:
        if not _is_pid_alive(pid):
            return
        time.sleep(0.2)

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except PermissionError:
        logger.warning("no permission to kill pid=%s", pid)
        return


def _is_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _read_pidfile(pid_file: Path) -> Optional[int]:
    try:
        if not pid_file.exists():
            return None
        raw = pid_file.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        return int(raw)
    except Exception:
        return None


def _write_pidfile(pid_file: Path, pid: int) -> None:
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(str(int(pid)), encoding="utf-8")


def _delete_pidfile(pid_file: Path) -> None:
    try:
        if pid_file.exists():
            pid_file.unlink()
    except Exception:
        pass


def _which(cmd: str) -> bool:
    try:
        subprocess.run([cmd, "--help"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return True
    except FileNotFoundError:
        return False
    except Exception:
        return True


def _redis_cli_ping(host: str, port: int) -> bool:
    try:
        out = subprocess.check_output(
            ["redis-cli", "-h", str(host), "-p", str(int(port)), "PING"],
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=0.6,
        ).strip()
        return out.upper() == "PONG"
    except Exception:
        return False


def _find_listen_pids_by_port(port: int) -> list[int]:
    """
    best-effort：lsof → ss → netstat
    """
    pids: list[int] = []

    # lsof
    try:
        out = subprocess.check_output(
            ["lsof", "-t", "-nP", f"-iTCP:{int(port)}", "-sTCP:LISTEN"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        for line in out.splitlines():
            line = line.strip()
            if line.isdigit():
                pids.append(int(line))
        if pids:
            return pids
    except Exception:
        pass

    # ss (Linux)
    try:
        out = subprocess.check_output(
            ["ss", "-ltnp"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        for line in out.splitlines():
            if f":{int(port)}" not in line:
                continue
            m = re.search(r"pid=(\d+)", line)
            if m:
                pids.append(int(m.group(1)))
        if pids:
            return pids
    except Exception:
        pass

    # netstat (fallback)
    try:
        out = subprocess.check_output(
            ["netstat", "-ltnp"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        for line in out.splitlines():
            if f":{int(port)}" not in line or "LISTEN" not in line:
                continue
            m = re.search(r"\s(\d+)/", line)
            if m:
                pids.append(int(m.group(1)))
    except Exception:
        pass

    return pids
