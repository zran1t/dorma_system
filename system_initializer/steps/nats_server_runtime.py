"""
File: system_initializer/steps/nats_server_runtime.py
Module: system_initializer.steps.nats_server_runtime

職責 (Responsibility):
    提供 system_initializer 專用的 NATS Server runtime 管理（stop → start → ready check）。
    以「初始化流程」為前提，確保舊進程被清乾淨，並在啟動後完成就緒檢查。

注意事項 (Notes):
    - 本模組只管理 nats-server process（OS/process 層）；不做 JetStream topology bootstrap。
    - stop 具破壞性：會終止占用指定 port 的 nats-server（或上次 pidfile 指向的進程）。
    - 連線與拓樸治理由 nats_bootstrap.py 負責（錯誤處理語意不同，必須分離）。
    - 以 TCP probe 作為主要就緒判斷；若 conf 有 http: <port>，可選擇做 /varz probe。
    - nats runtime artifacts 與 redis 同樣隔離於 run_dir。
    - node-1 命名保留未來 cluster 擴充彈性。
"""

from __future__ import annotations

# === 標準函式庫 (Standard Library) ===
import os
import re
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# === 系統內模組 (Internal Modules) ===
from system_initializer.logger import get_logger, run_dir


logger = get_logger(__name__)


@dataclass(frozen=True)
class NatsServerRuntimeOptions:
    """
    NatsServerRuntimeOptions NATS server runtime 選項集合。

    功能:
        - 定義 nats-server 的 conf 位置、log/pid 落點與就緒/關閉等待策略。
        - 支援 macOS/Ubuntu（避免強依賴單一工具；以 pidfile + 多策略查找 PID 作為防衛）。

    欄位說明:
        - conf_path: nats-server 設定檔路徑（通常為 configs/nats/server.conf）。
        - host: 探測 host（預設 127.0.0.1）。
        - port: 覆蓋 conf 的 port；None 表示以 conf 解析為準。
        - http_port: 覆蓋 conf 的 http port；None 表示以 conf 解析為準（或禁用）。
        - enable_http_probe: 若 http_port 存在，是否額外做 /varz probe。
        - startup_timeout_sec: 啟動就緒等待秒數。
        - shutdown_timeout_sec: 停止等待秒數（逾時則 SIGKILL）。
        - log_file: 固定落於
            <run_dir>/runtime/nats/node-1/nats.out
        - pid_file: 固定落於
            <run_dir>/runtime/nats/node-1/nats-server.pid

    契約 / 限制:
        - Strategy=B：會先 stop 再 start；stop 將嘗試終止占用指定 port 的進程。
        - 本模組不負責 JetStream（streams/consumers）治理；只確保 server 可用。

    備註:
        - 若系統環境缺少 lsof/ss/netstat，仍可依賴 pidfile 做 best-effort stop。
        - runtime artifacts 僅在單次 initializer run 範圍內有效。
    """
    conf_path: str | Path = "configs/nats/server.conf"
    host: str = "127.0.0.1"
    port: Optional[int] = None
    http_port: Optional[int] = None
    enable_http_probe: bool = True
    startup_timeout_sec: float = 8.0
    shutdown_timeout_sec: float = 6.0
    log_file: Optional[str | Path] = "logs/nats.out"
    pid_file: Optional[str | Path] = "logs/system_initializer/nats-server.pid"


def restart_nats_server(opts: NatsServerRuntimeOptions) -> int:
    """
    restart_nats_server 重啟 nats-server（stop → start → ready check）。

    功能:
        - 解析 conf 取得 port/http_port（除非 opts 覆蓋）。
        - 先停止舊進程（以 port 探測 + pidfile best-effort）。
        - 啟動新進程並等待就緒（TCP probe；可選 HTTP /varz probe）。

    參數:
        - opts: NatsServerRuntimeOptions。

    回傳:
        - result: 新啟動進程 PID。
        - error: RuntimeError（啟動失敗或就緒逾時）；ValueError（conf 解析失敗）。

    備註:
        - stop 是初始化流程的破壞性前置條件，避免「舊配置/舊狀態」污染本次初始化。
    """
    conf_path = _normalize_existing_file(opts.conf_path)

    conf_port, conf_http_port = _parse_ports_from_conf(conf_path)
    port = int(opts.port) if opts.port is not None else conf_port
    http_port = int(opts.http_port) if opts.http_port is not None else conf_http_port

    runtime_dir = run_dir() / "runtime" / "nats" / "node-1"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    pid_file = runtime_dir / "nats-server.pid"
    log_file = runtime_dir / "nats.out"

    # [將 runtime 的落地路徑先確定，避免後續在錯誤路徑上重試造成噪音。]
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger.info(
        "nats server restart begin | conf=%s | host=%s | port=%s | http=%s | pidfile=%s | logfile=%s",
        str(conf_path),
        opts.host,
        port,
        http_port if http_port is not None else "N/A",
        str(pid_file),
        str(log_file),
    )

    # [先確保舊進程被清乾淨，避免初始化流程在不可預期的既有狀態上運作。]
    stop_nats_server(
        conf_path=conf_path,
        host=opts.host,
        port=port,
        pid_file=pid_file,
        shutdown_timeout_sec=float(opts.shutdown_timeout_sec),
    )

    # [以單一 child process 作為本次 nats-server 實體，並以 pidfile 固化識別以利後續 stop。]
    pid = start_nats_server(
        conf_path=conf_path,
        log_file=log_file,
        pid_file=pid_file,
    )

    # [以 TCP probe 作為主要 readiness gate；必要時補充 HTTP /varz probe 提升確定性。]
    wait_nats_ready(
        host=opts.host,
        port=port,
        http_port=http_port,
        enable_http_probe=bool(opts.enable_http_probe),
        startup_timeout_sec=float(opts.startup_timeout_sec),
    )

    logger.info("nats server restart done | pid=%s | tcp=%s:%s", pid, opts.host, port)
    return pid


def stop_nats_server(
    *,
    conf_path: Path,
    host: str,
    port: int,
    pid_file: Path,
    shutdown_timeout_sec: float,
) -> None:
    """
    stop_nats_server 停止既有 nats-server 進程（破壞性）。

    功能:
        - 先嘗試以 pidfile 指向的 PID 做 stop（若仍存活）。
        - 再以 port-based 探測找出 listen PID（lsof/ss/netstat best-effort）並 stop。
        - 以 SIGTERM 優雅關閉；逾時則 SIGKILL。
        - 最終以「port 不再可連線」作為停止完成判斷。

    參數:
        - conf_path: conf 路徑（僅用於 log 錯誤上下文；不重讀內容）。
        - host: 探測 host（通常 127.0.0.1）。
        - port: 目標 TCP port。
        - pid_file: pidfile 路徑。
        - shutdown_timeout_sec: 停止等待秒數。

    回傳:
        - result: 無。
        - error: 無（best-effort；若 stop 不乾淨將在 start/ready 階段暴露並 fail-fast）。

    備註:
        - 初始化流程策略=B：stop 是必須的，但 stop 本身採 best-effort，避免在異常環境卡死。
    """
    # [先讀 pidfile 作為最直接的識別；再用 port 探測作為防衛，避免 pidfile stale。]
    pid_candidates: list[int] = []

    pid_from_file = _read_pidfile(pid_file)
    if pid_from_file is not None:
        pid_candidates.append(pid_from_file)

    pid_candidates.extend(_find_listen_pids_by_port(port))

    # 去重並保持順序
    seen: set[int] = set()
    unique_pids: list[int] = []
    for p in pid_candidates:
        if p not in seen:
            seen.add(p)
            unique_pids.append(p)

    if not unique_pids and not _tcp_probe(host, port, timeout_sec=0.25):
        logger.info("nats server not running | tcp=%s:%s", host, port)
        _delete_pidfile(pid_file)
        return

    if unique_pids:
        logger.info("nats server stop begin | pids=%s | tcp=%s:%s", unique_pids, host, port)
    else:
        logger.info("nats server stop begin | pid=unknown | tcp=%s:%s", host, port)

    for pid in unique_pids:
        _terminate_pid(pid, shutdown_timeout_sec)

    # 若找不到 PID，也至少等待 port 關閉（避免「已在關閉中」被誤判）
    _wait_port_down(host, port, timeout_sec=shutdown_timeout_sec)

    _delete_pidfile(pid_file)
    logger.info("nats server stop done | tcp=%s:%s", host, port)


def start_nats_server(*, conf_path: Path, log_file: Path, pid_file: Path) -> int:
    """
    start_nats_server 啟動 nats-server 進程。

    功能:
        - 執行 nats-server -c <conf>。
        - stdout/stderr 重新導向至 log_file。
        - 寫入 pidfile 供後續 stop 使用。

    參數:
        - conf_path: server.conf 路徑。
        - log_file: stdout/stderr log 檔案路徑。
        - pid_file: pidfile 路徑。

    回傳:
        - result: child process PID。
        - error: RuntimeError（找不到 nats-server 或啟動失敗）。

    備註:
        - 本函式不做 readiness；readiness 由 wait_nats_ready 控制。
    """
    if not _which("nats-server"):
        raise RuntimeError("nats-server binary not found in PATH")

    # [以單一 child process 作為 nats-server 的生命週期邊界，讓 initializer 可以一致地 stop/restart。]
    with log_file.open("ab", buffering=0) as f:
        try:
            proc = subprocess.Popen(
                ["nats-server", "-c", str(conf_path)],
                stdout=f,
                stderr=subprocess.STDOUT,
                close_fds=True,
            )
        except Exception as e:
            raise RuntimeError(f"failed to start nats-server: {e!r}") from e

    _write_pidfile(pid_file, proc.pid)
    logger.info("nats server started | pid=%s | conf=%s | log=%s", proc.pid, str(conf_path), str(log_file))
    return int(proc.pid)


def wait_nats_ready(
    *,
    host: str,
    port: int,
    http_port: Optional[int],
    enable_http_probe: bool,
    startup_timeout_sec: float,
) -> None:
    """
    wait_nats_ready 等待 nats-server 就緒。

    功能:
        - 以 TCP connect probe 確認 server 已 listen。
        - 若 http_port 存在且 enable_http_probe=True，嘗試 GET /varz 作為補充健檢。

    參數:
        - host: 探測 host。
        - port: TCP port。
        - http_port: HTTP port（None 表示禁用）。
        - enable_http_probe: 是否啟用 HTTP probe。
        - startup_timeout_sec: 等待逾時秒數。

    回傳:
        - result: 無。
        - error: RuntimeError（逾時仍未就緒）。

    備註:
        - readiness gate 是為了讓後續 JetStream bootstrap 的錯誤更具可解釋性（避免把連線問題包成治理問題）。
    """
    deadline = time.time() + float(startup_timeout_sec)

    # TCP probe
    while time.time() < deadline:
        if _tcp_probe(host, port, timeout_sec=0.25):
            break
        time.sleep(0.2)
    else:
        raise RuntimeError(f"nats-server not ready (tcp) within {startup_timeout_sec}s: {host}:{port}")

    # Optional HTTP probe
    if http_port is not None and enable_http_probe:
        url = f"http://{host}:{int(http_port)}/varz"
        deadline = time.time() + min(float(startup_timeout_sec), 4.0)
        while time.time() < deadline:
            try:
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=0.5) as resp:
                    if 200 <= int(resp.status) < 300:
                        logger.info("nats server ready | tcp=%s:%s | http=%s", host, port, http_port)
                        return
            except (urllib.error.URLError, TimeoutError):
                time.sleep(0.2)

        # HTTP probe 失敗不視為 fatal（可能 conf 沒開 http 或被防火牆策略擋）
        logger.info("nats server ready | tcp=%s:%s | http_probe=skipped_or_failed", host, port)
        return

    logger.info("nats server ready | tcp=%s:%s", host, port)


# ------------------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------------------

_PORT_RE = re.compile(r"^\s*port\s*:\s*(\d+)\s*$", re.IGNORECASE)
_HTTP_RE = re.compile(r"^\s*http\s*:\s*(\d+)\s*$", re.IGNORECASE)


def _parse_ports_from_conf(conf_path: Path) -> tuple[int, Optional[int]]:
    """
    _parse_ports_from_conf 解析 server.conf 的 TCP/HTTP 埠設定。

    功能:
        - 以 line-based regex 解析 port: / http: 設定。
        - 若未設定 port 則回退 4222；若未設定 http 則回傳 None。

    參數:
        - conf_path: server.conf 路徑。

    回傳:
        - result: (tcp_port, http_port_or_none)。
        - error: ValueError（conf 無法讀取）。

    備註:
        - 不做完整 NATS conf parser；只取初始化流程需要的最小資訊。
    """
    try:
        lines = conf_path.read_text(encoding="utf-8").splitlines()
    except OSError as e:
        raise ValueError(f"failed to read nats conf: {conf_path} | err={e!r}") from e

    tcp_port: Optional[int] = None
    http_port: Optional[int] = None

    for line in lines:
        m = _PORT_RE.match(line)
        if m:
            tcp_port = int(m.group(1))
            continue
        m = _HTTP_RE.match(line)
        if m:
            http_port = int(m.group(1))
            continue

    if tcp_port is None:
        tcp_port = 4222

    return int(tcp_port), http_port


def _normalize_existing_file(path: str | Path) -> Path:
    """
    _normalize_existing_file 正規化路徑並確保檔案存在。

    功能:
        - 支援 str/Path。
        - 相對路徑以 cwd 為基準。
        - 確認檔案存在且為一般檔案。

    參數:
        - path: 路徑。

    回傳:
        - result: 絕對 Path。
        - error: FileNotFoundError / ValueError。

    備註:
        - runtime 依賴 conf 作為單一真實來源（port/http/其他 server 行為）。
    """
    if isinstance(path, Path):
        p = path
    elif isinstance(path, str) and path.strip():
        p = Path(path.strip())
    else:
        raise ValueError("conf_path must be non-empty")

    if not p.is_absolute():
        p = Path.cwd() / p

    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"nats conf not found: {p}")

    return p.resolve()


def _resolve_path(path: str | Path) -> Path:
    """
    _resolve_path 解析相對路徑為絕對路徑。

    功能:
        - 相對路徑以 cwd 為基準。
        - 不做存在性檢查（供 log/pid 等輸出使用）。

    參數:
        - path: 路徑。

    回傳:
        - result: 絕對 Path。
        - error: 無。
    """
    p = Path(path) if not isinstance(path, Path) else path
    if not p.is_absolute():
        p = Path.cwd() / p
    return p


def _tcp_probe(host: str, port: int, timeout_sec: float) -> bool:
    """
    _tcp_probe 以 TCP connect 判斷 port 是否可連線。

    功能:
        - 作為 stop/ready 的唯一共同基礎判斷（避免 OS 工具差異）。

    參數:
        - host: host。
        - port: port。
        - timeout_sec: socket timeout。

    回傳:
        - result: True 表示可連線；False 表示不可連線。
        - error: 無。

    備註:
        - connect 成功不代表 JetStream ready，但足以作為 server process 就緒 gate。
    """
    try:
        with socket.create_connection((host, int(port)), timeout=float(timeout_sec)):
            return True
    except OSError:
        return False


def _wait_port_down(host: str, port: int, timeout_sec: float) -> None:
    """
    _wait_port_down 等待 port 不再可連線。

    功能:
        - stop 後以 tcp probe 確認 server 已停止。

    參數:
        - host: host。
        - port: port。
        - timeout_sec: 逾時秒數。

    回傳:
        - result: 無。
        - error: 無（best-effort）。

    備註:
        - 若 port 長期仍可連線，start 階段會 fail-fast（port 已占用）。
    """
    deadline = time.time() + float(timeout_sec)
    while time.time() < deadline:
        if not _tcp_probe(host, port, timeout_sec=0.25):
            return
        time.sleep(0.2)


def _terminate_pid(pid: int, timeout_sec: float) -> None:
    """
    _terminate_pid 終止指定 PID。

    功能:
        - 先 SIGTERM；若逾時仍存活則 SIGKILL。

    參數:
        - pid: PID。
        - timeout_sec: 等待秒數。

    回傳:
        - result: 無。
        - error: 無（best-effort）。

    備註:
        - 僅在同一台機器本地初始化使用；不設計為多租戶 process 管理器。
    """
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
    """
    _is_pid_alive 判斷 PID 是否存活。

    功能:
        - 使用 os.kill(pid, 0) 進行跨 Unix-like 的存活檢查。

    參數:
        - pid: PID。

    回傳:
        - result: True 表示存活；False 表示不存在。
        - error: 無。

    備註:
        - 此檢查不保證該 PID 一定是 nats-server；需搭配 port-based 探測使用。
    """
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _read_pidfile(pid_file: Path) -> Optional[int]:
    """
    _read_pidfile 讀取 pidfile。

    功能:
        - 若 pidfile 存在且內容為整數則回傳 PID。

    參數:
        - pid_file: pidfile 路徑。

    回傳:
        - result: PID 或 None。
        - error: 無。

    備註:
        - pidfile 可能 stale；上層需搭配存活/port probe 判斷。
    """
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
    """
    _write_pidfile 寫入 pidfile。

    功能:
        - 固化本次啟動的 PID 供後續 stop 使用。

    參數:
        - pid_file: pidfile 路徑。
        - pid: PID。

    回傳:
        - result: 無。
        - error: 無。

    備註:
        - pidfile 是 best-effort 辨識；仍需以 port probe 作為最終真實狀態判斷。
    """
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(str(int(pid)), encoding="utf-8")


def _delete_pidfile(pid_file: Path) -> None:
    """
    _delete_pidfile 刪除 pidfile。

    功能:
        - stop 後清理 pidfile，避免 stale pid 影響下一次初始化。

    參數:
        - pid_file: pidfile 路徑。

    回傳:
        - result: 無。
        - error: 無。
    """
    try:
        if pid_file.exists():
            pid_file.unlink()
    except Exception:
        pass


def _which(cmd: str) -> bool:
    """
    _which 檢查命令是否存在於 PATH。

    功能:
        - 避免 subprocess 直接拋出難辨識錯誤，提升初始化可診斷性。

    參數:
        - cmd: 命令名稱。

    回傳:
        - result: True 表示存在；False 表示不存在。
        - error: 無。
    """
    try:
        subprocess.run([cmd, "--help"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return True
    except FileNotFoundError:
        return False
    except Exception:
        return True


def _find_listen_pids_by_port(port: int) -> list[int]:
    """
    _find_listen_pids_by_port 以 best-effort 查找正在 listen 指定 TCP port 的 PID。

    功能:
        - 優先使用 lsof（macOS/Ubuntu 常見）。
        - 若無 lsof，嘗試 ss（Ubuntu 常見）。
        - 若無 ss，嘗試 netstat（fallback）。

    參數:
        - port: TCP port。

    回傳:
        - result: PID list（可能為空）。
        - error: 無（best-effort）。

    備註:
        - 初始化流程只需要 best-effort；真正狀態以 tcp probe 為準。
    """
    pids: list[int] = []

    # lsof (macOS/Ubuntu)
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
        # Example: LISTEN 0 4096 127.0.0.1:4222 ... users:(("nats-server",pid=123,fd=3))
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
        # Example: tcp 0 0 127.0.0.1:4222 0.0.0.0:* LISTEN 123/nats-server
        for line in out.splitlines():
            if f":{int(port)}" not in line or "LISTEN" not in line:
                continue
            m = re.search(r"\s(\d+)/", line)
            if m:
                pids.append(int(m.group(1)))
    except Exception:
        pass

    return pids
