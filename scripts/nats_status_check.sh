#!/usr/bin/env bash
set -e
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
CONF="$ROOT/configs/nats/server.conf"

# 讀取 TCP 連線埠
PORT="$(grep -Eo '^[[:space:]]*port:[[:space:]]*[0-9]+' "$CONF" | awk '{print $2}')" || true
[ -z "$PORT" ] && PORT=4222

HOST="127.0.0.1"

# 1) TCP 探活
if command -v nc >/dev/null 2>&1; then
  if ! nc -z "$HOST" "$PORT" >/dev/null 2>&1; then
    printf 'FAIL: TCP %s:%s not reachable\n' "$HOST" "$PORT"
    exit 1
  fi
else
  (exec 3<>"/dev/tcp/$HOST/$PORT") >/dev/null 2>&1 || {
    printf 'FAIL: TCP %s:%s not reachable\n' "$HOST" "$PORT"
    exit 1
  }
  exec 3>&-
fi

# 2) HTTP 健檢（若 server.conf 有 http: <port>）
HTTP_PORT="$(grep -Eo '^[[:space:]]*http:[[:space:]]*[0-9]+' "$CONF" | awk '{print $2}')" || true
if [ -n "$HTTP_PORT" ]; then
  if curl -fsS "http://$HOST:$HTTP_PORT/varz" >/dev/null 2>&1; then
    printf 'OK: TCP=%s HTTP=%s\n' "$PORT" "$HTTP_PORT"
    exit 0
  else
    printf 'OK: TCP=%s HTTP probe failed or disabled (http:%s)\n' "$PORT" "$HTTP_PORT"
    exit 0
  fi
fi

# 只有 TCP
printf 'OK: TCP=%s HTTP=N/A\n' "$PORT"
exit 0