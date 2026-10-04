#!/usr/bin/env bash
set -e

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
CONF="$ROOT/configs/nats/server.conf"
LOG="$ROOT/logs/nats.out"

# 解析 TCP/HTTP 埠
PORT="$(grep -Eo '^[[:space:]]*port:[[:space:]]*[0-9]+' "$CONF" | awk '{print $2}')" || true
[ -z "$PORT" ] && PORT=4222
HTTP_PORT="$(grep -Eo '^[[:space:]]*http:[[:space:]]*[0-9]+' "$CONF" | awk '{print $2}')" || true

mkdir -p "$ROOT/logs" "$ROOT/jetstream"

# 啟動
nohup nats-server -c "$CONF" > "$LOG" 2>&1 &
PID=$!
echo "started nats-server pid=$PID, log=$LOG"

# 簡單就緒檢查（TCP）
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$PORT" >/dev/null 2>&1 && { echo "ready: tcp:$PORT"; break; }
  else
    (exec 3<>"/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1 && { exec 3>&-; echo "ready: tcp:$PORT"; break; }
  fi
  sleep 0.5
done

# 可選：HTTP 健檢
if [ -n "$HTTP_PORT" ]; then
  for _ in 1 2 3 4 5; do
    curl -fsS "http://127.0.0.1:$HTTP_PORT/varz" >/dev/null 2>&1 && { echo "ready: http:$HTTP_PORT"; break; }
    sleep 0.5
  done
fi
