#!/usr/bin/env bash
set -e

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
CONF="$ROOT/configs/nats/server.conf"

PORT="$(grep -Eo '^[[:space:]]*port:[[:space:]]*[0-9]+' "$CONF" | awk '{print $2}')" || true
[ -z "$PORT" ] && PORT=4222

# 找到在該埠 LISTEN 的 PID（可能不只一個）
PIDS="$(lsof -t -nP -iTCP:"$PORT" -sTCP:LISTEN || true)"

if [ -z "$PIDS" ]; then
  echo "nothing to stop on tcp:$PORT"
  exit 0
fi

echo "stopping pids: $PIDS (tcp:$PORT)"
kill -TERM $PIDS || true

# 等待優雅關閉
for _ in 1 2 3 4 5 6 7 8 9 10; do
  sleep 0.5
  if ! lsof -t -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "stopped (tcp:$PORT)"
    exit 0
  fi
done

echo "term failed, sending kill -9"
kill -9 $PIDS || true

# 最終確認
if lsof -t -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "failed to stop (tcp:$PORT)"
  exit 1
fi
echo "stopped (tcp:$PORT)"