#!/bin/bash
set -eu
echo "📊 Redis 連線狀態總覽："
for pair in "capital:6379" "kline:6380" "order:6381" "position:6382" "strategy:6383"; do
  name=${pair%%:*}; port=${pair#*:}
  if redis-cli -p "$port" ping >/dev/null 2>&1; then
    count=$(redis-cli -p "$port" info clients 2>/dev/null \
      | awk -F: '/^connected_clients:/ {gsub("\r",""); print $2}')
    echo "✅ [$name] (port: $port) → connected_clients:${count}"
  else
    echo "❌ [$name] (port: $port) 無法連線或未啟動"
  fi
done