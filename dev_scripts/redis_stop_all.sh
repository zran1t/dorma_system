#!/bin/bash
set -eu

for port in 6379 6380 6381 6382 6383; do
  if redis-cli -p "$port" ping >/dev/null 2>&1; then
    echo "關閉 Redis at $port ..."
    redis-cli -p "$port" shutdown
  else
    echo "埠 $port 未啟動或已關閉"
  fi
done
