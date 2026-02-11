#!/bin/bash
set -Ee -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

# 防止有系統層的 brew service 佔住埠
brew services stop redis >/dev/null 2>&1 || true

mkdir -p redis_data/{capital,kline,order,position,strategy,state}
mkdir -p logs/redis

CONFIGS=(
  "configs/redis/capital_pool.conf"
  "configs/redis/kline_pool.conf"
  "configs/redis/order_pool.conf"
  "configs/redis/position_pool.conf"
  "configs/redis/strategy_pool.conf"
  "config/redis/state_pool.conf"
)

for conf in "${CONFIGS[@]}"; do
  echo "啟動 Redis：$conf"
  nohup redis-server "$conf" >"logs/redis/$(basename "$conf").out" 2>&1 &
done

echo "✅ 所有 Redis 實例已啟動"
