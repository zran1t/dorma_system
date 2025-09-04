#!/bin/bash
set -Ee -o pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

# 防止有系統層的 brew service 佔住埠
brew services stop redis >/dev/null 2>&1 || true

mkdir -p redis_data/{capital,kline,order,position,strategy}
mkdir -p logs/redis

CONFIGS=(
  "configs/redis/redis_capital_pool.conf"
  "configs/redis/redis_kline_pool.conf"
  "configs/redis/redis_order_pool.conf"
  "configs/redis/redis_position_pool.conf"
  "configs/redis/redis_strategy_pool.conf"
)

for conf in "${CONFIGS[@]}"; do
  echo "啟動 Redis：$conf"
  nohup redis-server "$conf" >"logs/redis/$(basename "$conf").out" 2>&1 &
done

echo "✅ 所有 Redis 實例已啟動"