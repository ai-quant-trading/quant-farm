#!/usr/bin/env bash
# Coin-AI 策略农场服务器守护器（生产+影子+多策略账本+Web UI 一体）
# 与 run-paper-db.sh 二选一运行（两者都跑会双开生产账本！）
#
# 用法:
#   nohup bash scripts/run-farm-server.sh >/dev/null 2>&1 &
#   查看: tail -f logs/farm-server.log   UI: http://127.0.0.1:8787/
#   停止: pkill -f "coin-ai farm-server" && pkill -f run-farm-server.sh

set -u
cd "$(dirname "$0")/.." || exit 1

BIN=./target/release/coin-ai
LOG=logs/farm-server.log
SUP_LOG=logs/supervisor.log
MAX_BYTES=$((50 * 1024 * 1024))

mkdir -p logs

if [ ! -x "$BIN" ]; then
    echo "[farm-supervisor] 找不到可执行文件 $BIN，请先运行 cargo build --release" >&2
    exit 1
fi

while true; do
    if [ -f "$LOG" ] && [ "$(wc -c < "$LOG")" -gt "$MAX_BYTES" ]; then
        mv -f "$LOG" "$LOG.1"
        echo "[farm-supervisor] $(date -u +%FT%TZ) 日志超过 50MB，已轮转为 $LOG.1" >> "$SUP_LOG"
    fi

    "$BIN" farm-server --port 8787 "$@" >> "$LOG" 2>&1
    code=$?
    echo "[farm-supervisor] $(date -u +%FT%TZ) farm-server 退出 (code=$code)，5 秒后自动重启" >> "$SUP_LOG"
    sleep 5
done
