#!/usr/bin/env bash
# Coin-AI 7x24 守护进程监管器
#
# 作用：
#   1. paper-db 进程若因网络抖动、panic 或人为 kill 退出，5 秒后自动拉起；
#   2. 单个日志文件超过 50MB 自动轮转为 .1，避免长期运行把磁盘写满；
#   3. 所有子进程输出统一追加到 logs/paper-db.log。
#
# 用法：
#   nohup bash scripts/run-paper-db.sh >/dev/null 2>&1 &
#   查看：tail -f logs/paper-db.log
#   停止：pkill -f "coin-ai paper-db" && pkill -f run-paper-db.sh

set -u

cd "$(dirname "$0")/.." || exit 1

BIN=./target/release/coin-ai
LOG=logs/paper-db.log
SUP_LOG=logs/supervisor.log
MAX_BYTES=$((50 * 1024 * 1024))

mkdir -p logs

if [ ! -x "$BIN" ]; then
    echo "[supervisor] 找不到可执行文件 $BIN，请先运行 cargo build --release" >&2
    exit 1
fi

while true; do
    if [ -f "$LOG" ] && [ "$(wc -c < "$LOG")" -gt "$MAX_BYTES" ]; then
        mv -f "$LOG" "$LOG.1"
        echo "[supervisor] $(date -u +%FT%TZ) 日志超过 50MB，已轮转为 $LOG.1" >> "$SUP_LOG"
    fi

    "$BIN" paper-db --cycles 999999 --interval-secs 10 "$@" >> "$LOG" 2>&1
    code=$?

    echo "[supervisor] $(date -u +%FT%TZ) paper-db 退出 (code=$code)，5 秒后自动重启" >> "$SUP_LOG"
    sleep 5
done
