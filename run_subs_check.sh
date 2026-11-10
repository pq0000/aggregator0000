#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# run_subs_check.sh
# =================
#
# 单条 Actions 内的 subs-check 调度脚本：
#   1. 用 tools/serve_aggregator_output.py 把 aggregator000 的输出暴露为本机 HTTP 订阅链接
#   2. 用 subscribe/subs-check-conf/config.yaml 启动 subs-check 检测
#      （node-type 限定为 vless / hysteria2 / mieru，save-method=local）
#   3. subs-check 检测完成后通过 callback.sh 自动退出，结果写入 output/all.yaml
#
# 用法:
#   ./run_subs_check.sh <aggregator_output.yaml> [timeout_seconds]
#
# 环境变量:
#   SUBS_CHECK_BIN    subs-check 二进制路径（默认 ./subs-check）
#   SUBS_CHECK_CONF   subs-check 配置文件路径（默认 subscribe/subs-check-conf/config.yaml）
#   SERVE_PORT        HTTP 服务端口（默认 11023）
#   SERVE_PATH        订阅请求路径（默认 /all.yaml）
#   SUBS_CHECK_TIMEOUT subs-check 主进程超时秒数（默认 900）

set -euo pipefail

AGG_OUTPUT="${1:-output/checked/config.yaml}"
SUBS_TIMEOUT="${SUBS_CHECK_TIMEOUT:-90000}"
SUBS_CHECK_BIN="${SUBS_CHECK_BIN:-./subs-check}"
SUBS_CHECK_CONF="${SUBS_CHECK_CONF:-subscribe/subs-check-conf/config.yaml}"
SERVE_PORT="${SERVE_PORT:-11023}"
SERVE_PATH="${SERVE_PATH:-/all.yaml}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

# 确保输出目录存在
mkdir -p output

echo "[run_subs_check] repo root: $ROOT"
echo "[run_subs_check] aggregator output: $AGG_OUTPUT"
echo "[run_subs_check] subs-check binary: $SUBS_CHECK_BIN"
echo "[run_subs_check] subs-check config: $SUBS_CHECK_CONF"
echo "[run_subs_check] serving on http://127.0.0.1:${SERVE_PORT}${SERVE_PATH}"

if [ ! -f "$AGG_OUTPUT" ]; then
    echo "[run_subs_check] ERROR: aggregator output file not found: $AGG_OUTPUT" >&2
    exit 1
fi

if [ ! -f "$SUBS_CHECK_BIN" ]; then
    echo "[run_subs_check] ERROR: subs-check binary not found: $SUBS_CHECK_BIN" >&2
    exit 1
fi

if [ ! -f "$SUBS_CHECK_CONF" ]; then
    echo "[run_subs_check] ERROR: subs-check config not found: $SUBS_CHECK_CONF" >&2
    exit 1
fi

# 确保 callback 脚本可执行（subs-check 检测完成后会执行它来安全退出）
chmod +x subscribe/subs-check-conf/callback.sh 2>/dev/null || true

# 测试 subs-check binary 是否可运行
echo "[run_subs_check] testing subs-check binary..."
"$SUBS_CHECK_BIN" --version 2>&1 | head -5 || true

# 启动本机 HTTP 服务暴露聚合结果（捕获 stderr 到文件用于调试）
SERVE_LOG="/tmp/serve_aggregator.log"
echo "[run_subs_check] starting serve_aggregator_output.py (log: $SERVE_LOG)..."
python3 tools/serve_aggregator_output.py \
    --file "$AGG_OUTPUT" \
    --port "$SERVE_PORT" \
    --path "$SERVE_PATH" \
    --timeout "${SERVE_TIMEOUT:-0}" >"$SERVE_LOG" 2>&1 &
SERVE_PID=$!

echo "[run_subs_check] serve PID: $SERVE_PID"

# 等待服务就绪
echo "[run_subs_check] waiting for serve to be ready..."
for i in $(seq 1 30); do
    if curl -fsS -m 2 "http://127.0.0.1:${SERVE_PORT}${SERVE_PATH}" >/dev/null 2>&1; then
        echo "[run_subs_check] serve is ready (attempt $i)"
        break
    fi
    # 检查 serve 进程是否还活着
    if ! kill -0 "$SERVE_PID" 2>/dev/null; then
        echo "[run_subs_check] ERROR: serve process died prematurely!" >&2
        echo "[run_subs_check] serve log:"
        cat "$SERVE_LOG" >&2
        exit 1
    fi
    sleep 1
done

if ! curl -fsS -m 2 "http://127.0.0.1:${SERVE_PORT}${SERVE_PATH}" >/dev/null 2>&1; then
    echo "[run_subs_check] ERROR: serve failed to become ready after 30s" >&2
    echo "[run_subs_check] serve log:"
    cat "$SERVE_LOG" >&2
    kill "$SERVE_PID" 2>/dev/null || true
    exit 1
fi

# 运行 subs-check（callback.sh 会在检测完成后 kill 它）
echo "[run_subs_check] starting subs-check..."
set +e
timeout "$SUBS_TIMEOUT" "$SUBS_CHECK_BIN" -f "$SUBS_CHECK_CONF"
SUBS_EXIT=$?
set -e

echo "[run_subs_check] subs-check exit code: $SUBS_EXIT"

# 停止 HTTP 服务
kill "$SERVE_PID" 2>/dev/null || true
wait "$SERVE_PID" 2>/dev/null || true

echo "[run_subs_check] subs-check output files:"
if [ -d output ]; then
    find output -type f | sort
fi

# subs-check 以非零退出（被 callback kill 或超时）视为正常完成，只要产出了结果
if [ -f output/all.yaml ]; then
    echo "[run_subs_check] OK: output/all.yaml produced"
    exit 0
fi

echo "[run_subs_check] ERROR: no output/all.yaml produced" >&2
echo "[run_subs_check] serve log (for debugging):"
cat "$SERVE_LOG" >&2
exit 1
