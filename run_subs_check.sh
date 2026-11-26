#!/usr/bin/env bash
# Two real subs-check v1.6.5 rounds: strict HTTPS + speed/policy, then a distinct
# HTTPS-only recheck of the strict survivors. Timeout is PER ROUND, in seconds.
# Usage: ./run_subs_check.sh output/checked/filtered.yaml [900]
# Positional $2 wins over SUBS_CHECK_TIMEOUT; no file-only success and no pkill.
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
PYTHON_BIN="${PYTHON_BIN:-python3}"
AGG_OUTPUT="${1:-output/checked/filtered.yaml}"
SUBS_TIMEOUT="${2:-${SUBS_CHECK_TIMEOUT:-900}}"
SUBS_CHECK_BIN="${SUBS_CHECK_BIN:-./subs-check}"
SUBS_CHECK_CONF="${SUBS_CHECK_CONF:-subscribe/subs-check-conf/config.yaml}"
SERVE_PORT="${SERVE_PORT:-11023}"
SERVE_PATH="${SERVE_PATH:-/all.yaml}"
API_PORT="${SUBS_CHECK_API_PORT:-11024}"
OUTPUT_DIR="${PIPELINE_OUTPUT_DIR:-output}"
CHECKED_DIR="${OUTPUT_DIR}/checked"
COLLECTION_REPORT="${COLLECTION_REPORT:-${OUTPUT_DIR}/raw/collection-report.json}"
RAW_AGGREGATE="${RAW_AGGREGATE:-${OUTPUT_DIR}/raw/aggregate.yaml}"
FILTER_MANIFEST="${FILTER_MANIFEST:-${CHECKED_DIR}/filter-manifest.json}"
REPORT="${CHECKED_DIR}/validation-report.json"
EVIDENCE="${CHECKED_DIR}/evidence.json"
VALIDATOR="$ROOT/tools/validate_checked_output.py"
SERVE_PID=""
CHECK_PID=""
PHASE="prepare"
FAILURE_CODE="check_not_completed"

# Only PIDs launched as our own children are ever signalled. Auxiliary SubStore
# processes are disabled in the runtime config; no process-name/global cleanup.
stop_child() {
    local pid="$1"
    STOP_EXIT=0
    # An implicit return inside an EXIT trap can inherit the triggering failure.
    if [[ -z "$pid" ]]; then return 0; fi
    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        for ((j=0; j<50; j++)); do
            if ! kill -0 "$pid" 2>/dev/null; then break; fi
            sleep 0.1
        done
        if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
    fi
    set +e
    wait "$pid" 2>/dev/null
    STOP_EXIT=$?
    set -e
}

cleanup() {
    local exit_code=$?
    trap - EXIT INT TERM
    stop_child "$CHECK_PID"
    CHECK_PID=""
    stop_child "$SERVE_PID"
    SERVE_PID=""
    if (( exit_code != 0 )); then
        # Preserve only the precise current failed report, never an old passed
        # marker or the initial incomplete placeholder. Invalidate evidence too.
        "$PYTHON_BIN" "$VALIDATOR" cleanup-markers --report "$REPORT" --evidence "$EVIDENCE" \
            --phase "$PHASE" --reason "$FAILURE_CODE" >/dev/null 2>&1 || true
        printf '[run_subs_check] status=failed phase=%s code=%s\n' "$PHASE" "$FAILURE_CODE" >&2
    fi
    exit "$exit_code"
}
trap cleanup EXIT
trap 'FAILURE_CODE=interrupted; exit 130' INT
trap 'FAILURE_CODE=terminated; exit 143' TERM

# Invalidate both fixed success markers BEFORE validating timeout, binary,
# config or YAML dependencies. This CLI uses only Python's standard library.
FAILURE_CODE=marker_initialization_failed
"$PYTHON_BIN" "$VALIDATOR" initialize-markers --report "$REPORT" --evidence "$EVIDENCE"
FAILURE_CODE=check_not_completed

if [[ ! "$SUBS_TIMEOUT" =~ ^[0-9]+$ ]] || (( SUBS_TIMEOUT < 1 || SUBS_TIMEOUT > 3600 )); then
    FAILURE_CODE=invalid_timeout
    exit 2
fi
for port in "$SERVE_PORT" "$API_PORT"; do
    if [[ ! "$port" =~ ^[0-9]+$ ]] || (( port < 1 || port > 65535 )); then FAILURE_CODE=invalid_port; exit 2; fi
done
if [[ "$SERVE_PORT" == "$API_PORT" || "$SERVE_PATH" != /* || "$SERVE_PATH" == *'?'* || "$SERVE_PATH" == *'#'* ]]; then
    FAILURE_CODE=invalid_local_route
    exit 2
fi
if [[ -n "${SUB_CHECK_SKIP:-}" ]]; then FAILURE_CODE=check_bypass_forbidden; exit 2; fi
if [[ ! -f "$AGG_OUTPUT" || ! -x "$SUBS_CHECK_BIN" || ! -f "$SUBS_CHECK_CONF" ]]; then
    FAILURE_CODE=missing_input_binary_or_config
    exit 1
fi
mkdir -p "$CHECKED_DIR/runs"
WORK_PARENT="$(mktemp -d "${CHECKED_DIR}/runs/run.XXXXXXXX")"
RUN_DIR="${WORK_PARENT}/session"
chmod +x "$ROOT/subscribe/subs-check-conf/callback.sh"
FAILURE_CODE=session_preparation_failed
"$PYTHON_BIN" "$VALIDATOR" prepare-session --input "$AGG_OUTPUT" --aggregate "$RAW_AGGREGATE" \
    --collection-report "$COLLECTION_REPORT" --filter-manifest "$FILTER_MANIFEST" \
    --config "$SUBS_CHECK_CONF" --run-dir "$RUN_DIR" \
    --serve-url "http://127.0.0.1:${SERVE_PORT}${SERVE_PATH}" --api-port "$API_PORT"
printf '[run_subs_check] timeout_seconds_per_round=%s supported_protocols=4\n' "$SUBS_TIMEOUT"

run_phase() {
    PHASE="$1"
    FAILURE_CODE=round_preparation_failed
    "$PYTHON_BIN" "$VALIDATOR" prepare-phase --run-dir "$RUN_DIR" --phase "$PHASE"
    local phase_dir="${RUN_DIR}/${PHASE}"
    mkdir -p "$phase_dir/private-tmp"
    "$PYTHON_BIN" "$ROOT/tools/serve_aggregator_output.py" --file "$phase_dir/input.yaml" \
        --port "$SERVE_PORT" --path "$SERVE_PATH" --ready-file "$phase_dir/serve-ready.json" \
        >"$phase_dir/serve-private.log" 2>&1 &
    SERVE_PID=$!
    local ready=false
    for ((i=0; i<100; i++)); do
        if ! kill -0 "$SERVE_PID" 2>/dev/null; then FAILURE_CODE=local_server_failed; return 1; fi
        if [[ -f "$phase_dir/serve-ready.json" ]]; then ready=true; break; fi
        sleep 0.1
    done
    if [[ "$ready" != true ]]; then FAILURE_CODE=local_server_not_ready; return 1; fi
    "$PYTHON_BIN" - "$phase_dir" "$SERVE_PID" "$SERVE_PORT" <<'PY'
import hashlib, json, sys
from pathlib import Path
root = Path(sys.argv[1])
ready = json.loads((root / 'serve-ready.json').read_text())
assert ready['pid'] == int(sys.argv[2]) and ready['port'] == int(sys.argv[3]), 'wrong serving child'
assert ready['sha256'] == hashlib.sha256((root / 'input.yaml').read_bytes()).hexdigest(), 'wrong served snapshot'
PY
    # Raw upstream errors can include URIs/credentials. Keep them private and
    # never echo/upload them. No inherited publish Secrets reach this process.
    env -u PUSH_TOKEN -u GH_TOKEN -u SUBSCRIBE_CONF -u SUB_CHECK_SKIP -u MIHOMO_DEBUG \
        -u SUB_CHECK_MEM_MONITOR -u SUB_CHECK_MEM_LIMIT LOG_LEVEL=error \
        TMPDIR="$phase_dir/private-tmp" SUBS_CHECK_PYTHON="$PYTHON_BIN" \
        SUBS_CHECK_VALIDATOR="$VALIDATOR" SUBS_CHECK_PHASE_DIR="$phase_dir" \
        "$SUBS_CHECK_BIN" -f "$phase_dir/config.yaml" >"$phase_dir/checker-private.log" 2>&1 &
    CHECK_PID=$!
    printf '%s\n' "$CHECK_PID" >"$phase_dir/checker.pid"
    local started=$SECONDS reason=error checker_exit=0
    while true; do
        # A deadline crossing NEVER becomes callback success just because a
        # stale/late YAML or completion marker happens to exist.
        if (( SECONDS - started >= SUBS_TIMEOUT )); then reason=timeout; break; fi
        if [[ -f "$phase_dir/callback.json" ]]; then reason=callback; break; fi
        if ! kill -0 "$CHECK_PID" 2>/dev/null; then reason=error; break; fi
        sleep 0.2
    done
    stop_child "$CHECK_PID"
    checker_exit=$STOP_EXIT
    CHECK_PID=""
    stop_child "$SERVE_PID"
    SERVE_PID=""
    "$PYTHON_BIN" "$VALIDATOR" record-exit --phase-dir "$phase_dir" --reason "$reason" --exit-code "$checker_exit"
    if [[ "$reason" == timeout ]]; then
        FAILURE_CODE=checker_timeout
        "$PYTHON_BIN" "$VALIDATOR" failure --report "$REPORT" --phase "$PHASE" --reason "$FAILURE_CODE"
        return 1
    fi
    if [[ "$reason" != callback ]]; then
        FAILURE_CODE=checker_exited_without_callback
        "$PYTHON_BIN" "$VALIDATOR" failure --report "$REPORT" --phase "$PHASE" --reason "$FAILURE_CODE"
        return 1
    fi
    FAILURE_CODE=round_validation_failed
    "$PYTHON_BIN" "$VALIDATOR" validate-phase --run-dir "$RUN_DIR" --phase "$PHASE" --report "$REPORT"
}

run_phase primary
run_phase recheck
PHASE=finalize
FAILURE_CODE=publication_evidence_failed
"$PYTHON_BIN" "$VALIDATOR" finalize --run-dir "$RUN_DIR" --output "${OUTPUT_DIR}/all.yaml" \
    --evidence "$EVIDENCE" --report "$REPORT"
printf '[run_subs_check] status=passed dual_https_rounds=2 fresh_complete_nonempty=true\n'
