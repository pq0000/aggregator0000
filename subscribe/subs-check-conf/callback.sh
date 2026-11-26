#!/bin/sh
# v1.6.5 supplies SUCCESS_COUNT after SaveConfig. Saving can fail independently,
# so only record current-round evidence; NEVER find/kill processes by name.
set -eu
: "${SUBS_CHECK_PHASE_DIR:?must be launched by run_subs_check.sh}"
: "${SUBS_CHECK_VALIDATOR:?must be launched by run_subs_check.sh}"
: "${SUBS_CHECK_PYTHON:?must be launched by run_subs_check.sh}"
# In upstream Unix exec.Command this PPID is the checker, not the supervisor.
exec "$SUBS_CHECK_PYTHON" "$SUBS_CHECK_VALIDATOR" record-callback \
    --phase-dir "$SUBS_CHECK_PHASE_DIR" --parent-pid "$PPID"
