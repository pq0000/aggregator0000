#!/bin/sh
# Callback: kill subs-check after detection completes
echo "[callback] detection done, stopping subs-check"
kill -TERM $PPID 2>/dev/null || true
