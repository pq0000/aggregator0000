#!/bin/sh
# Callback: kill subs-check after detection completes
# Find and kill the subs-check process (not the parent shell)
echo "[callback] detection done, stopping subs-check"
pkill -f "subs-check.*-f.*config.yaml" 2>/dev/null || true
