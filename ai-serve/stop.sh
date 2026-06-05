#!/usr/bin/env bash
# Stop the running MLX server (started by serve.sh).
set -euo pipefail

cd "$(dirname "$0")"
PIDFILE="${PIDFILE:-serve.pid}"

# Prefer the exact PID serve.sh recorded; kill it if it's still alive.
if [[ -f "$PIDFILE" ]]; then
  PID="$(cat "$PIDFILE")"
  if kill -0 "$PID" 2>/dev/null; then
    echo "Stopping server (PID $PID from $PIDFILE)..."
    kill "$PID"
  fi
  rm -f "$PIDFILE"
fi

if ! pgrep -f mlx_lm.server >/dev/null; then
  echo "No mlx_lm.server process is running."
  exit 0
fi

echo "Stopping mlx_lm.server..."
pkill -f mlx_lm.server

# Give it a moment to shut down, then force-kill anything still alive.
for _ in $(seq 1 10); do
  pgrep -f mlx_lm.server >/dev/null || break
  sleep 0.5
done
if pgrep -f mlx_lm.server >/dev/null; then
  echo "Still running — forcing..."
  pkill -9 -f mlx_lm.server
fi

echo "Stopped. Port 8080 is free."
