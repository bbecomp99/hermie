#!/usr/bin/env bash
# Start an OpenAI-compatible MLX server for the coding model.
# Endpoint: http://localhost:8080/v1/chat/completions
set -euo pipefail

cd "$(dirname "$0")"
source .venv/bin/activate

# 4-bit Qwen2.5-Coder-3B-Instruct: ~1.8GB on disk, ~2GB resident. Snappy on 12GB.
#MODEL="${MODEL:-mlx-community/Qwen2.5-Coder-3B-Instruct-4bit}"
MODEL="${MODEL:-mlx-community/Qwen2.5-Coder-3B-Instruct-4bit}"
PORT="${PORT:-8080}"
# HOST=0.0.0.0 (default) = bind every IPv4 address on all NICs (reachable on the LAN).
# HOST=127.0.0.1 = localhost only.
# NOTE: the server has NO auth — only use 0.0.0.0 on a trusted network.
HOST="${HOST:-0.0.0.0}"

LOG="${LOG:-serve.log}"
PIDFILE="${PIDFILE:-serve.pid}"

# If a previous instance is still running, don't start a second one.
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  echo "Already running (PID $(cat "$PIDFILE")). Stop it with: kill \$(cat $PIDFILE)"
  exit 1
fi

echo "Serving $MODEL on http://$HOST:$PORT  (first run downloads the model)"

# Launch in the background, detached from this shell, with stdout+stderr -> $LOG.
nohup mlx_lm.server --model "$MODEL" --host "$HOST" --port "$PORT" \
  > "$LOG" 2>&1 &
echo $! > "$PIDFILE"

echo "Started in background (PID $(cat "$PIDFILE"))."
echo "  Logs:  tail -f $(pwd)/$LOG"
echo "  Stop:  kill \$(cat $(pwd)/$PIDFILE)"
