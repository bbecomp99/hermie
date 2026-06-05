#!/usr/bin/env bash
# Pre-download an MLX model into the local HuggingFace cache so serve.sh can
# load it instantly (no download on first request).
#
# Usage:
#   ./pull-model.sh <model-name>
#
# The model name may be a full repo id or a bare name (then mlx-community/ is
# assumed):
#   ./pull-model.sh mlx-community/Qwen2.5-Coder-3B-Instruct-4bit
#   ./pull-model.sh Qwen2.5-Coder-3B-Instruct-4bit      # -> mlx-community/...
#
# Then serve it with:
#   MODEL=<repo-id> ./serve.sh
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <model-name>" >&2
  echo "  e.g. $0 Qwen2.5-Coder-7B-Instruct-4bit" >&2
  exit 1
fi

cd "$(dirname "$0")"
source .venv/bin/activate

MODEL="$1"
# Bare name (no "/") -> assume the mlx-community org.
if [[ "$MODEL" != */* ]]; then
  MODEL="mlx-community/$MODEL"
fi

echo "Downloading $MODEL into the HuggingFace cache..."
hf download "$MODEL"

echo
echo "Done. Serve it with:"
echo "  MODEL=$MODEL ./serve.sh"
