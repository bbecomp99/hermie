# ai-serve

Local OpenAI-compatible LLM server on Apple Silicon (M1) using [MLX](https://github.com/ml-explore/mlx).

**Model:** `Qwen2.5-Coder-7B-Instruct` (4-bit) — a strong coding assistant that fits comfortably in 12GB of RAM (~5GB resident).

## Setup (one time)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run the server

```bash
./serve.sh
```

First launch downloads the model (~4.3GB) to `~/.cache/huggingface`. The endpoint is:

```
http://localhost:8080/v1/chat/completions
```

It speaks the OpenAI API, so any OpenAI client/SDK works — just point `base_url` at it.

> **Note:** this MLX server uses the request's `model` field to choose which model to
> load, so it must match the served repo id (e.g. `mlx-community/Qwen2.5-Coder-7B-Instruct-4bit`),
> not a placeholder like `"local"`. `chat.py` reads it from the `MODEL` env var.

## Test it

In another terminal:

```bash
source .venv/bin/activate
python chat.py "write a python function to debounce a callback"
```

## Swap models

Any [mlx-community](https://huggingface.co/mlx-community) model works:

```bash
# Lighter / faster (3B) — great if you want snappier responses:
MODEL=mlx-community/Qwen2.5-Coder-3B-Instruct-4bit ./serve.sh

# General chat instead of coding:
MODEL=mlx-community/Llama-3.1-8B-Instruct-4bit ./serve.sh
```

