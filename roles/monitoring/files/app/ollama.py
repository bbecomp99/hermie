"""Tiny pure-stdlib Ollama Cloud (ollama.com) API client for the Argus detail page.

Read-only diagnostics against the HOSTED, serverless Ollama API: /api/version,
/api/tags (the cloud model catalog), /api/show (the active model's config), plus
a tiny fixed "canary" generation to measure round-trip throughput (tok/s). Ollama
has NO passive metrics endpoint, so throughput can only be observed by actually
generating — the canary does that cheaply.

Cloud differs from a local server in two ways that shape this client:
  * every call needs an `Authorization: Bearer <key>` header, and
  * it is SERVERLESS — there is no resident model, VRAM, keep-alive, or /api/ps
    (that endpoint returns "unauthorized"), and the canary's phase durations
    (eval_duration/prompt_eval_duration) come back null, so throughput is derived
    from total_duration (end-to-end) instead.

No third-party deps; mirrors the style of mongo.py / hoststat.py.
"""
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone


def _headers(api_key):
    h = {"User-Agent": "argus-ollama/1"}
    if api_key:
        h["Authorization"] = f"Bearer {api_key}"
    return h


def _get(base, path, timeout, api_key=None):
    req = urllib.request.Request(base + path, headers=_headers(api_key))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _post(base, path, payload, timeout, api_key=None):
    headers = _headers(api_key)
    headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        base + path, data=json.dumps(payload).encode(),
        headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


# ---------------------------------------------------------------------------
# Response shaping (drop the megabyte-sized tokenizer arrays /api/show returns)
# ---------------------------------------------------------------------------


def _installed(m):
    d = m.get("details", {}) or {}
    return {
        "name": m.get("name"),
        "size": m.get("size"),
        "modified_at": m.get("modified_at"),
        "family": d.get("family"),
        "parameter_size": d.get("parameter_size"),
        "quantization_level": d.get("quantization_level"),
        "context_length": m.get("context_length"),
        "capabilities": m.get("capabilities", []) or [],
    }


def _model_info(mi):
    """Keep only scalar facts — skip tokenizer.ggml.{tokens,scores,merges,
    token_type} which are huge arrays we never render."""
    return {k: v for k, v in (mi or {}).items()
            if not isinstance(v, (list, dict))}


def _parse_params(text):
    """/api/show returns `parameters` as whitespace-aligned text lines."""
    params = {}
    for line in (text or "").splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2:
            params[parts[0]] = parts[1].strip()
    return params


def _show(base, model, timeout, api_key):
    d = _post(base, "/api/show", {"model": model}, timeout, api_key)
    return {
        "parameters": _parse_params(d.get("parameters")),
        "details": d.get("details", {}) or {},
        "capabilities": d.get("capabilities", []) or [],
        "model_info": _model_info(d.get("model_info", {})),
    }


# ---------------------------------------------------------------------------
# Live snapshot
# ---------------------------------------------------------------------------


def health(base, api_key, model, timeout=8):
    """Live snapshot of Ollama Cloud: version, the model catalog, and the active
    model's config/architecture. Shape mirrors the prior local client (ok/host/
    fetched + error on failure) so the detail page stays compatible. `running` is
    always [] — cloud is serverless, nothing is "resident"."""
    base = base.rstrip("/")
    out = {"ok": False, "host": base.split("://")[-1], "cloud": True,
           "fetched": datetime.now(timezone.utc).isoformat()}
    try:
        out["version"] = _get(base, "/api/version", timeout, api_key).get("version")
        out["running"] = []          # serverless — no resident models / VRAM
        tags = _get(base, "/api/tags", timeout, api_key)
        installed = tags.get("models", []) or []
        out["models"] = [_installed(m) for m in installed]
        # The "active" model is the configured one (cloud loads nothing eagerly).
        focus = model or (installed[0].get("model") if installed else None)
        out["focus"] = focus
        if focus:
            try:
                out["show"] = _show(base, focus, timeout, api_key)
            except Exception:  # noqa: BLE001 - show is best-effort detail
                out["show"] = None
        out["ok"] = True
    except urllib.error.URLError as exc:
        out["error"] = f"{type(exc).__name__}: {getattr(exc, 'reason', exc)}"
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


# ---------------------------------------------------------------------------
# Throughput canary (the only call that performs inference — and is billed)
# ---------------------------------------------------------------------------


def canary(base, api_key, model, num_predict=16, timeout=120):
    """Run a tiny fixed generation against the configured cloud model and return
    throughput metrics, or None. Each call is a real BILLED generation, so the
    caller keeps it tiny + infrequent.

    Cloud returns null phase durations (eval_duration/prompt_eval_duration), so
    eval_tps falls back to an end-to-end figure from total_duration (which also
    folds in queue + network — it's a round-trip throughput, not pure decode)."""
    base = base.rstrip("/")
    payload = {
        "model": model,
        "prompt": "In one short sentence, what is the Parthenon?",
        "stream": False,
        "options": {"num_predict": num_predict, "temperature": 0},
    }
    try:
        d = _post(base, "/api/generate", payload, timeout, api_key)
    except Exception as exc:  # noqa: BLE001 - any failure → no sample this cycle
        print(f"[argus] ollama canary error: {type(exc).__name__}: {exc}",
              flush=True)
        return None
    ec, ed = d.get("eval_count") or 0, d.get("eval_duration")
    pc, pd = d.get("prompt_eval_count") or 0, d.get("prompt_eval_duration")
    td, ld = d.get("total_duration") or 0, d.get("load_duration")
    # Prefer the true decode duration; fall back to end-to-end when cloud omits it.
    if ed:
        eval_tps = round(ec / (ed / 1e9), 1)
    elif td:
        eval_tps = round(ec / (td / 1e9), 1)
    else:
        eval_tps = None
    # Round-trip (total) throughput. Cloud omits every per-phase clock —
    # prompt_eval_duration is null — so prefill speed can't be isolated. Instead
    # report the combined end-to-end rate: ALL tokens processed (prompt + generated)
    # over total_duration. Carried in the prompt_tps field/column, which has only
    # ever been null on cloud (no real prefill data is being displaced).
    roundtrip_tps = round((pc + ec) / (td / 1e9), 1) if td else None
    return {
        "model": model,
        "eval_count": ec,
        "prompt_eval_count": pc,
        "eval_tps": eval_tps,
        "prompt_tps": roundtrip_tps,
        "load_ms": round(ld / 1e6, 1) if ld else None,
        "total_ms": round(td / 1e6, 1) if td else None,
    }
