"""Tiny pure-stdlib Ollama API client for the Argus Ollama detail page.

Read-only diagnostics: queries Ollama's HTTP API (/api/version, /api/ps,
/api/tags, /api/show) for a live snapshot, and runs a tiny fixed "canary"
generation to measure throughput (tok/s). Ollama has NO passive metrics
endpoint, so throughput can only be observed by actually generating — the
canary does that cheaply (a handful of tokens against an already-loaded model).

No third-party deps; mirrors the style of mongo.py / hoststat.py.
"""
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone


def _get(base, path, timeout):
    req = urllib.request.Request(base + path,
                                 headers={"User-Agent": "argus-ollama/1"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _post(base, path, payload, timeout):
    req = urllib.request.Request(
        base + path, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json",
                 "User-Agent": "argus-ollama/1"},
        method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


# ---------------------------------------------------------------------------
# Response shaping (drop the megabyte-sized tokenizer arrays /api/show returns)
# ---------------------------------------------------------------------------


def _running(m):
    return {
        "name": m.get("name"),
        "model": m.get("model"),
        "size": m.get("size"),
        "size_vram": m.get("size_vram"),
        "context_length": m.get("context_length"),
        "expires_at": m.get("expires_at"),
        "details": m.get("details", {}) or {},
    }


def _installed(m):
    d = m.get("details", {}) or {}
    return {
        "name": m.get("name"),
        "size": m.get("size"),
        "modified_at": m.get("modified_at"),
        "family": d.get("family"),
        "parameter_size": d.get("parameter_size"),
        "quantization_level": d.get("quantization_level"),
        "context_length": d.get("context_length"),
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


def _show(base, model, timeout):
    d = _post(base, "/api/show", {"model": model}, timeout)
    return {
        "parameters": _parse_params(d.get("parameters")),
        "details": d.get("details", {}) or {},
        "capabilities": d.get("capabilities", []) or [],
        "model_info": _model_info(d.get("model_info", {})),
    }


# ---------------------------------------------------------------------------
# Live snapshot
# ---------------------------------------------------------------------------


def health(host, port, timeout=8):
    """Live snapshot: version, running models, installed models, and the active
    model's config/architecture. Shape mirrors mongo.health() (ok/host/fetched
    + error on failure)."""
    base = f"http://{host}:{port}"
    out = {"ok": False, "host": f"{host}:{port}",
           "fetched": datetime.now(timezone.utc).isoformat()}
    try:
        out["version"] = _get(base, "/api/version", timeout).get("version")
        ps = _get(base, "/api/ps", timeout)
        running = ps.get("models", []) or []
        out["running"] = [_running(m) for m in running]
        tags = _get(base, "/api/tags", timeout)
        installed = tags.get("models", []) or []
        out["models"] = [_installed(m) for m in installed]
        # Show config for the loaded model (fall back to the first installed).
        focus = (running[0].get("model") if running
                 else (installed[0].get("model") if installed else None))
        out["focus"] = focus
        if focus:
            try:
                out["show"] = _show(base, focus, timeout)
            except Exception:  # noqa: BLE001 - show is best-effort detail
                out["show"] = None
        out["ok"] = True
    except urllib.error.URLError as exc:
        out["error"] = f"{type(exc).__name__}: {getattr(exc, 'reason', exc)}"
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


# ---------------------------------------------------------------------------
# Throughput canary (the only call that performs inference)
# ---------------------------------------------------------------------------


def loaded(host, port, timeout=8):
    """Return the model currently RESIDENT in memory as {model, context_length},
    or None. The canary uses this so it only ever probes an already-loaded model
    — it must never be the thing that triggers a load."""
    try:
        ps = _get(f"http://{host}:{port}", "/api/ps", timeout)
    except Exception:  # noqa: BLE001
        return None
    models = ps.get("models", []) or []
    if not models:
        return None
    m = models[0]
    return {"model": m.get("model") or m.get("name"),
            "context_length": m.get("context_length")}


def canary(host, port, model, num_ctx=None, num_predict=16, keep_alive=-1,
           timeout=120):
    """Run a tiny fixed generation and return throughput metrics, or None.

    SAFETY (this model is shared with the live agent): we pass num_ctx matching
    the already-loaded runner and keep_alive=-1, so Ollama reuses the exact
    resident instance — it never reloads, resizes, or shortens the keep-alive of
    the production model. Omitting num_ctx/keep_alive would load a SECOND default
    (4096, 5-min) instance and evict the agent's — so both are always sent.
    Caller must only pass a model that is currently loaded (see loaded())."""
    base = f"http://{host}:{port}"
    options = {"num_predict": num_predict, "temperature": 0}
    if num_ctx:
        options["num_ctx"] = num_ctx          # match the runner → no reload
    payload = {
        "model": model,
        "prompt": "In one short sentence, what is the Parthenon?",
        "stream": False,
        "keep_alive": keep_alive,             # -1 = preserve the existing pin
        "options": options,
    }
    try:
        d = _post(base, "/api/generate", payload, timeout)
    except Exception as exc:  # noqa: BLE001 - any failure → no sample this cycle
        print(f"[argus] ollama canary error: {type(exc).__name__}: {exc}",
              flush=True)
        return None
    ec, ed = d.get("eval_count") or 0, d.get("eval_duration") or 0
    pc, pd = d.get("prompt_eval_count") or 0, d.get("prompt_eval_duration") or 0
    return {
        "model": model,
        "eval_count": ec,
        "prompt_eval_count": pc,
        "eval_tps": round(ec / (ed / 1e9), 1) if ed else None,
        "prompt_tps": round(pc / (pd / 1e9), 1) if pd else None,
        "load_ms": round((d.get("load_duration") or 0) / 1e6, 1),
        "total_ms": round((d.get("total_duration") or 0) / 1e6, 1),
    }
