#!/usr/bin/env python3
"""Argus — a tiny, dependency-free service monitor (the all-seeing watch).

Black-box probes a configured list of HTTP/TCP targets on a background thread,
serves a JSON status API + an astonks-styled dashboard, and posts to Mattermost
on every up<->down transition. Pure stdlib so the image stays ~50MB.

Config: JSON file at $CONFIG_PATH (default /app/config.json).
Secrets: Mattermost bot token via $MM_TOKEN and the Ollama Cloud key via
$OLLAMA_API_KEY (never in the config file).
"""
import json
import os
import socket
import ssl
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import hoststat
import mongo
import ollama
import store as store_mod

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config.json")
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
HISTORY = 60  # samples kept per target (for uptime % + sparkline)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def load_config():
    with open(CONFIG_PATH) as fh:
        cfg = json.load(fh)
    cfg.setdefault("interval", 30)
    cfg.setdefault("listen_port", 9200)
    cfg.setdefault("heartbeat_interval", 0)  # seconds; 0 disables the heartbeat
    mm = cfg.setdefault("mattermost", {})
    mm.setdefault("enable", False)
    mm.setdefault("url", "")
    mm.setdefault("channel_id", "")
    # The token is injected via env, never persisted in the config file.
    mm["token"] = os.environ.get("MM_TOKEN", "")
    if not mm["token"] or not mm["url"] or not mm["channel_id"]:
        mm["enable"] = False
    mp = cfg.setdefault("mongo_perf", {})
    mp.setdefault("enable", False)
    mp.setdefault("write_latency_ms", 25)  # live avg write latency alert ceiling
    mp.setdefault("disk_read_ms", 20)      # avg ms to fault a page from disk
    mp.setdefault("queue_len", 1)          # storage-engine ops waiting on a ticket
    db = cfg.setdefault("db", {})
    db.setdefault("path", "/app/data/argus.db")
    db.setdefault("retention_days", 30)    # prune samples older than this
    hm = cfg.setdefault("host_metrics", {})
    hm.setdefault("enable", False)
    hm.setdefault("ssh_user", "")
    hm.setdefault("ssh_host", "")
    hm.setdefault("ssh_key", "/app/.ssh/id_rsa")
    hm.setdefault("known_hosts", "/app/.ssh/known_hosts")
    ol = cfg.setdefault("ollama", {})
    ol.setdefault("enable", False)
    ol.setdefault("base_url", "")          # Ollama Cloud endpoint (https://ollama.com)
    # Bearer token injected via env, never persisted in the 0644 config file.
    ol["api_key"] = os.environ.get("OLLAMA_API_KEY", "")
    ol.setdefault("model", "")             # model to canary (cloud has no resident)
    ol.setdefault("canary_interval", 900)  # seconds between throughput canaries
    ol.setdefault("num_predict", 16)       # tokens generated per canary
    ol.setdefault("timeout", 120)          # generous: canary may queue server-side
    return cfg


# ---------------------------------------------------------------------------
# Probes
# ---------------------------------------------------------------------------


def probe_http(target):
    """Return (ok, latency_ms, detail). ok if status in expect_status and
    (no expect_body, or expect_body is a substring of the response body)."""
    url = target["url"]
    timeout = target.get("timeout", 5)
    expect_status = set(target.get("expect_status", [200]))
    expect_body = target.get("expect_body")
    ctx = ssl.create_default_context()
    if target.get("insecure"):
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(url, headers={"User-Agent": "hermie-monitor/1"})
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            status = resp.status
            body = resp.read(2048).decode("utf-8", "replace") if expect_body else ""
        latency = round((time.monotonic() - started) * 1000)
        if status not in expect_status:
            return False, latency, f"HTTP {status}"
        if expect_body and expect_body not in body:
            return False, latency, f"body missing {expect_body!r}"
        return True, latency, f"HTTP {status}"
    except urllib.error.HTTPError as exc:
        latency = round((time.monotonic() - started) * 1000)
        # Some services answer auth-gated roots with 401/403 yet are "up".
        if exc.code in expect_status:
            return True, latency, f"HTTP {exc.code}"
        return False, latency, f"HTTP {exc.code}"
    except Exception as exc:  # noqa: BLE001 - any failure is "down"
        latency = round((time.monotonic() - started) * 1000)
        return False, latency, type(exc).__name__


def probe_tcp(target):
    host = target["host"]
    port = int(target["port"])
    timeout = target.get("timeout", 5)
    started = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            latency = round((time.monotonic() - started) * 1000)
            return True, latency, "connected"
    except Exception as exc:  # noqa: BLE001
        latency = round((time.monotonic() - started) * 1000)
        return False, latency, type(exc).__name__


PROBES = {"http": probe_http, "tcp": probe_tcp}


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


class State:
    def __init__(self, targets):
        self.lock = threading.Lock()
        self.targets = {}
        for t in targets:
            self.targets[t["name"]] = {
                "name": t["name"],
                "group": t.get("group", ""),
                "kind": t["type"],
                "label": t.get("label", _describe(t)),
                "status": "unknown",   # up | down | unknown
                "detail": "",
                "degraded": False,
                "degraded_detail": "",
                "latency_ms": None,
                "since": None,         # ISO ts of the current status
                "last_checked": None,
                "history": deque(maxlen=HISTORY),  # 1=up, 0=down
            }

    def seed(self, name, hist):
        """Reseed a target's history ring from persisted samples after a restart,
        so uptime% survives a redeploy."""
        with self.lock:
            if name in self.targets and hist:
                self.targets[name]["history"].extend(hist)

    def set_degraded(self, name, is_degraded, detail=""):
        with self.lock:
            if name in self.targets:
                self.targets[name]["degraded"] = is_degraded
                self.targets[name]["degraded_detail"] = detail

    def update(self, name, ok, latency, detail):
        now = datetime.now(timezone.utc)
        with self.lock:
            s = self.targets[name]
            new = "up" if ok else "down"
            transitioned = s["status"] not in ("unknown", new)
            first = s["status"] == "unknown"
            if s["status"] != new:
                s["since"] = now.isoformat()
            s["status"] = new
            s["detail"] = detail
            s["latency_ms"] = latency
            s["last_checked"] = now.isoformat()
            s["history"].append(1 if ok else 0)
        return transitioned, first, new

    def snapshot(self):
        with self.lock:
            out = []
            up = down = 0
            for s in self.targets.values():
                hist = list(s["history"])
                uptime = round(100 * sum(hist) / len(hist), 1) if hist else None
                
                status_out = "degraded" if s["status"] == "up" and s.get("degraded") else s["status"]
                detail_out = s["degraded_detail"] if status_out == "degraded" and s.get("degraded_detail") else s["detail"]
                
                if status_out in ("up", "degraded"):
                    up += 1
                elif status_out == "down":
                    down += 1
                out.append({
                    "name": s["name"],
                    "group": s["group"],
                    "kind": s["kind"],
                    "label": s["label"],
                    "status": status_out,
                    "detail": detail_out,
                    "latency_ms": s["latency_ms"],
                    "since": s["since"],
                    "last_checked": s["last_checked"],
                    "uptime": uptime,
                    "spark": hist,
                })
            out.sort(key=lambda x: (x["status"] != "down", x["group"], x["name"]))
            return {
                "generated": datetime.now(timezone.utc).isoformat(),
                "summary": {"total": len(out), "up": up, "down": down},
                "targets": out,
            }


def _describe(t):
    if t["type"] == "http":
        return t["url"]
    if t["type"] == "tcp":
        return f"tcp://{t['host']}:{t['port']}"
    return t["type"]


# ---------------------------------------------------------------------------
# Mattermost alerting (transitions only)
# ---------------------------------------------------------------------------


def post_mattermost(mm, text):
    url = mm["url"].rstrip("/") + "/api/v4/posts"
    payload = json.dumps({"channel_id": mm["channel_id"], "message": text}).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {mm['token']}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            return True
    except Exception as exc:  # noqa: BLE001
        print(f"[argus] mattermost post failed: {exc}", flush=True)
        return False


def alert(mm, target, new_status, detail):
    if not mm.get("enable"):
        return
    if new_status == "down":
        text = f"👁 **Argus** · :red_circle: **{target}** is DOWN — {detail}"
    else:
        text = f"👁 **Argus** · :large_green_circle: **{target}** RECOVERED — {detail}"
    post_mattermost(mm, text)


def heartbeat(mm, snap):
    """Periodic 'still alive' post — sent regardless of transitions, so silence
    from the monitor can't be mistaken for everything being healthy."""
    if not mm.get("enable"):
        return
    s = snap["summary"]
    if s["down"] == 0:
        text = f"👁 **Argus** · :white_check_mark: **All clear** — {s['up']}/{s['total']} services up"
    else:
        downs = ", ".join(t["name"] for t in snap["targets"] if t["status"] == "down")
        text = (f"👁 **Argus** · :yellow_heart: **Heartbeat** — {s['up']}/{s['total']} up, "
                f"{s['down']} DOWN: {downs}")
    post_mattermost(mm, text)


# ---------------------------------------------------------------------------
# Monitor loop
# ---------------------------------------------------------------------------


def _avg_ms(cur, prev, key):
    dops = (cur[key]["ops"] or 0) - (prev[key]["ops"] or 0)
    dlat = (cur[key]["latency"] or 0) - (prev[key]["latency"] or 0)
    return round(dlat / dops / 1000, 2) if dops > 0 and dlat >= 0 else None


def eval_mongo_perf(prev, cur, thr):
    """Compare two perf_sample()s → (degraded_bool, reasons, metrics). These are
    the in-engine equivalents of classic 'disk queue rising / CPU saturated':
    op latency climbing, storage tickets exhausted, ops queued, IO stalls."""
    m = {
        "write_ms": _avg_ms(cur, prev, "writes"),
        "read_ms": _avg_ms(cur, prev, "reads"),
        "cmd_ms": _avg_ms(cur, prev, "commands"),
        "queue": max((cur["qWrite"].get("queueLength") or 0),
                     (cur["qRead"].get("queueLength") or 0)),
        "w_tickets_avail": cur["qWrite"].get("available"),
        "w_tickets_total": cur["qWrite"].get("totalTickets"),
        "conns": cur.get("connsCurrent"),
    }
    dc = (cur["appDiskReadCount"] or 0) - (prev["appDiskReadCount"] or 0)
    dt = (cur["appDiskReadTimeUs"] or 0) - (prev["appDiskReadTimeUs"] or 0)
    m["disk_ms"] = round(dt / dc / 1000, 2) if dc > 0 and dt >= 0 else None
    m["dirty_pct"] = (round(100 * cur["dirtyBytes"] / cur["cacheMaxBytes"], 2)
                      if cur.get("dirtyBytes") is not None and cur.get("cacheMaxBytes")
                      else None)

    reasons = []
    if m["write_ms"] is not None and m["write_ms"] > thr["write_latency_ms"]:
        reasons.append(f"write latency {m['write_ms']:.0f}ms (>{thr['write_latency_ms']})")
    if m["disk_ms"] is not None and m["disk_ms"] > thr["disk_read_ms"]:
        reasons.append(f"disk read {m['disk_ms']:.0f}ms/page (>{thr['disk_read_ms']})")
    if m["queue"] >= thr["queue_len"]:
        reasons.append(f"storage queue {m['queue']} op(s) waiting")
    if cur["qWrite"].get("available") == 0 or cur["qRead"].get("available") == 0:
        reasons.append("tickets exhausted (0 available)")
    if cur.get("isLagged"):
        reasons.append("flow control: replication lagged")
    return bool(reasons), reasons, m


def post_mongo_perf(mm, degraded, reasons):
    if not mm.get("enable"):
        return
    if degraded:
        text = "👁 **Argus** · :warning: **MongoDB degraded** — " + "; ".join(reasons)
    else:
        text = ("👁 **Argus** · :large_green_circle: **MongoDB recovered** — "
                "latency / storage queue back to normal")
    post_mattermost(mm, text)


def monitor_loop(cfg, state, store=None, host_latest=None):
    targets = {t["name"]: t for t in cfg["targets"]}
    interval = cfg["interval"]
    hb_interval = cfg["heartbeat_interval"]
    retention_days = cfg["db"]["retention_days"]
    mm = cfg["mattermost"]
    mp = cfg["mongo_perf"]
    hm = cfg["host_metrics"]
    mongo_t = next((t for t in cfg["targets"]
                    if t["type"] == "tcp" and "mongo" in t["name"].lower()), None)
    mongo_prev = None      # previous perf_sample for delta-based latency
    mongo_degraded = False  # last posted state (alert only on transitions)
    # Count the heartbeat clock from startup, so the first one lands a full
    # interval in (no spam on every redeploy/restart).
    last_heartbeat = time.monotonic()
    last_prune = 0.0
    while True:
        cycle_start = time.monotonic()
        cycle_rows = []
        for name, t in targets.items():
            probe = PROBES.get(t["type"])
            if probe is None:
                continue
            ok, latency, detail = probe(t)
            transitioned, first, new = state.update(name, ok, latency, detail)
            tag = "OK " if ok else "DOWN"
            print(f"[argus] {tag} {name} ({latency}ms) {detail}", flush=True)
            cycle_rows.append((name, new, latency))
            if transitioned:
                alert(mm, name, new, detail)
                if store:
                    store.insert_event(name, "transition", f"{new}: {detail}")
        if store and cycle_rows:
            store.insert_samples(cycle_rows)

        if mp["enable"] and mongo_t is not None:
            cur = mongo.perf_sample(mongo_t["host"], int(mongo_t["port"]),
                                    timeout=mongo_t.get("timeout", 5))
            if cur and mongo_prev:
                degraded, reasons, metrics = eval_mongo_perf(mongo_prev, cur, mp)
                state.set_degraded(mongo_t["name"], degraded, "; ".join(reasons))
                print(f"[argus] mongo perf {'DEGRADED' if degraded else 'ok'} "
                      f"{metrics}", flush=True)
                if store:
                    store.insert_mongo_perf(metrics)
                if degraded != mongo_degraded:
                    mongo_degraded = degraded
                    post_mongo_perf(mm, degraded, reasons)
                    if store:
                        store.insert_event(
                            "mongodb", "perf",
                            ("degraded: " + "; ".join(reasons)) if degraded
                            else "recovered")
            if cur:
                mongo_prev = cur

        if hm["enable"] and hm["ssh_host"]:
            hs = hoststat.sample(hm["ssh_user"], hm["ssh_host"],
                                 hm["ssh_key"], hm["known_hosts"])
            if hs:
                hs["fetched"] = datetime.now(timezone.utc).isoformat()
                hs["host"] = hm["ssh_host"]
                if host_latest is not None:
                    host_latest.clear()
                    host_latest.update(hs)
                print(f"[argus] host {hm['ssh_host']} cpu={hs['cpu_pct']}% "
                      f"mem={hs['mem_pct']}%", flush=True)
                if store:
                    store.insert_host_metrics(hm["ssh_host"], hs)

        if store and (time.monotonic() - last_prune) >= 3600:
            store.prune(retention_days)
            last_prune = time.monotonic()

        if hb_interval > 0 and (time.monotonic() - last_heartbeat) >= hb_interval:
            snap = state.snapshot()
            print(f"[argus] heartbeat — {snap['summary']}", flush=True)
            heartbeat(mm, snap)
            last_heartbeat = time.monotonic()
        elapsed = time.monotonic() - cycle_start
        time.sleep(max(1, interval - elapsed))


# ---------------------------------------------------------------------------
# Ollama throughput canary (own thread — a slow generation must not stall probes)
# ---------------------------------------------------------------------------


def canary_loop(cfg, store=None, ollama_latest=None):
    """Periodically measure Ollama Cloud round-trip throughput (tok/s) by running
    a tiny generation against the configured model. Cloud is serverless — there's
    no resident model to detect — and EACH canary is a real billed generation, so
    it's kept tiny + infrequent. Runs on its own thread so a slow / queued
    generation can't delay the black-box probe cycle."""
    ol = cfg["ollama"]
    interval = max(60, int(ol.get("canary_interval", 900)))
    timeout = int(ol.get("timeout", 120))
    base = ol.get("base_url", "")
    api_key = ol.get("api_key", "")
    model = (ol.get("model") or "").strip()
    while True:
        if not model:
            print("[argus] ollama canary skipped — no model configured",
                  flush=True)
        else:
            res = ollama.canary(base, api_key, model,
                                num_predict=int(ol.get("num_predict", 16)),
                                timeout=timeout)
            if res:
                res["fetched"] = datetime.now(timezone.utc).isoformat()
                if ollama_latest is not None:
                    ollama_latest.clear()
                    ollama_latest.update(res)
                print(f"[argus] ollama canary {res['model']} "
                      f"eval={res['eval_tps']} tok/s "
                      f"total={res['total_ms']}ms", flush=True)
                if store:
                    store.insert_ollama_perf(res)
        time.sleep(interval)


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------


def make_handler(state, cfg, store=None, host_latest=None, ollama_latest=None):
    # The mongo detail endpoint targets the first tcp probe named like "mongo".
    mongo_target = next(
        (t for t in cfg["targets"]
         if t["type"] == "tcp" and "mongo" in t["name"].lower()), None)
    hm_host = cfg["host_metrics"].get("ssh_host", "")
    ol_cfg = cfg["ollama"]

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # silence per-request logging
            pass

        def _send(self, code, body, ctype="application/json"):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _query(self):
            from urllib.parse import parse_qs, urlparse
            return parse_qs(urlparse(self.path).query)

        def do_GET(self):  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path == "/api/status":
                self._send(200, json.dumps(state.snapshot()).encode())
            elif path == "/api/mongo":
                if mongo_target is None:
                    self._send(404, b'{"ok":false,"error":"no mongo target configured"}')
                else:
                    data = mongo.health(
                        mongo_target["host"], int(mongo_target["port"]),
                        timeout=mongo_target.get("timeout", 5))
                    # attach the latest host CPU/mem (sampled by the loop via SSH)
                    if host_latest:
                        data["hostMetrics"] = dict(host_latest)
                    self._send(200, json.dumps(data).encode())
            elif path == "/api/host/history":
                if store is None or not hm_host:
                    self._send(200, b"[]")
                else:
                    q = self._query()
                    hours = float(q.get("hours", ["6"])[0])
                    self._send(200, json.dumps(
                        store.host_history(hm_host, hours=hours)).encode())
            elif path == "/api/ollama":
                if not ol_cfg.get("enable") or not ol_cfg.get("base_url"):
                    self._send(404, b'{"ok":false,"error":"ollama not configured"}')
                else:
                    data = ollama.health(ol_cfg["base_url"],
                                         ol_cfg.get("api_key", ""),
                                         ol_cfg.get("model", ""), timeout=8)
                    if ollama_latest:
                        data["canary"] = dict(ollama_latest)
                    self._send(200, json.dumps(data).encode())
            elif path == "/api/ollama/history":
                if store is None:
                    self._send(200, b"[]")
                else:
                    q = self._query()
                    hours = float(q.get("hours", ["24"])[0])
                    self._send(200, json.dumps(
                        store.ollama_history(hours=hours)).encode())
            elif path == "/api/mongo/history":
                if store is None:
                    self._send(200, b"[]")
                else:
                    q = self._query()
                    hours = float(q.get("hours", ["6"])[0])
                    self._send(200, json.dumps(store.mongo_history(hours=hours)).encode())
            elif path == "/api/history":
                if store is None:
                    self._send(200, b"[]")
                else:
                    q = self._query()
                    target = q.get("target", [""])[0]
                    hours = float(q.get("hours", ["24"])[0])
                    self._send(200, json.dumps(store.history(target, hours=hours)).encode())
            elif path == "/healthz":
                self._send(200, b'{"status":"ok"}')
            elif path in ("/", "/index.html"):
                self._serve_static("index.html", "text/html; charset=utf-8")
            else:
                # only serve known static files, no traversal
                name = os.path.basename(path)
                if name and os.path.isfile(os.path.join(STATIC_DIR, name)):
                    ctype = "text/html; charset=utf-8" if name.endswith(".html") else "text/plain"
                    self._serve_static(name, ctype)
                else:
                    self._send(404, b'{"error":"not found"}')

        def _serve_static(self, name, ctype):
            try:
                with open(os.path.join(STATIC_DIR, name), "rb") as fh:
                    self._send(200, fh.read(), ctype)
            except OSError:
                self._send(404, b'{"error":"not found"}')

    return Handler


def main():
    cfg = load_config()
    state = State(cfg["targets"])

    store = None
    try:
        store = store_mod.Store(cfg["db"]["path"])
        for t in cfg["targets"]:
            state.seed(t["name"], store.recent_status(t["name"], HISTORY))
        print(f"[argus] persistence: {cfg['db']['path']} "
              f"(retention {cfg['db']['retention_days']}d)", flush=True)
    except Exception as exc:  # noqa: BLE001 - run without persistence if it fails
        print(f"[argus] persistence DISABLED: {type(exc).__name__}: {exc}", flush=True)
        store = None

    host_latest = {}  # shared: loop writes the newest host CPU/mem, handler reads
    t = threading.Thread(target=monitor_loop,
                         args=(cfg, state, store, host_latest), daemon=True)
    t.start()

    # Ollama throughput canary on its own thread (slow generations must not
    # block probes); handler reads the newest sample from ollama_latest.
    ollama_latest = {}
    if cfg["ollama"]["enable"] and cfg["ollama"]["base_url"]:
        threading.Thread(target=canary_loop,
                         args=(cfg, store, ollama_latest), daemon=True).start()

    port = int(cfg["listen_port"])
    print(f"[argus] serving on :{port}, {len(cfg['targets'])} targets, "
          f"interval={cfg['interval']}s, mattermost={cfg['mattermost']['enable']}, "
          f"host_metrics={cfg['host_metrics']['enable']}, "
          f"ollama={cfg['ollama']['enable']}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port),
                        make_handler(state, cfg, store, host_latest,
                                     ollama_latest)).serve_forever()


if __name__ == "__main__":
    main()
