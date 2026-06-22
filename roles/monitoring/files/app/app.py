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

import elastic
import hoststat
import kafka
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
    mm.setdefault("rich", True)   # colour-barred attachments + metric fields + sparkline
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
    es = cfg.setdefault("elastic", {})
    es.setdefault("enable", False)
    es.setdefault("url", "")               # ES REST base (http://host:9200)
    es.setdefault("username", "")          # optional HTTP basic auth (anon by default)
    es["password"] = os.environ.get("ES_PASSWORD", "")  # injected via env, never in config
    es.setdefault("heap_pct", 85)          # degrade if any node's JVM heap exceeds this
    es.setdefault("cpu_pct", 90)           # degrade if any node's CPU exceeds this
    es.setdefault("degrade_on_yellow", False)  # single-node clusters sit yellow normally
    kf = cfg.setdefault("kafka", {})
    kf.setdefault("enable", False)
    kf.setdefault("host", "")
    kf.setdefault("port", 9092)
    kf.setdefault("client_id", "argus")
    kf.setdefault("min_brokers", 1)        # degrade if fewer brokers than this are up
    kf.setdefault("track_traffic", True)   # pull log-end offsets → throughput
    kf.setdefault("track_lag", True)        # pull consumer-group committed offsets → lag
    kf.setdefault("lag_degrade", 0)        # total lag above this degrades (0 = display only)
    it = cfg.setdefault("internet", {})
    it.setdefault("enable", False)
    it.setdefault("group", "internet")     # probe group that feeds the QoS panel
    it.setdefault("poor_ms", 400)          # latency above this is "poor" (amber)
    it.setdefault("degrade_ms", 800)       # reachable but slower than this => degraded
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
# Internet QoS — aggregate the "internet"-group probes into a quality-of-service
# view (uptime, avg/p95 latency, jitter). No new sampling or table: the per-cycle
# status + latency_ms for these targets is already persisted to `samples`, so the
# QoS stats are computed straight from store.history().
# ---------------------------------------------------------------------------


def _percentile(values, pct):
    """Linear-interpolated percentile of a value list (values need not be sorted)."""
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * pct / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


def _qos_stats(store, name, hours):
    """QoS aggregates for one URL from the samples table: uptime %, latency
    avg/min/max/p95, and jitter (mean abs delta between consecutive latencies).
    Latency stats use successful probes only — a failed probe's latency is just
    the time-to-failure and would skew the numbers."""
    rows = store.history(name, hours=hours) if store else []
    if not rows:
        return {"samples": 0, "uptime_pct": None, "avg_ms": None, "min_ms": None,
                "max_ms": None, "p95_ms": None, "jitter_ms": None}
    ups = sum(1 for r in rows if r["status"] == "up")
    lat = [r["latency_ms"] for r in rows
           if r["status"] == "up" and r["latency_ms"] is not None]
    jitter = None
    if len(lat) >= 2:
        diffs = [abs(lat[i] - lat[i - 1]) for i in range(1, len(lat))]
        jitter = round(sum(diffs) / len(diffs), 1)
    return {
        "samples": len(rows),
        "uptime_pct": round(100 * ups / len(rows), 1),
        "avg_ms": round(sum(lat) / len(lat)) if lat else None,
        "min_ms": min(lat) if lat else None,
        "max_ms": max(lat) if lat else None,
        "p95_ms": round(_percentile(lat, 95)) if lat else None,
        "jitter_ms": jitter,
    }


def internet_snapshot(state, store, int_cfg, hours=1):
    """Live status (from the in-memory ring) + QoS aggregates (from SQLite) for
    every probe in the configured internet group — the Global URL trackers."""
    group = int_cfg.get("group", "internet")
    urls = []
    up = down = degraded = 0
    avg_acc = []
    for t in state.snapshot()["targets"]:
        if t.get("group") != group:
            continue
        qos = _qos_stats(store, t["name"], hours)
        urls.append({
            "name": t["name"], "label": t["label"], "group": t["group"],
            "status": t["status"], "latency_ms": t["latency_ms"],
            "uptime": t["uptime"], "since": t["since"],
            "last_checked": t["last_checked"], "detail": t["detail"],
            "spark": t["spark"], "qos": qos,
        })
        if t["status"] == "down":
            down += 1
        elif t["status"] == "degraded":
            degraded += 1
        else:
            up += 1
        if qos.get("avg_ms") is not None:
            avg_acc.append(qos["avg_ms"])
    urls.sort(key=lambda u: u["name"])
    return {
        "ok": True,
        "generated": datetime.now(timezone.utc).isoformat(),
        "group": group,
        "hours": hours,
        "thresholds": {"poor_ms": int_cfg.get("poor_ms", 400),
                       "degrade_ms": int_cfg.get("degrade_ms", 800)},
        "summary": {
            "total": len(urls), "up": up, "down": down, "degraded": degraded,
            "avg_ms": round(sum(avg_acc) / len(avg_acc)) if avg_acc else None,
        },
        "urls": urls,
    }


# ---------------------------------------------------------------------------
# Mattermost alerting (transitions only)
# ---------------------------------------------------------------------------


# Attachment colour bar (Slack-compatible; Mattermost renders props.attachments).
C_DOWN = "#d24b4e"   # red    — down / degraded
C_UP = "#3db887"     # green  — recovered / all clear
C_WARN = "#e0a800"   # amber  — heartbeat with outstanding issues
SPARK = "▁▂▃▄▅▆▇█"   # pure-stdlib "graph": no matplotlib, keeps Argus dep-free


def _sparkline(values, width=30):
    """A Unicode trend line from a numeric series (None entries skipped). Returns
    '' when there are <2 real points so callers can omit the field entirely."""
    vals = [v for v in values if isinstance(v, (int, float))][-width:]
    if len(vals) < 2:
        return ""
    lo, hi = min(vals), max(vals)
    span = hi - lo
    if span == 0:
        return SPARK[0] * len(vals)
    n = len(SPARK) - 1
    return "".join(SPARK[int((v - lo) / span * n)] for v in vals)


def _field(title, value, short=True):
    return {"title": title, "value": value, "short": short}


def _spark_field(label, values, unit="", fmt="{:.0f}"):
    """A full-width attachment field: a sparkline + the current value and lo→hi
    range of the series. None when there isn't enough history to draw."""
    spark = _sparkline(values)
    if not spark:
        return None
    vals = [v for v in values if isinstance(v, (int, float))]
    cur = fmt.format(vals[-1]) + unit
    rng = f"{fmt.format(min(vals))}–{fmt.format(max(vals))}{unit}"
    return _field(f"{label} · last {len(vals)} (now {cur})",
                  f"`{spark}`  range {rng}", short=False)


def post_mattermost(mm, text, attachments=None):
    url = mm["url"].rstrip("/") + "/api/v4/posts"
    body = {"channel_id": mm["channel_id"], "message": text}
    if attachments:
        body["props"] = {"attachments": attachments}
    payload = json.dumps(body).encode()
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


def _post(mm, text, color=None, fields=None):
    """Post a headline plus (when rich alerts are enabled) a colour-barred
    attachment carrying metric fields and a sparkline. Falls back to the plain
    one-liner when mm.rich is off, so the alert is never lost."""
    attachments = None
    if mm.get("rich", True) and fields:
        attachments = [{"color": color or C_DOWN, "mrkdwn_in": ["text", "fields"],
                        "fields": fields, "footer": "Argus · the all-seeing watch"}]
    post_mattermost(mm, text, attachments)


def alert(mm, target, new_status, detail, store=None):
    if not mm.get("enable"):
        return
    down = new_status == "down"
    if down:
        text = f"👁 **Argus** · :red_circle: **{target}** is DOWN — {detail}"
    else:
        text = f"👁 **Argus** · :large_green_circle: **{target}** RECOVERED — {detail}"
    fields = []
    if store:
        hist = store.history(target, hours=2, limit=240)
        sf = _spark_field("Latency", [h["latency_ms"] for h in hist], unit="ms")
        if sf:
            fields.append(sf)
        statuses = [h["status"] for h in hist]
        if statuses:
            up = sum(1 for s in statuses if s == "up")
            fields.append(_field("Uptime (2h)",
                                 f"{100 * up / len(statuses):.0f}% ({up}/{len(statuses)} probes)"))
    _post(mm, text, C_UP if not down else C_DOWN, fields)


def heartbeat(mm, snap):
    """Periodic 'still alive' post — sent regardless of transitions, so silence
    from the monitor can't be mistaken for everything being healthy."""
    if not mm.get("enable"):
        return
    s = snap["summary"]
    targets = snap.get("targets", [])
    if s["down"] == 0:
        text = f"👁 **Argus** · :white_check_mark: **All clear** — {s['up']}/{s['total']} services up"
        color = C_UP
    else:
        downs = ", ".join(t["name"] for t in targets if t["status"] == "down")
        text = (f"👁 **Argus** · :yellow_heart: **Heartbeat** — {s['up']}/{s['total']} up, "
                f"{s['down']} DOWN: {downs}")
        color = C_WARN
    fields = [_field("Services up", f"{s['up']}/{s['total']}")]
    downs = [t["name"] for t in targets if t["status"] == "down"]
    if downs:
        fields.append(_field("Down", ", ".join(downs), short=False))
    degraded = [t["name"] for t in targets if t.get("status") == "degraded"]
    if degraded:
        fields.append(_field("Degraded", ", ".join(degraded), short=False))
    _post(mm, text, color, fields)


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


def post_mongo_perf(mm, degraded, reasons, metrics=None, store=None):
    if not mm.get("enable"):
        return
    if degraded:
        text = "👁 **Argus** · :warning: **MongoDB degraded** — " + "; ".join(reasons)
    else:
        text = ("👁 **Argus** · :large_green_circle: **MongoDB recovered** — "
                "latency / storage queue back to normal")
    fields = []
    if metrics:
        for title, key in (("Write latency", "write_ms"), ("Read latency", "read_ms"),
                           ("Disk/page", "disk_ms")):
            v = metrics.get(key)
            if v is not None:
                fields.append(_field(title, f"{v:.1f}ms"))
        if metrics.get("queue") is not None:
            fields.append(_field("Storage queue", str(metrics["queue"])))
        if metrics.get("conns") is not None:
            fields.append(_field("Connections", str(metrics["conns"])))
    if store:
        sf = _spark_field("Write latency",
                          [h["write_ms"] for h in store.mongo_history(hours=3)],
                          unit="ms", fmt="{:.1f}")
        if sf:
            fields.append(sf)
    _post(mm, text, C_DOWN if degraded else C_UP, fields)


def eval_elastic_perf(prev, cur, thr, dt):
    """Compare two elastic.perf_sample()s → (degraded, reasons, metrics). Cluster
    RED and saturation (heap / cpu / thread-pool rejections) always degrade; a
    yellow status (normal for single-node clusters with replicas) only degrades
    when degrade_on_yellow is set. QPS is derived from cumulative-counter deltas."""
    m = {
        "status": cur.get("status"), "nodes": cur.get("nodes"),
        "unassigned": cur.get("unassigned"), "heap_pct": cur.get("heap_pct"),
        "cpu_pct": cur.get("cpu_pct"), "search_qps": None, "index_qps": None,
    }
    rej = 0
    if prev and dt > 0:
        ds = (cur.get("search_total") or 0) - (prev.get("search_total") or 0)
        di = (cur.get("index_total") or 0) - (prev.get("index_total") or 0)
        m["search_qps"] = round(ds / dt, 2) if ds >= 0 else None
        m["index_qps"] = round(di / dt, 2) if di >= 0 else None
        rej = (cur.get("rejected") or 0) - (prev.get("rejected") or 0)

    reasons = []
    st = cur.get("status")
    if st == "red":
        reasons.append("cluster status RED")
    elif st == "yellow" and thr.get("degrade_on_yellow"):
        reasons.append("cluster status yellow")
        if (cur.get("unassigned") or 0) > 0:
            reasons.append(f"{cur['unassigned']} unassigned shard(s)")
    if cur.get("heap_pct") is not None and cur["heap_pct"] > thr["heap_pct"]:
        reasons.append(f"heap {cur['heap_pct']:.0f}% (>{thr['heap_pct']})")
    if cur.get("cpu_pct") is not None and cur["cpu_pct"] > thr["cpu_pct"]:
        reasons.append(f"cpu {cur['cpu_pct']:.0f}% (>{thr['cpu_pct']})")
    if rej > 0:
        reasons.append(f"{rej} thread-pool rejection(s)")
    return bool(reasons), reasons, m


def eval_kafka_perf(prev, cur, thr, dt):
    """Derive (degraded, reasons, metrics) from two kafka.perf_sample()s. Cluster
    signals are instantaneous (offline / under-replicated partitions, a missing
    controller, a shrunken broker count); throughput (msgs/sec) comes from the
    log-end-offset delta, and consumer lag degrades only above lag_degrade (>0)."""
    m = {k: cur.get(k) for k in
         ("brokers", "topics", "partitions", "under_replicated", "offline")}
    m["total_lag"] = cur.get("total_lag")
    m["consumer_groups"] = cur.get("consumer_groups")
    m["msgs_per_sec"] = None
    if prev and dt > 0:
        de = (cur.get("total_end_offset") or 0) - (prev.get("total_end_offset") or 0)
        m["msgs_per_sec"] = round(de / dt, 2) if de >= 0 else None  # counter reset → skip

    reasons = []
    if (cur.get("offline") or 0) > 0:
        reasons.append(f"{cur['offline']} offline partition(s)")
    if (cur.get("under_replicated") or 0) > 0:
        reasons.append(f"{cur['under_replicated']} under-replicated partition(s)")
    if not cur.get("has_controller"):
        reasons.append("no active controller")
    if cur.get("brokers") is not None and cur["brokers"] < thr["min_brokers"]:
        reasons.append(f"only {cur['brokers']} broker(s) (<{thr['min_brokers']})")
    lag_deg = thr.get("lag_degrade", 0)
    if lag_deg and (cur.get("total_lag") or 0) > lag_deg:
        reasons.append(f"consumer lag {cur['total_lag']:,} (>{lag_deg:,})")
    return bool(reasons), reasons, m


def post_perf(mm, name, degraded, reasons, fields=None):
    """Mattermost transition alert for a service's degraded<->healthy flip. The
    caller passes service-specific metric/sparkline `fields` (see _es_fields /
    _kafka_fields / _internet_fields)."""
    if not mm.get("enable"):
        return
    if degraded:
        text = f"👁 **Argus** · :warning: **{name} degraded** — " + "; ".join(reasons)
    else:
        text = (f"👁 **Argus** · :large_green_circle: **{name} recovered** — "
                "back to normal")
    _post(mm, text, C_DOWN if degraded else C_UP, fields)


def _es_fields(metrics, store):
    """Metric + heap-trend fields for an Elasticsearch degraded/recovered alert."""
    fields = []
    if metrics:
        if metrics.get("status"):
            fields.append(_field("Cluster", str(metrics["status"]).upper()))
        for title, key in (("Heap", "heap_pct"), ("CPU", "cpu_pct")):
            v = metrics.get(key)
            if v is not None:
                fields.append(_field(title, f"{v:.0f}%"))
        if metrics.get("unassigned") is not None:
            fields.append(_field("Unassigned shards", str(metrics["unassigned"])))
        if metrics.get("search_qps") is not None:
            fields.append(_field("Search q/s", f"{metrics['search_qps']:.1f}"))
    if store:
        sf = _spark_field("Heap %",
                          [h["heap_pct"] for h in store.elastic_history(hours=3)], unit="%")
        if sf:
            fields.append(sf)
    return fields


def _kafka_fields(metrics, store):
    """Metric + lag/throughput-trend fields for a Kafka degraded/recovered alert."""
    fields = []
    if metrics:
        for title, key in (("Brokers", "brokers"), ("Partitions", "partitions"),
                           ("Under-replicated", "under_replicated"),
                           ("Offline", "offline"), ("Consumer groups", "consumer_groups")):
            v = metrics.get(key)
            if v is not None:
                fields.append(_field(title, str(v)))
        if metrics.get("total_lag") is not None:
            fields.append(_field("Consumer lag", f"{metrics['total_lag']:,}"))
        if metrics.get("msgs_per_sec") is not None:
            fields.append(_field("Throughput", f"{metrics['msgs_per_sec']:.1f} msg/s"))
    if store:
        hist = store.kafka_history(hours=3)
        sf = _spark_field("Consumer lag", [h["total_lag"] for h in hist])
        if sf is None:  # lag flat/empty → show throughput instead
            sf = _spark_field("Throughput", [h["msgs_per_sec"] for h in hist],
                              unit=" msg/s", fmt="{:.1f}")
        if sf:
            fields.append(sf)
    return fields


def _internet_fields(slow_pairs, store):
    """Per-URL latency fields + a latency sparkline for the worst URL."""
    fields = [_field(nm, f"{lat}ms") for nm, lat in slow_pairs]
    if store and slow_pairs:
        worst = max(slow_pairs, key=lambda p: p[1])[0]
        sf = _spark_field(f"{worst} latency",
                          [h["latency_ms"] for h in store.history(worst, hours=2, limit=240)],
                          unit="ms")
        if sf:
            fields.append(sf)
    return fields


def monitor_loop(cfg, state, store=None, host_latest=None,
                 elastic_latest=None, kafka_latest=None):
    targets = {t["name"]: t for t in cfg["targets"]}
    interval = cfg["interval"]
    hb_interval = cfg["heartbeat_interval"]
    retention_days = cfg["db"]["retention_days"]
    mm = cfg["mattermost"]
    mp = cfg["mongo_perf"]
    hm = cfg["host_metrics"]
    es = cfg["elastic"]
    kf = cfg["kafka"]
    it = cfg["internet"]
    internet_group = it.get("group", "internet")
    internet_degrade_ms = int(it.get("degrade_ms", 800))
    internet_names = {t["name"] for t in cfg["targets"]
                      if t.get("group") == internet_group and t["type"] == "http"}
    internet_degraded = False  # last posted panel state (alert on transitions only)
    mongo_t = next((t for t in cfg["targets"]
                    if t["type"] == "tcp" and "mongo" in t["name"].lower()), None)
    mongo_prev = None      # previous perf_sample for delta-based latency
    mongo_degraded = False  # last posted state (alert only on transitions)
    es_prev = None         # previous ES perf_sample for delta-based QPS
    es_degraded = False
    kafka_prev = None      # previous Kafka perf_sample for delta-based throughput
    kafka_degraded = False
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
                alert(mm, name, new, detail, store)
                if store:
                    store.insert_event(name, "transition", f"{new}: {detail}")
        if store and cycle_rows:
            store.insert_samples(cycle_rows)

        # Internet QoS: a reachable-but-slow URL degrades the panel (amber on the
        # dashboard + drill-down). One panel-level Mattermost transition, not per
        # URL, so a flapping endpoint can't spam the channel.
        if it["enable"] and internet_names:
            slow = []
            slow_pairs = []
            for nm, new, latency in cycle_rows:
                if nm not in internet_names:
                    continue
                deg = new == "up" and latency is not None and latency > internet_degrade_ms
                state.set_degraded(
                    nm, deg,
                    f"latency {latency}ms (>{internet_degrade_ms}ms)" if deg else "")
                if deg:
                    slow.append(f"{nm} {latency}ms")
                    slow_pairs.append((nm, latency))
            if bool(slow) != internet_degraded:
                internet_degraded = bool(slow)
                post_perf(mm, "Internet QoS", internet_degraded, slow,
                          _internet_fields(slow_pairs, store))
                if store:
                    store.insert_event(
                        "internet", "perf",
                        ("degraded: " + "; ".join(slow)) if internet_degraded
                        else "recovered")

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
                    post_mongo_perf(mm, degraded, reasons, metrics, store)
                    if store:
                        store.insert_event(
                            "mongodb", "perf",
                            ("degraded: " + "; ".join(reasons)) if degraded
                            else "recovered")
            if cur:
                mongo_prev = cur

        if es["enable"] and es["url"]:
            cur = elastic.perf_sample(es["url"], es.get("username", ""),
                                      es.get("password", ""), timeout=8)
            if cur:
                degraded, reasons, metrics = eval_elastic_perf(
                    es_prev, cur, es, dt=interval)
                state.set_degraded("elasticsearch", degraded, "; ".join(reasons))
                if elastic_latest is not None:
                    elastic_latest.clear()
                    elastic_latest.update(metrics)
                print(f"[argus] elastic {cur.get('status')} heap={cur.get('heap_pct')}% "
                      f"cpu={cur.get('cpu_pct')}% unassigned={cur.get('unassigned')}"
                      f"{' DEGRADED' if degraded else ''}", flush=True)
                if store:
                    store.insert_elastic_perf(metrics)
                if degraded != es_degraded:
                    es_degraded = degraded
                    post_perf(mm, "Elasticsearch", degraded, reasons,
                              _es_fields(metrics, store))
                    if store:
                        store.insert_event(
                            "elasticsearch", "perf",
                            ("degraded: " + "; ".join(reasons)) if degraded
                            else "recovered")
                es_prev = cur

        if kf["enable"] and kf["host"]:
            cur = kafka.perf_sample(
                kf["host"], int(kf["port"]), kf.get("client_id", "argus"),
                timeout=5, track_traffic=kf.get("track_traffic", True),
                track_lag=kf.get("track_lag", True))
            if cur:
                degraded, reasons, metrics = eval_kafka_perf(
                    kafka_prev, cur, kf, dt=interval)
                state.set_degraded("kafka", degraded, "; ".join(reasons))
                if kafka_latest is not None:
                    kafka_latest.clear()
                    kafka_latest.update(metrics)
                print(f"[argus] kafka brokers={cur.get('brokers')} "
                      f"partitions={cur.get('partitions')} "
                      f"under_repl={cur.get('under_replicated')} "
                      f"offline={cur.get('offline')} "
                      f"lag={cur.get('total_lag')} "
                      f"msg/s={metrics.get('msgs_per_sec')}"
                      f"{' DEGRADED' if degraded else ''}", flush=True)
                if store:
                    store.insert_kafka_perf(metrics)
                if degraded != kafka_degraded:
                    kafka_degraded = degraded
                    post_perf(mm, "Kafka", degraded, reasons,
                              _kafka_fields(metrics, store))
                    if store:
                        store.insert_event(
                            "kafka", "perf",
                            ("degraded: " + "; ".join(reasons)) if degraded
                            else "recovered")
                kafka_prev = cur

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


def make_handler(state, cfg, store=None, host_latest=None, ollama_latest=None,
                 elastic_latest=None, kafka_latest=None):
    # The mongo detail endpoint targets the first tcp probe named like "mongo".
    mongo_target = next(
        (t for t in cfg["targets"]
         if t["type"] == "tcp" and "mongo" in t["name"].lower()), None)
    hm_host = cfg["host_metrics"].get("ssh_host", "")
    ol_cfg = cfg["ollama"]
    es_cfg = cfg["elastic"]
    kf_cfg = cfg["kafka"]
    int_cfg = cfg["internet"]

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
            elif path == "/api/elastic":
                if not es_cfg.get("enable") or not es_cfg.get("url"):
                    self._send(404, b'{"ok":false,"error":"elasticsearch not configured"}')
                else:
                    data = elastic.health(es_cfg["url"], es_cfg.get("username", ""),
                                          es_cfg.get("password", ""), timeout=8)
                    if elastic_latest:
                        data["rates"] = dict(elastic_latest)
                    self._send(200, json.dumps(data).encode())
            elif path == "/api/elastic/history":
                if store is None:
                    self._send(200, b"[]")
                else:
                    q = self._query()
                    hours = float(q.get("hours", ["6"])[0])
                    self._send(200, json.dumps(
                        store.elastic_history(hours=hours)).encode())
            elif path == "/api/kafka":
                if not kf_cfg.get("enable") or not kf_cfg.get("host"):
                    self._send(404, b'{"ok":false,"error":"kafka not configured"}')
                else:
                    data = kafka.health(
                        kf_cfg["host"], int(kf_cfg["port"]),
                        kf_cfg.get("client_id", "argus"), timeout=5,
                        track_traffic=kf_cfg.get("track_traffic", True),
                        track_lag=kf_cfg.get("track_lag", True))
                    if kafka_latest:
                        data["rates"] = dict(kafka_latest)  # loop-computed msgs/sec
                    self._send(200, json.dumps(data).encode())
            elif path == "/api/kafka/history":
                if store is None:
                    self._send(200, b"[]")
                else:
                    q = self._query()
                    hours = float(q.get("hours", ["6"])[0])
                    self._send(200, json.dumps(
                        store.kafka_history(hours=hours)).encode())
            elif path == "/api/internet":
                if not int_cfg.get("enable"):
                    self._send(404, b'{"ok":false,"error":"internet qos not configured"}')
                else:
                    q = self._query()
                    hours = float(q.get("hours", ["1"])[0])
                    self._send(200, json.dumps(
                        internet_snapshot(state, store, int_cfg, hours=hours)).encode())
            elif path == "/api/internet/history":
                # Merged per-URL latency/status trend for the QoS chart — one call
                # returns every tracked URL's series (reusing the samples table).
                if store is None:
                    self._send(200, b'{"names":[],"series":{}}')
                else:
                    q = self._query()
                    hours = float(q.get("hours", ["6"])[0])
                    group = int_cfg.get("group", "internet")
                    names = [t["name"] for t in cfg["targets"]
                             if t.get("group") == group and t["type"] == "http"]
                    series = {n: store.history(n, hours=hours) for n in names}
                    self._send(200, json.dumps(
                        {"names": names, "series": series}).encode())
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

    host_latest = {}     # shared: loop writes the newest host CPU/mem, handler reads
    elastic_latest = {}  # shared: loop writes derived ES rates, handler attaches them
    kafka_latest = {}    # shared: loop writes the newest Kafka summary counts
    t = threading.Thread(
        target=monitor_loop,
        args=(cfg, state, store, host_latest, elastic_latest, kafka_latest),
        daemon=True)
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
          f"ollama={cfg['ollama']['enable']}, elastic={cfg['elastic']['enable']}, "
          f"kafka={cfg['kafka']['enable']}, internet={cfg['internet']['enable']}",
          flush=True)
    ThreadingHTTPServer(("0.0.0.0", port),
                        make_handler(state, cfg, store, host_latest,
                                     ollama_latest, elastic_latest,
                                     kafka_latest)).serve_forever()


if __name__ == "__main__":
    main()
