"""Tiny pure-stdlib Elasticsearch client for the Argus detail page.

Read-only diagnostics against the ES REST API (HTTP/JSON — far simpler than
Mongo's hand-rolled wire protocol). Hits the standard cluster introspection
endpoints and curates the result down to the fields the detail page renders:

  GET /                  -> version, cluster name, lucene version
  GET /_cluster/health   -> green/yellow/red, shard allocation
  GET /_cluster/stats    -> docs, store size, aggregate jvm/os/fs
  GET /_nodes/stats      -> per-node heap/cpu/gc, thread pools, breakers
  GET /_cat/indices      -> per-index health/docs/size

Optional HTTP basic auth (username + password). The live target runs with
security disabled (anonymous http), so auth defaults off. No third-party deps;
mirrors the style of mongo.py / ollama.py.
"""
import base64
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone


def _headers(username, password):
    h = {"User-Agent": "argus-elastic/1", "Accept": "application/json"}
    if username:
        token = base64.b64encode(f"{username}:{password}".encode()).decode()
        h["Authorization"] = f"Basic {token}"
    return h


def _get(base, path, timeout, username, password):
    req = urllib.request.Request(base + path, headers=_headers(username, password))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def _g(d, *path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


# Thread pools whose queue/rejection counts signal backpressure. Newer ES folds
# bulk into "write"; older versions used "bulk" — track both, report whatever
# the node exposes.
_POOLS = ("search", "write", "bulk", "get")


def _node_row(name, n):
    """Curate one /_nodes/stats node entry → the fields the page renders."""
    jvm, os_, fs, proc = (n.get("jvm", {}), n.get("os", {}),
                          n.get("fs", {}), n.get("process", {}))
    pools = {}
    for p in _POOLS:
        tp = _g(n, "thread_pool", p)
        if isinstance(tp, dict):
            pools[p] = {"active": tp.get("active"), "queue": tp.get("queue"),
                        "rejected": tp.get("rejected")}
    breakers = {}
    for b, bd in (n.get("breakers", {}) or {}).items():
        breakers[b] = {"tripped": bd.get("tripped"),
                       "limit": bd.get("limit_size_in_bytes"),
                       "estimated": bd.get("estimated_size_in_bytes")}
    return {
        "name": name,
        "roles": n.get("roles", []) or [],
        "heap_pct": _g(jvm, "mem", "heap_used_percent"),
        "heap_used_bytes": _g(jvm, "mem", "heap_used_in_bytes"),
        "heap_max_bytes": _g(jvm, "mem", "heap_max_in_bytes"),
        "gc_young_count": _g(jvm, "gc", "collectors", "young", "collection_count"),
        "gc_young_ms": _g(jvm, "gc", "collectors", "young", "collection_time_in_millis"),
        "gc_old_count": _g(jvm, "gc", "collectors", "old", "collection_count"),
        "gc_old_ms": _g(jvm, "gc", "collectors", "old", "collection_time_in_millis"),
        "cpu_pct": _g(os_, "cpu", "percent"),
        "load_1m": _g(os_, "cpu", "load_average", "1m"),
        "uptime_ms": _g(jvm, "uptime_in_millis"),
        "fs_total_bytes": _g(fs, "total", "total_in_bytes"),
        "fs_available_bytes": _g(fs, "total", "available_in_bytes"),
        "open_fds": proc.get("open_file_descriptors"),
        "max_fds": proc.get("max_file_descriptors"),
        "docs_count": _g(n, "indices", "docs", "count"),
        "indexing_total": _g(n, "indices", "indexing", "index_total"),
        "search_total": _g(n, "indices", "search", "query_total"),
        "thread_pools": pools,
        "breakers": breakers,
    }


def health(base, username="", password="", timeout=8):
    """Connect to the ES REST API, run the introspection endpoints, and return a
    curated JSON-able dict. Shape mirrors mongo.health (ok/host/fetched + error on
    failure) so the detail page stays consistent."""
    base = base.rstrip("/")
    started = datetime.now(timezone.utc).isoformat()
    out = {"ok": False, "host": base.split("://")[-1], "fetched": started}
    try:
        root = _get(base, "/", timeout, username, password)
        ch = _get(base, "/_cluster/health", timeout, username, password)
        cs = _get(base, "/_cluster/stats", timeout, username, password)
        ns = _get(base, "/_nodes/stats/jvm,os,fs,process,thread_pool,indices,breaker",
                  timeout, username, password)
        try:
            cats = _get(base, "/_cat/indices?format=json&bytes=b&h=health,status,"
                        "index,pri,rep,docs.count,docs.deleted,store.size,"
                        "pri.store.size&s=store.size:desc",
                        timeout, username, password)
        except Exception:  # noqa: BLE001 - index list is best-effort
            cats = []

        nodes = [_node_row(nd.get("name", nid), nd)
                 for nid, nd in (ns.get("nodes", {}) or {}).items()]
        # tally headline thread-pool / breaker pressure across the cluster
        sq = sr = wq = wr = breakers_tripped = 0
        for nrow in nodes:
            for p, pd in nrow["thread_pools"].items():
                if p == "search":
                    sq += pd.get("queue") or 0
                    sr += pd.get("rejected") or 0
                elif p in ("write", "bulk"):
                    wq += pd.get("queue") or 0
                    wr += pd.get("rejected") or 0
            for bd in nrow["breakers"].values():
                breakers_tripped += bd.get("tripped") or 0

        indices = [{
            "health": i.get("health"), "status": i.get("status"),
            "index": i.get("index"), "pri": _to_int(i.get("pri")),
            "rep": _to_int(i.get("rep")), "docs": _to_int(i.get("docs.count")),
            "deleted": _to_int(i.get("docs.deleted")),
            "store_bytes": _to_int(i.get("store.size")),
            "pri_store_bytes": _to_int(i.get("pri.store.size")),
        } for i in (cats or []) if isinstance(i, dict)]

        out.update({
            "ok": True,
            "host": root.get("name") or ch.get("cluster_name") or out["host"],
            "cluster": {
                "name": ch.get("cluster_name") or root.get("cluster_name"),
                "status": ch.get("status"),
                "version": _g(root, "version", "number"),
                "lucene_version": _g(root, "version", "lucene_version"),
                "nodes": ch.get("number_of_nodes"),
                "data_nodes": ch.get("number_of_data_nodes"),
                "active_primary_shards": ch.get("active_primary_shards"),
                "active_shards": ch.get("active_shards"),
                "relocating_shards": ch.get("relocating_shards"),
                "initializing_shards": ch.get("initializing_shards"),
                "unassigned_shards": ch.get("unassigned_shards"),
                "delayed_unassigned_shards": ch.get("delayed_unassigned_shards"),
                "pending_tasks": ch.get("number_of_pending_tasks"),
                "active_shards_percent": ch.get("active_shards_percent_as_number"),
                "max_task_wait_ms": ch.get("task_max_waiting_in_queue_millis"),
            },
            "stats": {
                "indices_count": _g(cs, "indices", "count"),
                "docs_count": _g(cs, "indices", "docs", "count"),
                "docs_deleted": _g(cs, "indices", "docs", "deleted"),
                "store_bytes": _g(cs, "indices", "store", "size_in_bytes"),
                "shards_total": _g(cs, "indices", "shards", "total"),
                "shards_primaries": _g(cs, "indices", "shards", "primaries"),
                "shards_replication": _g(cs, "indices", "shards", "replication"),
                "segments_count": _g(cs, "indices", "segments", "count"),
                "segments_bytes": _g(cs, "indices", "segments", "memory_in_bytes"),
                "fielddata_bytes": _g(cs, "indices", "fielddata", "memory_size_in_bytes"),
                "query_cache_bytes": _g(cs, "indices", "query_cache", "memory_size_in_bytes"),
                "jvm_heap_used_bytes": _g(cs, "nodes", "jvm", "mem", "heap_used_in_bytes"),
                "jvm_heap_max_bytes": _g(cs, "nodes", "jvm", "mem", "heap_max_in_bytes"),
                "os_mem_total_bytes": _g(cs, "nodes", "os", "mem", "total_in_bytes"),
                "os_mem_used_pct": _g(cs, "nodes", "os", "mem", "used_percent"),
                "fs_total_bytes": _g(cs, "nodes", "fs", "total_in_bytes"),
                "fs_available_bytes": _g(cs, "nodes", "fs", "available_in_bytes"),
                "process_cpu_pct": _g(cs, "nodes", "process", "cpu", "percent"),
                "node_versions": _g(cs, "nodes", "versions", default=[]),
                "node_count_total": _g(cs, "nodes", "count", "total"),
            },
            "nodes": nodes,
            "indices": indices,
            "totals": {
                "search_queue": sq, "search_rejected": sr,
                "write_queue": wq, "write_rejected": wr,
                "breakers_tripped": breakers_tripped,
            },
        })
    except urllib.error.HTTPError as exc:
        out["error"] = f"HTTP {exc.code} {exc.reason}"
    except urllib.error.URLError as exc:
        out["error"] = f"{type(exc).__name__}: {getattr(exc, 'reason', exc)}"
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


def _to_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def perf_sample(base, username="", password="", timeout=8):
    """Cheap two-call sample (cluster health + node stats) of the latency /
    saturation primitives, for the background alerting + history loop. Returns
    raw cumulative counters (search/index totals) so the caller can derive QPS
    from deltas, plus instantaneous heap/cpu/shard state. None on failure."""
    base = base.rstrip("/")
    try:
        ch = _get(base, "/_cluster/health", timeout, username, password)
        ns = _get(base, "/_nodes/stats/jvm,os,indices,thread_pool",
                  timeout, username, password)
    except Exception:  # noqa: BLE001
        return None
    heap = cpu = 0.0
    search_total = index_total = rejected = 0
    for nd in (ns.get("nodes", {}) or {}).values():
        heap = max(heap, _g(nd, "jvm", "mem", "heap_used_percent") or 0)
        cpu = max(cpu, _g(nd, "os", "cpu", "percent") or 0)
        search_total += _g(nd, "indices", "search", "query_total") or 0
        index_total += _g(nd, "indices", "indexing", "index_total") or 0
        for p in _POOLS:
            rejected += (_g(nd, "thread_pool", p, "rejected") or 0)
    return {
        "status": ch.get("status"),
        "nodes": ch.get("number_of_nodes"),
        "unassigned": ch.get("unassigned_shards"),
        "heap_pct": round(heap, 1),
        "cpu_pct": round(cpu, 1),
        "search_total": search_total,
        "index_total": index_total,
        "rejected": rejected,
    }


def freshness(base, index, ts_field="@timestamp", username="", password="",
              timeout=8):
    """Age (seconds) of the newest document in `index` by `ts_field` — the
    end-to-end 'is fresh data still arriving?' canary. One cheap `max` aggregation
    (size 0). Returns {ok, age_seconds, newest_iso, newest_ms}; age_seconds is
    None when the index is empty. A STALE data stream while the cluster itself is
    green means an UPSTREAM stall (Kafka / poller / distributor), not an ES fault
    — which is exactly the 2026-06-24 wedge the per-service health checks missed."""
    base = base.rstrip("/")
    body = json.dumps({
        "size": 0,
        "track_total_hits": False,
        "aggs": {"newest": {"max": {"field": ts_field}}},
    }).encode()
    try:
        req = urllib.request.Request(
            f"{base}/{index}/_search", data=body, method="POST",
            headers={**_headers(username, password),
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            doc = json.loads(resp.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as exc:
        return {"ok": False, "error": f"HTTP {exc.code} {exc.reason}"}
    except urllib.error.URLError as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: "
                f"{getattr(exc, 'reason', exc)}"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    newest_ms = _g(doc, "aggregations", "newest", "value")
    if newest_ms is None:
        return {"ok": True, "age_seconds": None, "newest_iso": None,
                "newest_ms": None}
    newest = datetime.fromtimestamp(newest_ms / 1000.0, tz=timezone.utc)
    age = (datetime.now(timezone.utc) - newest).total_seconds()
    return {"ok": True, "age_seconds": round(age),
            "newest_iso": newest.isoformat(), "newest_ms": newest_ms}
