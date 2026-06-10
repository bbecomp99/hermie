"""Tiny pure-stdlib MongoDB client — just enough BSON + OP_MSG to run admin
diagnostic commands (serverStatus, dbStats, listDatabases, isMaster,
replSetGetStatus) against an UNAUTHENTICATED mongod. No pymongo, no pip deps.

Scope on purpose: read-only diagnostics for the Argus Mongo detail page. It does
NOT implement auth (SCRAM), cursors, or writes — the target mongod
(192.168.88.142:27017) accepts anonymous admin commands.
"""
import socket
import struct
from datetime import datetime, timezone

OP_MSG = 2013
_req_id = 0


# ---------------------------------------------------------------------------
# BSON decode (enough types to cover serverStatus / dbStats output)
# ---------------------------------------------------------------------------


def _read_cstring(buf, i):
    end = buf.index(b"\x00", i)
    return buf[i:end].decode("utf-8", "replace"), end + 1


def _decode_doc(buf, i):
    size = struct.unpack_from("<i", buf, i)[0]
    end = i + size
    i += 4
    out = {}
    while buf[i] != 0:
        etype = buf[i]
        i += 1
        key, i = _read_cstring(buf, i)
        val, i = _decode_value(buf, i, etype)
        out[key] = val
    return out, end


def _decode_value(buf, i, etype):
    if etype == 0x01:  # double
        return struct.unpack_from("<d", buf, i)[0], i + 8
    if etype == 0x02:  # string
        ln = struct.unpack_from("<i", buf, i)[0]
        i += 4
        return buf[i:i + ln - 1].decode("utf-8", "replace"), i + ln
    if etype in (0x03, 0x04):  # document / array
        sub, ni = _decode_doc(buf, i)
        return (list(sub.values()) if etype == 0x04 else sub), ni
    if etype == 0x05:  # binary
        ln = struct.unpack_from("<i", buf, i)[0]
        return {"$binary_len": ln}, i + 4 + 1 + ln
    if etype == 0x07:  # ObjectId
        return buf[i:i + 12].hex(), i + 12
    if etype == 0x08:  # bool
        return buf[i] != 0, i + 1
    if etype == 0x09:  # UTC datetime (int64 ms)
        ms = struct.unpack_from("<q", buf, i)[0]
        try:
            iso = datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat()
        except (OverflowError, OSError, ValueError):
            iso = None
        return iso, i + 8
    if etype == 0x0A:  # null
        return None, i
    if etype == 0x0B:  # regex (two cstrings)
        _, i = _read_cstring(buf, i)
        _, i = _read_cstring(buf, i)
        return None, i
    if etype in (0x0D, 0x0E):  # code / symbol (string-like)
        ln = struct.unpack_from("<i", buf, i)[0]
        i += 4
        return buf[i:i + ln - 1].decode("utf-8", "replace"), i + ln
    if etype == 0x10:  # int32
        return struct.unpack_from("<i", buf, i)[0], i + 4
    if etype == 0x11:  # timestamp
        inc, ts = struct.unpack_from("<II", buf, i)
        return {"t": ts, "i": inc}, i + 8
    if etype == 0x12:  # int64
        return struct.unpack_from("<q", buf, i)[0], i + 8
    if etype == 0x13:  # decimal128 — skip (rare in diagnostics)
        return None, i + 16
    if etype in (0x06, 0xFF, 0x7F):  # undefined / minkey / maxkey
        return None, i
    raise ValueError(f"unhandled BSON type {etype:#x}")


# ---------------------------------------------------------------------------
# BSON encode (only what a command document needs)
# ---------------------------------------------------------------------------


def _cstr(s):
    return s.encode("utf-8") + b"\x00"


def _encode_doc(d):
    body = b""
    for k, v in d.items():
        if isinstance(v, bool):
            body += b"\x08" + _cstr(k) + (b"\x01" if v else b"\x00")
        elif isinstance(v, int):
            body += b"\x10" + _cstr(k) + struct.pack("<i", v)
        elif isinstance(v, str):
            sb = v.encode("utf-8") + b"\x00"
            body += b"\x02" + _cstr(k) + struct.pack("<i", len(sb)) + sb
        elif isinstance(v, dict):
            body += b"\x03" + _cstr(k) + _encode_doc(v)
        else:
            raise ValueError(f"cannot encode {type(v)}")
    body += b"\x00"
    return struct.pack("<i", len(body) + 4) + body


# ---------------------------------------------------------------------------
# OP_MSG transport
# ---------------------------------------------------------------------------


def _recv_n(sock, n):
    chunks = []
    got = 0
    while got < n:
        chunk = sock.recv(n - got)
        if not chunk:
            raise ConnectionError("socket closed mid-message")
        chunks.append(chunk)
        got += len(chunk)
    return b"".join(chunks)


def _command(sock, db, cmd):
    global _req_id
    doc = dict(cmd)
    doc["$db"] = db
    payload = struct.pack("<I", 0) + b"\x00" + _encode_doc(doc)  # flags + section 0
    _req_id += 1
    header = struct.pack("<iiii", 16 + len(payload), _req_id, 0, OP_MSG)
    sock.sendall(header + payload)

    hdr = _recv_n(sock, 16)
    msg_len = struct.unpack_from("<i", hdr, 0)[0]
    rest = _recv_n(sock, msg_len - 16)
    kind = rest[4]  # after 4-byte flagBits
    if kind != 0:
        raise ValueError(f"unexpected OP_MSG section kind {kind}")
    doc, _ = _decode_doc(rest, 5)
    return doc


def _try(sock, db, cmd):
    try:
        return _command(sock, db, cmd)
    except Exception as exc:  # noqa: BLE001
        return {"ok": 0, "errmsg": f"{type(exc).__name__}: {exc}"}


# ---------------------------------------------------------------------------
# Curated health snapshot
# ---------------------------------------------------------------------------


def _g(d, *path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def health(host, port, timeout=5):
    """Connect, run diagnostics, return a curated JSON-able dict."""
    started_iso = datetime.now(timezone.utc).isoformat()
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            im = _command(sock, "admin", {"isMaster": 1})
            ss = _command(sock, "admin", {"serverStatus": 1})
            hi = _try(sock, "admin", {"hostInfo": 1})
            dbs = _try(sock, "admin", {"listDatabases": 1})
            repl = _try(sock, "admin", {"replSetGetStatus": 1})

            db_list = dbs.get("databases", []) if isinstance(dbs, dict) else []
            db_stats = []
            for d in db_list[:20]:
                name = d.get("name")
                if not name:
                    continue
                st = _try(sock, name, {"dbStats": 1, "scale": 1})
                if st.get("ok"):
                    db_stats.append({
                        "name": name,
                        "collections": st.get("collections"),
                        "objects": st.get("objects"),
                        "dataSize": st.get("dataSize"),
                        "storageSize": st.get("storageSize"),
                        "indexes": st.get("indexes"),
                        "indexSize": st.get("indexSize"),
                        "sizeOnDisk": d.get("sizeOnDisk"),
                    })
                else:
                    db_stats.append({"name": name, "sizeOnDisk": d.get("sizeOnDisk")})

        wt_cache = _g(ss, "wiredTiger", "cache", default={})
        # Mongo <7 reported tickets here; 7+/8 moved them to queues.execution.
        wt_tickets = _g(ss, "wiredTiger", "concurrentTransactions", default={})
        return {
            "ok": True,
            "host": ss.get("host") or f"{host}:{port}",
            "fetched": started_iso,
            "server": {
                "version": ss.get("version"),
                "process": ss.get("process"),
                "pid": ss.get("pid"),
                "uptime": ss.get("uptime"),
                "uptimeMillis": ss.get("uptimeMillis"),
                "localTime": ss.get("localTime"),
                "storageEngine": _g(ss, "storageEngine", "name"),
                "maxWireVersion": im.get("maxWireVersion"),
                "readOnly": im.get("readOnly"),
            },
            "topology": {
                "isWritablePrimary": im.get("isWritablePrimary", im.get("ismaster")),
                "secondary": im.get("secondary"),
                "setName": im.get("setName"),
                "hosts": im.get("hosts"),
                "primary": im.get("primary"),
                "me": im.get("me"),
                "isReplicaSet": bool(im.get("setName")),
            },
            "connections": ss.get("connections", {}),
            "network": ss.get("network", {}),
            "opcounters": ss.get("opcounters", {}),
            "opcountersRepl": ss.get("opcountersRepl", {}),
            "mem": ss.get("mem", {}),
            "extra_info": ss.get("extra_info", {}),
            "globalLock": {
                "currentQueue": _g(ss, "globalLock", "currentQueue", default={}),
                "activeClients": _g(ss, "globalLock", "activeClients", default={}),
            },
            "wiredTiger": {
                "cacheBytes": wt_cache.get("bytes currently in the cache"),
                "cacheMaxBytes": wt_cache.get("maximum bytes configured"),
                "cacheDirtyBytes": wt_cache.get("tracked dirty bytes in the cache"),
                "bytesReadInto": wt_cache.get("bytes read into cache"),
                "bytesWrittenFrom": wt_cache.get("bytes written from cache"),
                "pagesReadIntoCache": wt_cache.get("pages read into cache"),
                "pagesWrittenFromCache": wt_cache.get("pages written from cache"),
                # The "disk latency hitting queries" signal: time app threads
                # spend blocked reading pages from disk into cache (cache misses).
                "appDiskReadCount": wt_cache.get(
                    "application threads page read from disk to cache count"),
                "appDiskReadTimeUs": wt_cache.get(
                    "application threads page read from disk to cache time (usecs)"),
                "appEvictTimeUs": wt_cache.get(
                    "application thread time evicting (usecs)"),
                "evictionTriggerReached": wt_cache.get(
                    "number of times eviction trigger was reached"),
                "dirtyTriggerReached": wt_cache.get(
                    "number of times dirty trigger was reached"),
            },
            # Storage-engine concurrency tickets = the modern "disk queue depth":
            # when available hits 0 and queueLength > 0, ops are stalled waiting.
            "queues": {
                "read": _shape_queue(ss, "read"),
                "write": _shape_queue(ss, "write"),
            },
            # Cumulative op latency (microseconds) + op counts → the page derives
            # live avg latency per op from deltas between polls.
            "opLatencies": {
                "reads": _shape_lat(ss, "reads"),
                "writes": _shape_lat(ss, "writes"),
                "commands": _shape_lat(ss, "commands"),
            },
            "host": {
                "numCores": _g(hi, "system", "numCores"),
                "numPhysicalCores": _g(hi, "system", "numPhysicalCores"),
                "memSizeMB": _g(hi, "system", "memSizeMB"),
                "cpuArch": _g(hi, "system", "cpuArch"),
            },
            "asserts": ss.get("asserts", {}),
            "metricsDocument": _g(ss, "metrics", "document", default={}),
            "queryExecutor": _g(ss, "metrics", "queryExecutor", default={}),
            "transactions": ss.get("transactions", {}),
            "flowControl": ss.get("flowControl", {}),
            "databases": db_stats,
            "totalDbSize": dbs.get("totalSize") if isinstance(dbs, dict) else None,
            "replSet": _shape_repl(repl),
        }
    except Exception as exc:  # noqa: BLE001 - any failure → clean error payload
        return {
            "ok": False,
            "host": f"{host}:{port}",
            "fetched": started_iso,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _shape_queue(ss, side):
    q = _g(ss, "queues", "execution", side, default={})
    npl = q.get("normalPriority", {}) if isinstance(q, dict) else {}
    return {
        "out": q.get("out"),
        "available": q.get("available"),
        "totalTickets": q.get("totalTickets"),
        "queueLength": npl.get("queueLength"),
        "totalTimeQueuedMicros": npl.get("totalTimeQueuedMicros"),
        "maxDelinquencyMillis": npl.get("maxAcquisitionDelinquencyMillis"),
    }


def _shape_lat(ss, key):
    d = _g(ss, "opLatencies", key, default={})
    return {"latency": d.get("latency"), "ops": d.get("ops")}


def perf_sample(host, port, timeout=5):
    """Cheap single-command (serverStatus) sample of the latency/saturation
    primitives, for the background alerting loop. Returns raw cumulative
    counters; the caller computes deltas between samples. None on failure."""
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            ss = _command(sock, "admin", {"serverStatus": 1})
    except Exception:  # noqa: BLE001
        return None
    wt = _g(ss, "wiredTiger", "cache", default={})
    fc = ss.get("flowControl", {})
    return {
        "t": ss.get("uptimeMillis"),  # monotonic-ish server clock (ms)
        "writes": _shape_lat(ss, "writes"),
        "reads": _shape_lat(ss, "reads"),
        "commands": _shape_lat(ss, "commands"),
        "qWrite": _shape_queue(ss, "write"),
        "qRead": _shape_queue(ss, "read"),
        "appDiskReadCount": wt.get(
            "application threads page read from disk to cache count"),
        "appDiskReadTimeUs": wt.get(
            "application threads page read from disk to cache time (usecs)"),
        "flowAcquireMicros": fc.get("timeAcquiringMicros"),
        "isLagged": fc.get("isLagged"),
        "connsCurrent": _g(ss, "connections", "current"),
        "dirtyBytes": wt.get("tracked dirty bytes in the cache"),
        "cacheMaxBytes": wt.get("maximum bytes configured"),
    }


def _shape_repl(repl):
    if not isinstance(repl, dict) or not repl.get("ok"):
        return None  # standalone / not a replica set
    members = []
    for m in repl.get("members", []):
        members.append({
            "name": m.get("name"),
            "stateStr": m.get("stateStr"),
            "health": m.get("health"),
            "uptime": m.get("uptime"),
            "self": m.get("self"),
        })
    return {"set": repl.get("set"), "members": members}
