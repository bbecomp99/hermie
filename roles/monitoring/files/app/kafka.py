"""Tiny pure-stdlib Apache Kafka client for the Argus detail page.

Speaks just enough of the Kafka wire protocol (binary, over TCP 9092) to pull
cluster metadata — no kafka-python, no deps; same spirit as mongo.py's hand-rolled
OP_MSG. Two requests, both on PRE-FLEXIBLE (non-tagged-field) API versions so the
encoding stays a plain length-prefixed format and the parser is trivial:

  ApiVersions v0 (key 18)  -> broker reachable + supported API-version range
  Metadata     v1 (key 3)  -> brokers, controller, topics, partitions, ISR

From Metadata we derive the classic Kafka health signals: broker count, the
active controller, under-replicated partitions (ISR < assigned replicas) and
offline partitions (no elected leader). Read-only: no produce / fetch / consumer
-group calls. Assumes a PLAINTEXT listener (no TLS/SASL) — matches the LAN target.
"""
import socket
import struct
from datetime import datetime, timezone

API_VERSIONS = 18
METADATA = 3


# ---------------------------------------------------------------------------
# Transport — request header v1, response header v0 (both APIs are non-flexible)
# ---------------------------------------------------------------------------


def _string(s):
    b = s.encode("utf-8")
    return struct.pack(">h", len(b)) + b


def _recv_n(sock, n):
    chunks, got = [], 0
    while got < n:
        chunk = sock.recv(n - got)
        if not chunk:
            raise ConnectionError("socket closed mid-message")
        chunks.append(chunk)
        got += len(chunk)
    return b"".join(chunks)


def _request(sock, api_key, api_version, body, client_id, corr):
    header = (struct.pack(">hhi", api_key, api_version, corr) + _string(client_id))
    msg = header + body
    sock.sendall(struct.pack(">i", len(msg)) + msg)
    size = struct.unpack(">i", _recv_n(sock, 4))[0]
    payload = _recv_n(sock, size)
    return payload  # starts with the int32 correlation_id (response header v0)


class _R:
    """Big-endian reader over a Kafka response payload."""

    def __init__(self, buf, i=0):
        self.b, self.i = buf, i

    def int8(self):
        v = self.b[self.i]
        self.i += 1
        return v

    def int16(self):
        v = struct.unpack_from(">h", self.b, self.i)[0]
        self.i += 2
        return v

    def int32(self):
        v = struct.unpack_from(">i", self.b, self.i)[0]
        self.i += 4
        return v

    def string(self):
        n = self.int16()
        if n < 0:
            return None
        s = self.b[self.i:self.i + n].decode("utf-8", "replace")
        self.i += n
        return s

    def int32_array(self):
        n = self.int32()
        if n < 0:
            return []
        return [self.int32() for _ in range(n)]


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------


def _api_versions(sock, client_id):
    """ApiVersions v0 — empty body. Response v0: error_code + array of
    (api_key, min_version, max_version). No throttle field at v0."""
    payload = _request(sock, API_VERSIONS, 0, b"", client_id, corr=1)
    r = _R(payload, 4)  # skip correlation_id
    error = r.int16()
    n = r.int32()
    max_meta = None
    for _ in range(n):
        key, _mn, mx = r.int16(), r.int16(), r.int16()
        if key == METADATA:
            max_meta = mx
    return {"error_code": error, "api_count": n, "max_metadata_version": max_meta}


def _metadata(sock, client_id):
    """Metadata v1 — body is a single topics array; null (-1) means ALL topics.
    Response v1: brokers[node_id,host,port,rack], controller_id,
    topics[error_code,name,is_internal,partitions[error_code,id,leader,replicas,isr]]."""
    body = struct.pack(">i", -1)  # topics = null → all topics
    payload = _request(sock, METADATA, 1, body, client_id, corr=2)
    r = _R(payload, 4)  # skip correlation_id

    brokers = []
    for _ in range(r.int32()):
        node_id = r.int32()
        host = r.string()
        port = r.int32()
        rack = r.string()
        brokers.append({"node_id": node_id, "host": host, "port": port, "rack": rack})

    controller_id = r.int32()

    topics = []
    for _ in range(r.int32()):
        err = r.int16()
        name = r.string()
        is_internal = r.int8() != 0
        partitions = []
        for _ in range(r.int32()):
            p_err = r.int16()
            p_id = r.int32()
            leader = r.int32()
            replicas = r.int32_array()
            isr = r.int32_array()
            partitions.append({"error_code": p_err, "id": p_id, "leader": leader,
                               "replicas": replicas, "isr": isr})
        topics.append({"error_code": err, "name": name,
                       "is_internal": is_internal, "partitions": partitions})

    return {"brokers": brokers, "controller_id": controller_id, "topics": topics}


# ---------------------------------------------------------------------------
# Curated health snapshot
# ---------------------------------------------------------------------------


def health(host, port, client_id="argus", timeout=5):
    """Connect, pull ApiVersions + Metadata, and derive a curated health dict.
    Shape mirrors mongo.health (ok/host/fetched + error on failure)."""
    started = datetime.now(timezone.utc).isoformat()
    out = {"ok": False, "host": f"{host}:{port}", "fetched": started}
    try:
        with socket.create_connection((host, port), timeout=timeout) as sock:
            sock.settimeout(timeout)
            api = _api_versions(sock, client_id)
            meta = _metadata(sock, client_id)
    except Exception as exc:  # noqa: BLE001 - any failure → clean error payload
        out["error"] = f"{type(exc).__name__}: {exc}"
        return out

    brokers = meta["brokers"]
    controller_id = meta["controller_id"]
    topics = meta["topics"]
    leader_count = {b["node_id"]: 0 for b in brokers}

    partition_count = under_replicated = offline = user_topics = internal_topics = 0
    topic_rows = []
    for t in topics:
        parts = t["partitions"]
        ur = sum(1 for p in parts if len(p["isr"]) < len(p["replicas"]))
        off = sum(1 for p in parts if p["leader"] is None or p["leader"] < 0)
        partition_count += len(parts)
        under_replicated += ur
        offline += off
        if t["is_internal"]:
            internal_topics += 1
        else:
            user_topics += 1
        for p in parts:
            if p["leader"] in leader_count:
                leader_count[p["leader"]] += 1
        rf = max((len(p["replicas"]) for p in parts), default=0)
        topic_rows.append({
            "name": t["name"], "internal": t["is_internal"],
            "partitions": len(parts), "replication": rf,
            "under_replicated": ur, "offline": off,
            "error_code": t["error_code"] or None,
        })
    topic_rows.sort(key=lambda x: (-(x["under_replicated"] + x["offline"]),
                                   x["internal"], x["name"] or ""))

    broker_rows = [{
        "node_id": b["node_id"], "host": b["host"], "port": b["port"],
        "rack": b["rack"], "controller": b["node_id"] == controller_id,
        "leader_partitions": leader_count.get(b["node_id"], 0),
    } for b in brokers]
    broker_rows.sort(key=lambda x: x["node_id"])

    out.update({
        "ok": True,
        "api": api,
        "controller_id": controller_id,
        "has_controller": controller_id is not None and controller_id >= 0,
        "brokers": broker_rows,
        "topics": topic_rows,
        "summary": {
            "brokers": len(brokers),
            "topics": len(topics),
            "user_topics": user_topics,
            "internal_topics": internal_topics,
            "partitions": partition_count,
            "under_replicated": under_replicated,
            "offline": offline,
        },
    })
    return out


def perf_sample(host, port, client_id="argus", timeout=5):
    """Lightweight sample for the background loop — the derived summary counts
    plus controller presence. None on failure."""
    h = health(host, port, client_id, timeout)
    if not h.get("ok"):
        return None
    m = dict(h["summary"])
    m["has_controller"] = h["has_controller"]
    m["controller_id"] = h["controller_id"]
    return m
