"""Embedded SQLite persistence for Argus — no DB service, stdlib sqlite3 only.

Chosen over a standalone DB (Postgres/Redis/Mongo) because it costs ~zero RAM
(just a file on NVMe) on a 3GB-total box, needs no daemon, and keeps Argus
dependency-free. WAL mode + a single lock-guarded connection (write volume is
tiny: ~7 rows / 30s). Bounded by time-based retention pruning.
"""
import os
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS samples (
    ts INTEGER NOT NULL, target TEXT NOT NULL, status TEXT NOT NULL, latency_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_samples ON samples(target, ts);

CREATE TABLE IF NOT EXISTS mongo_perf (
    ts INTEGER NOT NULL, write_ms REAL, read_ms REAL, cmd_ms REAL, disk_ms REAL,
    queue INTEGER, w_avail INTEGER, w_total INTEGER, dirty_pct REAL, conns INTEGER
);
CREATE INDEX IF NOT EXISTS idx_mongo_ts ON mongo_perf(ts);

CREATE TABLE IF NOT EXISTS events (
    ts INTEGER NOT NULL, target TEXT, kind TEXT, detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);

CREATE TABLE IF NOT EXISTS host_metrics (
    ts INTEGER NOT NULL, host TEXT NOT NULL,
    cpu_pct REAL, mem_pct REAL, mem_total_kb INTEGER, mem_avail_kb INTEGER
);
CREATE INDEX IF NOT EXISTS idx_host_ts ON host_metrics(host, ts);

CREATE TABLE IF NOT EXISTS ollama_perf (
    ts INTEGER NOT NULL, model TEXT, eval_tps REAL, prompt_tps REAL,
    load_ms REAL, total_ms REAL, eval_count INTEGER
);
CREATE INDEX IF NOT EXISTS idx_ollama_ts ON ollama_perf(ts);
"""


class Store:
    def __init__(self, path):
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(SCHEMA)
        self._db.commit()

    # ---- writes -----------------------------------------------------------
    def insert_samples(self, rows):
        """rows: iterable of (target, status, latency_ms)."""
        ts = int(time.time())
        with self._lock:
            self._db.executemany(
                "INSERT INTO samples(ts,target,status,latency_ms) VALUES(?,?,?,?)",
                [(ts, t, s, l) for (t, s, l) in rows])
            self._db.commit()

    def insert_mongo_perf(self, m):
        ts = int(time.time())
        with self._lock:
            self._db.execute(
                "INSERT INTO mongo_perf(ts,write_ms,read_ms,cmd_ms,disk_ms,queue,"
                "w_avail,w_total,dirty_pct,conns) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (ts, m.get("write_ms"), m.get("read_ms"), m.get("cmd_ms"),
                 m.get("disk_ms"), m.get("queue"), m.get("w_tickets_avail"),
                 m.get("w_tickets_total"), m.get("dirty_pct"), m.get("conns")))
            self._db.commit()

    def insert_host_metrics(self, host, m):
        with self._lock:
            self._db.execute(
                "INSERT INTO host_metrics(ts,host,cpu_pct,mem_pct,mem_total_kb,"
                "mem_avail_kb) VALUES(?,?,?,?,?,?)",
                (int(time.time()), host, m.get("cpu_pct"), m.get("mem_pct"),
                 m.get("mem_total_kb"), m.get("mem_avail_kb")))
            self._db.commit()

    def insert_ollama_perf(self, m):
        with self._lock:
            self._db.execute(
                "INSERT INTO ollama_perf(ts,model,eval_tps,prompt_tps,load_ms,"
                "total_ms,eval_count) VALUES(?,?,?,?,?,?,?)",
                (int(time.time()), m.get("model"), m.get("eval_tps"),
                 m.get("prompt_tps"), m.get("load_ms"), m.get("total_ms"),
                 m.get("eval_count")))
            self._db.commit()

    def insert_event(self, target, kind, detail):
        with self._lock:
            self._db.execute(
                "INSERT INTO events(ts,target,kind,detail) VALUES(?,?,?,?)",
                (int(time.time()), target, kind, detail))
            self._db.commit()

    def prune(self, days):
        cutoff = int(time.time()) - days * 86400
        with self._lock:
            for tbl in ("samples", "mongo_perf", "events", "host_metrics",
                        "ollama_perf"):
                self._db.execute(f"DELETE FROM {tbl} WHERE ts<?", (cutoff,))
            self._db.commit()

    # ---- reads ------------------------------------------------------------
    def _range(self, sql, params, limit):
        with self._lock:
            rows = self._db.execute(sql, params).fetchall()
        if limit and len(rows) > limit:            # even downsample to cap payload
            step = len(rows) // limit + 1
            rows = rows[::step]
        return rows

    def mongo_history(self, hours=6, limit=1000):
        since = int(time.time()) - int(hours * 3600)
        rows = self._range(
            "SELECT ts,write_ms,read_ms,cmd_ms,disk_ms,queue,dirty_pct,conns "
            "FROM mongo_perf WHERE ts>=? ORDER BY ts", (since,), limit)
        return [{"ts": r[0], "write_ms": r[1], "read_ms": r[2], "cmd_ms": r[3],
                 "disk_ms": r[4], "queue": r[5], "dirty_pct": r[6], "conns": r[7]}
                for r in rows]

    def ollama_history(self, hours=24, limit=1000):
        since = int(time.time()) - int(hours * 3600)
        rows = self._range(
            "SELECT ts,eval_tps,prompt_tps,load_ms FROM ollama_perf WHERE ts>=? "
            "ORDER BY ts", (since,), limit)
        return [{"ts": r[0], "eval_tps": r[1], "prompt_tps": r[2],
                 "load_ms": r[3]} for r in rows]

    def history(self, target, hours=24, limit=2000):
        since = int(time.time()) - int(hours * 3600)
        rows = self._range(
            "SELECT ts,status,latency_ms FROM samples WHERE target=? AND ts>=? "
            "ORDER BY ts", (target, since), limit)
        return [{"ts": r[0], "status": r[1], "latency_ms": r[2]} for r in rows]

    def host_history(self, host, hours=6, limit=1000):
        since = int(time.time()) - int(hours * 3600)
        rows = self._range(
            "SELECT ts,cpu_pct,mem_pct FROM host_metrics WHERE host=? AND ts>=? "
            "ORDER BY ts", (host, since), limit)
        return [{"ts": r[0], "cpu_pct": r[1], "mem_pct": r[2]} for r in rows]

    def recent_status(self, target, n=60):
        """Last n status values (oldest→newest) as 1/0, to reseed the in-memory
        ring after a restart so uptime% survives a redeploy."""
        with self._lock:
            rows = self._db.execute(
                "SELECT status FROM samples WHERE target=? ORDER BY ts DESC LIMIT ?",
                (target, n)).fetchall()
        return [1 if r[0] == "up" else 0 for r in reversed(rows)]
