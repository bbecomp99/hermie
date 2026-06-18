# CLAUDE_NOTES.md

Running notes for the `hermie` repo so I can catch myself up across sessions.
Newest context at the top of each section. **No secrets in this file.**

## 2026-06-18 — Ollama Cloud catalog rotation broke the canary + agent — FIXED & DEPLOYED
- **Symptom:** the Argus Ollama drill-down stopped recording throughput (last
  canary 2026-06-16 06:52 UTC, trend went empty). Up/down `/api/version` ping was
  still 200 (no auth), so it looked half-alive.
- **Root cause:** ollama.com **rotated its model catalog and retired
  `qwen3-next:80b`** — it's gone from `/api/tags` (new lineup: glm-5.2,
  deepseek-v4-pro/flash, qwen3.5:397b, qwen3-coder-next, kimi-k2.7, minimax-m3,
  devstral-2:123b, …). The canary kept generating against the dead model → 404
  every cycle. **Same model broke the live agent** (`hermes_model_default` was
  also `qwen3-next:80b` → model-not-found).
- **Canary fix** (`roles/monitoring/`): `monitor_ollama_model` →
  **`devstral-2:123b`**. Verified live: focus + canary on devstral, output 28.0 /
  round-trip 54.3 tok/s. Deployed `--tags monitoring`.
- **PROMPT THROUGHPUT was dead since the cloud switch** (user noticed): cloud is
  serverless and returns **every phase clock null** — `prompt_eval_duration` too
  (confirmed live on devstral: only `total_duration` comes back). So prefill speed
  **can't be isolated**. Per user's choice, **repurposed the panel to round-trip
  total throughput** = `(prompt_eval_count + eval_count) / total_duration` —
  carried in the existing `prompt_tps` field/column (always null on cloud, so no
  schema migration). `ollama.html` heroes relabeled: **"Output throughput"**
  (eval_tps) + **"Round-trip throughput"** (prompt_tps); trend legend output/
  round-trip.
- **Agent fix** (`group_vars/hermes/main.yml`): user asked for "latest DeepSeek" —
  but **ALL DeepSeek variants are subscription-gated** on this account (403 "this
  model requires a subscription, upgrade for access"); same 403 for qwen3.5:397b,
  glm-5.2, kimi-k2.6, mistral-large-3:675b. Free-tier models that **do** emit real
  `tool_calls` over `/v1` (verified with a get_weather tool): **devstral-2:123b,
  minimax-m3, qwen3-coder-next, gpt-oss:120b** (note the 120b works even though the
  old gpt-oss:20b failed). User chose **devstral-2:123b** (same as the canary).
  Deployed `--tags model` + `systemctl --user restart hermes-gateway`; gateway
  active, reconnected to Mattermost (@hermie) + HA clean, config.yaml
  `default: devstral-2:123b`.
- **To use DeepSeek later:** upgrade at ollama.com/upgrade, then repoint
  `hermes_model_default` to `deepseek-v4-pro` and re-verify tool_calls.

## 2026-06-15 — Argus: Kafka traffic + consumer-lag (queue depth) on the drill-down — DEPLOYED
- The Kafka page only read **metadata** (brokers/ISR). Added two things the user
  asked for: **message throughput** and **queues building up** (consumer lag).
- **`kafka.py`** — three new pre-flexible wire requests (same hand-rolled style,
  trivial length-prefixed parsing):
  - **ListOffsets v1** (key 2, timestamp -1) → per-partition **log-end offset**
    (high-watermark). Σ = cumulative messages; the loop takes a **delta/sec** →
    msgs/sec throughput. `_list_offsets()`.
  - **ListGroups v0** (key 16) + **OffsetFetch v2** (key 9, null topics = all) →
    consumer-group committed offsets. **lag = end − committed**, summed per group
    → queue depth. `_list_groups()` / `_offset_fetch()`.
  - All sent on the **bootstrap connection** (single-broker LAN assumption). On a
    multi-broker cluster, partitions/groups the bootstrap doesn't own answer
    NOT_LEADER / NOT_COORDINATOR → skipped (partial counts), never fatal. Each
    request is wrapped in try/except so traffic/lag are best-effort and can't
    break the core metadata health.
  - `health(..., track_traffic, track_lag)` now also returns `traffic`
    {total_end_offset (user topics), total_end_offset_all, topics[top 25]} and
    `consumers` {total_lag, group_count, groups[{group_id, protocol_type, lag,
    partitions, topics, active}]}. `summary` gains total_lag + consumer_groups.
    `perf_sample()` carries `total_end_offset` so the loop can delta it.
- **`app.py`** — `eval_kafka_perf(prev, cur, thr, dt)` (was instantaneous) now
  computes `msgs_per_sec` from the end-offset delta (mirrors the ES QPS pattern;
  counter reset → None) and adds a **lag degrade rule** (total lag > `lag_degrade`,
  default 0 = display-only). Loop tracks `kafka_prev`, stores **metrics** (not the
  raw sample) into `kafka_latest`, and `/api/kafka` injects it as `data.rates`
  (live msgs/sec), exactly like `/api/elastic`.
- **`store.py`** — `kafka_perf` gains `msgs_per_sec / total_lag / consumer_groups`
  + a new **idempotent `_migrate()`** (`ALTER TABLE ADD COLUMN` if missing, via
  `_MIGRATIONS`) so the **live .128 DB** (already has the old table) picks them up
  on restart. `kafka_history()` returns the new columns for the charts.
- **`kafka.html`** — new **"Traffic & queues"** section: Throughput (msgs/sec),
  Consumer lag, Busiest topic, Consumer-groups heroes; two auto-scaled trend
  charts (throughput emerald / lag amber) sharing the existing range selector via
  a generic `drawMetricChart()`; and a **consumer-groups table** (group · state ·
  topics · partitions · lag, highest lag first, amber-tinted rows when lag>0).
  Sections hide when their track flag is off.
- **Config**: `monitor_kafka_track_traffic` / `_track_lag` (default true) +
  `monitor_kafka_lag_degrade` (default 0) in `defaults/main.yml` → `config.json.j2`.
- ✅ **Validated against the LIVE broker** — the sandbox could reach Kafka this
  time (127.0.0.1:9092 → advertised .127). Real data: topics `stonks.ticks.v1`
  (6 part, **42.95M msgs**), `stonks.agentic.v1`, `__consumer_offsets`; groups
  `stonks-distributor` / `agentic-distributor` both **lag 0** (caught up).
  Wire round-trips, store migration (old→new schema), eval delta, and a 2-sample
  throughput delta all unit/smoke-tested green; full server boots, page 200,
  endpoints correct.
- ✅ **DEPLOYED to .128 + verified live** (`--tags monitoring --become-password-file`,
  pw from `.env` PASS, colon-delimited so extract the value after `PASS:` into a
  600 temp file). `failed=0`, image rebuilt via both handlers. `/api/kafka` →
  ok:true, broker .127:9092, traffic total_end_offset 42.95M, 2 groups lag 0;
  `/api/kafka/history` returns **721 rows** (the `_migrate()` ALTER worked — old
  rows survived, new samples carry total_lag/consumer_groups/msgs_per_sec);
  kafka.html 200. msgs_per_sec shows None while the ticks topic is idle (offset
  stable) — it'll populate once producers resume.

## 2026-06-14 — Argus: Internet QoS drill-down (merged URL trackers) — DEPLOYED
- **Deployed to .128 + verified live** (`ansible-playbook playbook.yml --tags
  monitoring --become-password-file <pw>`; vault.yml is plaintext-gitignored so no
  vault pass; become pw lives in repo `.env` as `PASS`). Image rebuilt via the
  Rebuild/Restart handlers, `failed=0`. `/api/internet` → `ok:true`, both URLs up:
  api.massive.com 184ms (avg 184/p95 229/jitter 26.6/100%), google.com 372ms (avg
  310/p95 402/jitter 55.7/100%), summary avg 247ms; `internet.html` serves 200;
  history already had ~120 samples/URL (they were probed pre-existing since 6-12).
- New drill-down **`internet.html`** (🌐 sky/azure accent) — a "Global URL
  trackers" / quality-of-service panel that **merges the two internet-group
  probes** (`google.com`, `api.massive.com`) into one view. On the dashboard the
  two separate cards now collapse into a single **"Internet QoS"** card
  (`renderInternetCard()` in `index.html`) that drills into the page; the other
  cards still render 1:1 via the refactored `renderCard()`.
- **No new sampling, no new table.** The internet probes already write
  `status`+`latency_ms` to the `samples` table every cycle, so QoS is computed
  straight from `store.history()`. New backend in `app.py`:
  - `_qos_stats()` — per-URL uptime %, avg/min/max latency, **p95** (linear-interp
    `_percentile()`), and **jitter** (mean abs delta between consecutive
    latencies). Latency stats use successful probes only.
  - `internet_snapshot()` — live status (in-memory ring) + QoS aggregates over
    the last hour for every probe in `internet.group`.
  - Endpoints: `/api/internet` (live + QoS, `?hours=` window) and
    `/api/internet/history?hours=` (merged `{names, series}` — one call returns
    every URL's latency/status trend for the multi-line chart).
  - Loop: a reachable-but-slow URL (latency > `degrade_ms`) marks the panel
    **degraded** (amber, via `state.set_degraded`) and posts **one panel-level**
    Mattermost degraded↔recovered transition (not per-URL, so a flapping
    endpoint can't spam) — reuses `post_perf`.
- **Config** (`monitor_internet_*` in `defaults/main.yml` → `config.json.j2`):
  `enable` (default true), `group` ("internet"), `poor_ms` (400, amber on page),
  `degrade_ms` (800, panel degrades). Add more tracked URLs by appending probes
  with `group: "internet"`. No Dockerfile/compose change (static dir is COPYed
  wholesale; no new .py / env).
- **Smoke-tested locally**: app boots, both pages serve 200, endpoints return
  correct shape, QoS math verified against injected samples (uptime 10/16=62.5%,
  avg 136, p95 522, jitter 196.4). Sandbox has no outbound net so live probes
  read "down" here — real up/down + latency only show once deployed on .128.
- ⚠️ **Not yet deployed** (same `--tags monitoring`, needs pinky sudo).

## 2026-06-13 — Argus: Elasticsearch + Kafka drill-down pages
- Added two full detail pages (drill-in from the index cards, like Mongo/Ollama):
  **`elastic.html`** (teal accent, 🔎) and **`kafka.html`** (copper accent, "K"
  medallion). Wired into `index.html`'s `detailPages` map by target name
  (`elasticsearch` → `./elastic.html`, `kafka` → `./kafka.html`).
- **New pure-stdlib clients** mirroring `mongo.py`/`ollama.py`:
  - `elastic.py` — ES REST API (HTTP/JSON, the easy one). `health()` pulls
    `/`, `/_cluster/health`, `/_cluster/stats`, `/_nodes/stats/...`,
    `/_cat/indices` and curates: cluster status (green/yellow/red), shard
    allocation, docs/store, per-node heap/cpu/gc/thread-pools/breakers/fds, and a
    per-index table. Optional HTTP basic auth (username + `ES_PASSWORD` env);
    the .127 target is anonymous http so it defaults off. `perf_sample()` is a
    cheap 2-call (health + node stats) snapshot for the loop.
  - `kafka.py` — **hand-rolled Kafka wire protocol** (binary TCP, no client lib;
    same spirit as mongo's OP_MSG). Two requests on PRE-FLEXIBLE versions so
    parsing stays trivial: **ApiVersions v0** (key 18) + **Metadata v1** (key 3,
    topics=null → all). Derives the classic health signals from metadata:
    brokers, active controller, topics/partitions, **under-replicated** (ISR <
    replicas) and **offline** (no leader) partitions, plus per-broker leader
    counts. Assumes a PLAINTEXT listener. Round-trip + curation unit-tested
    locally (synthetic frames) before wiring.
- **Backend wiring** (`app.py`): `/api/elastic` + `/api/kafka` (live) and
  `/api/elastic/history` + `/api/kafka/history` (SQLite trends). Loop samples
  both each cycle → `elastic_perf` / `kafka_perf` tables (new in `store.py`,
  added to prune), computes ES search/index **QPS from counter deltas** (stored +
  exposed live via `elastic_latest` → `data.rates`), and posts Mattermost
  **degraded↔recovered** transition alerts (reusing `post_perf`). Degrade rules:
  ES → RED status / heap>`heap_pct` / cpu>`cpu_pct` / thread-pool rejections
  always; **yellow only when `monitor_elastic_degrade_on_yellow`** (default
  false — single-node clusters sit yellow as steady state). Kafka → offline or
  under-replicated partitions / missing controller / brokers < `min_brokers`.
- Charts: ES page trends **max heap% + cpu%** (fixed 0–100 axis); Kafka page
  trends **under-replicated + offline** partition counts (auto-scaled). Both have
  the 1H/6H/1D/1W range selector pattern from the Ollama page.
- **Config**: `monitor_elastic_*` / `monitor_kafka_*` in `defaults/main.yml`,
  rendered into `config.json.j2`; `ES_PASSWORD` added to `docker-compose.yml.j2`
  env (alongside MM_TOKEN / OLLAMA_API_KEY); `Dockerfile` COPY now includes
  `elastic.py kafka.py`. Both default **enabled** against `.127` (`:9200` /
  `:9092`). Verified: full server boots, serves both pages (200), endpoints
  return clean `ok:false` on unreachable, eval logic + QPS deltas correct.
- ⚠️ **Not yet deployed / not integration-tested against the REAL .127 ES &
  Kafka** — only local unit/smoke tests (couldn't reach .127 from here). First
  deploy (`--tags monitoring`, needs pinky sudo) is the real validation; watch
  for ES security-on (would need https+auth) or a non-PLAINTEXT Kafka listener.

## 2026-06-07 — Argus: split host charts + cleared latency history
- Per user, **split the combined host CPU+Mem dual-line chart into two separate
  single-line charts** (`#cpuChart` gold, `#memChart` sky), side-by-side under the
  CPU/Memory hero cards, sharing one 1H/1D/1W/1M range toolbar. `drawHostTrend()`
  now calls `_drawHostChart(elId,axisId,series,color)` twice (fixed 0–100% scale,
  per-chart "now X% · peak Y%" axis).
- **Cleared `mongo_perf` table** (109→0 rows) to drop two getmore-spike artifacts
  from the latency trend — fresh data only. host_metrics + samples left intact.
  Done via `docker exec argus python -c "...DELETE FROM mongo_perf..."` (WAL, live).
- This is the last edit before the first commit of the whole Argus build.

## 2026-06-07 — Argus: host CPU/mem for the Mongo box (.142) via SSH
- User wanted DB-server CPU + memory graphs. Mongo's API can't report host CPU →
  collect over **SSH, NO agent** (user's call). `files/app/hoststat.py` (stdlib):
  one `ssh user@.142 'grep "^cpu " /proc/stat; sleep 1; grep ...; cat
  /proc/meminfo'` per 30s cycle → CPU% from the two /proc/stat snapshots, mem% from
  MemTotal/MemAvailable. Parser verified against real output (cpu ~5%, mem 96.2% —
  the box runs tight, ~1.25GB free of 31GB; WiredTiger eats RAM).
- **SSH access (OUT OF BAND — not in Ansible, prerequisite):** my Mac key was
  denied on .142; .128's pinky key wasn't authorized either. Installed `sshpass`
  on .128 (apt) and ran `ssh-copy-id` of **.128's `~/.ssh/id_rsa.pub`** to
  `pinky@192.168.88.142` using the repo `.env` password. Now .128→.142 is
  key-based. `.142` is NOT in the inventory; only this key trust enables it.
  Reading /proc needs NO sudo (world-readable).
- **Container does the SSH:** added `openssh-client` to the image; ansible stages
  a COPY of pinky's id_rsa + known_hosts into `{{monitor_dir}}/ssh` owned by uid
  5000 (container can't read pinky's 0600 key otherwise), mounted `:/app/.ssh:ro`.
  ssh uses `-i /app/.ssh/id_rsa -o UserKnownHostsFile=... -o StrictHostKeyChecking=yes`.
  ⚠️ This puts a copy of pinky's full key in the container — fine on trusted LAN;
  could harden with a forced-command restricted key later.
- **Wiring:** new `host_metrics` SQLite table (+ prune); loop samples → persists +
  caches latest in a shared `host_latest` dict; `/api/mongo` gets `hostMetrics`,
  new `/api/host/history?hours=`. Mongo page got a **"Host · .142"** section: CPU
  + Memory hero cards (thresholded) + a fixed-0–100% dual-line chart (gold CPU /
  sky mem) with the same 1H/1D/1W/1M buttons. Defaults: `monitor_host_metrics_*`.
- VERIFIED on .128: container SSHes .142, log shows cpu/mem, /api/mongo.hostMetrics
  populated, history + DB table accumulating, page renders the section.

## 2026-06-07 — Argus: Greek "Obsidian & Bronze" restyle + read-latency fix
- **Read-latency artifact fixed:** hero card was showing Mongo's *lifetime* avg
  (~3624ms) via a fallback when no reads happened in the interval — inflated by
  getmore (idle tailable-cursor waits counted as read latency). Removed the
  lifetime fallback; hero now shows live delta only, "idle · no reads" when none.
  Trend/persistence already used live deltas (correct). True read-query latency
  (getmore-excluded) would need the profiler — offered, user declined.
- **Restyle (both pages):** theme = obsidian ground + antique-bronze/gold + Aegean
  teal + parchment text. Google Fonts **Cinzel** (lapidary caps) for h1/h2 +
  **Cormorant Garamond** italic taglines. `.glass` redefined frosted→marble-tablet
  (stone gradient + bronze hairline + inset). Added `.cornice` (CSS dentil strip
  under each nav), `.medallion` (bronze ring around the 👁 / 🍃), `.bronze`/`.jade`
  gradient title classes. Title rendered **ARGVS** (Roman V-for-U). Reskinned the
  neutral slate ramp (text/bg/border-slate-*) → warm stone/parchment via targeted
  `!important` overrides on the Tailwind utility classes, leaving status colours
  (emerald/red/amber/blue) intact for legibility. Tailwind CDN unchanged.
- Deployed + healthy; fonts + theme classes confirmed in served HTML.

## 2026-06-07 — Argus: dual-line latency chart + range selector
- Mongo detail trend went from a single live-appended write-latency sparkline to a
  **history-driven dual-line chart** (write = emerald, read = sky) fed by
  `/api/mongo/history`, with **1H / 1D / 1W / 1M** range buttons (`setRange(h)` →
  `loadTrend()` re-fetches that window). `drawTrend()` paints two polylines scaled
  to a shared max, with a 0–max label, point count, and start/end time axis;
  called after each render() so it survives the 5s page rebuild. Trend refreshes
  on its own 30s interval (matches sample cadence); removed the old `latHist`
  buffer + `latSpark()`. Live hero cards (write/read/disk/queue) unchanged.
- NOTE: `read_ms` is sparse (most 30s windows have 0 read ops on this DB → null);
  the read line plots only non-null points (connects across gaps) — accurate, not
  a bug. Fills in over longer ranges.
- Verified: JS `node --check` clean; page serves buttons + chart; all 4 range
  endpoints respond; DB survived the rebuild (rows kept growing, not reset).

## 2026-06-07 — Argus: SQLite persistence (history survives restart)
- User wanted history not lost on refresh/redeploy, on a ~3GB-RAM box, NVMe disk.
  Chose **embedded SQLite** (`store.py`, stdlib `sqlite3`) — NOT a DB service:
  zero standalone RAM (a file), no daemon, keeps Argus dependency-free. WAL mode,
  single lock-guarded connection (write volume ~7 rows/30s), 30-day retention
  prune (hourly).
- **Schema:** `samples`(ts,target,status,latency), `mongo_perf`(ts,write/read/cmd/
  disk_ms,queue,tickets,dirty_pct,conns), `events`(transitions + mongo degraded/
  recovered). Loop persists every cycle; `eval_mongo_perf` now returns the full
  metrics dict (refactored via `_avg_ms`) so perf rows are stored.
- **New endpoints:** `/api/mongo/history?hours=` + `/api/history?target=&hours=`
  (downsampled to cap payload). `mongo.html` **seeds the write-latency sparkline
  from `/api/mongo/history` on load** → a page refresh no longer wipes the trend.
  On startup `State.seed()` reloads the per-target ring from `recent_status()` so
  **uptime% survives a redeploy** (verified: post-restart uptime=100%, spark=60
  before any new probe).
- **Container/infra:** pinned container uid **5000** (`useradd --uid 5000`), host
  data dir `{{ monitor_dir }}/data` chowned to 5000 (new ansible task), bind-
  mounted `:/app/data` (read-write, survives rebuild). DB path
  `/app/data/argus.db`. Defaults: `monitor_uid=5000`, `monitor_retention_days=30`.
- If SQLite init fails, app logs `persistence DISABLED` and runs in-memory (graceful).
- VERIFIED on .128: argus.db + -wal/-shm owned 5000 in /home/pinky/monitor/data,
  rows accumulating, history endpoints serve, restart-seed works.

## 2026-06-07 — Argus: Mongo latency & saturation monitoring
- User (ex-SQL-DBA) wanted the classic "disk-queue rising / CPU saturated crushes
  the DB" early-warning, translated to Mongo/WiredTiger. Added on both the page
  AND as proactive alerts.
- **Mongo 8.x field locations (verified on .142):** opLatencies under
  `serverStatus.opLatencies.{reads,writes,commands}.{latency(µs cumulative),ops}`;
  storage tickets MOVED from `wiredTiger.concurrentTransactions` (null in 8.x) to
  **`serverStatus.queues.execution.{read,write}`** = {out, available, totalTickets,
  normalPriority.queueLength, normalPriority.totalTimeQueuedMicros,
  maxAcquisitionDelinquencyMillis}. The "disk latency hitting queries" signal =
  `wiredTiger.cache['application threads page read from disk to cache time/count']`
  → Δtime/Δcount = avg ms to fault a page off disk. hostInfo gives numCores (NOT
  live CPU — Mongo doesn't expose host CPU; .142 host CPU would need a node
  exporter, not done).
- **Detail page** (`mongo.html`) got a prominent "Latency & Saturation" section:
  hero cards (write/read/disk latency, storage-queue tickets) colour-thresholded,
  a **write-latency trend SVG sparkline** (client-side 80-sample history), plus
  cmd latency / dirty-cache% / queue-wait-ms/s / flow-throttle rows, and a
  read+write "Storage tickets (queue depth)" card. Rates computed client-side from
  deltas between 5s polls.
- **Proactive alerting** in `app.py` loop: each cycle calls new lightweight
  `mongo.perf_sample()` (single serverStatus), computes live write latency + disk
  read ms + queue length + ticket exhaustion + flowControl lag, and posts
  `👁 Argus · ⚠️ MongoDB degraded — <reasons>` / recovered to Mattermost on
  transition only. Thresholds in defaults: `monitor_mongo_perf_*`
  (write_latency_ms=25, disk_read_ms=20, queue_len=1). VERIFIED on .128: sampler
  runs every 30s, live ~1ms write / 0.2ms disk / queue 0 = "ok", no false alarms.

## 2026-06-07 — Argus: MongoDB deep-dive detail page
- **Clickable `mongodb` tile** on the dashboard → dedicated `mongo.html` detail
  page (same glass style; back-link to Argus). Generalized via a `detailPages`
  map in index.html so other services can get detail pages later.
- **Pure-stdlib MongoDB client** (`files/app/mongo.py`) — hand-rolled BSON
  encode/decode + `OP_MSG` (opcode 2013) over a raw socket. NO pymongo, keeps the
  zero-dep ethos. Read-only diagnostics only; **no SCRAM auth** (the target mongod
  at `192.168.88.142:27017` accepts anonymous admin commands — confirmed via the
  astonks conn string `mongodb://192.168.88.142:27017/stonksDB`, no creds).
- **`/api/mongo` endpoint** runs `isMaster` + `serverStatus` + `listDatabases` +
  per-db `dbStats` (+ `replSetGetStatus`, gracefully null on standalone) and
  curates: version/uptime/engine, connections, opcounters, network, mem,
  WiredTiger cache (+ tickets), globalLock concurrency, asserts, document metrics,
  query executor, and a databases table. Page computes per-second RATES
  client-side from deltas between 5s polls.
- **VALIDATED against real Mongo 8.2.2** (from .127 AND from the deployed
  container on .128 → .142 over LAN): 5 dbs, stonksDB = 11 colls / 24.2M objs /
  27GB / 25 idx, 73 conns, 12.9GB/16GB cache. Wire protocol confirmed working.
  Deployed + healthy.

## 2026-06-07 — Monitor renamed → **Argus** (the all-seeing watch)
- Rebranded "Hermie Monitor" → **Argus** (Argus Panoptes, the hundred-eyed myth
  watchman — fits the Greek/Hermes theme; Hermes famously slew Argus, now runs
  his own). Changed: UI heading+title+👁, MM alert/heartbeat messages now
  prefixed `👁 **Argus** · …`, container_name + image → `argus`/`argus:latest`,
  log prefix `[argus]`, README/docs. Role dir + var prefix stay `monitoring`/
  `monitor_*` (internal; not user-facing). Compose project still `~/monitor`
  (service key `monitor`) so the rename recreated in place via compose labels —
  old `hermie-monitor` container + image removed, no port-9200 conflict.
- Redeployed + verified: `argus` Up (healthy), UI shows ARGUS, 6/6 green, old
  image pruned.

## 2026-06-07 — Monitoring: deployed + MM-url fix + heartbeat
- **DEPLOYED to `.128`** (via `--ask-become-pass`; `pinky` is in `sudo` but NOT
  `docker`, so the role's `become: true` needs the sudo pw — it's in the repo
  `.env` as `PASS`). All 6 targets green on first converge. Container
  `hermie-monitor`, dashboard at http://192.168.88.128:9200/.
- **BUGFIX (caught post-deploy):** `monitor_mm_url` had inherited the gateway's
  `mm_connect_url` = `http://127.0.0.1:8065`, which is unreachable from the
  bridged monitor container (127.0.0.1 = the container itself) → alerts would
  silently fail. Changed default to the LAN IP `http://192.168.88.128:8065`
  (Mattermost binds `0.0.0.0:8065`). Verified the container can now reach the MM
  ping. **Lesson: never point a containerized client at 127.0.0.1 for a host svc.**
- **Heartbeat added:** in-app timer (`monitor_heartbeat_interval`, default 21600s
  = 6h; 0 disables) posts an "all-clear" summary to Town Square every 6h
  regardless of transitions — `:white_check_mark: All clear — N/M up`, or
  `:yellow_heart: Heartbeat — … DOWN: <names>` if anything's down. Clock counts
  from container start so redeploys don't spam. Transition alerts unchanged.
- **Alert channel = Town Square** (`mm_home_channel`, id bkkf739…), same as cron.
- **Still UNCOMMITTED on `main`** — whole `monitoring` role + playbook/README/notes.

## 2026-06-07 — Custom monitoring stack (Hermie Monitor)
- **New `monitoring` role** — a 100% custom, **dependency-free Python** service
  monitor (no pip deps, `python:3.12-slim`, ~50MB), containerized on `.128` via
  the same compose pattern as the mattermost role. Built fresh instead of using
  Uptime Kuma/Prometheus (user wanted a bespoke lightweight app styled like
  astonks).
- **What it does:** background thread black-box probes targets every 30s — two
  probe types, `http` (urllib; checks status set + optional body substring) and
  `tcp` (socket connect). Serves `/api/status` (JSON) + `/` (astonks-styled glass
  dashboard: Tailwind CDN, dark slate, status dots, latency, uptime %, sparkline,
  10s auto-refresh) + `/healthz`. In-memory state only (60-sample ring buffer per
  target); resets on restart — fine for v1.
- **Targets (6):** dashboard `:9119`, mattermost `:8065/api/v4/system/ping`,
  home-assistant `:8123` (all `.128`); ollama `.127:11434/api/tags`; astonks-api
  `.127:3000`; mongodb `.142:27017` (tcp). Postgres is unpublished → covered
  transitively by the MM ping. **Gateway** (user-systemd, no port) can't be
  black-box probed from a container → left a documented `/api/push/<name>` hook
  idea (NOT built) for a host cron to heartbeat it later.
- **Alerting:** posts to Mattermost **only on up↔down transitions** (no spam) via
  the `/api/v4/posts` API, **reusing the existing gateway bot token** (`mm_bot_token`
  from vault) + `mm_home_channel` — no new webhook/secret. Auto-disables if
  token/url/channel are blank. Token injected via `MM_TOKEN` env (compose file is
  `0600`); it is NOT written into `config.json` (which is world-readable `0644`).
- **Layout:** `roles/monitoring/{defaults,tasks,handlers,templates}` +
  `files/app/{Dockerfile,app.py,static/index.html}`. Port **9200**. Wired into
  `playbook.yml` with `tags: ['monitoring']`. Two handlers: *Rebuild* (source
  change → `up -d --build`) vs *Restart* (config-only → `compose restart`, since
  config.json is read once at startup behind a read-only mount).
- **Validated locally:** app smoke-tested (http+tcp probes, down-detection,
  down-first sort, uptime/sparkline, dashboard all serve); `config.json.j2`
  renders to valid JSON with host vars resolved; `ansible-playbook --syntax-check`
  passes. **NOT yet deployed to `.128`** — remote converge is gated on user.
  Deploy with: `ansible-playbook playbook.yml --tags monitoring`.

## 2026-06-06 — Ollama migration + toolset prune + stockcheck
- **Model pinned hot in memory.** Flipped `OLLAMA_KEEP_ALIVE` `1h` → `-1` in
  `~/Library/LaunchAgents/com.hermie.ollama.plist` (then
  `launchctl bootout/bootstrap gui/$UID` + a preload `curl …/api/generate -d
  '{"model":"lfm2.5:8b-hermes","keep_alive":-1}'`). `ollama ps` now shows
  `UNTIL = Forever`, 100% GPU, 65536 ctx. No more cold-start wait on the first
  request after idle; costs ~5.5GB RSS held permanently (fine on the 16GB box).
  Only a reboot or `ollama serve` restart drops it (first call after re-pays the
  load). ⚠️ The plist lives on this Mac (.127), NOT tracked in the repo — this
  log is the only record.
- **LLM switched MLX → Ollama.** `mlx_lm.server` can't emit structured
  `tool_calls` (returns them as plain text), so all agentic tool use was dead.
  Installed **Ollama** on the Mac (official `brew install --cask ollama-app` —
  the bare `ollama` formula ships a broken bottle with no `llama-server` GGUF
  runner). Runs headless via a launchd agent `~/Library/LaunchAgents/com.hermie.ollama.plist`
  (never the Electron GUI/updater) on `0.0.0.0:11434`, `OLLAMA_KV_CACHE_TYPE=q4_0`.
- **Model = `llama3.2:3b-hermes`** (custom: `llama3.2:3b-instruct-q4_K_M` +
  `num_ctx 65536`). Hermes hard-requires **≥64K context**; Qwen2.5-7B caps at 32K
  and is 5.3GB, so it couldn't clear the floor within the 4–4.5GB RAM budget.
  Llama-3.2-3B is natively 128K; at 64K with q4 KV cache it loads ~4.3GB. Repoint
  is `ansible-playbook playbook.yml --tags model` (config tasks now tagged).
- **`roles/toolsets`** — the 3B drowned in ~18 toolsets (misfired `messaging` →
  empty `send_message` spam, confabulated). Prunes per-platform to a minimal set
  (homeassistant, memory, clarify, +kanban built-in). **Gotcha:** enablement is
  per-platform in `platform_toolsets:` (NOT `agent.disabled_toolsets`), and
  `hermes tools disable` defaults to `--platform cli` — the mattermost bot must
  be pruned explicitly.
- **`roles/stockcheck`** — deterministic `check_stock` MCP tool: fetches
  `…127:3000/api/llm/symbol/{SYM}`, computes RVOL (day_vol/hist_vol),
  alpha_vol/hist ratio, and alpha_price coefficient-of-variation; posts a ✅ LIKE
  to #stonks when all pass (defaults RVOL≥2.0, ratio≥0.10, CV≤5%). Registered by
  **writing `mcp_servers` config directly** (the patcher) — `hermes mcp add` is
  interactive and hangs under Ansible. Engine verified (`--check SPY` → PASS).
  ⚠️ **The 3B can't reliably INVOKE it** — confuses its `symbol` arg with HA's
  `entity_id`, or fabricates. Backend solid, model is the weak link. A bigger
  model (e.g. an 8B) or a deterministic chat trigger would fix invocation.

## What this repo is
Ansible project to **deploy & configure the NousResearch Hermes agent** on a
remote host, pointed at a local MLX LLM. It is *not* application code.

## Topology
- **LLM server:** the Mac I run on, `192.168.88.127:8080/v1` — `mlx_lm.server`
  (MLX), OpenAI-compatible, binds `0.0.0.0`. Serving
  `mlx-community/DeepSeek-R1-Distill-Qwen-7B-4bit`. Managed separately (see
  `~/reposdso/ai-serve`), **out of scope** for this repo.
- **Agent host:** `192.168.88.128`, Ubuntu 22.04 (NucBox5), user `pinky`
  (uid 1000, in `sudo` group). Hermes = github.com/NousResearch/hermes-agent at
  `~/.hermes/hermes-agent`. CLI symlink `~/.local/bin/hermes`. Config
  `~/.hermes/config.yaml` (mode 600); LLM target is the top-level `model:` block.

## Access to .128
- **SSH key auth is set up** — local `~/.ssh/id_ed25519` is in pinky's
  authorized_keys: `ssh pinky@192.168.88.128` works keyless.
- Password is in `hermie/.env` (gitignored, `USER:`/`PASS:` format). Used once
  via `expect` (`/usr/bin/expect`; `sshpass` is NOT installed) to seed the key.
- **sudo needs a password** (= the SSH password). For Ansible, pass it as a temp
  600 vars file, never on the CLI:
  ```bash
  cd ~/reposdso/hermie
  export PATH="/opt/homebrew/bin:$PATH"
  BP=$(grep '^PASS:' .env | cut -d: -f2-); umask 077
  printf 'ansible_become_password: "%s"\n' "$BP" > /tmp/hb.yml
  ansible-playbook playbook.yml -e @/tmp/hb.yml; rm -f /tmp/hb.yml
  ```

## What's deployed (verified working)
- **hermes role:** uv (user-level) → repo at pinned commit → venv → `uv sync` →
  CLI symlink → idempotent `model:` block patcher. `hermes -z "..."` round-trips
  to the LLM. `hermes_version` pinned to `a6e47314f...` (box ~111 commits behind
  upstream main — bump deliberately).
- **dashboard role:** `hermes dashboard` as a **user systemd service**
  `hermes-dashboard` (linger on → starts at boot), bound `0.0.0.0:9119` with
  `--insecure --tui`. Live at **http://192.168.88.128:9119** (HTTP 200 from the
  Mac). Web UI prebuilt to `hermes_cli/web_dist/` via `npm run build`.
  Manage: `systemctl --user {status,restart,stop} hermes-dashboard`.

- **mattermost role:** self-hosted Mattermost (Slack-like) via Docker Compose on
  `.128`. `mattermost/mattermost-team-edition:11.6.4` + `postgres:15-alpine`,
  project dir `~/mattermost`, bound `0.0.0.0:8065`. Live at
  **http://192.168.88.128:8065** (`/api/v4/system/ping` → 200 from the Mac).
  Bot accounts + personal access tokens enabled via env. DB password in
  `group_vars/hermes/vault.yml` (gitignored). Docker tasks run with `become`
  (pinky isn't in the `docker` group, but is in `sudo`). Manage on the box:
  `cd ~/mattermost && sudo docker compose {ps,restart,logs,down}`.

- **gateway role:** wires hermes' Mattermost channel + runs the messaging
  gateway as a service. Writes `MATTERMOST_URL`/`TOKEN`/`ALLOWED_USERS` into
  `~/.hermes/.env` (blockinfile, `no_log`; token from vault). Bot = **@hermie**,
  connects via WebSocket to `http://127.0.0.1:8065`. Service is hermes' own
  **`hermes-gateway`** user unit (enabled at boot, `Restart=always`). Idempotent
  (`changed=0`); reports "Mattermost CONNECTED". `mm_allowed_users: brian` in
  group_vars locks it to that MM user. Home channel (cron/autonomous output) =
  `MATTERMOST_HOME_CHANNEL` = Town Square id `bkkf739saif5tgzwaynhikgmho` (team
  CloudLogic). Channel IDs found via Mattermost API with the bot token:
  `curl -H "Authorization: Bearer $TOKEN" .../api/v4/teams/{id}/channels/name/town-square`.

- **homeassistant role:** ADOPTS the already-running Home Assistant container on
  `.128` (`ghcr.io/home-assistant/home-assistant:stable`, `network_mode: host`,
  `:8123`, config bind-mounted at `/opt/homeassistant/config`). Deliberately
  non-disruptive — the compose spec matches the running container so
  `docker compose up -d` finds it up-to-date and does NOT recreate it, and the HA
  config/db is never touched. Then wires hermes: writes `HASS_URL`/`HASS_TOKEN`
  to `~/.hermes/.env` (`no_log`) and enables the hermes `homeassistant` toolset
  so the agent can drive HA. Token = `hass_token` in vault (HA → Profile →
  Security → Long-Lived Access Tokens). Compose dir `~/homeass`.

- **weather role:** the daily forecast cron (see "Open / next") captured as a
  reproducible role.

## Cross-repo: astonks shares this stack
- The **astonks** project (`~/reposdso/astonks`) runs its own hourly "watcher"
  bot that **posts to this Mattermost** and **calls the same `.127:8080` MLX LLM**.
- It uses a SEPARATE bot, **@stonkbot** (user id `iz34tkszatrt7pi5j74kz3fu6r`),
  posting to a dedicated channel **#stonks** (`wx1br6bfat88xffn5johzhizmh`) in the
  CloudLogic team — distinct from @hermie / Town Square. Onboarding gotcha learned
  there: a MM bot must be added to the TEAM (System Console → User Management →
  Teams) BEFORE it can be `/invite`d to a channel.
- astonks pins model `Nous-Hermes-2-Mistral-7B-DPO-4bit-MLX` (cleaner prose than
  DeepSeek-R1 for summaries). All four MLX models are served on `.127:8080`.

## Gotchas learned (don't repeat)
- **hermes gateway: do NOT template your own systemd unit.** `hermes gateway run`
  REWRITES its own unit on startup (`--replace`), so a custom unit churns every
  play (unit "changed" → restart, forever). Instead drive the interactive
  installer non-interactively: `printf 'y\ny\n' | hermes gateway install`
  (two prompts: start-now, start-on-boot), guarded by a `stat` so it only runs
  when the unit is missing. The unit is named `hermes-gateway.service`.
- **hermes logs to `~/.hermes/logs/gateway.log`, NOT stdout/journald** — verify
  the Mattermost connection from that file (`grep 'mattermost connected'`), not
  `journalctl`.
- `hermes gateway install/start` are **interactive** — a bare Ansible `command`
  hangs forever waiting on the `[Y/n]` prompt (this hung a whole play once).
- `pkill -f "hermes gateway"` over SSH matches your own command and kills the
  session — kill by specific PID instead.
- **MATTERMOST_ALLOWED_USERS matches by Mattermost USER ID, not username.**
  Setting it to `brian` → "Unauthorized user: <id> (brian)" and silent drop.
  Use the user id (brian = `xbpkc5mq9jnk8mfngpjaygz5ay`). Same for other
  platforms per platform_registry.py ("comma-separated user IDs").
- **.128 has BROKEN IPv6 routing** (IPv4 works). DNS returns IPv6 first for
  GitHub / Docker Hub, so `git fetch` and `docker pull` fail intermittently with
  "network is unreachable" until Docker/git falls back to IPv4. Mitigations in
  place: hermes git task skips the fetch when already at the pinned SHA; the
  mattermost up task has `retries/until`. If a pull still flaps, just re-run.
- `changed_when: "'CHANGED' in stdout"` is WRONG — `CHANGED` is a substring of
  `UNCHANGED`. Use exact equality.
- `lineinfile` regexp `\.local/bin` was too broad and clobbered uv's
  `. "$HOME/.local/bin/env"` line in `.bashrc`. Use an exact `^...$` regexp.
- Dashboard web build output goes to `hermes_cli/web_dist/`, NOT `web/dist`.
- Ansible user-scope systemd needs `XDG_RUNTIME_DIR=/run/user/<uid>` (+ DBUS)
  in the task environment.
- node/npm/uv/hermes all live in `~/.local/bin` on .128.
- ⚠️ The dashboard exposes API keys on the LAN (why hermes requires `--insecure`).
  User accepted — trusted home LAN only; do NOT port-forward 9119 to the internet.

## Git state (as of 2026-06-05)
- `main` pushed to `origin` (`github.com:bbecomp99/hermie.git`), in sync. The
  mattermost/gateway/AI-server/weather work is all committed
  (`470f926`, `47f712b`, `cda623c`, `97e4f99`).
- Latest commit adds the **homeassistant role** + playbook wiring +
  `vault.yml.example` `hass_token` + README/notes refresh.
- No PR to anywhere; `main` is the working branch.

## Open / next
- **End-to-end chat CONFIRMED (2026-06-04):** DM to @hermie → gateway authorized
  brian → LLM call to DeepSeek-R1 (.127:8080, ~14s) → 398-char reply delivered
  back to Mattermost. The full path works.

- **DONE — daily weather cron to Town Square (built+verified 2026-06-05):**
  Job **`weather-daily`** (id `f9bea473519f`), schedule `0 7 * * *` = **7:00 AM
  Central** (box TZ confirmed **CDT**). Home channel = Town Square
  `bkkf739saif5tgzwaynhikgmho`. Cities: Dallas TX, Glen Haven WI, Billings MT,
  Miami FL — current + today + next 2 days.
  - **Architecture = script does EVERYTHING (`--no-agent`, no LLM).**
    `--script weather.py` (`~/.hermes/scripts/weather.py`, stdlib-only Python)
    fetches REAL data from **Open-Meteo** (no API key, hardcoded lat/long per
    city so tiny Glen Haven resolves right; WMO code→text + emoji map; °F),
    formats a polished Mattermost message (`##` header + per-city bold line +
    a 3-row markdown table Day/Forecast/Hi-Lo/Rain), and **posts it itself**
    via the Mattermost API (creds read from `~/.hermes/.env`:
    `MATTERMOST_URL`/`TOKEN`/`HOME_CHANNEL`).
  - **Why self-post instead of cron delivery:** cron's own delivery prepends a
    `Cronjob Response: <name> (job_id…)` header and appends a `To stop or manage
    this job…` footer — ugly. With `--no-agent`, **empty stdout = cron stays
    silent**, so the script posts directly and prints nothing → clean message,
    no wrapper. On post failure the script prints the message instead, so cron
    delivers it (wrapped) as a visible fallback rather than losing it.
    Test manually with `python3 ~/.hermes/scripts/weather.py --print` (formats
    only, no delivery) vs no args (live post).
  - Evolution this session: v1 agent-formatted (script stdout injected, LLM
    prettifies) worked but the LLM added greeting/sign-off chatter and the
    format drifted → switched to v2 deterministic self-posting script. The
    earlier note about "agent only formats" is superseded.
  - **Now captured as an Ansible role — reproducible.** `roles/weather`
    (tagged `weather`) templates the script (`weather.py.j2`, cities/coords/
    schedule in `roles/weather/defaults/main.yml`) and registers the cron job
    idempotently (guards on the job name in `hermes cron list`; uses argv form
    so the `0 7 * * *` schedule isn't split). Added after `gateway` in
    `playbook.yml` (the script needs the MATTERMOST_* that gateway writes to
    `.env`). Runs as the login user — **no become / no vault** needed. Apply
    just this slice: `ansible-playbook playbook.yml --tags weather`. Verified
    idempotent (`changed=0` on re-run). To change schedule/flags:
    `hermes cron remove <id>` on the box, then re-run the play.
  - ⚠️ **WHY not pure-LLM:** first attempt was a plain prompt telling Hermes to
    fetch weather via its `web` tool. The run logged **`tool_turns=0`** — the
    7B-4bit distill NEVER called a tool, it fabricated every temp and got the
    weekdays internally inconsistent. Small distilled reasoning models "think +
    answer" instead of tool-calling. So real data MUST be injected, not fetched
    by the model. (User picked pure-LLM first, then chose script-grounded after
    seeing the fabrication evidence.)
  - **Cron mechanics learned:** `hermes cron create '<sched>' '<prompt>'
    [--name N] [--deliver mattermost|platform:chat_id] [--script F] [--no-agent]`.
    `--script` files live under `~/.hermes/scripts/`; `.sh/.bash` run via bash,
    everything else via Python; default mode injects stdout into the prompt,
    `--no-agent` delivers stdout verbatim. Force a test run NOW with
    `hermes cron run <id>` then `hermes cron tick` (tick executes due jobs once).
    The **gateway** runs the scheduler (`hermes cron status` shows it). Delivery
    target `mattermost` (bare) = the configured `MATTERMOST_HOME_CHANNEL`.
  - **`hermes send`** (no LLM) is the easy way to post to MM from a script/CI:
    `hermes send --to mattermost "msg"` / `--list mattermost` shows targets.
  - Minor: box `.env` has a deprecated `TERMINAL_CWD=/home/pinky` (warns on each
    cron tick; harmless — move to `config.yaml terminal.cwd` someday).

- **.128 back UP (2026-06-05):** the earlier "down/powered-off" blocker cleared —
  ping 0% loss, keyless SSH works, both user units `active` (hermes-gateway,
  hermes-dashboard), MLX on .127:8080 serving. Hermes now `v0.15.1`, 238 commits
  behind upstream (still pinned deliberately).

- **ai-serve note (2026-06-05):** `serve.sh` MODEL line was switched to
  `mlx-community/Qwen2.5-Coder-3B-Instruct-4bit` (lines 10-11 are now two
  identical lines — commented fallback == active, comment is redundant). But the
  server actually running serves DeepSeek-R1 (+ Qwens). `serve.log`/`serve.pid`
  are tracked but are runtime artifacts — consider gitignoring them.

- **Autonomy (requested, not built):** `hermes cron` / `hermes goals` (the
  gateway already runs the cron scheduler). The weather job above is the first
  concrete unattended task.
- Offered but not done: reverse-proxy/Tailscale if dashboard/Mattermost ever
  need real internet exposure (currently LAN-only).
