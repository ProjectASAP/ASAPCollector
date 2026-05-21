# gorilla-thanos-multinode

Simplified 4-node ASAP demo: Gorilla-compressed TSDB pipeline with Thanos query engine + MinIO storage. No gorilla-gateway — agents build per-emit Gorilla TSDB blocks at the configurable agent-emit interval (`tsdb_block_duration`, default 60s) and **POST each block over HTTP** to the backend merger's ingest endpoint (`gorilla-head-merger:9099/ingest`, set via the `gorillas3` `ship_endpoint` key); agents no longer write to MinIO/S3 at all. **gorilla-head-merger** on the backend node durably writes each received per-emit block (atomic temp+fsync+rename) into a single **served dir** (`/var/gorilla-buffer/served`) — that dir IS a block-level WAL, so crash recovery just re-reads it. The served dir holds both the CURRENT (open) tumbling window's per-emit blocks (the **head** — fresh, immediately queryable, Compaction.Level 1) and one CUT block per completed window (Compaction.Level 2). On window close (now ≥ window_end + grace) the merger **cuts the window once** — concatenating the per-series Gorilla chunks of that window's per-emit blocks into one block (copying chunk bytes, no sample decode/re-encode), uploading it to MinIO/S3, then pruning the window's per-emit blocks; the tumbling window (`-window` / env `MERGE_WINDOW`, default 1h) is the granularity at which the head is cut and flushed to S3. The **hot-store** (`gorilla-buffer-store`) reads the served dir via a FILESYSTEM objstore — serving both the fresh current-window per-emit blocks and the recent cut blocks. The **archive-store** (`thanos-store-gateway`) serves the full history from MinIO, which now holds only cut blocks (~`window/emit` fewer PUTs than direct per-emit writes, ~60× fewer at the 1h/60s defaults). **ASAPQuery-backend** also runs on the backend node as the ASAP-facing PromQL frontend and forwards Gorilla/archive queries to `thanos-query` via `ASAP_THANOS_QUERY_URL=http://thanos-query:10903`.

## Store roles (naming)

The two Thanos StoreAPI endpoints are distinguished by their **role** (the upstream `thanos-` / `gorilla-` names are kept for the container/service names, but think of them by role):

| Role | Service | Reads from | Serves |
|------|---------|-----------|--------|
| **archive-store** | `thanos-store-gateway` :10901 | MinIO (S3) — full bucket (cut blocks only) | The complete history of all cut window blocks |
| **hot-store** | `gorilla-buffer-store` :10921 | local FILESYSTEM `/served/` | The current window's per-emit head blocks + the recent cut blocks |

A **Thanos store-gateway** is the standard Thanos sidecar that exposes the blocks in an object-storage bucket over the StoreAPI gRPC interface so Thanos Query can read them; here it is the **archive-store** that serves everything in MinIO (now only the cut window blocks).

## Topology

| Node | IP | Role |
|------|----|------|
| node0 | 10.10.1.1 | Producers + agent-a (gorilla-only pipeline) |
| node1 | 10.10.1.2 | Idle (no services) |
| node2 | 10.10.1.3 | **ASAPQuery-backend** + MinIO + Thanos (store-gateway, query, compact) + **gorilla-head-merger** + **gorilla-buffer-store** |
| node3 | 10.10.1.4 | Producers + agent-b (gorilla-only pipeline) |

## Data Flow

```
node0                                              node2
┌──────────────────────┐                           ┌───────────────────────────────────────────────────────┐
│ fake-exporter ×N     │                           │  ┌─────────────────────────────────────────────────┐ │
│   │  OTLP gRPC       │  HTTP POST (60s blocks)    │  │ gorilla-head-merger                             │ │
│   ▼  port 4317       │──────────────────────────► │  │  ingest :9099/ingest (HTTP receive, no S3 GET)  │ │
│ asap-agent-a         │ gorilla-head-merger:9099   │  │  durable write (temp+fsync+rename) → /served/   │ │
│   [gorillas3         │      /ingest               │  │    /served/ IS the block-level WAL (re-read on  │ │
│     ship_endpoint:   │                            │  │    crash; no MinIO poll, no staging/merged dir) │ │
│     gorilla-head-    │                            │  │  /served/ holds the CURRENT window's per-emit   │ │
│       merger:9099    │                            │  │    head blocks (Level 1) + 1 cut block/window   │ │
│     drop_original:   │                            │  │    (Level 2)                                    │ │
│     true]            │                            │  │  on window close (≥ window_end+grace): CUT once │ │
└──────────────────────┘                            │  │    — concat per-series Gorilla chunks → 1 block │ │
                                                    │  │    (copy bytes, no decode), PUT to MinIO,       │ │
node3                                               │  │    then prune that window's per-emit blocks     │ │
┌──────────────────────┐  HTTP POST (60s blocks)    │  └───────┬──────────────────────────┬──────────────┘ │
│ fake-exporter ×N     │──────────────────────────► │          │ shared volume /served/   │ S3 PUT (cut)     │
│   │ OTLP gRPC        │ gorilla-head-merger:9099   │          │                          ▼                  │
│   ▼ port 4317        │      /ingest               │          │             ┌──────────────────────────────┐ │
│ asap-agent-b         │                            │          │             │ MinIO (port 9000)            │ │
└──────────────────────┘                            │          │             │   asap-gorilla-tsdb/         │ │
                                                    │          │             │     <ULID>/chunks/           │ │
                                                    │          │             │     <ULID>/index             │ │
                                                    │          │             │     <ULID>/meta.json         │ │
                                                    │          │             │   (cut blocks only,          │ │
                                                    │          │             │    one dir per window)       │ │
                                                    │          │             └──────────────┬───────────────┘ │
                                                    │  ┌───────▼─────────────────────────┐  │ S3 sync 30s      │
                                                    │  │ hot-store (gorilla-buffer-store) │  │                  │
                                                    │  │   :10921                         │  │                  │
                                                    │  │  FILESYSTEM objstore → /served/  │  │                  │
                                                    │  │  serves fresh current-window     │  │                  │
                                                    │  │    per-emit head + recent cut    │  │                  │
                                                    │  └───────────────┬──────────────────┘  │                  │
                                                    │                  │ StoreAPI :10921      │                  │
                                                    │  ┌───────────────────────────────────▼┐ │                 │
                                                    │  │ archive-store (thanos-store-gateway)│ │                 │
                                                    │  │   :10901                            │ │                 │
                                                    │  │  reads MinIO (cut blocks), sync 30s │ │                 │
                                                    │  │  serves full cut-block history      │ │                 │
                                                    │  └───────────────┬─────────────────────┘ │                 │
                                                    │                  │ StoreAPI gRPC :10901   │                 │
                                                    │  ┌───────────────▼───────────────────┐  │                  │
                                                    │  │ Thanos query  :10903               │  │                  │
                                                    │  │  --endpoint=…store-gateway:10901   │  │                  │
                                                    │  │      (archive, cut blocks)         │  │                  │
                                                    │  │  --endpoint=…buffer-store:10921    │  │                  │
                                                    │  │      (hot, head + cut)             │  │                  │
                                                    │  │  fan-out to BOTH, dedup chunks,    │  │                  │
                                                    │  │  run PromQL                        │  │                  │
                                                    │  └────────────────────────────────────┘ │                  │
                                                    └───────────────────────────────────────────────────────┘
```

## Where are the blocks stored, and in what format?

**Location.** Agents POST every per-emit block over HTTP to the merger; the merger durably writes
each one (atomic temp+fsync+rename) into a single **served dir** on the **backend node's local disk**
(`/var/gorilla-buffer/served/` inside the container, bind-mounted from `/tmp/gorilla-buffer/served`
on node2). That served dir IS the block-level WAL — there is no separate staging/merged dir and no
MinIO polling; crash recovery just re-reads it. The served dir holds **both**: (a) the CURRENT (open)
tumbling window's per-emit head blocks (Compaction.Level 1 — fresh, immediately queryable), and
(b) one CUT block per completed window (Compaction.Level 2). On window close the merger cuts the
window once and uploads that single cut block to **MinIO / S3**; S3 therefore holds **only cut
blocks** (one per window), not per-emit blocks. Per-emit head blocks vs cut blocks are distinguished
by their `compaction.level` (1 vs 2).

**Format.** Both the per-emit head blocks and the cut window blocks are standard **Prometheus TSDB
blocks** (the same on-disk layout `gorilla-buffer-store` loads via the FILESYSTEM objstore and that
`thanos-store-gateway` loads from MinIO):

```
/var/gorilla-buffer/served/<ULID>/
├── chunks/000001   Gorilla XOR / delta-of-delta encoded sample data
├── index           series → label index + chunk references
└── meta.json       ULID, MinTime/MaxTime, numSeries/numChunks, compaction level (1=per-emit head, 2=cut)
```

A cut block is built by **concatenating the per-series Gorilla chunks** of the window's per-emit
blocks — copying chunk bytes, with no sample decode/re-encode — so it is byte-for-byte the same
Gorilla-compressed data.

Example `meta.json` for a cut window block (Compaction.Level 2):

```json
{
  "ulid": "01KRGYN9DW0JMDDF11YZV0FRV1",
  "minTime": 1778685486472,
  "maxTime": 1778685487480,
  "stats": { "numSamples": 4006, "numSeries": 2003, "numChunks": 2003 },
  "compaction": { "level": 2, "sources": ["01KRGYN9DW0JMDDF11YZV0FRV1"] },
  "version": 1
}
```

No custom container format is used — it is the Prometheus TSDB block format, so any Thanos/Prometheus
reader can open it.

## Head-block + cut-at-close model

The merger uses a **tumbling window** (`-window` / env `MERGE_WINDOW`, configurable; default 1h):
fixed, non-overlapping windows, not a sliding/rolling one. The window is the granularity at which
the **head** is cut and flushed to the backend S3/MinIO archive; the agent's per-block emit interval
(`tsdb_block_duration`, default 60s) is independently configurable.
A block belongs to window `w = floor(block.minTime / window)`, covering `[w*window, (w+1)*window)`.
While a window is open, its per-emit blocks accumulate in the served dir as the **head** — fresh,
immediately queryable, Compaction.Level 1 — they are NOT re-merged on a poll. Once the window is
complete (`now ≥ window_end + grace`) the merger **cuts it once**: it concatenates the per-series
Gorilla chunks of that window's per-emit blocks into **one** cut block (Compaction.Level 2, copying
chunk bytes — no sample decode/re-encode), uploads that single block to MinIO/S3, then prunes
(deletes) the window's per-emit blocks from the served dir. A late-arriving block re-cuts the window
(folding in the prior cut block, so no data is lost). The served dir keeps the recent cut blocks for
`BUFFER_STORE_DURATION` so a boundary-straddling query stays covered; older cut blocks live in MinIO
and are served by `thanos-store-gateway`.

| Property | No-merge (old v2) | Head-block + cut-at-close (current v5) |
|----------|---------------|---------------------------------|
| Agent ships to | MinIO S3 directly (per-emit PUT) | merger over HTTP (`:9099/ingest`); agents never touch S3 |
| hot-store reads from | MinIO S3 directly | Local filesystem served dir (FILESYSTEM objstore) |
| Blocks served per open window | Up to ~`window/emit` blocks (~60 at defaults) | the per-emit head blocks (fresh) + 1 cut block once closed |
| S3 PUTs | one per per-emit block | one cut block per window (~`window/emit` fewer, ~60× at defaults) |
| Durability / recovery | n/a (S3 is the record) | served dir IS the block-level WAL (re-read on crash) |
| Windows | n/a | fixed `[0,w)`, `[w,2w)`, … (no overlap; `w`=`-window`, default 1h) |

```
(example at default config: -window=1h, tsdb_block_duration=60s → 60 per-emit blocks/window)
window 0 = [0h, 1h)        window 1 = [1h, 2h)        window 2 = [2h, 3h)
b1 b2 … b60 (minTime<1h)   b61 … b120 (minTime<2h)    b121 … (minTime<3h)   ← head (Level 1, in served/)
        │ cut on close             │ cut on close             │ (still open)
        ▼                          ▼
   cut block C0               cut block C1
 [b1+…+b60]                 [b61+…+b120]               (b121… still accumulating as head)
   → PUT MinIO                 → PUT MinIO
   prune b1..b60              prune b61..b120
```

`[b1..b60] → cut C0` (then b1..b60 pruned from served/), then a NEW window `[b61..b120] → cut C1`,
etc. C0 and C1 are distinct, non-overlapping cut blocks — each cut happens **once** per window, not
a single block that keeps getting rewritten on every poll. The per-emit block count per window scales
with `window / tsdb_block_duration`; both are configurable (the diagram uses the 1h / 60s defaults).

For the hot range, Thanos query receives data from both the hot-store (`gorilla-buffer-store`, the
served dir's per-emit head + recent cut blocks) and the archive-store (`thanos-store-gateway`, the
same cut blocks from MinIO). Identical timestamps collapse via Thanos's chunk-level merge — no
replica-label config needed.

## Multi-store fan-out: how one query spans both stores

A single PromQL query that spans both stores (e.g. `quantile_over_time(...[2h])` where the most
recent data is on the hot-store and the older cut blocks are only on the archive-store) is served
like this:

1. **Fan-out.** Thanos Query sends the same Series request (matchers + `[mint, maxt]`) to **both**
   stores **in parallel** over the StoreAPI.
2. **Each store returns what it has.** The archive-store returns the older cut-block chunks it loaded
   from MinIO; the hot-store returns the current-window per-emit head chunks and the recent cut-block
   chunks it loaded from the local served dir. For the overlapping range both stores return chunks
   for the same series.
3. **Thanos merges chunks.** For each series, Thanos merges the chunk streams from both stores;
   chunks with **identical timestamps dedup at the chunk level** (the overlap collapses to one copy).
4. **PromQL on the combined result.** Thanos then runs the PromQL engine
   (`quantile_over_time`, `rate`, …) over the **single combined, deduplicated** series — so a 2h
   query split across the two stores returns exactly what a single store holding all 2h would.

## Query entrypoints

Node2 exposes two PromQL query surfaces:

| Entrypoint | Port | Purpose |
|------------|------|---------|
| `asap-backend` | `:9091` | ASAPQuery-backend frontend for ASAP/Grafana clients. |
| `thanos-query` | `:10903` | Direct Thanos endpoint and the upstream used by `asap-backend`. |

`asap-backend` does not read Gorilla blocks directly. It forwards archive/Gorilla queries to
`thanos-query` through `ThanosQueryEngine` (`ASAP_THANOS_QUERY_URL=http://thanos-query:10903`).

### Backend bootstrap configs

Two backend config files are mounted into `asap-backend`:

| File | Purpose |
|------|---------|
| `configs/backend-storage-routing.yaml` | Routes queries to the archive slot by default (`default: gorilla_s3_archive`). With `ASAP_THANOS_QUERY_URL` set, that archive slot is served by `ThanosQueryEngine`, so unlisted metrics still go to Thanos. |
| `configs/backend-streaming.yaml` | Startup bootstrap required by the backend binary's `--streaming-config` flag. In this Gorilla-only stack it is not the source of truth for archive queryability; Thanos serves archive data from TSDB blocks. |

If a real ASAP warm tier is enabled later, these files should be expanded or emitted by the control
plane to describe warm-tier aggregations and warm-first/archive-fallback routing.

## Configuration

`BUFFER_STORE_DURATION` (default `1h`) controls the retention horizon: how far back cut blocks are
kept queryable on the hot-store's served dir. The tumbling window size itself is the merger's
`-window` flag (env `MERGE_WINDOW`, also `1h` by default) — the granularity at which the head is cut
and flushed to S3. Set in `topology.env`:

```bash
BUFFER_STORE_DURATION=30m  # keep ~30 minutes of cut blocks hot
BUFFER_STORE_DURATION=2h   # keep ~2 hours of cut blocks hot
```

### gorilla-head-merger flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-served-dir` | `/var/gorilla-buffer/served` | Single dir holding per-emit head blocks + cut blocks; IS the block-level WAL |
| `-ingest-addr` | `:9099` | HTTP listen address for the `/ingest` endpoint agents POST per-emit blocks to |
| `-cut-interval` | `15s` | How often the merger checks for closed windows to cut + flush |
| `-window` (env `MERGE_WINDOW`) | `1h` | Tumbling window size; the cut+flush-to-S3 granularity |
| `-retention` | — | How long cut blocks are kept in the served dir before pruning |
| `-bucket`, `-endpoint`, `-access-key`, `-secret-key` | — | MinIO/S3 target for cut-block uploads |

Removed in v5 (no longer accepted): `-staging-dir`, `-output-dir`, `-poll-interval` — there is no
MinIO poll and no separate staging/merged dir anymore.

## Quick Start

### Single-node (local testing with docker-compose)

**Prerequisite — build the gorilla-head-merger and ASAPQuery-backend images:**
```bash
cd /mydata/ASAPCollector
DOCKER_BUILDKIT=1 docker build \
  -f deploy/docker/Dockerfile.gorilla-head-merger \
  -t asap/gorilla-head-merger:dev .
DOCKER_BUILDKIT=1 docker build \
  -f deploy/docker/Dockerfile.backend \
  -t asap/query-backend:dev .
```

**Start the stack:**
```bash
cd deploy/gorilla-thanos-multinode/docker-compose

# Default 1h buffer window:
docker compose -f gorilla-thanos.yml up -d

# Custom buffer window:
BUFFER_STORE_DURATION=30m docker compose -f gorilla-thanos.yml up -d

# Wait ~60s for the first per-emit block to be POSTed to the merger and land in
# the served dir (head; queryable immediately — no window cut needed), then:
curl 'http://localhost:19092/api/v1/label/__name__/values'   # metric names via Thanos
curl 'http://localhost:19091/api/v1/query?query=up'           # ASAPQuery-backend → ThanosQueryEngine → Thanos
curl http://localhost:18890/metrics | grep gorilla            # agent self-metrics

# MinIO console: http://localhost:19001 (user: asap, pass: asap-local-only)

# Teardown:
docker compose -f gorilla-thanos.yml down -v
```

### Multi-node (4-node CloudLab)

**Prerequisite — rebuild images on node0 (or whichever node runs the build):**
```bash
cd /mydata/ASAPCollector

# 1. OTel agent image (gorillas3 ship-to-merger mode, POSTs blocks to ship_endpoint):
bash restore_otel_collector_contrib_patches.sh
bash build_asap_otel.sh

# 2. gorilla-head-merger image (HTTP ingest + block-level WAL + cut-at-close):
DOCKER_BUILDKIT=1 docker build \
  -f deploy/docker/Dockerfile.gorilla-head-merger \
  -t asap/gorilla-head-merger:dev .

# 3. ASAPQuery-backend image (PromQL frontend; forwards archive/Gorilla queries to Thanos):
DOCKER_BUILDKIT=1 docker build \
  -f deploy/docker/Dockerfile.backend \
  -t asap/query-backend:dev .

# Distribute images to all nodes that need them:
# asap/asap-otel:dev → node0, node3
# asap/gorilla-head-merger:dev → node2
# asap/query-backend:dev → node2
# (use docker save | ssh node2 docker load)
```

**Run the stack:**
```bash
cd /mydata/gorilla-thanos-multinode

# Full run (sync + up + soak + verify + down):
bash scripts/run_demo.sh all

# Or step by step:
bash scripts/run_demo.sh sync    # rsync configs to nodes 0, 2, 3
bash scripts/run_demo.sh up      # backend (node2) + agents (node0, node3)
# wait ~90s: 60s per-emit block POST to merger + 20s thanos store sync
bash scripts/run_demo.sh verify  # run success-metric checks
bash scripts/run_demo.sh down    # stop all containers
```

**Tune buffer window:**
```bash
BUFFER_STORE_DURATION=30m bash scripts/run_demo.sh all
```

## Verification

| Part | Script / method | Checks | Status |
|------|----------------|--------|--------|
| 1 — Pipeline smoke-test | `verify_gorilla_compression.sh` | 5/5 | verified pre-refactor |
| 2 — Exact-value test | hand-computed vs Thanos | 4/4 | verified pre-refactor |
| 3 — Buffer-store serving | check head-merger + buffer-store running + data visible | — | re-verify after head-block refactor |
| 4 — Advanced PromQL | `verif_part4.py` | 25/25 | verified pre-refactor |

Parts 1, 2, 4 remain valid (cut blocks in MinIO → thanos-store-gateway path unchanged). Part 3 needs re-verification with the new head-merger architecture (HTTP ingest + served dir).

## Success Metrics

| Check | Command | Expected |
|-------|---------|----------|
| MinIO has cut blocks | `ssh node2 'docker exec asap-minio mc ls local/asap-gorilla-tsdb --recursive'` | ULID dirs (one per cut window) visible after first window close |
| Served dir populated | `ssh node2 'ls /tmp/gorilla-buffer/served/'` | ULID dirs: per-emit head blocks (Level 1) ~60s after start, plus cut blocks (Level 2) once windows close |
| Cut blocks present | `ssh node2 'for d in /tmp/gorilla-buffer/served/*/; do grep -l "\"level\": 2" "$d"meta.json; done'` | One Level-2 cut block dir **per completed tumbling window** (not a single rewritten block) |
| Head-merger logs healthy | `ssh node2 'docker logs asap-gorilla-head-merger 2>&1 \| tail -5'` | `cut+flushed window` lines (with `window=` and `ulid=`) once a window closes; ingest lines before that |
| Thanos query healthy | `curl http://10.10.1.3:10903/api/v1/query?query=up` | `status: success` |
| ASAPQuery-backend healthy | `curl http://10.10.1.3:9091/api/v1/query?query=up` | `status: success`; `asap-backend` logs show `ThanosQueryEngine` registered |
| Metric names visible | `curl http://10.10.1.3:10903/api/v1/label/__name__/values` | Non-empty list ~90s after start |
| gorilla-buffer-store up | `ssh node2 'docker ps \| grep asap-gorilla-buffer-store'` | Container Up |
| buffer-store registered | `curl http://10.10.1.3:10903/api/v1/stores \| python3 -m json.tool \| grep 10921` | node2:10921 in store list |
| buffer-store serving data | `ssh node2 'docker logs asap-gorilla-buffer-store 2>&1 \| tail -10'` | Block sync messages, 1 block |
| No outbound gRPC | `ssh node0 'tcpdump -n "dst port 4317" -c 5'` | No packets (drop_original: true) |

## File Structure

```
gorilla-thanos-multinode/
├── README.md                          (this file)
├── topology.env                       (node IPs, BUFFER_STORE_DURATION, image set)
├── thanos-query-verif.md              (PromQL verification results)
├── configs/
│   ├── agent-gorilla-only.yaml        (OTel agent: gorillas3 ship_endpoint→gorilla-head-merger:9099/ingest)
│   ├── thanos-objstore.yaml           (Thanos S3 config → MinIO asap-gorilla-tsdb, cut blocks)
│   ├── buffer-objstore.yaml           (legacy S3 config — no longer used by buffer-store)
│   ├── buffer-fs-objstore.yaml        (Thanos FILESYSTEM objstore → /var/gorilla-buffer/served)
│   ├── backend-storage-routing.yaml   (ASAPQuery-backend default route → Thanos-backed archive slot)
│   └── backend-streaming.yaml         (ASAPQuery-backend startup bootstrap config)
├── docker-compose/
│   └── gorilla-thanos.yml             (single-node compose for local testing)
└── scripts/
    ├── run_demo.sh                    (multinode orchestrator: backend+head-merger+buffer-store → agents)
    ├── verif_part4.py                 (PromQL verification: rate/avg/quantile)
    ├── verify_buffer_store.sh         (buffer-store checks — needs update for head-merger arch)
    └── verify_gorilla_compression.sh  (pipeline smoke-test checks)
```

The `gorilla-head-merger` binary lives in the ASAPCollector repo at:
```
asap-gorilla-go/
└── cmd/
    └── gorilla-head-merger/
        └── main.go                    (HTTP ingest → served dir / block-level WAL; cut+flush 1 block/window to S3)
deploy/docker/
└── Dockerfile.gorilla-head-merger     (multi-stage Go build → distroless image)
```
