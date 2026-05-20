# gorilla-thanos-multinode

Simplified 4-node ASAP demo: Gorilla-compressed TSDB pipeline with Thanos query engine + MinIO storage. No gorilla-gateway — agents write blocks directly to MinIO at the configurable agent-emit interval (`tsdb_block_duration`, default 60s). **gorilla-buffer-merger** on the backend node merges the per-emit blocks of each **tumbling window** (configurable `-window`, default 1h — the granularity at which merged state is flushed to backend S3/MinIO) into a single merged block (one per window); the **hot-store** (`gorilla-buffer-store`) serves the recent merged windows via Thanos StoreAPI, cutting per-window query fan-out from ~`window/emit` blocks (~60 at the defaults) to 1. The **archive-store** (`thanos-store-gateway`) serves the full history straight from MinIO.

## Store roles (naming)

The two Thanos StoreAPI endpoints are distinguished by their **role** (the upstream `thanos-` / `gorilla-` names are kept for the container/service names, but think of them by role):

| Role | Service | Reads from | Serves |
|------|---------|-----------|--------|
| **archive-store** | `thanos-store-gateway` :10901 | MinIO (S3) — full bucket | The complete history of all blocks |
| **hot-store** | `gorilla-buffer-store` :10921 | local FILESYSTEM `/merged/` | The recent merged tumbling window(s) only |

A **Thanos store-gateway** is the standard Thanos sidecar that exposes the blocks in an object-storage bucket over the StoreAPI gRPC interface so Thanos Query can read them; here it is the **archive-store** that serves everything in MinIO.

## Topology

| Node | IP | Role |
|------|----|------|
| node0 | 10.10.1.1 | Producers + agent-a (gorilla-only pipeline) |
| node1 | 10.10.1.2 | Idle (no services) |
| node2 | 10.10.1.3 | MinIO + Thanos (store-gateway, query, compact) + **gorilla-buffer-merger** + **gorilla-buffer-store** |
| node3 | 10.10.1.4 | Producers + agent-b (gorilla-only pipeline) |

## Data Flow

```
node0                                              node2
┌──────────────────────┐                           ┌───────────────────────────────────────────────────────┐
│ fake-exporter ×N     │                           │ MinIO (port 9000)                                     │
│   │  OTLP gRPC       │    S3 PUT (60s blocks)    │   asap-gorilla-tsdb/                                  │
│   ▼  port 4317       │──────────────────────────►│     <ULID>/chunks/                                    │
│ asap-agent-a         │       minio:9000           │     <ULID>/index                                      │
│   [gorillas3         │    (no gateway hop)        │     <ULID>/meta.json                                  │
│     tsdb_bucket:     │                            │         ...×N (one dir per 60s flush)                 │
│     asap-gorilla-tsdb│                            │                                                       │
│     drop_original:   │                            │  ┌─────────────────────────────────────────────────┐ │
│     true]            │                            │  │ gorilla-buffer-merger                           │ │
└──────────────────────┘                            │  │  polls MinIO every 15s                          │ │
                                                    │  │  downloads new in-retention blocks → /staging/  │ │
node3                                               │  │  buckets blocks by tumbling window (def 1h)     │ │
┌──────────────────────┐   S3 PUT (60s blocks)      │  │  merges each window's blocks → 1 block/window   │ │
│ fake-exporter ×N     │──────────────────────────► │  │  writes /merged/{ULID}/ (one per window)        │ │
│   │ OTLP gRPC        │      minio:9000             │  └──────────────────────┬────────────────────────┘ │
│   ▼ port 4317        │                            │                          │ shared volume             │
│ asap-agent-b         │                            │  ┌───────────────────────▼────────────────────────┐ │
└──────────────────────┘                            │  │ hot-store (gorilla-buffer-store)  :10921       │ │
                                                    │  │  FILESYSTEM objstore → /merged/                │ │
                                                    │  │  sync every 20s (picks up new merged windows)  │ │
                                                    │  │  serves recent merged tumbling window(s)       │ │
                                                    │  └───────────────────────┬────────────────────────┘ │
                                                    │                          │ StoreAPI gRPC :10921       │
                                                    │  ┌───────────────────────────────────────────────┐  │
                                                    │  │ archive-store (thanos-store-gateway)  :10901  │  │
                                                    │  │  reads MinIO, sync every 30s                  │  │
                                                    │  │  serves ALL blocks (full history)             │  │
                                                    │  └───────────────────────┬───────────────────────┘  │
                                                    │                          │ StoreAPI gRPC :10901       │
                                                    │  ┌───────────────────────▼───────────────────────┐  │
                                                    │  │ Thanos query  :10903                          │  │
                                                    │  │  --endpoint=…store-gateway:10901  (archive)   │  │
                                                    │  │  --endpoint=…buffer-store:10921   (hot)       │  │
                                                    │  │  fan-out to BOTH, merge chunks, run PromQL    │  │
                                                    │  └───────────────────────────────────────────────┘  │
                                                    └───────────────────────────────────────────────────────┘
```

## Where are the merged blocks stored, and in what format?

**Location.** In this demo the merged blocks are written to the **backend node's local disk**
(`/var/gorilla-buffer/merged/` inside the container, bind-mounted from `/tmp/gorilla-buffer/merged`
on node2). The merger reuses the on-disk staging area as a local buffer; it does **not** write a
new file-management system of its own. The **design intent** is for these merged blocks to live in
**MinIO / S3** (so the hot-store reads them over object storage like the archive-store does), but
that requires AWS credits / an extra object-storage hop, so the demo keeps them on local disk for
now. The 60s source blocks already live in MinIO; the merged windows are a local-disk derivative.

**Format.** Each merged block is a standard **Prometheus TSDB block** (the same on-disk layout the
agents write to MinIO and that `gorilla-buffer-store` loads via the FILESYSTEM objstore):

```
/var/gorilla-buffer/merged/<ULID>/
├── chunks/000001   Gorilla XOR / delta-of-delta encoded sample data
├── index           series → label index + chunk references
└── meta.json       ULID, MinTime/MaxTime, numSeries/numChunks, compaction level
```

Example `meta.json` for a merged window block:

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

## Tumbling-window merge model

The merger uses a **tumbling window** (`-window`, configurable; default 1h): fixed, non-overlapping
windows, not a sliding/rolling one. The window is the granularity at which merged state is flushed
to the backend S3/MinIO archive; the agent's per-block emit interval (`tsdb_block_duration`, default
60s) is independently configurable.
A block belongs to window `w = floor(block.minTime / window)`, covering `[w*window, (w+1)*window)`.
All per-emit blocks of a window merge into **one** output block keyed to that window. While a window
is still in progress it is re-merged each poll as new blocks land; once it is complete
(`now ≥ window_end + grace`) it is finalized and never re-merged. The output dir keeps the last N
windows (N = `ceil(BUFFER_STORE_DURATION / window)`, min 2 so a boundary-straddling query is always
covered) and drops older windows once `thanos-store-gateway` has synced them from MinIO.

| Property | No-merge (old) | Tumbling-window merge (current) |
|----------|---------------|---------------------------------|
| hot-store reads from | MinIO S3 directly | Local filesystem (FILESYSTEM objstore) |
| Blocks served per window | Up to ~`window/emit` blocks (~60 at defaults) | 1 merged block per window |
| Block expiry | `--min-time=-BUFFER_STORE_DURATION` flag | merger drops windows older than retention |
| Windows | n/a | fixed `[0,w)`, `[w,2w)`, … (no overlap; `w`=`-window`, default 1h) |
| Memory / IO at query time | Fan-out across all hot blocks | One block per window touched |

```
(example at default config: -window=1h, tsdb_block_duration=60s → 60 blocks/window)
window 0 = [0h, 1h)        window 1 = [1h, 2h)        window 2 = [2h, 3h)
b1 b2 … b60 (minTime<1h)   b61 … b120 (minTime<2h)    b121 … (minTime<3h)
        │                          │                          │
        ▼                          ▼                          ▼
   merged M0                  merged M1                  merged M2
 [b1+…+b60]                 [b61+…+b120]               [b121+…]
```

`[b1..b60] → merged`, then a NEW window `[b61..b120] → merged`, etc. M0 and M1 are distinct,
non-overlapping outputs — not a single block that keeps getting rewritten. The block count per
window scales with `window / tsdb_block_duration`; both are configurable (the diagram uses the
1h / 60s defaults).

For the hot range, Thanos query receives data from both the hot-store (`gorilla-buffer-store`, the
merged window blocks) and the archive-store (`thanos-store-gateway`, the same data from MinIO).
Identical timestamps collapse via Thanos's chunk-level merge — no replica-label config needed.

## Multi-store fan-out: how one query spans both stores

A single PromQL query that spans both stores (e.g. `quantile_over_time(...[2h])` where the last
~1h of merged data is on the hot-store and the older ~1h is only on the archive-store) is served
like this:

1. **Fan-out.** Thanos Query sends the same Series request (matchers + `[mint, maxt]`) to **both**
   stores **in parallel** over the StoreAPI.
2. **Each store returns what it has.** The archive-store returns the older chunks it loaded from
   MinIO; the hot-store returns the recent merged-window chunks it loaded from local disk. For the
   overlapping ~1h both stores return chunks for the same series.
3. **Thanos merges chunks.** For each series, Thanos merges the chunk streams from both stores;
   chunks with **identical timestamps dedup at the chunk level** (the overlap collapses to one copy).
4. **PromQL on the combined result.** Thanos then runs the PromQL engine
   (`quantile_over_time`, `rate`, …) over the **single combined, deduplicated** series — so a 2h
   query split across the two stores returns exactly what a single store holding all 2h would.

## Configuration

`BUFFER_STORE_DURATION` (default `1h`) controls the retention horizon: how far back merged windows
are kept queryable on the hot-store. The tumbling window size itself is the merger's `-window` flag
(also `1h` by default). Set in `topology.env`:

```bash
BUFFER_STORE_DURATION=30m  # keep ~30 minutes of merged windows hot
BUFFER_STORE_DURATION=2h   # keep ~2 hours of merged windows hot
```

## Quick Start

### Single-node (local testing with docker-compose)

**Prerequisite — build the gorilla-buffer-merger image:**
```bash
cd /mydata/ASAPCollector
DOCKER_BUILDKIT=1 docker build \
  -f deploy/docker/Dockerfile.gorilla-buffer-merger \
  -t asap/gorilla-buffer-merger:dev .
```

**Start the stack:**
```bash
cd deploy/gorilla-thanos-multinode/docker-compose

# Default 1h buffer window:
docker compose -f gorilla-thanos.yml up -d

# Custom buffer window:
BUFFER_STORE_DURATION=30m docker compose -f gorilla-thanos.yml up -d

# Wait ~60s for first TSDB block flush, then another ~15s for first merge, then:
curl 'http://localhost:19092/api/v1/label/__name__/values'   # metric names via Thanos
curl http://localhost:18890/metrics | grep gorilla            # agent self-metrics

# MinIO console: http://localhost:19001 (user: asap, pass: asap-local-only)

# Teardown:
docker compose -f gorilla-thanos.yml down -v
```

### Multi-node (4-node CloudLab)

**Prerequisite — rebuild images on node0 (or whichever node runs the build):**
```bash
cd /mydata/ASAPCollector

# 1. OTel agent image (gorillas3 Phase 3, removes `bucket:` field):
bash restore_otel_collector_contrib_patches.sh
bash build_asap_otel.sh

# 2. gorilla-buffer-merger image (tumbling-window merger):
DOCKER_BUILDKIT=1 docker build \
  -f deploy/docker/Dockerfile.gorilla-buffer-merger \
  -t asap/gorilla-buffer-merger:dev .

# Distribute images to all nodes that need them:
# asap/asap-otel:dev → node0, node3
# asap/gorilla-buffer-merger:dev → node2
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
# wait ~90s: 60s block flush + 15s merger poll + 20s thanos store sync
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
| 3 — Buffer-store serving | check merger + buffer-store running + data visible | — | re-verify after merge refactor |
| 4 — Advanced PromQL | `verif_part4.py` | 25/25 | verified pre-refactor |

Parts 1, 2, 4 remain valid (MinIO → thanos-store-gateway path unchanged). Part 3 needs re-verification with the new merger-based architecture.

## Success Metrics

| Check | Command | Expected |
|-------|---------|----------|
| MinIO has TSDB blocks | `ssh node2 'docker exec asap-minio mc ls local/asap-gorilla-tsdb --recursive'` | ULID dirs visible ~60s after start |
| Merger staging populated | `ssh node2 'ls /tmp/gorilla-buffer/staging/'` | ULID dirs (downloaded 60s blocks) |
| Merged windows written | `ssh node2 'ls /tmp/gorilla-buffer/merged/'` | One ULID dir **per retained tumbling window** (not a single rewritten block) |
| Merger logs healthy | `ssh node2 'docker logs asap-gorilla-buffer-merger 2>&1 \| tail -5'` | `merged window block written` lines (with `window=` index) |
| Thanos query healthy | `curl http://10.10.1.3:10903/api/v1/query?query=up` | `status: success` |
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
│   ├── agent-gorilla-only.yaml        (OTel agent: gorillas3→minio:9000, Phase 3 format)
│   ├── thanos-objstore.yaml           (Thanos S3 config → MinIO asap-gorilla-tsdb)
│   ├── buffer-objstore.yaml           (legacy S3 config — no longer used by buffer-store)
│   └── buffer-fs-objstore.yaml        (Thanos FILESYSTEM objstore → /var/gorilla-buffer/merged)
├── docker-compose/
│   └── gorilla-thanos.yml             (single-node compose for local testing)
└── scripts/
    ├── run_demo.sh                    (multinode orchestrator: backend+merger+buffer-store → agents)
    ├── verif_part4.py                 (PromQL verification: rate/avg/quantile)
    ├── verify_buffer_store.sh         (buffer-store checks — needs update for merger arch)
    └── verify_gorilla_compression.sh  (pipeline smoke-test checks)
```

The `gorilla-buffer-merger` binary lives in the ASAPCollector repo at:
```
asap-gorilla-go/
└── cmd/
    └── gorilla-buffer-merger/
        └── main.go                    (tumbling-window TSDB merger: MinIO→staging→merged/window)
deploy/docker/
└── Dockerfile.gorilla-buffer-merger   (multi-stage Go build → distroless image)
```

## Design history

| Iteration | Buffer-store reads from | Blocks served | Notes |
|-----------|------------------------|---------------|-------|
| v1 — gorilla-gateway era | Local disk (gorilla-gateway buffer) | 1 merged block | gateway on node1 wrote merged block |
| v2 — no-merge (2026-05-18) | MinIO S3 directly | Up to ~60 individual 60s blocks | `--min-time` filter, chunk-level Thanos dedup |
| v3 — sliding-window merge (2026-05-19) | Local FILESYSTEM (merged by gorilla-buffer-merger) | 1 merged block | custom merger binary, single rolling merged block |
| v4 — tumbling-window merge (2026-05-20) | Local FILESYSTEM (merged by gorilla-buffer-merger) | 1 merged block per tumbling window (`-window`, default 1h) | fixed-size non-overlapping windows; finalized windows frozen |
