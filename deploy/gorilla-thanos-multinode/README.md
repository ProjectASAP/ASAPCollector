# gorilla-thanos-multinode

Simplified 4-node ASAP demo: Gorilla-compressed TSDB pipeline with Thanos query engine + MinIO storage. No gorilla-gateway — agents write 60s blocks directly to MinIO. **gorilla-buffer-merger** on the backend node continuously merges in-window blocks into a single local block (sliding 1h window); **gorilla-buffer-store** serves that one merged block via Thanos StoreAPI, cutting query fan-out from ~60 blocks to 1.

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
                                                    │  │  downloads new in-window blocks → /staging/     │ │
node3                                               │  │  drops blocks older than BUFFER_STORE_DURATION  │ │
┌──────────────────────┐   S3 PUT (60s blocks)      │  │  merges all staging blocks → one block          │ │
│ fake-exporter ×N     │──────────────────────────► │  │  writes to /merged/{ULID}/                      │ │
│   │ OTLP gRPC        │      minio:9000             │  └──────────────────────┬────────────────────────┘ │
│   ▼ port 4317        │                            │                          │ shared volume             │
│ asap-agent-b         │                            │  ┌───────────────────────▼────────────────────────┐ │
└──────────────────────┘                            │  │ gorilla-buffer-store  :10921                   │ │
                                                    │  │  FILESYSTEM objstore → /merged/                │ │
                                                    │  │  sync every 20s (picks up new merged block)    │ │
                                                    │  │  serves ONE merged block (≤ BUFFER_STORE_DURATION)│ │
                                                    │  └───────────────────────┬────────────────────────┘ │
                                                    │                          │ StoreAPI gRPC :10921       │
                                                    │  ┌───────────────────────────────────────────────┐  │
                                                    │  │ Thanos store-gateway  :10901                  │  │
                                                    │  │  reads MinIO, sync every 30s                  │  │
                                                    │  │  serves ALL blocks (full history)             │  │
                                                    │  └───────────────────────┬───────────────────────┘  │
                                                    │                          │ StoreAPI gRPC :10901       │
                                                    │  ┌───────────────────────▼───────────────────────┐  │
                                                    │  │ Thanos query  :10903                          │  │
                                                    │  │  --endpoint=thanos-store-gateway:10901        │  │
                                                    │  │  --endpoint=gorilla-buffer-store:10921        │  │
                                                    │  │  PromQL: /api/v1/query                        │  │
                                                    │  └───────────────────────────────────────────────┘  │
                                                    └───────────────────────────────────────────────────────┘
```

## Sliding-window merge model

The key design change from the previous no-merge design:

| Property | No-merge (old) | Sliding-window merge (current) |
|----------|---------------|-------------------------------|
| gorilla-buffer-store reads from | MinIO S3 directly | Local filesystem (FILESYSTEM objstore) |
| Blocks served per query | Up to 60 individual 60s blocks | 1 merged block |
| Block expiry | `--min-time=-BUFFER_STORE_DURATION` flag | gorilla-buffer-merger drops expired staging blocks |
| Data freshness | ~75s (60s block + 15s sync) | ~95s (60s block + 15s merge poll + 20s store sync) |
| Memory / IO at query time | Fan-out across all hot blocks | Single block read |

```
time →

t=0    staging: [b1]                    merged: [b1]
t=1    staging: [b1][b2]                merged: [b1+b2]
t=2    staging: [b1][b2][b3]            merged: [b1+b2+b3]
...
t=60   staging: [b1]...[b60]            merged: [b1+...+b60]   ← full 1h window
t=61   staging: [b2]...[b61]            merged: [b2+...+b61]   ← b1 expired, b61 added
t=62   staging: [b3]...[b62]            merged: [b3+...+b62]   ← b2 expired, b62 added
```

For the hot window, Thanos query receives data from both gorilla-buffer-store (the merged local block) and thanos-store-gateway (the same data from MinIO). Identical timestamps collapse via Thanos's chunk-level merge — no replica-label config needed.

## Configuration

`BUFFER_STORE_DURATION` (default `1h`) controls the sliding window size. Set in `topology.env`:

```bash
BUFFER_STORE_DURATION=30m  # 30-minute hot window
BUFFER_STORE_DURATION=2h   # 2-hour hot window
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

# 2. gorilla-buffer-merger image (new sliding-window merger):
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
| Merged block written | `ssh node2 'ls /tmp/gorilla-buffer/merged/'` | One ULID dir (merged block) |
| Merger logs healthy | `ssh node2 'docker logs asap-gorilla-buffer-merger 2>&1 \| tail -5'` | `merged block written` lines |
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
        └── main.go                    (sliding-window TSDB merger: MinIO→staging→merged)
deploy/docker/
└── Dockerfile.gorilla-buffer-merger   (multi-stage Go build → distroless image)
```

## Design history

| Iteration | Buffer-store reads from | Blocks served | Notes |
|-----------|------------------------|---------------|-------|
| v1 — gorilla-gateway era | Local disk (gorilla-gateway buffer) | 1 merged block | gateway on node1 wrote merged block |
| v2 — no-merge (2026-05-18) | MinIO S3 directly | Up to ~60 individual 60s blocks | `--min-time` filter, chunk-level Thanos dedup |
| v3 — sliding-window merge (2026-05-19) | Local FILESYSTEM (merged by gorilla-buffer-merger) | 1 merged block | custom merger binary, no fan-out at query time |
