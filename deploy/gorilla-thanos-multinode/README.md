# gorilla-thanos-multinode

Simplified 4-node ASAP demo: Gorilla-compressed TSDB pipeline with Thanos query engine + MinIO storage. No gorilla-gateway — agents write 60s blocks directly to MinIO. gorilla-buffer-store on the backend node serves the last `BUFFER_STORE_DURATION` of blocks with 15s sync latency; thanos-store-gateway covers all historical data.

## Topology

| Node | IP | Role |
|------|----|------|
| node0 | 10.10.1.1 | Producers + agent-a (gorilla-only pipeline) |
| node1 | 10.10.1.2 | Idle (no services) |
| node2 | 10.10.1.3 | MinIO + Thanos (store-gateway, query, compact) + **gorilla-buffer-store** |
| node3 | 10.10.1.4 | Producers + agent-b (gorilla-only pipeline) |

## Data Flow

```
node0                                              node2
┌──────────────────────┐                           ┌──────────────────────────────────────────────┐
│ fake-exporter ×N     │                           │ MinIO (port 9000)                            │
│   │  OTLP gRPC       │    S3 PUT (60s blocks)    │   asap-gorilla-tsdb/                         │
│   ▼  port 4317       │──────────────────────────►│     <ULID>/chunks/                           │
│ asap-agent-a         │       minio:9000           │     <ULID>/index                             │
│   [otlp receiver]    │    (no gateway hop)        │     <ULID>/meta.json                         │
│   [memory_limiter]   │                            │                                              │
│   [gorillas3         │                            │  ┌──────────────────────────────────────┐   │
│     endpoint:minio   │                            │  │ gorilla-buffer-store  :10921         │   │
│     tsdb_bucket:     │                            │  │  reads MinIO, sync every 15s         │   │
│     asap-gorilla-tsdb│                            │  │  --min-time=-BUFFER_STORE_DURATION   │   │
│     drop_original:   │                            │  │  disk cache: /tmp/thanos-buffer-store│   │
│     true]            │                            │  │  serves last N blocks (hot window)   │   │
│   [batch]            │                            │  └──────────────────┬───────────────────┘   │
│   [nop exporter]     │                            │                     │ StoreAPI gRPC :10921   │
└──────────────────────┘                            │  ┌──────────────────────────────────────┐   │
                                                    │  │ Thanos store-gateway  :10901         │   │
node3                                               │  │  reads MinIO, sync every 30s         │   │
┌──────────────────────┐   S3 PUT (60s blocks)      │  │  serves ALL blocks (full history)    │   │
│ fake-exporter ×N     │──────────────────────────► │  └──────────────────┬───────────────────┘   │
│   │ OTLP gRPC        │      minio:9000             │                     │ StoreAPI gRPC :10901   │
│   ▼ port 4317        │                            │  ┌──────────────────▼───────────────────┐   │
│ asap-agent-b         │                            │  │ Thanos query  :10903                 │   │
└──────────────────────┘                            │  │  --endpoint=thanos-store-gateway     │   │
                                                    │  │  --endpoint=gorilla-buffer-store     │   │
                                                    │  │  PromQL: /api/v1/query               │   │
                                                    │  └──────────────────────────────────────┘   │
                                                    └──────────────────────────────────────────────┘
```

## Two-tier freshness model

| Tier | Sync interval | Serves | Total freshness |
|------|--------------|--------|-----------------|
| gorilla-buffer-store | 15s | last `BUFFER_STORE_DURATION` blocks | ~75s (60s block + 15s sync) |
| thanos-store-gateway | 30s | all historical blocks | ~90s (60s block + 30s sync) |

`BUFFER_STORE_DURATION` (default `1h`) is the configurable hot-window. Set in `topology.env`:
```bash
BUFFER_STORE_DURATION=30m  # 30-minute hot window
BUFFER_STORE_DURATION=2h   # 2-hour hot window
```

Both stores read from the same MinIO bucket. For overlapping blocks (within the hot window), Thanos Query's chunk-level merge deduplicates identical timestamps — no extra replica-label config needed.

## Quick Start

### Single-node (local testing with docker-compose)

```bash
cd deploy/gorilla-thanos-multinode/docker-compose

# Default 1h buffer window:
docker compose -f gorilla-thanos.yml up -d

# Custom buffer window:
BUFFER_STORE_DURATION=30m docker compose -f gorilla-thanos.yml up -d

# Wait ~60s for first TSDB block flush, then:
curl 'http://localhost:19092/api/v1/label/__name__/values'   # metric names via Thanos
curl http://localhost:18890/metrics | grep gorilla            # agent self-metrics

# MinIO console: http://localhost:19001 (user: asap, pass: asap-local-only)

# Teardown:
docker compose -f gorilla-thanos.yml down -v
```

### Multi-node (4-node CloudLab)

**Prerequisite — rebuild asap-otel image (gorillas3 Phase 3, removes `bucket:` field):**
```bash
cd /mydata/ASAPCollector
bash restore_otel_collector_contrib_patches.sh
bash build_asap_otel.sh
# Then distribute the image to node0, node3 via docker save/load
```

**Run the stack:**
```bash
cd /mydata/gorilla-thanos-multinode

# Full run (sync + up + soak + verify + down):
bash scripts/run_demo.sh all

# Or step by step:
bash scripts/run_demo.sh sync    # rsync configs to nodes 0, 2, 3
bash scripts/run_demo.sh up      # backend (node2) + agents (node0, node3)
# ... wait 60-90s for first block flush to MinIO + buffer-store sync ...
bash scripts/run_demo.sh verify  # run success-metric checks
bash scripts/run_demo.sh down    # stop all containers
```

**Tune buffer window:**
```bash
# Edit topology.env before running:
BUFFER_STORE_DURATION=30m bash scripts/run_demo.sh all
```

## Verification

| Part | Script / method | Checks | Status |
|------|----------------|--------|--------|
| 1 — Pipeline smoke-test | `verify_gorilla_compression.sh` | 5/5 | verified pre-refactor |
| 2 — Exact-value test | hand-computed vs Thanos | 4/4 | verified pre-refactor |
| 3 — Two-tier store | check buffer-store registered + serving | — | re-verify after refactor |
| 4 — Advanced PromQL | `verif_part4.py` | 25/25 | verified pre-refactor |

Parts 1, 2, 4 remain valid (MinIO+Thanos path unchanged). Part 3 verification scripts reference gorilla-gateway and need updating to check gorilla-buffer-store directly on node2.

## Success Metrics

| Check | Command | Expected |
|-------|---------|----------|
| MinIO has TSDB blocks | `ssh node2 'docker exec asap-minio mc ls local/asap-gorilla-tsdb --recursive'` | ULID dirs visible ~60s after start |
| Thanos query healthy | `curl http://10.10.1.3:10903/api/v1/query?query=up` | `status: success` |
| Metric names visible | `curl http://10.10.1.3:10903/api/v1/label/__name__/values` | Non-empty list ~30s after first block |
| Agent writing to MinIO | `ssh node0 'docker logs asap-agent-a 2>&1 \| grep -i "gorillas3\|s3\|put"'` | S3 PUT lines to minio:9000 |
| gorilla-buffer-store up | `ssh node2 'docker ps \| grep asap-gorilla-buffer-store'` | Container Up |
| buffer-store registered | `curl http://10.10.1.3:10903/api/v1/stores \| python3 -m json.tool \| grep 10921` | node2:10921 in store list |
| buffer-store serving data | `ssh node2 'docker logs asap-gorilla-buffer-store 2>&1 \| tail -10'` | Block sync messages |
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
│   └── buffer-objstore.yaml           (Thanos S3 config → MinIO asap-gorilla-tsdb, for buffer-store)
├── docker-compose/
│   └── gorilla-thanos.yml             (single-node compose for local testing)
└── scripts/
    ├── run_demo.sh                    (multinode orchestrator: backend+buffer-store → agents)
    ├── verif_part4.py                 (PromQL verification: rate/avg/quantile)
    ├── verify_buffer_store.sh         (buffer-store checks — needs update for new arch)
    └── verify_gorilla_compression.sh  (pipeline smoke-test checks)
```

## Comparison vs. previous design (with gorilla-gateway)

| Aspect | Old (gorilla-gateway on node1) | New (direct MinIO) |
|--------|-------------------------------|-------------------|
| Node1 role | gorilla-gateway (S3 proxy) + gorilla-buffer-store | Idle |
| Agent write target | gorilla-gateway:9100 | minio:9000 (node2) |
| Buffer-store location | node1 (reads local disk) | node2 (reads MinIO via S3) |
| Buffer-store objstore | FILESYSTEM (/var/gorilla-gateway/buffer) | S3 (MinIO) |
| Buffer duration config | hard-coded in gateway (GATEWAY_FLUSH_INTERVAL) | configurable BUFFER_STORE_DURATION |
| block_source label | injected by gateway (gateway-buffer) | not needed (chunk-level dedup) |
| thanos-query dedup | --query.replica-label=block_source | none (timestamp merge) |
| Images required | asap-otel, fake-exporter, gorilla-gateway, minio, mc, thanos | asap-otel, fake-exporter, minio, mc, thanos |
| Data freshness | ~95s (60s block + 20s gateway flush + 15s buffer sync) | ~75s (60s block + 15s buffer sync) |
