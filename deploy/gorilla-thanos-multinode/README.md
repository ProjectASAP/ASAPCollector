# gorilla-thanos-multinode

Simplified 4-node ASAP demo: gorillas3-only compression pipeline with Thanos query engine + MinIO storage. Strips down the full `mvp-multinode` stack to the minimum needed to demonstrate Gorilla-compressed TSDB block ingestion and queryability.

## Topology

| Node | IP | Role |
|------|----|------|
| node0 | 10.10.1.1 | Producers + agent-a (gorilla-only pipeline) |
| node1 | 10.10.1.2 | **Idle** (no gateway in this stack) |
| node2 | 10.10.1.3 | MinIO (S3 storage) + Thanos (store-gateway, query, compact) |
| node3 | 10.10.1.4 | Producers + agent-b (gorilla-only pipeline) |

## Data Flow

```
node0                         node2
┌──────────────────────┐      ┌──────────────────────────────────────┐
│ fake-exporter ×N     │      │ MinIO (port 9000)                    │
│   │  OTLP gRPC       │      │   asap-gorilla-tsdb/                 │
│   ▼  port 4317       │      │     <ULID>/chunks/                   │
│ asap-agent-a         │      │     <ULID>/index                     │
│   [otlp receiver]    │ S3   │     <ULID>/meta.json                 │
│   [memory_limiter]   │ PUT  │         │                            │
│   [gorillas3] ───────┼─────►│         │ sync (30s)                 │
│     drop_original:   │ 9000 │         ▼                            │
│     true             │      │ Thanos store-gateway (port 10901)    │
│   [batch]            │      │         │ StoreAPI gRPC              │
│   [nop exporter]     │      │         ▼                            │
└──────────────────────┘      │ Thanos query (port 10903)            │
                              │   PromQL: /api/v1/query              │
node3                         └──────────────────────────────────────┘
┌──────────────────────┐
│ fake-exporter ×N     │
│   │  OTLP gRPC       │
│   ▼  port 4317       │
│ asap-agent-b ────────┼─────► same S3 PUT path to MinIO
└──────────────────────┘

Key: NO raw OTLP crosses the network to node2.
     Only Gorilla-compressed TSDB block S3 PUTs on port 9000.
```

## Quick Start

### Single-node (local testing with docker-compose)

```bash
cd deploy/gorilla-thanos-multinode/docker-compose
docker compose -f gorilla-thanos.yml up -d

# Wait ~60s for first TSDB block flush, then:
curl 'http://localhost:19092/api/v1/label/__name__/values'  # metric names via Thanos
curl http://localhost:18890/metrics | grep gorilla           # agent self-metrics

# MinIO console: http://localhost:19001 (user: asap, pass: asap-local-only)

# Teardown:
docker compose -f gorilla-thanos.yml down -v
```

### Multi-node (4-node CloudLab)

```bash
cd deploy/gorilla-thanos-multinode

# Full run (sync + up + soak + verify + down):
bash scripts/run_demo.sh all

# Or step by step:
bash scripts/run_demo.sh sync    # rsync configs to nodes 0, 2, 3
bash scripts/run_demo.sh up      # start backend + agents + producers
# ... wait 60-90s for first block flush ...
bash scripts/run_demo.sh verify  # run success-metric checks
bash scripts/run_demo.sh down    # stop all containers
```

## Success Metrics

| Check | What to look for | Notes |
|-------|-----------------|-------|
| MinIO has TSDB blocks | `mc ls asap-gorilla-tsdb --recursive` shows ULID dirs | First block after ~60s (tsdb_block_duration) |
| Thanos query API healthy | `/api/v1/query?query=up` returns `status: success` | Should be immediate after containers start |
| Thanos serves metric names | `/api/v1/label/__name__/values` returns non-empty list | ~30s after first block (store-gateway sync) |
| Agent gorilla self-metrics | `curl :8890/metrics \| grep gorilla` has output | Confirms gorillas3 is active |
| No outbound gRPC | `tcpdump -n 'dst port 4317'` on node0 shows only inbound | Confirms `drop_original: true` is in effect |

## Comparison vs. mvp-multinode

| Feature | mvp-multinode (asap arm) | gorilla-thanos-multinode |
|---------|--------------------------|--------------------------|
| Controller (OpAMP) | Yes — asap/query-backend:dev | **No** |
| Sketch processors | 5 (ddsketch/KLL/HLL/countsketch/countmin) | **None** |
| Routing connector | Yes (6 pipelines) | **No (1 pipeline)** |
| Gateway (node1) | Yes — asap/asap-otel:dev | **Idle** |
| Query engine | ASAPQuery-backend (Rust, port 9091) | **Thanos (port 10903)** |
| Storage | MinIO + Prometheus | **MinIO only** |
| Agent → backend traffic | Raw OTLP gRPC (`drop_original: false`) | **S3 PUTs only (`drop_original: true`)** |
| Outbound gRPC from agent | Yes (gateway:4317) | **No** |
| Images required | asap/asap-otel, asap/fake-exporter, asap/query-backend, minio, mc, prometheus, thanos | **asap/asap-otel, asap/fake-exporter, minio, mc, thanos** |

## File Structure

```
gorilla-thanos-multinode/
├── README.md                          (this file)
├── topology.env                       (node IPs, image set, workload sizing)
├── configs/
│   ├── agent-gorilla-only.yaml        (OTel agent: gorillas3+nop, no OpAMP)
│   └── thanos-objstore.yaml           (Thanos S3 config → asap-gorilla-tsdb)
├── docker-compose/
│   └── gorilla-thanos.yml             (single-node compose for local testing)
└── scripts/
    ├── run_demo.sh                    (multinode orchestrator)
    └── verify_gorilla_compression.sh  (success-metric checks)
```
