# gorilla-thanos-multinode

Simplified 4-node ASAP demo: two-tier Gorilla compression pipeline with Thanos query engine + MinIO storage. Strips down the full `mvp-multinode` stack to the minimum needed to demonstrate Gorilla-compressed TSDB block ingestion through a buffering gateway and queryability via Thanos.

## Topology

| Node | IP | Role |
|------|----|------|
| node0 | 10.10.1.1 | Producers + agent-a (gorilla-only pipeline) |
| node1 | 10.10.1.2 | **gorilla-gateway** (S3 buffering proxy, 20s flush to MinIO) |
| node2 | 10.10.1.3 | MinIO (S3 storage) + Thanos (store-gateway, query, compact) |
| node3 | 10.10.1.4 | Producers + agent-b (gorilla-only pipeline) |

## Data Flow

```
node0                         node1                    node2
┌──────────────────────┐      ┌──────────────────┐     ┌──────────────────────────────────────┐
│ fake-exporter ×N     │      │ gorilla-gateway   │     │ MinIO (port 9000)                    │
│   │  OTLP gRPC       │      │  port 9100        │     │   asap-gorilla-tsdb/                 │
│   ▼  port 4317       │ S3   │                   │ S3  │     <ULID>/chunks/                   │
│ asap-agent-a         │ PUT  │  buffers 10s      │ PUT │     <ULID>/index                     │
│   [otlp receiver]    │:9100 │  blocks in mem    │:9000│     <ULID>/meta.json                 │
│   [memory_limiter]   │─────►│  flush every 20s ─┼────►│         │                            │
│   [gorillas3]        │      │                   │     │         │ sync (~30s)                │
│     drop_original:   │      └──────────────────┘     │         ▼                            │
│     true             │                               │ Thanos store-gateway (port 10901)    │
│   [batch]            │                               │         │ StoreAPI gRPC              │
│   [nop exporter]     │                               │         ▼                            │
└──────────────────────┘                               │ Thanos query (port 10903)            │
                                                       │   PromQL: /api/v1/query              │
node3                                                  └──────────────────────────────────────┘
┌──────────────────────┐
│ fake-exporter ×N     │
│   │  OTLP gRPC       │
│   ▼  port 4317       │
│ asap-agent-b ────────┼──────► same two-tier path via gorilla-gateway
└──────────────────────┘

Key: Agents write 10s Gorilla TSDB blocks to gorilla-gateway:9100 (NOT directly to MinIO).
     Gateway batches blocks in memory and flushes to MinIO every 20s.
     NO raw OTLP crosses the network — only S3 PUTs.
```

### Two-tier timing

| Tier | Interval | Who |
|------|----------|-----|
| Agent → gateway | 10s (`window_interval: 10s`) | gorillas3 processor flush |
| Gateway → MinIO | 20s (`GATEWAY_FLUSH_INTERVAL=20s`) | gorilla-gateway batch upload |

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
bash scripts/run_demo.sh sync    # rsync configs to all 4 nodes
bash scripts/run_demo.sh up      # backend → gateway → agents → producers
# ... wait 60-90s for first block flush through gateway ...
bash scripts/run_demo.sh verify  # run success-metric checks
bash scripts/run_demo.sh down    # stop all containers
```

## Verification Status

All verification scripts pass. Full per-step calculations are in [thanos-query-verif.md](thanos-query-verif.md).

| Part | Script / method | Checks | Result |
|------|----------------|--------|--------|
| 1 — Pipeline smoke-test | `verify_gorilla_compression.sh` | 5/5 | PASS |
| 2 — Exact-value test (gauge + rate) | hand-computed vs Thanos | 4/4 | PASS |
| 3 — Two-tier gateway (`run_demo.sh all`) | end-to-end 5-check suite | 5/5 | PASS |
| 4 — Advanced PromQL (rate/avg/quantile) | `verif_part4.py` | 25/25 | PASS |

## PromQL Query Coverage

Test workload: `EXPORTER_FIXED_LATENCY=42.0`, `EXPORTER_FREQ_HZ=1`, 8 series (4 zones × 2 producers), `[120s]` window.  
All queries verified against `http://10.10.1.3:10903/api/v1/query`.

| # | PromQL type | Expression | Manual calculation | Expected | Thanos result | Match |
|---|---|---|---|---|---|---|
| 1 | instant | `http_requests_total_latency_ms{producer_id="p-a-1",zone="z0"}` | `LastValue` of fixed gauge | `42.0` | `42.0` | **Exact** |
| 2 | `sum by (zone)` | `sum by (zone)(http_requests_total_latency_ms)` | 2 producers × 42.0 | `84.0` | `84.0` | **Exact** |
| 3 | `rate()` | `rate(http_requests_total{...}[120s])` | `extrapolatedRate()` on TSDB samples | `0.694792` | `0.694792` | **0.0000%** |
| 4 | `sum(rate())` | `sum by (zone)(rate(http_requests_total[120s]))` | 2 × per-series rate | `≈1.390` | `1.391–1.433` | `≤2%` |
| 5 | `avg(rate())` | `avg by (zone)(rate(http_requests_total[120s]))` | 1 × per-series rate | `≈0.695` | `0.695–0.716` | `≤2%` |
| 6 | `avg_over_time()` | `avg_over_time(http_requests_total_latency_ms{...}[120s])` | sum(121 × 42.0) / 121 | `42.0` | `42.000000` | **Exact** |
| 7 | `avg` | `avg(http_requests_total_latency_ms)` | sum(8 × 42.0) / 8 | `42.0` | `42.000000` | **Exact** |
| 8 | `avg by (zone)` | `avg by (zone)(http_requests_total_latency_ms)` | sum(2 × 42.0) / 2 per zone | `42.0` | `42.0` × 4 | **Exact** |
| 9 | `quantile_over_time` p50 | `quantile_over_time(0.5, http_requests_total_latency_ms{...}[120s])` | `sorted[rank=60]` of 121 values | `42.0` | `42.0` | **Exact** |
| 10 | `quantile_over_time` p95 | `quantile_over_time(0.95, http_requests_total_latency_ms{...}[120s])` | `sorted[rank=114]` of 121 values | `42.0` | `42.0` | **Exact** |
| 11 | `quantile` p50 | `quantile(0.5, http_requests_total_latency_ms)` | p50 across 8 identical series | `42.0` | `42.0` | **Exact** |
| 12 | `quantile` p95 | `quantile(0.95, http_requests_total_latency_ms)` | p95 across 8 identical series | `42.0` | `42.0` | **Exact** |

**Key findings:**

- **`rate()` exact match** — The manual formula reproduces Thanos's result to floating-point identity (diff = 6.44 × 10⁻¹²) when using a **matrix instant query** (`metric[120s]`) instead of `query_range`. The matrix query returns actual TSDB sample timestamps at millisecond precision (e.g., `t=1778748233.875`); `query_range` step=1s projects these onto an integer grid and loses the ~0.875s sub-second OTel SDK offset, causing ~0.3% error.
- **Freshness lag ~37s** — The observed rate is ~0.69 (not 1.0) because data buffered in the pipeline (10s gorillas3 block + 20s gateway flush + ~7s store sync ≈ 37s) isn't visible yet. Prometheus correctly refuses to extrapolate across gaps > 1.1× avg_step, adding only `avg_step/2 = 0.5s` extrapolation at the tail.
- **Gauge/quantile: machine-precision exact** — All avg/quantile queries on the fixed-42.0 gauge return exactly 42.000000 with zero error.

## Success Metrics

| Check | What to look for | Notes |
|-------|-----------------|-------|
| MinIO has TSDB blocks | `mc ls asap-gorilla-tsdb --recursive` shows ULID dirs | Blocks arrive via gateway flush every 20s |
| Thanos query API healthy | `/api/v1/query?query=up` returns `status: success` | Immediate after containers start |
| Thanos serves metric names | `/api/v1/label/__name__/values` returns non-empty list | ~30s after first block (store-gateway sync) |
| Agent gorilla self-metrics | `curl :8890/metrics \| grep gorilla` — check `gorillas3_chunks_written_total` | Confirms gorillas3 is active; `s3_put_failures_total` must be 0 |
| Gateway buffering | `ssh node1 docker logs asap-gorilla-gateway \| grep -E "recv\|flush"` | Must show `recv s3://...` lines and `flush done ok=N fail=0` |
| No outbound gRPC | `tcpdump -n 'dst port 4317'` on node0 shows only inbound | Confirms `drop_original: true` |

### Additional gateway-specific checks

```bash
# Confirm agent writes to gateway (not directly to MinIO):
docker logs asap-agent-a 2>&1 | grep endpoint
# Expected: "endpoint": "http://gorilla-gateway:9100"

# Confirm gateway receives and flushes blocks:
ssh node1 'docker logs asap-gorilla-gateway 2>&1 | grep -E "recv|flush" | tail -20'
# Expected pattern:
#   recv  s3://asap-gorilla-tsdb/<ULID>/chunks/000001  NNN B  buf=1
#   recv  s3://asap-gorilla-tsdb/<ULID>/index  NNN B  buf=2
#   recv  s3://asap-gorilla-tsdb/<ULID>/meta.json  NNN B  buf=3
#   flush: pushing 6 objects upstream
#   flush done  ok=6 fail=0
```

## Comparison vs. mvp-multinode

| Feature | mvp-multinode (asap arm) | gorilla-thanos-multinode |
|---------|--------------------------|--------------------------|
| Controller (OpAMP) | Yes — asap/query-backend:dev | **No** |
| Sketch processors | 5 (ddsketch/KLL/HLL/countsketch/countmin) | **None** |
| Routing connector | Yes (6 pipelines) | **No (1 pipeline)** |
| Gateway (node1) | Yes — asap/asap-otel:dev (OTLP fan-out) | **gorilla-gateway (S3 buffering proxy)** |
| Query engine | ASAPQuery-backend (Rust, port 9091) | **Thanos (port 10903)** |
| Storage | MinIO + Prometheus | **MinIO only** |
| Agent → backend traffic | Raw OTLP gRPC (`drop_original: false`) | **S3 PUTs to gateway:9100 only** |
| Outbound gRPC from agent | Yes (gateway:4317) | **No** |
| Images required | asap/asap-otel, asap/fake-exporter, asap/query-backend, minio, mc, prometheus, thanos | **asap/asap-otel, asap/fake-exporter, asap/gorilla-gateway, minio, mc, thanos** |

## File Structure

```
gorilla-thanos-multinode/
├── README.md                          (this file)
├── topology.env                       (node IPs, image set, workload sizing)
├── thanos-query-verif.md              (verification results: smoke-test + exact-value + two-tier gateway + advanced PromQL)
├── configs/
│   ├── agent-gorilla-only.yaml        (OTel agent: gorillas3→gateway:9100, no OpAMP)
│   └── thanos-objstore.yaml           (Thanos S3 config → asap-gorilla-tsdb)
├── docker-compose/
│   └── gorilla-thanos.yml             (single-node compose for local testing)
└── scripts/
    ├── run_demo.sh                    (multinode orchestrator: backend→gateway→agents)
    ├── verif_part4.py                 (PromQL verification: rate/avg/quantile, 25/25 checks)
    └── verify_gorilla_compression.sh  (pipeline smoke-test checks, 5/5 pass)

deploy/gorilla-gateway/                (custom Go S3-buffering proxy service)
├── main.go                            (HTTP S3 API server + MinIO upstream flush)
└── go.mod                             (module: github.com/ProjectASAP/gorilla-gateway)

deploy/docker/
└── Dockerfile.gorilla-gateway         (builds asap/gorilla-gateway:dev image)
```
