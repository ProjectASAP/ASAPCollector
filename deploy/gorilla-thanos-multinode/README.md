# gorilla-thanos-multinode

Simplified 4-node ASAP demo: two-tier Gorilla compression pipeline with Thanos query engine + MinIO storage. Strips down the full `mvp-multinode` stack to the minimum needed to demonstrate Gorilla-compressed TSDB block ingestion through a buffering gateway and queryability via Thanos.

## Topology

| Node | IP | Role |
|------|----|------|
| node0 | 10.10.1.1 | Producers + agent-a (gorilla-only pipeline) |
| node1 | 10.10.1.2 | **gorilla-gateway** (S3 buffering proxy, 20s flush) + **gorilla-buffer-store** (Thanos Store gRPC :10921) |
| node2 | 10.10.1.3 | MinIO (S3 storage) + Thanos (store-gateway, query, compact) |
| node3 | 10.10.1.4 | Producers + agent-b (gorilla-only pipeline) |

## Data Flow

```
node0                         node1                              node2
┌──────────────────────┐      ┌──────────────────────────┐      ┌──────────────────────────────────────┐
│ fake-exporter ×N     │      │ gorilla-gateway :9100     │      │ MinIO (port 9000)                    │
│   │  OTLP gRPC       │ S3   │  writes blocks to disk    │ S3   │   asap-gorilla-tsdb/                 │
│   ▼  port 4317       │ PUT  │  GATEWAY_BUFFER_DIR        │ PUT  │     <ULID>/chunks/                   │
│ asap-agent-a         │:9100 │  (block_source=gateway-   │:9000 │     <ULID>/index                     │
│   [otlp receiver]    │─────►│   buffer in meta.json)    ├─────►│     <ULID>/meta.json                 │
│   [memory_limiter]   │      │  flush every 20s           │      │  (block_source=minio)                │
│   [gorillas3]        │      │  (GATEWAY_MINIO_SYNC_GRACE │      │         │                            │
│     drop_original:   │      │   = 90s before deleting)  │      │         │ sync (~30s)                │
│     true             │      │                           │      │         ▼                            │
│   [batch]            │      │ gorilla-buffer-store :10921│      │ Thanos store-gateway (port 10901)   │
│   [nop exporter]     │      │  reads GATEWAY_BUFFER_DIR  │      │         │ StoreAPI gRPC              │
└──────────────────────┘      │  (read-only mount)         │      └─────────┼────────────────────────────┘
                              └────────────┬──────────────┘                │
node3                                      │ StoreAPI gRPC :10921           │ StoreAPI gRPC :10901
┌──────────────────────┐                   │                                │
│ fake-exporter ×N     │                   └──────────────┐  ┌─────────────┘
│   │  OTLP gRPC       │                                  ▼  ▼
│   ▼  port 4317       │                   ┌──────────────────────────────────────┐
│ asap-agent-b ────────┼──────────────────►│ Thanos query (port 10903)            │
└──────────────────────┘  same path        │  --endpoint=thanos-store-gateway     │
                          via node1        │  --endpoint=gorilla-buffer-store     │
                                           │  --query.replica-label=block_source  │
                                           │  PromQL: /api/v1/query               │
                                           └──────────────────────────────────────┘

Key: Agents write 10s Gorilla TSDB blocks to gorilla-gateway:9100 (NOT directly to MinIO).
     Gateway persists blocks to disk and flushes to MinIO every 20s (block_source=minio).
     gorilla-buffer-store reads the same disk dir and serves blocks via Store gRPC (~30s freshness).
     Thanos deduplicates via --query.replica-label=block_source during the 90s grace overlap.
     NO raw OTLP crosses the network — only S3 PUTs.
```

### Three-tier timing

| Tier | Interval | Who | Data freshness |
|------|----------|-----|----------------|
| Agent → gateway | 10s (`window_interval: 10s`) | gorillas3 processor flush | blocks written to disk immediately |
| Buffer → Thanos Query | 30s (`sync-block-duration=30s`) | gorilla-buffer-store sidecar | **~30–40s total lag** via buffer path |
| Gateway → MinIO | 20s (`GATEWAY_FLUSH_INTERVAL=20s`) | gorilla-gateway flush | ~50–60s total lag via MinIO path |

During the 90s grace window (`GATEWAY_MINIO_SYNC_GRACE`) after a MinIO upload, both
stores serve the same block simultaneously. Thanos deduplicates via `block_source`.

### Why `sync-block-duration=30s` is the new freshness ceiling

The gateway writes each block to disk the moment all 3 files arrive. But
`gorilla-buffer-store` does not watch the directory in real-time — it polls on a fixed
timer controlled by `--sync-block-duration`:

```
gorilla-gateway writes:           gorilla-buffer-store scans:
t=0s   block arrives on disk      t=0s   last scan just ran
t=1s   ...                        t=1s   (sleeping)
...                                ...
t=10s  next block arrives         t=10s  (sleeping)
...
t=30s  ...                        t=30s  ← next scan, discovers both blocks
...                                ...

```

So data lands on disk almost immediately but only becomes visible to Thanos Query at
the next scan boundary. Total freshness breakdown via the buffer path:

| Stage | Duration |
|-------|----------|
| gorillas3 window (agent accumulates samples) | 10s |
| gorilla-buffer-store scan interval | up to 30s |
| **Total visible lag (buffer path)** | **~30–40s** |

Compare to the original 1-hour flush design:

| Stage | Duration |
|-------|----------|
| gorillas3 window | 10s |
| gateway flush interval | up to 3600s (1h) |
| Thanos store-gateway MinIO sync | up to 30s |
| **Total visible lag (original)** | **~10–3630s (≈1h)** |

`--sync-block-duration` is tunable: setting it to `10s` tightens the ceiling to ~20s
total at the cost of more frequent disk scans. `30s` matches the MinIO store-gateway
default and is a reasonable balance. The 1-hour MinIO flush becomes a **durability and
long-term retention** operation only — it no longer controls when data is queryable.

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
| 5 — Disk-buffer + buffer-store upgrade | `verify_buffer_store.sh` | 7/7 | PENDING |

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
| Buffer-store running | `ssh node1 docker ps | grep asap-gorilla-buffer-store` | Must show container Up |
| Buffer-store registered | `curl 10.10.1.3:10903/api/v1/stores | grep 10921` | Thanos Query must list it |
| block_source label set | `cat /mydata/gorilla-gateway/buffer/<ULID>/meta.json` on node1 | Must show `"block_source":"gateway-buffer"` |
| /v1/blocks API | `curl node1:9100/v1/blocks` shows `complete:true` entries | Disk buffer active |

### Additional gateway-specific checks

```bash
# Confirm agent writes to gateway (not directly to MinIO):
docker logs asap-agent-a 2>&1 | grep endpoint
# Expected: "endpoint": "http://gorilla-gateway:9100"

# Confirm gateway receives and flushes blocks:
ssh node1 'docker logs asap-gorilla-gateway 2>&1 | grep -E "recv|flush" | tail -20'
# Expected pattern:
#   recv  s3://asap-gorilla-tsdb/<ULID>/chunks/000001  NNN B  buf=1  complete=false
#   recv  s3://asap-gorilla-tsdb/<ULID>/index  NNN B  buf=2  complete=false
#   recv  s3://asap-gorilla-tsdb/<ULID>/meta.json  NNN B  buf=3  complete=true
#   flush: uploading 2 complete block(s) to s3://asap-gorilla-tsdb
#   flush OK  s3://asap-gorilla-tsdb/<ULID>/meta.json  NNN B
#   flush: block <ULID> uploaded OK; waiting 15s grace before deleting local
#   flush: deleted local block <ULID>

# Confirm block_source label is injected in on-disk meta.json:
ssh node1 'find /mydata/gorilla-gateway/buffer -name meta.json | head -1 | xargs cat | python3 -m json.tool | grep block_source'
# Expected: "block_source": "gateway-buffer"

# Confirm gorilla-buffer-store is running and reading the buffer:
ssh node1 'docker logs asap-gorilla-buffer-store 2>&1 | tail -10'
# Expected: "lset=... block_source="gateway-buffer"" entries (blocks discovered)

# Check Thanos Query has buffer-store registered:
curl -s http://10.10.1.3:10903/api/v1/stores | python3 -m json.tool | grep -E '"name"|10921'
# Expected: "10.10.1.2:10921" in the store list

# List buffered blocks via /v1/blocks API:
curl -s http://10.10.1.2:9100/v1/blocks | python3 -m json.tool
# Expected: JSON array of {"ulid":"...","complete":true,"flushing":false} entries

# Run all buffer-store checks:
bash scripts/verify_buffer_store.sh --gateway-host node1 --node1-ip 10.10.1.2 --thanos-host 10.10.1.3
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
│   ├── thanos-objstore.yaml           (Thanos S3 config → MinIO asap-gorilla-tsdb)
│   └── buffer-objstore.yaml           (Thanos FILESYSTEM config → GATEWAY_BUFFER_DIR)
├── docker-compose/
│   └── gorilla-thanos.yml             (single-node compose for local testing)
└── scripts/
    ├── run_demo.sh                    (multinode orchestrator: backend→gateway→buffer-store→agents)
    ├── verify_buffer_store.sh         (disk-buffer + buffer-store upgrade: 7-check suite)
    ├── verif_part4.py                 (PromQL verification: rate/avg/quantile, 25/25 checks)
    └── verify_gorilla_compression.sh  (pipeline smoke-test checks, 5/5 pass)

deploy/gorilla-gateway/                (custom Go S3-buffering proxy service)
├── main.go                            (HTTP S3 API + /v1/blocks endpoint + runFlushLoop)
├── diskbuf.go                         (DiskBuffer: disk persistence, label injection, crash recovery)
├── flush.go                           (flush lifecycle: upload→grace→delete; block_source=minio relabel)
└── go.mod                             (module: github.com/ProjectASAP/gorilla-gateway)

deploy/docker/
└── Dockerfile.gorilla-gateway         (builds asap/gorilla-gateway:dev image)
```
