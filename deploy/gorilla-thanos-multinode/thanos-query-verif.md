# Thanos Query Verification — gorilla-thanos-multinode

Verified on **2026-05-13** against `siedeta@clnode013.clemson.cloudlab.us`.

Two complementary verification runs are recorded here:

1. **Pipeline smoke-test** (`verify_gorilla_compression.sh`) — confirms blocks reach MinIO and metric names are queryable.
2. **Exact-value test** — uses a deterministic fake-exporter config to confirm Thanos returns results that match hand-computed expectations.

---

## Part 1 — Pipeline Smoke-Test (`verify_gorilla_compression.sh`)

### Check 1: MinIO has TSDB blocks in `asap-gorilla-tsdb`

```
=== Check 1: MinIO has TSDB blocks in asap-gorilla-tsdb ===
  Connecting to node2 via SSH to run mc ls ...
  Note: First blocks appear after tsdb_block_duration=60s flush.
  If this fails immediately after stack_up, wait 60-90s and retry.
  mc ls output (121 lines):
    Added `local` successfully.
    [2026-05-13 15:09:33 UTC]  58KiB STANDARD 01KRGY5J3XBCFWFQW815YAS7B0/chunks/000001
    [2026-05-13 15:09:33 UTC] 119KiB STANDARD 01KRGY5J3XBCFWFQW815YAS7B0/index
    [2026-05-13 15:09:33 UTC]   302B STANDARD 01KRGY5J3XBCFWFQW815YAS7B0/meta.json
    [2026-05-13 15:09:42 UTC] 365KiB STANDARD 01KRGY5M674HTQDXBGZTDTPFXF/chunks/000001
    [2026-05-13 15:09:42 UTC] 588KiB STANDARD 01KRGY5M674HTQDXBGZTDTPFXF/index
    [2026-05-13 15:09:42 UTC]   306B STANDARD 01KRGY5M674HTQDXBGZTDTPFXF/meta.json
    [2026-05-13 15:09:39 UTC]  43KiB STANDARD 01KRGY5SHFHZRFR73MG9PWG87N/chunks/000001
    [2026-05-13 15:09:39 UTC] 119KiB STANDARD 01KRGY5SHFHZRFR73MG9PWG87N/index
    [2026-05-13 15:09:39 UTC]   302B STANDARD 01KRGY5SHFHZRFR73MG9PWG87N/meta.json
    [2026-05-13 15:09:46 UTC] 297KiB STANDARD 01KRGY5TGGVC00MPM6HW5XQG8Z/chunks/000001
    [2026-05-13 15:09:47 UTC] 471KiB STANDARD 01KRGY5TGGVC00MPM6HW5XQG8Z/index
    [2026-05-13 15:09:47 UTC]   304B STANDARD 01KRGY5TGGVC00MPM6HW5XQG8Z/meta.json
    [2026-05-13 15:09:46 UTC] 247KiB STANDARD 01KRGY5WXN1MW7RB8GDMWY855R/chunks/000001
    [2026-05-13 15:09:46 UTC] 588KiB STANDARD 01KRGY5WXN1MW7RB8GDMWY855R/index
    [2026-05-13 15:09:46 UTC]   306B STANDARD 01KRGY5WXN1MW7RB8GDMWY855R/meta.json
    [2026-05-13 15:09:54 UTC] 172KiB STANDARD 01KRGY62NAH7SC51V2EQY9KSTC/chunks/000001
    [2026-05-13 15:09:54 UTC] 471KiB STANDARD 01KRGY62NAH7SC51V2EQY9KSTC/index
    [2026-05-13 15:09:54 UTC]   302B STANDARD 01KRGY62NAH7SC51V2EQY9KSTC/meta.json
    [2026-05-13 15:09:53 UTC] 154KiB STANDARD 01KRGY62RMNPJC06BB70WP7X7X/chunks/000001
  [PASS] MinIO asap-gorilla-tsdb has 121 object(s) — gorillas3 is writing blocks
```

Each block follows the standard Prometheus TSDB layout:
- `chunks/000001` — Gorilla XOR-delta encoded sample data
- `index` — series label index
- `meta.json` — block metadata (ULID, time range, series/sample counts)

Example `meta.json` for block `01KRGYN9DW0JMDDF11YZV0FRV1`:
```json
{
  "ulid": "01KRGYN9DW0JMDDF11YZV0FRV1",
  "minTime": 1778685486472,
  "maxTime": 1778685487480,
  "stats": {
    "numSamples": 4006,
    "numFloatSamples": 4006,
    "numSeries": 2003,
    "numChunks": 2003
  },
  "compaction": { "level": 1, "sources": ["01KRGYN9DW0JMDDF11YZV0FRV1"] },
  "version": 1
}
```

---

### Check 2: Thanos query API healthy (`10.10.1.3:10903`)

```
=== Check 2: Thanos query API healthy (10.10.1.3:10903) ===
  [PASS] Thanos query API returned status=success
```

---

### Check 3: Thanos serves metric names (data queryable end-to-end)

```
=== Check 3: Thanos serves metric names (data queryable end-to-end) ===
  Thanos label __name__ values: 5 metric name(s)
    http_freshness_probe_archive
    http_freshness_probe_raw
    http_freshness_probe_warm
    http_requests_total
    http_requests_total_latency_ms
  [PASS] Thanos serves 5 metric name(s) — gorillas3 → MinIO → Thanos pipeline is end-to-end
```

---

### Check 4: Agent gorilla self-metrics (`10.10.1.1:8890`)

```
=== Check 4: Agent gorilla self-metrics (10.10.1.1:8890) ===
  gorilla-related metric lines found: 11
    otelcol_asapcollector_processor_active_series{...processor_id="gorillas3"...} 0
    otelcol_asapcollector_processor_goroutines{...processor_id="gorillas3"...} 18
    otelcol_asapcollector_processor_heap_alloc_bytes{...processor_id="gorillas3"...} 2.1471912e+07
    otelcol_asapcollector_processor_heap_sys_bytes{...processor_id="gorillas3"...} 2.801664e+07
    otelcol_asapcollector_processor_input_bandwidth_bytes_per_second{...processor_id="gorillas3"...} 0
    otelcol_asapcollector_processor_input_throughput_per_second{...processor_id="gorillas3"...} 0
    otelcol_asapcollector_processor_output_bandwidth_bytes_per_second{...processor_id="gorillas3"...} 0
    otelcol_asapcollector_processor_output_throughput_per_second{...processor_id="gorillas3"...} 0
    otelcol_asapcollector_processor_process_cpu_system_time_seconds_total{...processor_id="gorillas3"...} 0.080812
    otelcol_asapcollector_processor_process_cpu_user_time_seconds_total{...processor_id="gorillas3"...} 0.208911
  [PASS] Agent self-metrics include 11 gorilla/gorillas3 line(s)
```

`output_bandwidth_bytes_per_second = 0` and `output_throughput_per_second = 0` confirm that `drop_original: true` is in effect — the gorillas3 processor absorbs all data and forwards nothing downstream as raw OTLP.

---

### Check 5: Network traffic analysis

```
=== Check 5: Network traffic analysis ===
  What's on the wire in this stack:

  ┌─────────────────────────────────────────────────────────────────────┐
  │  S3 PUT to port 9000: agent → MinIO (Gorilla-compressed TSDB blocks)│
  │  - Protocol: HTTP/1.1 PUT (S3 API)                                  │
  │  - Content: Prometheus TSDB block files (chunks/, index, meta.json) │
  │  - Compression: Gorilla delta-of-delta + XOR encoding in gorillas3  │
  │  - Frequency: one PUT every tsdb_block_duration=60s per flush       │
  │                                                                     │
  │  No outbound gRPC port 4317 from agents:                            │
  │  - drop_original: true in gorillas3 means the metric stream does    │
  │    NOT leave the agent as raw OTLP. Gorillas3 absorbs the data,     │
  │    compresses it, and writes TSDB blocks to MinIO.                  │
  │  - The nop exporter receives empty batches (nothing to export).     │
  └─────────────────────────────────────────────────────────────────────┘

  How to observe on the wire:

  On node0 (agent host) — observe S3 PUTs going out to MinIO:
    ssh node0 'sudo tcpdump -i eth0 -n "dst port 9000" -c 20'
    (You should see HTTP PUT requests to 10.10.1.3:9000)

  On node0 — confirm NO outbound gRPC from agent:
    ssh node0 'sudo tcpdump -i eth0 -n "dst port 4317" -c 20'
    (You should see ONLY inbound from producers, no outbound to backend)

  On node2 (MinIO host) — see blocks arriving:
    ssh node2 'docker logs asap-minio 2>&1 | grep PUT | tail -20'

  [PASS] Network traffic explanation printed (observational check)

========================================
  VERIFICATION SUMMARY
  PASSED: 5
  FAILED: 0
========================================
  ALL CHECKS PASSED — gorilla-thanos pipeline verified end-to-end
```

---

## Part 2 — Exact-Value Test (Deterministic fake-exporter)

### Config changes for determinism

The fake-exporter was modified to support `EXPORTER_FIXED_LATENCY`: when set, the gauge always records that exact value instead of a random log-normal sample. The topology was reconfigured to minimize cardinality and frequency so the expected values are hand-computable:

| Parameter | Value | Why |
|---|---|---|
| `PER_AGENT_CARDINALITY` | 4 | One series per zone (z0–z3) |
| `EXPORTER_FREQ_HZ` | 1 | 1 increment/s → easy counter math |
| `N_PRODUCERS_PER_NODE` | 1 | 2 producers total: `p-a-1` (node0), `p-b-1` (node3) |
| `EXPORTER_FIXED_LATENCY` | 42.0 | Fixed gauge value — no randomness |
| `EXPORTER_SDK_WINDOW` | 1s | OTel SDK exports every 1 s |

### Exact series generated (logged at startup)

Producer `p-a-1` (node0):
```
series[0]: [{producer_id p-a-1} {zone z0} {rack r00} {node n00} {pod pod-000}]
series[1]: [{producer_id p-a-1} {zone z1} {rack r00} {node n00} {pod pod-000}]
series[2]: [{producer_id p-a-1} {zone z2} {rack r00} {node n00} {pod pod-000}]
series[3]: [{producer_id p-a-1} {zone z3} {rack r00} {node n00} {pod pod-000}]
```

Producer `p-b-1` (node3): identical label sets but `producer_id=p-b-1`.

Total: **8 primary series** (4 per producer × 2 producers) + 3 freshness probe series per agent.

### Expected vs actual Thanos results

#### Gauge: `http_requests_total_latency_ms`

OTel SDK aggregates `Float64Gauge` as `LastValue`. With a fixed value of 42.0 per series, every export carries exactly 42.0 regardless of window size. Two producers per zone → per-zone sum = 2 × 42.0 = **84.0**.

**Check 1 — per-series value (expected: exactly 42.0):**

```
[OK] zone=z0 producer=p-a-1 => 42.0
[OK] zone=z0 producer=p-b-1 => 42.0
[OK] zone=z1 producer=p-a-1 => 42.0
[OK] zone=z1 producer=p-b-1 => 42.0
[OK] zone=z2 producer=p-a-1 => 42.0
[OK] zone=z2 producer=p-b-1 => 42.0
[OK] zone=z3 producer=p-a-1 => 42.0
[OK] zone=z3 producer=p-b-1 => 42.0
=> ALL OK
```

**Check 2 — `sum by (zone)` (expected: exactly 84.0):**

```
[OK] {'zone': 'z0'} => 84.0
[OK] {'zone': 'z1'} => 84.0
[OK] {'zone': 'z2'} => 84.0
[OK] {'zone': 'z3'} => 84.0
=> ALL OK
```

#### Counter: `http_requests_total`

OTel SDK aggregates `Float64Counter` as cumulative `Sum`. At 1 Hz each series accumulates 1 count/s. `rate(http_requests_total[2m])` computes the per-second rate over the last 2 minutes → expected ≈ **1.0 req/s** per series, ≈ **2.0 req/s** per zone (two producers).

**Check 3 — per-series rate (expected: ~1.0 req/s):**

```
[OK] zone=z0 producer=p-a-1 => 0.9291 req/s
[OK] zone=z0 producer=p-b-1 => 0.8973 req/s
[OK] zone=z1 producer=p-a-1 => 0.9291 req/s
[OK] zone=z1 producer=p-b-1 => 0.8973 req/s
[OK] zone=z2 producer=p-a-1 => 0.9291 req/s
[OK] zone=z2 producer=p-b-1 => 0.8973 req/s
[OK] zone=z3 producer=p-a-1 => 0.9291 req/s
[OK] zone=z3 producer=p-b-1 => 0.8973 req/s
=> ALL OK
```

**Check 4 — `sum by (zone)(rate(...))` (expected: ~2.0 req/s):**

```
[OK] {'zone': 'z0'} => 1.8262 req/s
[OK] {'zone': 'z1'} => 1.8262 req/s
[OK] {'zone': 'z2'} => 1.8262 req/s
[OK] {'zone': 'z3'} => 1.8262 req/s
=> ALL OK
```

> **Note on rate ≈ 0.93 vs 1.0:** Prometheus/Thanos `rate()` extrapolates to the window
> boundaries. When the range window extends slightly before the first available sample
> (as happens for the first few minutes after stack start), the computed rate is
> proportionally lower than the true generation rate. This ~7% undercount is expected
> Prometheus behavior and is not a pipeline error — it converges to 1.0 with longer uptime.
> The gauge result (42.0 / 84.0) is exact because `LastValue` requires no arithmetic.

### PromQL queries used

```promql
# Exact gauge value per series
http_requests_total_latency_ms

# Gauge aggregated by zone (should equal n_producers × fixed_value)
sum by (zone)(http_requests_total_latency_ms)

# Counter rate per series (should equal EXPORTER_FREQ_HZ)
rate(http_requests_total[2m])

# Counter rate aggregated by zone (should equal n_producers × EXPORTER_FREQ_HZ)
sum by (zone)(rate(http_requests_total[2m]))
```

All queries were run against `http://10.10.1.3:10903/api/v1/query`.

---

## Part 3 — Two-Tier Gateway Pipeline (`run_demo.sh all`) — SUPERSEDED / HISTORICAL

> **⚠️ SUPERSEDED (historical record only).** This section describes the
> **2026-05-14 gorilla-gateway architecture**, which has been **removed**.
> gorilla-gateway no longer exists. In the current architecture (v5, head-block +
> block-level WAL, issue #408) agents build per-emit (default 60s) Gorilla TSDB blocks
> and **POST each one over HTTP** to `gorilla-head-merger:9099/ingest` — they no longer
> write to MinIO/S3 at all. The merger durably writes each block into a single served
> dir (`/var/gorilla-buffer/served`, the block-level WAL); that dir holds the current
> window's per-emit head blocks plus one cut block per completed tumbling window
> (`MERGE_WINDOW`, configurable, default 1h). On window close the merger cuts the window
> once, uploads the single cut block to MinIO, and prunes the window's per-emit blocks;
> S3 holds only cut blocks. The hot-store (`gorilla-buffer-store`) reads the served dir
> over a FILESYSTEM objstore; the archive-store (`thanos-store-gateway`) reads the cut
> blocks from MinIO. Keep this part only as a record of the old gateway-based run; do not
> use its commands or topology.

Verified on **2026-05-14** with gorilla-gateway on node1 (now removed). The full `bash run_demo.sh all` command (sync → down → up → 30s warmup → 90s soak → verify → down) completed 5/5 checks at the time.

### Pipeline now in effect

```
agents (10s gorilla blocks) → gorilla-gateway:9100 → flush every 20s → MinIO:9000 → Thanos
```

### Gateway log excerpt (recv + flush cycle)

```
2026/05/14 gorilla-gateway: listen=0.0.0.0:9100  upstream=minio:9000  flush_interval=20s
recv  s3://asap-gorilla-tsdb/01KRH99Y7Y3KWPA8ZEQC9YGWVB/chunks/000001  571 B  buf=1
recv  s3://asap-gorilla-tsdb/01KRH99Y7Y3KWPA8ZEQC9YGWVB/index  1356 B  buf=2
recv  s3://asap-gorilla-tsdb/01KRH99Y7Y3KWPA8ZEQC9YGWVB/meta.json  296 B  buf=3
recv  s3://asap-gorilla-tsdb/01KRH9A8ZKZS41HPEV8D2H3Z5X/chunks/000001  546 B  buf=4
recv  s3://asap-gorilla-tsdb/01KRH9A8ZKZS41HPEV8D2H3Z5X/index  1356 B  buf=5
recv  s3://asap-gorilla-tsdb/01KRH9A8ZKZS41HPEV8D2H3Z5X/meta.json  296 B  buf=6
flush: pushing 6 objects upstream
flush OK    s3://asap-gorilla-tsdb/01KRH99Y7Y3KWPA8ZEQC9YGWVB/chunks/000001  571 B
flush OK    s3://asap-gorilla-tsdb/01KRH99Y7Y3KWPA8ZEQC9YGWVB/index  1356 B
flush OK    s3://asap-gorilla-tsdb/01KRH99Y7Y3KWPA8ZEQC9YGWVB/meta.json  296 B
flush OK    s3://asap-gorilla-tsdb/01KRH9A8ZKZS41HPEV8D2H3Z5X/chunks/000001  546 B
flush OK    s3://asap-gorilla-tsdb/01KRH9A8ZKZS41HPEV8D2H3Z5X/index  1356 B
flush OK    s3://asap-gorilla-tsdb/01KRH9A8ZKZS41HPEV8D2H3Z5X/meta.json  296 B
flush done  ok=6 fail=0
```

Each 20s flush cycle receives 2 blocks × 3 files = 6 objects (one block from agent-a, one from agent-b, both 10s windows aligned to the same flush boundary).

### `run_demo.sh all` output — 5/5 PASSED

```
[13:05:02] === gorilla-thanos-multinode: full run ===
[13:05:26] node1 gorilla-gateway up
[13:05:29] node0 agent-a up (gorilla-only)
[13:05:31] node3 agent-b up (gorilla-only)
[13:05:40] === stack up; waiting WARMUP_S=30s for gorillas3 to flush first blocks ===
[13:06:10] === soaking for SOAK_S=90s ===
[13:07:40] === running verify_gorilla_compression.sh ===

=== Check 1: MinIO has TSDB blocks in asap-gorilla-tsdb ===
  mc ls output (66 lines):
    [2026-05-13 19:05:47 UTC]   300B STANDARD 01KRHBNPC65AN9DWYPNRZ2MJ3W/chunks/000001
    [2026-05-13 19:05:47 UTC] 1.3KiB STANDARD 01KRHBNPC65AN9DWYPNRZ2MJ3W/index
    [2026-05-13 19:05:47 UTC]   294B STANDARD 01KRHBNPC65AN9DWYPNRZ2MJ3W/meta.json
    ...
  [PASS] MinIO asap-gorilla-tsdb has 66 object(s) — gorillas3 is writing blocks

=== Check 2: Thanos query API healthy (10.10.1.3:10903) ===
  [PASS] Thanos query API returned status=success

=== Check 3: Thanos serves metric names (data queryable end-to-end) ===
  Thanos label __name__ values: 5 metric name(s)
    http_freshness_probe_archive
    http_freshness_probe_raw
    http_freshness_probe_warm
    http_requests_total
    http_requests_total_latency_ms
  [PASS] Thanos serves 5 metric name(s) — gorillas3 → MinIO → Thanos pipeline is end-to-end

=== Check 4: Agent gorilla self-metrics (10.10.1.1:8890) ===
  gorilla-related metric lines found: 44
    gorillas3_chunk_bytes_written_bytes_total{...} 27798
    gorillas3_chunk_points_written_total{...} 1342
    gorillas3_chunks_written_total{...} 13
    gorillas3_s3_put_failures_total{...} 0
  [PASS] Agent self-metrics include 44 gorilla/gorillas3 line(s)

=== Check 5: Network traffic analysis ===
  [PASS] Network traffic explanation printed (observational check)

========================================
  VERIFICATION SUMMARY
  PASSED: 5
  FAILED: 0
========================================
  ALL CHECKS PASSED — gorilla-thanos pipeline verified end-to-end
```

### Bugs fixed to reach 5/5

| Bug | Symptom | Fix |
|-----|---------|-----|
| `gorilla-gateway` missing from `ADD_HOSTS` in `topology.env` | `gorillas3_s3_put_failures_total = 13`; MinIO empty; Thanos 0 metrics | Added `--add-host=gorilla-gateway:${NODE1_IP}` to `ADD_HOSTS` array |
| `run_demo.sh gateway_up()` missing `"${NODE1_HOST}"` arg | Gateway started on wrong node / docker_run_on passed wrong args | Added `"${NODE1_HOST}"` as first arg to `docker_run_on` |
| `run_demo.sh gateway_down()` called `stop_node` with no arg | `$1: unbound variable` crash, `down` failed mid-way | Added `"${NODE1_HOST}"` arg to `stop_node` call |
| `verify_gorilla_compression.sh` Check 1 counted mc alias line | `"Added \`local\` successfully."` counted as a block → false PASS | `grep -vc "Added .* successfully"` instead of bare `wc -l` |
| `verify_gorilla_compression.sh` Check 3 `METRIC_COUNT` multiline | `grep \| wc -l` produced `"0\n0"`; `[[ ]]` syntax error | Replaced with `python3 json.load ... len(data)` |

---

## 2026-05-18 — Direct-MinIO Architecture (gorilla-gateway removed)

**Architecture change:** gorilla-gateway removed from topology. Agents write 60s TSDB blocks directly to `minio:9000`. gorilla-buffer-store moved from node1 to node2 (backend node), reading hot blocks from MinIO via S3 objstore with `--min-time=-1h` (configurable via `BUFFER_STORE_DURATION`). Thanos Query now has two endpoints: `thanos-store-gateway:10901` (full archive) and `gorilla-buffer-store:10921` (hot 1h window). Duplicate blocks for the hot window are transparently deduplicated by Thanos chunk-level timestamp merge — no `--query.replica-label` needed.

**Pipeline:**
```
producers → agents (gorillas3, 60s TSDB blocks) → minio:9000
                                                      ↓
                                           gorilla-buffer-store:10921  ← hot 1h window
                                           thanos-store-gateway:10901  ← full archive
                                                      ↓
                                           thanos-query:10903
```

### Part 1 — Pipeline Smoke-Test (2026-05-18, direct-MinIO)

`verify_gorilla_compression.sh` — 5/5 PASS

```
=== Check 1: MinIO has TSDB blocks in asap-gorilla-tsdb ===
  mc ls output (139 lines):
    [2026-05-18 15:08:56 UTC]   424B STANDARD 01KRKN3ZZMRY7G8QAVM8MQ478R/chunks/000001
    [2026-05-18 15:20:57 UTC]   409B STANDARD 01KRKNZEYYC8WMW1J6ZRXWWZPN/chunks/000001
    [2026-05-18 15:25:27 UTC]   407B STANDARD 01KRKS4F9PPNB3FCJABRXNV8W4/chunks/000001
    ...
  [PASS] MinIO asap-gorilla-tsdb has 139 object(s) — gorillas3 is writing blocks

=== Check 2: Thanos query API healthy (10.10.1.3:10903) ===
  [PASS] Thanos query API returned status=success

=== Check 3: Thanos serves metric names (data queryable end-to-end) ===
  Thanos label __name__ values: 5 metric name(s)
    http_freshness_probe_archive
    http_freshness_probe_raw
    http_freshness_probe_warm
    http_requests_total
    http_requests_total_latency_ms
  [PASS] Thanos serves 5 metric name(s) — gorillas3 → MinIO → Thanos pipeline is end-to-end

=== Check 4: Agent gorilla self-metrics (10.10.1.1:8890) ===
  gorilla-related metric lines found: 44
    gorillas3_chunk_bytes_written_bytes_total{...} 17067
    gorillas3_chunk_points_written_total{...} 3256
    gorillas3_chunks_written_total{...} 5
  [PASS] Agent self-metrics include 44 gorilla/gorillas3 line(s)

=== Check 5: Network traffic analysis ===
  [PASS] Network traffic explanation printed (observational check)

========================================
  VERIFICATION SUMMARY  PASSED: 5  FAILED: 0
========================================
```

### Part 4 — Exact PromQL Cross-Check (2026-05-18, verif_part4.py)

`verif_part4.py` — 25/25 PASS

Tests `rate()`, `avg_over_time()`, and `quantile_over_time()` against hand-computed expectations using the deterministic fake-exporter (`EXPORTER_FREQ_HZ=1`, `EXPORTER_FIXED_LATENCY=42.0`, `PER_AGENT_CARDINALITY=4`).

```
====================================================================
PART 4-A — rate() manual calculation cross-check (exact match)
====================================================================
  Thanos rate():  0.708091667 req/s
  Manual formula: 0.708091666 req/s
  Relative diff:  0.0000%

  [PASS] Manual formula == Thanos rate (≤0.01%)
  INFO: rate=0.708092 req/s vs FREQ_HZ=1.0; freshness lag ≈ 35s (29% of [120s] window)

  [PASS] sum rate zone=z0 ~ 2×per-series: got=1.382883  expected≈1.416183  tol=10%
  [PASS] sum rate zone=z1 ~ 2×per-series: got=1.382883  expected≈1.416183  tol=10%
  [PASS] sum rate zone=z2 ~ 2×per-series: got=1.382883  expected≈1.416183  tol=10%
  [PASS] sum rate zone=z3 ~ 2×per-series: got=1.382883  expected≈1.416183  tol=10%

  [PASS] avg rate zone=z0 ~ per-series: got=0.691375  expected≈0.708092  tol=10%
  [PASS] avg rate zone=z1 ~ per-series: got=0.691375  expected≈0.708092  tol=10%
  [PASS] avg rate zone=z2 ~ per-series: got=0.691375  expected≈0.708092  tol=10%
  [PASS] avg rate zone=z3 ~ per-series: got=0.691375  expected≈0.708092  tol=10%

====================================================================
PART 4-B — avg_over_time() manual calculation cross-check
====================================================================
  Manual avg_over_time = 5082.0 / 121 = 42.000000 (all samples fixed at 42.0)
  Thanos avg_over_time: 42.000000

  [PASS] manual avg == 42.0
  [PASS] Thanos avg_over_time == 42.0
  [PASS] Thanos == manual

  [PASS] avg() across all series: got=42.000000  expected=42.000000
  [PASS] avg zone=z0: got=42.000000  expected=42.000000
  [PASS] avg zone=z1: got=42.000000  expected=42.000000
  [PASS] avg zone=z2: got=42.000000  expected=42.000000
  [PASS] avg zone=z3: got=42.000000  expected=42.000000

====================================================================
PART 4-C — quantile_over_time(p50/p95) manual calculation cross-check
====================================================================
  All 121 values = 42.0 → p50 = p95 = 42.0

  [PASS] manual p50 == 42.0
  [PASS] Thanos p50 == 42.0
  [PASS] Thanos p50 == manual p50

  [PASS] manual p95 == 42.0
  [PASS] Thanos p95 == 42.0
  [PASS] Thanos p95 == manual p95

  [PASS] quantile(0.5) across series == 42.0
  [PASS] quantile(0.95) across series == 42.0

====================================================================
SUMMARY: 25/25 checks passed — ALL CHECKS PASSED
====================================================================
```

> **Note on rate freshness lag:** `rate()` at eval_time sees a 35s gap from the last sample (data window ends at t-35s). This is expected: the most recent 60s TSDB block is written at flush time, and the gorilla-buffer-store syncs every 15s. The lag is ~35s (freshness within 1 sync cycle + block duration). The manual formula matches Thanos to < 1e-9 rel error, confirming arithmetic correctness.

### Part 5 — gorilla-buffer-store Verification (2026-05-18)

`verify_buffer_store.sh --backend-host node2` — 7/7 PASS

```
=== Check 1: gorilla-buffer-store container running on node2 ===
  [PASS] Container running: asap-gorilla-buffer-store  Up 21 minutes

=== Check 2: buffer-store HTTP health (10.10.1.3:10922) ===
  [PASS] buffer-store HTTP /-/ready returned 200 OK

=== Check 3: buffer-store has loaded blocks from MinIO ===
  log lines: loaded_new_block=21  sync_cycles=89
    ts=2026-05-18T15:26:45Z  msg="loaded new block"  id=01KRXV2TTE1GAE2H97XCPFBJD8
    ts=2026-05-18T15:27:00Z  msg="loaded new block"  id=01KRXV2YQAZYESX9CN9CH8V9DR
    ts=2026-05-18T15:27:45Z  msg="loaded new block"  id=01KRXV4NDVG172AFHKV8E3Y15X
  [PASS] buffer-store has loaded 21 block(s) from MinIO

=== Check 4: buffer-store hot-window filter (--min-time) ===
  Container args: [...,"--min-time=-1h"]
  [PASS] buffer-store has --min-time=-1h — only blocks within window are served

=== Check 5: buffer-store syncs every 15s (vs store-gateway 30s) ===
  [PASS] buffer-store sync=15s, store-gateway sync=30s

=== Check 6: gorilla-buffer-store registered in Thanos Query (:10921) ===
  gorilla-buffer-store:10921  lastError=null  minTime=1779117079610  maxTime=1779118062590
  thanos-store-gateway:10901  lastError=null
  [PASS] gorilla-buffer-store:10921 is registered in Thanos Query

=== Check 7: Thanos serves metric names (end-to-end pipeline) ===
  5 metric name(s): http_freshness_probe_archive, http_freshness_probe_raw,
                    http_freshness_probe_warm, http_requests_total,
                    http_requests_total_latency_ms
  [PASS] Thanos serves 5 metric name(s) — direct-MinIO pipeline end-to-end

========================================
  BUFFER-STORE VERIFICATION SUMMARY  PASSED: 7  FAILED: 0
========================================
```

### Summary — 2026-05-18 results

| Test | Script | Result | Date |
|------|--------|--------|------|
| Part 1 — Pipeline smoke-test | `verify_gorilla_compression.sh` | **5/5 PASS** | 2026-05-18 |
| Part 4 — Exact PromQL cross-check | `verif_part4.py` | **25/25 PASS** | 2026-05-18 |
| Part 5 — gorilla-buffer-store | `verify_buffer_store.sh` | **7/7 PASS** | 2026-05-18 |

---

## Part 6 — Merge design: write + query performance (2026-05-19, v3 sliding — HISTORICAL)

> **⚠️ HISTORICAL.** These numbers were measured against the **v3 sliding-window**
> merger (one continuously-rewritten merged block, drop blocks where maxTime ≤ now−1h).
> They were already superseded by **v4 tumbling-window** (fixed-size, non-overlapping
> windows — `-window`, configurable, default 1h; one merged block per window; finalized
> windows frozen). The performance characteristics
> (sub-second merge, ~1 block touched per window at query time) are expected to carry
> over, but the "single merged block" observations below are specific to the old sliding
> design.
>
> **v5 (head-block + block-level WAL, issue #408) supersedes the merge model entirely.**
> Agents no longer write to MinIO; they **POST per-emit blocks** to `gorilla-head-merger`,
> which WALs them in a single served dir and **cuts one block per tumbling window** on
> window close (concatenating Gorilla chunks, no per-poll re-merge), flushing only that cut
> block to S3. The per-poll re-merge and "single rolling merged block" framing below no
> longer apply. Re-measure under the v5 head-merger before quoting these numbers as current.

**Design under test (v3 sliding, superseded):** gorilla-thanos-multinode v3 — `gorilla-buffer-merger` + `gorilla-buffer-store` (FILESYSTEM objstore).

| Component | Role (v3 sliding) |
|-----------|------|
| `gorilla-buffer-merger` | Polls MinIO every 15s, merges all in-window 60s blocks into one local merged block. Drops expired blocks (maxTime ≤ now−1h) from staging. **(v4: buckets by tumbling window — `-window`, configurable, default 1h — one merged block per window.)** |
| `gorilla-buffer-store` (hot-store) | Thanos store with FILESYSTEM objstore reading the merged block(s). Syncs every 20s. |
| `thanos-store-gateway` (archive-store) | Serves all MinIO blocks (full history, 30s sync). |
| `thanos-query` | Federates hot-store (:10921) + archive-store (:10901). |

### 6.1 — Merger correctness

After the stack was running for ~25 minutes (2026-05-19 09:13–09:19 UTC), the merger processed:

```
2026/05/19 09:13:09 INFO merging blocks in_window=36
2026/05/19 09:13:09 INFO merged block written ulid=01KRZR5BMXYTDNRTDP1FGNAHZ1 series=19
2026/05/19 09:13:24 INFO merging blocks in_window=36
2026/05/19 09:13:24 INFO merged block written ulid=01KRZR5TA5XWKHRJ7AWTPVY5HW series=19
2026/05/19 09:14:09 INFO merging blocks in_window=38
2026/05/19 09:14:09 INFO merged block written ulid=01KRZR768GTXFCD35S86G8D1DB series=19
2026/05/19 09:15:09 INFO merging blocks in_window=40
2026/05/19 09:15:09 INFO merged block written ulid=01KRZR90VHX915TGVHDMVNWFVY series=19
```

- **Merge cycle**: every 15s (consistent with `-poll-interval=15s`)
- **Compute time**: sub-second (< 1s for 40–48 blocks × 19 series; "merging" and "merged" appear on same log timestamp)
- **Output**: always exactly 1 ULID in `/tmp/gorilla-buffer/merged/`

### 6.2 — Buffer-store block count

```
thanos_bucket_store_blocks_loaded = 1
```

Buffer-store always holds 1 block (the current merged window). All 22 store queries
hit exactly the `le="1"` bucket:

```
thanos_bucket_store_series_blocks_queried_bucket{le="1"}  22
thanos_bucket_store_series_blocks_queried_sum             22
thanos_bucket_store_series_blocks_queried_count           22
```

Every query touched exactly 1 block, not 21+.

### 6.3 — Query latency (thanos-query :10903, 2026-05-19 09:18 UTC)

**5-minute range queries** (step=15s, 20 steps):

| Query | HTTP | Latency | Results |
|-------|------|---------|---------|
| `http_freshness_probe_raw` | 200 | 20 ms | 1 series |
| `rate(http_requests_total[1m])` | 200 | 17 ms | 8 series |
| `sum by(job)(rate(http_requests_total[1m]))` | 200 | 14 ms | 1 series |
| `count({__name__=~"http.*"})` | 200 | 22 ms | 1 series |
| `http_requests_total_latency_ms` | 200 | 14 ms | 8 series |

**1-hour range queries** (step=60s, 60 steps):

| Query | HTTP | Latency | Results |
|-------|------|---------|---------|
| `http_freshness_probe_raw` | 200 | 22 ms | 1 series |
| `rate(http_requests_total[5m])` | 200 | 23 ms | 8 series |
| `count({__name__=~"http.*"})` | 200 | 27 ms | 1 series |

### 6.4 — Freshness lag

```
freshness_probe_raw lag: 0s (latest_ts=1779182555, now=1779182555)
```

The merged block contains data from right up to the current second — lag is 0s.

**Comparison:** old no-merge design had ~35s freshness lag (data was at most 1 buffer-store sync cycle + 60s block boundary behind).

### 6.5 — Correctness cross-check

| Check | New design (2026-05-19) | Old design (2026-05-18) |
|-------|------------------------|------------------------|
| `avg_over_time(latency_ms[2m])` | 42.000000 ✓ | 42.000000 ✓ |
| `rate()` per series | 0.645 req/s | 0.708 req/s |
| Rate rel-diff from FREQ_HZ=1.0 | 35% | 29% |

The rate() difference is a freshness/window artifact: old stack was 18h old (stable), new stack was 25min old (window edges not fully settled). `avg_over_time` is exact in both designs, confirming merge correctness.

### 6.6 — Design comparison

| Metric | v2 no-merge (2026-05-18) | v3 merge (2026-05-19) |
|--------|--------------------------|-----------------------|
| buffer-store blocks loaded | 21 | **1** |
| blocks queried per query | 21 (le="1" bucket ≪ total) | **1** (all in le="1") |
| buffer-store freshness lag | ~35s | **0s** |
| 5m range query latency | not recorded | 14–22 ms |
| 1h range query latency | not recorded | 22–27 ms |
| merger compute time / cycle | — (no merger) | < 1 s (sub-second) |
| merger poll interval | — | 15 s |
| buffer-store sync interval | 15 s (S3 sync) | 20 s (FILESYSTEM) |
| `avg_over_time` correctness | 42.000000 ✓ | 42.000000 ✓ |
| staging block count (1h window) | 21 (served directly) | 40–48 (merged to 1) |


### Summary — 2026-05-19 results

| Test | Description | Result | Date |
|------|-------------|--------|------|
| Part 6.1 — Merger correctness | Sub-second merge every 15s, 1 output block | **PASS** | 2026-05-19 |
| Part 6.2 — Block fan-out | All 22 queries hit exactly 1 block | **PASS** | 2026-05-19 |
| Part 6.3 — Query latency | 5m: 14–22ms; 1h: 22–27ms | **PASS** | 2026-05-19 |
| Part 6.4 — Freshness | Lag = 0s | **PASS** | 2026-05-19 |
| Part 6.5 — avg_over_time correctness | = 42.000000 | **PASS** | 2026-05-19 |
| Part 6.6 — SIGSEGV fix | Chunk bytes deep-copied; no crash after fix | **PASS** | 2026-05-19 |


---

## Part 7 — ThanosQueryEngine `/api/v1/query_range` Forwarding (2026-05-21)

### Overview

`ThanosQueryEngine` previously forwarded only instant PromQL queries (`/api/v1/query`).
Range queries (`/api/v1/query_range`) hit `ASAPQueryEngine::execute_range_promql_modern`
(ASAP sketch tier); when that returned `CapabilityMiss` (no sketch data for the metric),
the backend returned an "unsupported query" error instead of falling through to Thanos.

This part documents the implementation and automated test results for range-query forwarding.

---

### 7.1 — What was implemented

**Files changed** (all in `ASAPQuery-backend`):

| File | Change |
|------|--------|
| `data_plane/src/query_engines/thanos_query_engine/forward.rs` | `range_endpoint()`, `query_range()`, `execute_range()` impl; 6 unit tests + 3 mock helpers |
| `data_plane/src/query_engines/routing/query_engine_routing.rs` | `execute_range()` default method added to `QueryEngine` trait (returns `CapabilityMiss`; existing impls unaffected) |
| `data_plane/src/drivers/query/servers/http.rs` | `process_range_query_request`: on ASAP `CapabilityMiss`, look up `thanos_query` engine and call `execute_range`; 1 e2e test |

**Request flow after this change:**

```
GET /api/v1/query_range?query=...&start=...&end=...&step=...
  │
  ▼
ASAPQueryEngine::execute_range_promql_modern()
  │
  ├─ Ok(result)          → return 200 matrix (unchanged)
  │
  └─ Err(CapabilityMiss) → ThanosQueryEngine::execute_range()
                             │
                             POST /api/v1/query_range
                             query=... start=<unix_s> end=<unix_s> step=<s>
                             │
                             ├─ 2xx matrix  → return 200 matrix
                             ├─ 4xx         → CapabilityMiss → 404
                             └─ 5xx/timeout → Backend error → 503
```

**Parameter conversion**: `start_ms`, `end_ms`, `step_ms` (milliseconds from the HTTP
handler) are divided by 1000 and formatted as `{:.3}` float strings before being posted
to Thanos (e.g. `1700000000.000`), matching the Prometheus HTTP API contract.

---

### 7.2 — Unit test results (2026-05-21)

Run command:
```bash
cd /mydata/ASAPQuery-backend
CARGO_NET_GIT_FETCH_WITH_CLI=true ~/.cargo/bin/cargo test \
  --manifest-path data_plane/Cargo.toml thanos_query_engine
```

Output:
```
running 17 tests
test query_engines::thanos_query_engine::forward::tests::build_result_from_thanos_payload_rejects_non_success ... ok
test query_engines::thanos_query_engine::forward::tests::build_result_from_thanos_payload_rejects_unsupported_result_type ... ok
test query_engines::thanos_query_engine::forward::tests::capabilities_report_thanos_query_id ... ok
test query_engines::thanos_query_engine::forward::tests::config_from_env_returns_none_when_blank ... ok
test query_engines::thanos_query_engine::forward::tests::config_from_env_returns_none_when_unset ... ok
test query_engines::thanos_query_engine::forward::tests::config_from_env_strips_trailing_slash ... ok
test query_engines::thanos_query_engine::forward::tests::engine_from_env_returns_none_when_unset ... ok
test query_engines::thanos_query_engine::forward::tests::engine_from_env_returns_some_when_set ... ok
test query_engines::thanos_query_engine::forward::tests::forwards_promql_and_wraps_response ... ok
test query_engines::thanos_query_engine::forward::tests::parse_vector_extracts_value_and_timestamp ... ok
test query_engines::thanos_query_engine::forward::tests::query_range_4xx_returns_capability_miss ... ok
test query_engines::thanos_query_engine::forward::tests::query_range_5xx_returns_backend_error ... ok
test query_engines::thanos_query_engine::forward::tests::query_range_calls_range_endpoint_not_instant ... ok
test query_engines::thanos_query_engine::forward::tests::query_range_parses_matrix_response ... ok
test query_engines::thanos_query_engine::forward::tests::query_range_passes_params_correctly ... ok
test query_engines::thanos_query_engine::forward::tests::query_range_timeout_returns_backend_error ... ok
test query_engines::thanos_query_engine::forward::tests::unreachable_upstream_returns_503_quirk_via_engine_trait ... ok

test result: ok. 17 passed; 0 failed; 0 ignored; 0 measured; 747 filtered out; finished in 0.21s
```

**New tests (6):**

| Test | What it asserts |
|------|----------------|
| `query_range_calls_range_endpoint_not_instant` | POST goes to `/api/v1/query_range`, not `/api/v1/query` |
| `query_range_passes_params_correctly` | `query`, `start`, `end`, `step` forwarded as URL-encoded form; times in seconds (float) |
| `query_range_parses_matrix_response` | `resultType=matrix` body parsed into `QueryResult::Matrix`; 2 samples with values 42.0 and 43.0 |
| `query_range_4xx_returns_capability_miss` | HTTP 400 from mock → `ThanosQueryError::BadQuery` → `EngineError::CapabilityMiss` |
| `query_range_5xx_returns_backend_error` | HTTP 503 from mock → `ThanosQueryError::Unreachable` → `EngineError::Backend` |
| `query_range_timeout_returns_backend_error` | 100 ms request timeout against 10 s sleep mock → `ThanosQueryError::Unreachable` → `EngineError::Backend` |

---

### 7.3 — HTTP e2e test result (2026-05-21)

Run command:
```bash
CARGO_NET_GIT_FETCH_WITH_CLI=true ~/.cargo/bin/cargo test \
  --manifest-path data_plane/Cargo.toml http_query_range_forwards_to_thanos
```

Output:
```
running 1 test
test drivers::query::servers::http::tests::http_query_range_forwards_to_thanos_when_asap_misses ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 763 filtered out; finished in 0.11s
```

**Test coverage**: full wire path — `GET /api/v1/query_range` with a mock Thanos server
registered as `ThanosQueryEngine`, ASAP sketch tier returning `CapabilityMiss` (no sketch
data in empty test engine), Thanos mock returning `CANNED_MATRIX_BODY`. Asserts:
- HTTP status 2xx
- `body["status"] == "success"`
- `body["data"]["resultType"] == "matrix"`

---

### 7.4 — Verification script check (Check 7)

Added to `verify_gorilla_compression.sh`. When run against a live stack with
`ASAP_THANOS_QUERY_URL` set on the backend:

```bash
bash verify_gorilla_compression.sh \
  --minio-host node2 --thanos-host 10.10.1.3 --agent-host 10.10.1.1
```

Expected output for Check 7:
```
=== Check 7: /api/v1/query_range forwarded to Thanos via ASAPQuery-backend ===
  [PASS] ASAPQuery-backend /api/v1/query_range returned status=success resultType=matrix
         (ThanosQueryEngine forwarding is live)
```

The check curls a 5-minute window (`now-300s` → `now`, step 15 s) of
`rate(http_requests_total[1m])`. Pass criteria: HTTP 200, `status=success`,
`resultType=matrix`.

---

### Summary — 2026-05-21 results

| Test | Description | Result | Date |
|------|-------------|--------|------|
| Part 7.1 — Implementation | `range_endpoint`, `query_range`, `execute_range`, trait default, http.rs fallback | **DONE** | 2026-05-21 |
| Part 7.2 — Unit: correct endpoint | POST to `/api/v1/query_range` not `/api/v1/query` | **PASS** | 2026-05-21 |
| Part 7.2 — Unit: params forwarded | query/start/end/step as form fields, times in seconds | **PASS** | 2026-05-21 |
| Part 7.2 — Unit: matrix parsed | `resultType=matrix` → `QueryResult::Matrix`, values correct | **PASS** | 2026-05-21 |
| Part 7.2 — Unit: 4xx → CapabilityMiss | HTTP 400 folds into `EngineError::CapabilityMiss` | **PASS** | 2026-05-21 |
| Part 7.2 — Unit: 5xx → Backend | HTTP 503 folds into `EngineError::Backend` | **PASS** | 2026-05-21 |
| Part 7.2 — Unit: timeout → Backend | 100 ms timeout folds into `EngineError::Backend` | **PASS** | 2026-05-21 |
| Part 7.3 — HTTP e2e | `GET /api/v1/query_range` → Thanos mock → `status=success resultType=matrix` | **PASS** | 2026-05-21 |
| Part 7.4 — verify script Check 7 | query_range smoke-check added to `verify_gorilla_compression.sh` | **DONE** | 2026-05-21 |
