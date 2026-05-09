# Serf Compression — Architecture & Integration

This document describes where Serf compression is inserted in the
DataCollector pipeline, what each component does, and where decompression
is implemented.

---

## Pipeline overview

### Mode 1 — Local archival (serfprocessor)

```
┌──────────────────────────────────────────────────────────────────────┐
│  Instrumentation layer (SDK)                                         │
│                                                                      │
│  • OTel SDK / Prometheus client / Influx Line Protocol emitter       │
│  • Emits raw Gauge / Sum data points                                 │
│  • NO Serf compression here — raw float64 values leave the SDK       │
└────────────────────────────┬─────────────────────────────────────────┘
                             │ OTLP/gRPC  (raw metrics)
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Agent / Local collector  (OTel Collector — custom build via OCB)   │
│                                                                      │
│  Receiver  →  [serfprocessor]  →  Exporter                          │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ serfprocessor  (processor/serfprocessor/)                   │    │
│  │                                                             │    │
│  │  compression: "xor"  ── SerfXOR                            │    │
│  │    • Buffers Gauge/Sum points in a tumbling window          │    │
│  │    • Per value: FindAppLong() searches [v-ε, v+ε] for      │    │
│  │      a float64 bit-pattern that maximises leading zeros     │    │
│  │      in XOR with the previous stored value                  │    │
│  │    • Timestamps: Gorilla delta-of-delta                     │    │
│  │    • Values: Serf XOR with adaptive leading/trailing tables │    │
│  │    • Output: SERF1 binary objects (*.serf files)            │    │
│  │                                                             │    │
│  │  compression: "qt"   ── SerfQt                             │    │
│  │    • Buffers Gauge/Sum points in a tumbling window          │    │
│  │    • Per value: q = round((v - prevValue) / (2·maxDiff))   │    │
│  │      recoverValue = prevValue + 2·maxDiff·q                │    │
│  │    • Encodes q via ZigZag then Elias Gamma coding           │    │
│  │    • Output: SERF1 binary objects (*.serf files)            │    │
│  │                                                             │    │
│  │  Both modes flush on a configurable window_interval (10s   │    │
│  │  in bench configs) and write to local disk and/or S3.      │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  Original metrics continue downstream (drop_original: false)        │
└──────────┬──────────────────────────┬───────────────────────────────┘
           │ raw metrics (Prometheus) │ SERF1 blocks
           │ forwarded downstream     │
           ▼                          ▼
┌──────────────────┐        ┌──────────────────────────────────────────┐
│  Backend /       │        │  Object storage                          │
│  TSDB            │        │                                          │
│  (Prometheus,    │        │  • Local filesystem: /tmp/serf-bench/    │
│   VictoriaM,     │        │    or /tmp/serf-qt-bench/                │
│   etc.)          │        │  • S3: configurable bucket/prefix        │
│                  │        │                                          │
│  Stores the raw  │        │  SERF1 object layout (per series chunk): │
│  metric stream   │        │    "SERF1" (5 B) + version (1 B)         │
│  for querying    │        │    + series count (4 B)                   │
└──────────────────┘        │    + per series:                         │
                            │      metadata JSON (len u16 + bytes)     │
                            │      point count (u32)                   │
                            │      first timestamp (u64, UnixNano)     │
                            │      first value bits (u64, IEEE 754)    │
                            │      timestamp bit-stream length + bytes │
                            │      value bit-stream length + bytes     │
                            └──────────────────────────────────────────┘
```

### Mode 2 — Network transmission (serfexporter + serfreceiver)

```
┌──────────────────────────────────────────────────────────────────────┐
│  Load Generator  (otel_collector_benchmark / fakemetricload)         │
│  Emits raw Gauge metrics over OTLP/gRPC → agent port 4317           │
└─────────────────────────────┬────────────────────────────────────────┘
                              │ OTLP/gRPC  (raw metrics)
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Agent Collector  (cmd/serfagentcol — OCB build)                     │
│                                                                      │
│  otlpreceiver (4317) → [batch] → serfhttp exporter                  │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ serfhttp exporter  (exporter/serfexporter/)                 │    │
│  │                                                             │    │
│  │  • Buffers Gauge/Sum points in a tumbling window (10s)      │    │
│  │  • On flush: builds SERF1 binary object (XOR or Qt)        │    │
│  │  • HTTP POST compressed bytes to backend:9000/serf          │    │
│  │  • Records serf_exporter_bytes_sent_total counter           │    │
│  │    (exposed at agent telemetry port 8888)                   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  Telemetry: http://localhost:8888/metrics                            │
└──────────────────────────────┬───────────────────────────────────────┘
                               │ HTTP POST  (SERF1 compressed bytes)
                               ▼  port 9000
┌──────────────────────────────────────────────────────────────────────┐
│  Backend Collector  (cmd/serfbackendcol — OCB build)                 │
│                                                                      │
│  serfhttp receiver (9000) → [batch] → prometheus exporter (8891)    │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ serfhttp receiver  (receiver/serfreceiver/)                 │    │
│  │                                                             │    │
│  │  • HTTP server accepts SERF1 blobs on /serf                 │    │
│  │  • Decompresses:                                            │    │
│  │      Timestamps: inverse Gorilla delta-of-delta             │    │
│  │      Values XOR: inverse Serf XOR (3-case bit decode)       │    │
│  │      Values Qt:  EliasGamma decode → ZigZag decode → q      │    │
│  │                  recoverValue = prevValue + step·q           │    │
│  │  • Reconstructs pmetric.Metrics (Gauge data points)        │    │
│  │  • Records serf_receiver_bytes_received_total counter       │    │
│  │  • Records serf_receiver_decoded_points_total counter       │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
│  Telemetry: http://localhost:8890/metrics                            │
│  Prometheus scrape: http://localhost:8891/metrics                    │
└──────────────────────────────────────────────────────────────────────┘
```

---

## What is compressed vs what is not

| Layer | Serf applied? | Notes |
|---|---|---|
| SDK (instrumentation) | ❌ | Raw float64 emitted over OTLP |
| Agent collector — receive | ❌ | otlpreceiver decodes OTLP normally |
| Agent collector — serfprocessor | ✅ **COMPRESS** | Tumbling-window accumulation + Serf XOR or Qt encoding → local/S3 write |
| Agent collector — serfhttp exporter | ✅ **COMPRESS** | Tumbling-window accumulation + Serf XOR or Qt encoding → HTTP POST to backend |
| Agent collector — export (downstream) | ❌ | Original metrics pass through to Prometheus exporter |
| Backend collector — serfhttp receiver | ✅ **DECOMPRESS** | Receives SERF1 blobs, decodes back to pmetric.Metrics |
| Backend collector — export (downstream) | ❌ | Decoded metrics forwarded to Prometheus exporter |
| Object storage (S3 / local disk) | ✅ **SERF1 objects** | Compressed blocks written on each window flush (mode 1 only) |

---

## Compression modes compared

| Mode | Algorithm | Error type | Typical use |
|---|---|---|---|
| `xor` | SerfXOR: FindAppLong + XOR bit-packing | Bounded absolute (`max_diff`) | Sensor time-series; comparable to Gorilla but better CR |
| `qt` | SerfQt: quantization delta + ZigZag + Elias Gamma | Bounded absolute (`max_diff`) | Uniform or slowly-varying signals; very low CPU overhead |
| *(Gorilla, separate processor)* | Gorilla XOR (delta-of-delta timestamps, XOR values) | Lossless | Baseline reference |

---

## Decompression — where it is implemented

| Location | Implementation |
|---|---|
| **serfhttp receiver** (online path) | `receiver/serfreceiver/decoder.go` — `decodeSERF1()` parses the binary object header, then calls `decodeTimestamps()` (inverse delta-of-delta) and `decodeValues()` (inverse XOR or Qt bitstream). Reconstructed `pmetric.Metrics` forwarded to the pipeline's next consumer. |
| **Object storage reader** (offline batch) | Not yet implemented. A standalone Go/Python tool would call `decodeSERF1()` on local `.serf` files. The SERF1 format is self-describing — no external schema needed. |
| **Backend query path** | If a TSDB natively ingests SERF1 (e.g. custom remote-write endpoint), decompression would live inside the write handler using the same decode logic. |

### SerfXOR decode algorithm

```
storedVal = firstValBits (from object header)
storedLeading = 0, storedTrailing = 0

for each subsequent value:
  read 1 bit:
    1  →  case-1 (reuse window):
           centerBits = 64 - storedLeading - storedTrailing
           XOR = readBits(centerBits) << storedTrailing
           value = Float64frombits(storedVal ^ XOR)
    0  →  read 1 more bit:
           1  →  case-01: value = Float64frombits(storedVal)  [XOR=0]
           0  →  case-00 (new window):
                  leadCode  = readBits(3)  → leading  = xorLeadingRepr[leadCode]
                  trailCode = readBits(3)  → trailing = xorTrailingRepr[trailCode]
                  centerBits = 64 - leading - trailing
                  XOR = readBits(centerBits) << trailing
                  value = Float64frombits(storedVal ^ XOR)
                  storedLeading = leading; storedTrailing = trailing
  storedVal = Float64bits(value)
```

### SerfQt decode algorithm

```
prevValue = Float64frombits(firstValBits)   // = 2.0 (initial reference)
step = 2 * maxDiff * 0.999

for each value (including first):
  n = EliasGammaDecode() - 1               // undo the +1 from encoding
  q = ZigZagDecode(n)                      // (n>>1) ^ -(n&1)
  value = prevValue + step * q
  prevValue = value
```

---

## Parameter tuning

### 1. `max_diff` — primary accuracy / compression trade-off

The single most impactful parameter. Controls the maximum allowed absolute
error between an original and a recovered value.

| Value | Character | Paper finding |
|---|---|---|
| `1e-2` | Loose | ~10 % better compression ratio than baseline; ~10× coarser error |
| **`1e-3`** | **Default** | **Paper benchmark default; good balance for sensor data** |
| `1e-4` | Tight | ~30–40 % worse compression ratio; 10× finer error than default |

The error bound is **hard-guaranteed**: for XOR, `FindAppLong` picks only
bit-patterns within `[v-maxDiff, v+maxDiff]`; for Qt,
`|recoverValue - v| ≤ maxDiff * 0.999 < maxDiff`.

### 2. `adjust_digit` — domain-specific integer offset (XOR only)

For SerfXOR, `FindAppLong` searches for a float whose bits maximise common
leading zeros with the previous stored value. If your metric values cluster
around a large integer (e.g. stock prices near $245, GPS latitude near 355°),
setting `adjust_digit` to that integer shifts the values toward zero before
the search, where consecutive mantissa bits naturally share more common prefix.

The paper uses per-dataset values taken from `Perf_expr_config.hpp`:

| Dataset | `adjust_digit` | Notes |
|---|---|---|
| Air-pressure, Trajectories | `0` | Values already near zero |
| Wind speed | `8` | Small shift |
| Smart-grid | `4` | |
| IR / Motor / Dew-point temp | `109` | Temperatures in °C + offset |
| Basel temperature | `80` | |
| Basel wind | `126` | |
| PM10 dust | `256` | |
| Stocks (USA) | `245` | ~$245 mean price |
| City temperature | `355` | GPS-derived column near 355 |

**Safe default: `0`** — FindAppLong still works, just misses 5–20 %
compression ratio for data far from zero.

**How to choose**: round the mean of your metric series to the nearest
integer. For the synthetic Zipf load generator (values near 0–1) `0` is
correct.

### 3. `window_interval` — flush cadence

Our implementation uses a tumbling time window rather than the paper's
fixed block size of **1000 values**. Longer windows accumulate more points,
giving the encoder more context for delta-of-delta timestamps and XOR
alignment, at the cost of higher end-to-end latency before blocks are
written or transmitted.

| `window_interval` | Approximate block size at 10 k MPS | Notes |
|---|---|---|
| `5s` | ~50 k points | Low latency, smaller blocks |
| **`10s`** | **~100 k points** | **Benchmark default** |
| `30s` | ~300 k points | Better compression, high latency |

The paper's C++ benchmark uses fixed blocks of 50 values at a time across
multiple blocks (effectively a streaming model). Our time-based window is
more natural for the OTel pipeline and gives larger blocks, which generally
improves compression ratio.

### 4. Adaptive leading/trailing table — implementation gap

The C++ `SerfXORCompressor` (and `NetSerfXORCompressor`) tracks the
distribution of leading/trailing zero counts across a window of values and
periodically calls `PostOfficeSolver::InitRoundAndRepresentation()` to
rebalance the 3-bit codes toward the actual histogram. If the new assignment
improves the per-bit efficiency, updated tables are written into the
bitstream; the decoder reads them and switches.

**Our Go implementation** uses the fixed tables from the C++ constructor
defaults (the `serfLeadingRound` / `serfTrailingRepresentation` arrays) and
never adapts them. For synthetic load-gen data with uniformly random floats
this has negligible impact. For real sensor data with a specific distribution
of XOR patterns you can lose 5–15 % compression ratio.

Adding adaptive tables would require:
1. Accumulating `[64]int` lead and trail histograms during encoding.
2. At each window flush, running the post-office assignment algorithm and
   embedding the new table in the bitstream before the encoded values.
3. Updating the decoder to read the optional table update flag before each
   block boundary.

### 5. Qt `0.999` safety factor — hardcoded, not a knob

The Qt encoder uses `effective_step = 2 * maxDiff * 0.999` so that the
worst-case quantization error `(step/2 = 0.999 * maxDiff)` is strictly
below `maxDiff`, maintaining the paper's error guarantee even with IEEE 754
rounding. The C++ 32-bit variant uses `0.97` for extra margin. This value
is intentionally hardcoded and should not be changed.

---

## Benchmark harness

The legacy per-collector `cmd/bench.sh` harness was removed alongside the
`serfcol` / `gorillacol` / per-sketch builder dirs (cleanup PR #363). Re-run
the Serf comparisons through the unified `asap-otel` build with a config file
selecting the relevant processor pipeline; the `serfprocessor` itself is still
in the patched contrib tree (`opentelemetry-collector-contrib-patch/processor/serfprocessor/`)
and registered in `cmd/asap-otel/builder-config.yaml`.

Bandwidth / CPU / throughput measurements now share the MVP demo's
`measure_stages.py` and `measure_per_edge_bandwidth.py` instrumentation
under `deploy/scripts/`.
