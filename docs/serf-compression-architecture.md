# Serf Compression — Architecture & Integration

This document describes where Serf compression is inserted in the
DataCollector pipeline, what each component does, and where decompression
would be implemented when a reader is added.

---

## Pipeline overview

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

---

## What is compressed vs what is not

| Layer | Serf applied? | Notes |
|---|---|---|
| SDK (instrumentation) | ❌ | Raw float64 emitted over OTLP |
| Agent collector — receive | ❌ | otlpreceiver decodes OTLP normally |
| Agent collector — serfprocessor | ✅ **COMPRESS** | Tumbling-window accumulation + Serf XOR or Qt encoding |
| Agent collector — export (downstream) | ❌ | Original metrics pass through to Prometheus exporter |
| Gateway collector | ❌ | Not modified; receives the raw forwarded stream |
| Backend / TSDB | ❌ | Stores the raw metric stream |
| Object storage (S3 / local disk) | ✅ **SERF1 objects** | Compressed blocks written on each window flush |

---

## Compression modes compared

| Mode | Algorithm | Error type | Typical use |
|---|---|---|---|
| `xor` | SerfXOR: FindAppLong + XOR bit-packing | Bounded absolute (`max_diff`) | Sensor time-series; comparable to Gorilla but better CR |
| `qt` | SerfQt: quantization delta + ZigZag + Elias Gamma | Bounded absolute (`max_diff`) | Uniform or slowly-varying signals; very low CPU overhead |
| *(Gorilla, separate processor)* | Gorilla XOR (delta-of-delta timestamps, XOR values) | Lossless | Baseline reference |

---

## Decompression — where it would be added

Decompression is **not yet implemented** (the current integration is
write-only archival). When a reader is needed:

| Location | What to implement |
|---|---|
| **Object storage reader** (offline batch) | A standalone Go / Python tool that reads SERF1 files, iterates series chunks, and decodes: timestamps via inverse delta-of-delta; values via SerfXOR inverse (straightforward — the XOR encoding is self-describing) or SerfQt inverse (`recoverValue = prevValue + 2·maxDiff·q`, ZigZag+Elias Gamma decode). |
| **Gateway collector processor** | A mirror `serfdecompressorprocessor` that receives SERF1 blobs (e.g. via an S3 receiver or HTTP push), decodes them back to `pmetric.Metrics`, and forwards to the backend TSDB. This would close the compression loop. |
| **Backend query path** | If the TSDB natively ingests SERF1 (e.g. custom remote-write endpoint), decompression lives inside the write handler. |

SERF1 binary format is fully self-describing per object (metadata JSON
contains metric name, attributes, point count, start/end timestamps), so
a decoder needs only the object bytes — no external schema required.

---

## Benchmark targets

```bash
cd opentelemetry-collector-contrib-patch

./cmd/bench.sh gorillacol    # Gorilla XOR baseline
./cmd/bench.sh serfcol       # Serf XOR  (max_diff=1e-3, adjust_digit=0)
./cmd/bench.sh serfcol-qt    # Serf Qt   (max_diff=1e-3)
```

Results written to `otel_collector_benchmark/benchmark_results/{processor}/`.
