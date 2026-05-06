# Gorilla-S3 Cold Engine — Design

> **Status:** design doc, **Phase 0** (this PR). Doc-only; no code,
> no docker, no image rebuild. Forward-looking; gates the
> implementation phases enumerated in §11. Branched from
> `origin/main` post-#279 (controller Phase E `stage_split` shipped)
> and post-#270 (SDK byte-parity migration to `sketchlib-go`).
>
> **Audience:** anyone asking "we have lossy sketches today; can we
> add a lossless edge → S3 → exact-PromQL tier alongside the warm
> sketch path?". This doc says yes, names the moving pieces, and
> sequences the work.
>
> **Last refreshed:** 2026-05-06.
>
> **Sibling docs to read first** (linked again in §13):
> [`docs/pipeline-query-catalog.md`](pipeline-query-catalog.md),
> [`controller/docs/design.md`](../controller/docs/design.md),
> [`docs/design-asap-edge-framework.md`](design-asap-edge-framework.md),
> [`docs/paper-outline.md`](paper-outline.md).

---

## Table of contents

1. [Goal + use cases](#1-goal--use-cases)
2. [Non-goals](#2-non-goals)
3. [End-to-end architecture](#3-end-to-end-architecture)
4. [Wire format — Gorilla chunk + index](#4-wire-format--gorilla-chunk--index)
5. [Edge processor `gorillas3processor`](#5-edge-processor-gorillas3processor)
6. [Backend `GorillaQueryEngine`](#6-backend-gorillaqueryengine)
7. [`GorillaS3ColdStore` — `ColdStore` trait impl](#7-gorillas3coldstore--coldstore-trait-impl)
8. [Capability routing extension](#8-capability-routing-extension)
9. [Controller integration (5-layer fit)](#9-controller-integration-5-layer-fit)
10. [Paper / product mapping](#10-paper--product-mapping)
11. [Phased implementation plan](#11-phased-implementation-plan)
12. [Open questions](#12-open-questions)
13. [References](#13-references)

---

## 1. Goal + use cases

The pipeline today (catalogued exhaustively in
[`docs/pipeline-query-catalog.md`](pipeline-query-catalog.md))
turns raw observability samples into mergeable sketches at the
edge, ships them via the modified-OTLP wire to the backend's
warm tier, and falls back to a cold raw-JSONL store when the
warm-tier capability table doesn't cover an inbound query. That
shape is sketch-first, exact-as-emergency.

This design proposes a **third tier — primary, not fallback —**
for metrics where the operator has a different set of
constraints:

- **Lossless retention** is mandatory. The data must reproduce
  bit-for-bit (within the encoder's lossless guarantee — Gorilla
  XOR encoding for `f64` values + delta-of-delta for `i64`
  timestamps) at any point in the future, regardless of the
  current sketch catalog or controller plan.
- **Wire bandwidth must collapse to near-zero off-host.** The
  agent stays at the edge; chunks are written **straight to
  object storage** (S3 / MinIO / S3-compatible local store).
  Nothing about this metric crosses the OTLP wire to the
  gateway / backend.
- **Exact PromQL queries** must be answerable on demand. The
  backend's query engine reads chunks from S3, decodes, and
  computes the answer with `(ε=0, δ=0, kind=Exact)` per
  [`accuracy_profile.rs`](#13-references).
- **Query latency is allowed to be cold-tier-class**, not
  warm-tier-class. ≤ 2× the warm-tier p99 is the headline
  budget (see [`docs/paper-outline.md`](paper-outline.md) §6.5
  baseline target for cold-fallback latency).

### 1.1 Concrete use cases

The shape pays off whenever any one of the four constraints
above is load-bearing.

| Use case | Driving constraint | Why warm-tier sketches won't do |
|---|---|---|
| **Compliance / audit-grade metric archive** (financial trades, healthcare events, regulator-visible counters) | lossless retention + exact query | regulators reject "ε relative quantile error" answers; need the original sample on demand |
| **Long-tail debug / forensic queries** ("what was the per-pod CPU at 03:14:07 yesterday?") | exact + bounded edge fan-out | sketches summarise — they can't answer point-in-time per-resource queries |
| **Cost-sensitive metric tier** (low-traffic metrics where edge fleet bandwidth is the bottleneck, not query rate) | bandwidth + exact | sketch wire bytes are already small but Gorilla on a low-rate metric is even smaller because the sample stream itself is small |
| **Metrics that span the legal-required-retention boundary** (e.g. 90-day Prometheus, then 7-year cold retention) | lossless retention | sketch error compounds if the only retained representation is sketches |
| **Paper baseline** — Pareto comparison of sketch-warm-tier vs Gorilla-cold-tier vs raw-JSONL on the (accuracy × query latency × $/GB-month × wire bandwidth) axes | benchmarking | this design rounds out the trade-off space |

### 1.2 What this design is, in one sentence

**Add a Layer-4 codec (`gorillas3processor`) + Layer-5 sink
(direct-to-object-storage) combination per
[`docs/design-asap-edge-framework.md`](design-asap-edge-framework.md)
§7, with a matching backend `GorillaQueryEngine` + `ColdStore`
adapter so the same PromQL surface answers exact queries against
the cold-tier corpus.**

The design re-uses, rather than re-invents:
- Encode + S3 PUT logic from
  [`telegraf-patch/plugins/outputs/gorilla_s3/gorilla_s3.go`](../telegraf-patch/plugins/outputs/gorilla_s3/gorilla_s3.go)
  (the existing Telegraf-side plugin).
- The hour-bucketed key layout + JSONL-style index from
  [`asap-query-engine/src/drivers/query/fallback/cold_store/format.rs`](#13-references).
- The `ColdStore` trait from
  [`cold_store/mod.rs`](#13-references) with a third
  implementation alongside `LocalFsColdStore`.
- The `AccuracyProfile::exact()` branch in
  [`accuracy.rs`](#13-references).

---

## 2. Non-goals

This design deliberately **does not**:

1. **Replace the sketch warm-tier path.** The two are
   orthogonal product tiers selected per metric (or per metric
   pattern). A typical deployment runs warm sketches for
   high-cardinality / high-rate metrics + Gorilla-S3 for
   compliance / cost-sensitive metrics + raw JSONL fallback
   for capability-miss safety.
2. **Cover non-numeric data.** Scope = typed metrics with
   `(timestamp_i64, value_f64, label_set)`. Logs, traces,
   profiles, exemplars are out. The Telegraf plugin already
   restricts itself to the `gorilla_block` measurement; the
   OTel processor inherits that scope.
3. **Promise sub-second query latency.** Cold-tier reads carry
   IO cost + decode cost. The latency budget is documented in
   §10 row ⑤ and is **slower**, by design, than the warm-tier
   sketch path.
4. **Provide its own deletion / GDPR-erasure pipeline.** Object
   lifecycle policies (S3 lifecycle, MinIO ILM) handle TTL +
   purge; the engine assumes immutable chunks within a chunk's
   stated retention.
5. **Implement compaction in Phase 1 / 2.** Hourly compaction
   from 60 s chunks → 64 MB blocks is **specified** in §4 but
   **deferred** to a later phase (see §11 Phase 6+ and §12 Q4).
6. **Build a new sketch family.** Gorilla XOR encoding + delta-
   of-delta timestamps are 2015 Facebook-paper algorithms with
   a working Go implementation already vendored in
   `telegraf-patch/`. The crate factored out in §11 Phase 1
   wraps that implementation, it does not redesign it.
7. **Reach for sub-edge optimisations.** No on-the-fly
   quantisation knobs (that's b4-tunable's job per
   [`PROGRESS.md`](#13-references)), no per-stream adaptive
   parameters. Defaults match Facebook 2015.
8. **Subsume the multi-language SDK byte-parity work.** The
   Gorilla-S3 path goes through one language at the agent (Go
   in `sketchcol` + `sketchtelegraf`, Rust in `sketchotap`)
   and one language at the backend (Rust). Cross-language
   byte-parity (the `sketchlib-go` ↔ `asap_sketchlib` work in
   [#243](#13-references)) does not extend to Gorilla blocks
   in this revision; the producer side is Go-only or Rust-only
   per agent runtime, the consumer side is Rust-only.

---

## 3. End-to-end architecture

The current pipeline (sketch warm tier + cold-JSONL fallback)
**does not change**. The Gorilla-S3 branch is **added** alongside
the warm path, sharing the agent runtime + the backend's PromQL
front-end, while owning its own codec, sink, store, and engine.

### 3.1 Block diagram (current + new)

```
                ┌──────────────────────────────────────────┐
                │ Workloads (apps, Prometheus, Kafka, …)   │
                └──────────────────┬───────────────────────┘
                                   │ raw metric streams
                                   ▼
   ┌─────────────────────────────────────────────────────────────┐
   │ ASAPCollector edge runtime (3 variants, all support both    │
   │ paths — see §5 per-runtime variant table)                   │
   │                                                             │
   │   sketchcol (Go OTel)  sketchotap (Rust OTAP)  sketchtelegraf │
   │   processors:                                               │
   │     ├─ ddsketchprocessor / kllprocessor / hllprocessor /    │
   │     │  countminsketchprocessor / countsketchprocessor       │
   │     │      → builds per-window sketch  ─── WARM PATH ───┐  │
   │     │                                                    │  │
   │     └─ gorillas3processor      [NEW]                     │  │
   │            → window → Gorilla XOR encode → S3 PUT        │  │
   │            → drop_original=true (stays at edge)          │  │
   │                                                          │  │
   │   raw-tee exporter (cold JSONL)        ─── FALLBACK ─┐   │  │
   │            → MinIO/S3 raw JSONL                      │   │  │
   └──────────────────────┬─────────────────┬─────────────┴───┴──┘
                          │                 │             │   │
                          │ OTLP            │ S3 PUT      │   │
                          │ (sketches)      │ (gorilla)   │   │
                          ▼                 ▼             │   │
                ┌──────────────────┐  ┌──────────────────┐│   │
                │  Gateway         │  │   S3 / MinIO     ││   │
                │  (optional       │  │  / S3-compat     ││   │
                │  spatial collapse│  │   (Gorilla       ││   │
                │  + OTLP forward) │  │    chunks +      ││   │
                └────────┬─────────┘  │    index.json)   ││   │
                         │            └─────────┬────────┘│   │
                         │ OTLP                 │         │   │
                         ▼                      │         │   │
   ┌──────────────────────────────────────────┐ │         │   │
   │ ASAPQuery-backend                        │ │         │   │
   │   ┌─────────────────────────────┐        │ │         │   │
   │   │ OtlpReceiver                │        │ │         │   │
   │   │   typed sketch decoders     │ ◄──────┘ │         │   │
   │   │   (asap-precompute-rs)      │          │         │   │
   │   └────────────┬────────────────┘          │         │   │
   │                ▼                            │         │   │
   │   ┌─────────────────────────────┐          │         │   │
   │   │ Precompute engine workers    │          │         │   │
   │   │   per (agg_id, group_key)    │          │         │   │
   │   │   window panes               │          │         │   │
   │   └────────────┬────────────────┘          │         │   │
   │                ▼                            │         │   │
   │   ┌─────────────────────────────┐          │         │   │
   │   │ SimpleMapStore (warm sketch │          │         │   │
   │   │ DB)                         │          │         │   │
   │   └────────────┬────────────────┘          │         │   │
   │                ▼                            │         │   │
   │   ┌──────────────────────────────────────────────┐   │   │
   │   │ SimpleEngine — PromQL/SQL/ElasticDSL surface │   │   │
   │   │                                              │   │   │
   │   │   capability_matching::find_compatible_     │   │   │
   │   │     aggregation(metric, stat, …) →          │   │   │
   │   │                                              │   │   │
   │   │   ┌──────────────┐  ┌──────────────────────┐│   │   │
   │   │   │ warm path:   │  │ Gorilla-S3 path:     ││   │   │
   │   │   │ accumulator  │  │ GorillaQueryEngine   ││   │   │
   │   │   │ .query_      │  │ .execute(…)  [NEW]   ││   │   │
   │   │   │  statistic() │  │   ↓                  ││   │   │
   │   │   │              │  │ GorillaS3ColdStore   ││   │   │
   │   │   │              │  │ .list_chunks /       ││   │   │
   │   │   │              │  │  .read_chunk  [NEW]  ││──┘   │
   │   │   └──────┬───────┘  └──────────┬───────────┘│      │
   │   │          │                     │            │      │
   │   │          ▼                     ▼            │      │
   │   │   warm-tier answer       exact answer       │      │
   │   │   (accuracy: ε,δ)        (accuracy: 0,0)   │      │
   │   │                                              │      │
   │   │   cold-JSONL fallback (forwarding adapters / │      │
   │   │   LocalFsColdStore) — UNCHANGED              │ ◄───┘
   │   └──────────────────────────────────────────────┘
   └──────────────────────────────────────────────────────┘

      ┌────────────────────────────────────────────────────┐
      │ Controller (5-layer pipeline — see §9)             │
      │   L1 query_language  L2 logical_plan               │
      │   L3 intent_algebra (AggIntent::Quantile{exact} …) │
      │   L4 sketch_algebra:  + BindGorillaExact rule      │
      │   L5 stage_split:     + StageId::Storage           │
      │                                                    │
      │   pushes config to:                                │
      │     edge agents (gorillas3processor)               │
      │     gateway (no-op for Gorilla path)               │
      │     backend (GorillaQueryEngine routing)           │
      │                                                    │
      │   replans on workload drift (per                   │
      │   memory/feedback_controller_plan_triggers.md):    │
      │   only NEW queries / workloads, not capability     │
      │   misses — those fall back to cold tier            │
      └────────────────────────────────────────────────────┘
```

### 3.2 Component-by-component status (current vs new)

| Component | Status | What it does in this design |
|---|---|---|
| `sketchcol` Go OTel runtime | existing — see [`docs/pipeline-query-catalog.md`](pipeline-query-catalog.md) §2.1 | hosts `gorillas3processor` (Phase 2) alongside the existing sketch processors |
| `sketchotap` Rust OTAP runtime | existing — see [`docs/design-asap-otap-rust-integration.md`](#13-references) | hosts the Rust port of the Gorilla-S3 processor (Phase 7+) |
| `sketchtelegraf` Go Telegraf runtime | existing — see [`docs/design-asap-telegraf-integration.md`](#13-references) and the `gorilla_s3` output plugin already in `telegraf-patch/plugins/outputs/gorilla_s3/` | hosts the Telegraf-flavored Gorilla output (already partly built) + the matching `gorilla_aggregator` (Phase 7+) |
| `gorillas3processor` (OTel) | **NEW**, Phase 2 | window 60 s of inbound `pmetric.Metrics`, encode to a Gorilla chunk per metric/labelset, PUT to S3, update `index.json`. `drop_original: true` keeps the metric off the OTLP wire |
| Sketch warm path (gateway → backend) | unchanged | continues to serve the queries described in [`docs/pipeline-query-catalog.md`](pipeline-query-catalog.md) §3 |
| S3 / MinIO / S3-compatible store | **NEW dependency**, optional in dev (use MinIO local) | holds the chunk corpus + per-hour `index.json` |
| `GorillaQueryEngine` (Rust) | **NEW**, Phase 4 | fetches chunks via the cold-store adapter, decodes, accumulates per query family |
| `GorillaS3ColdStore` (Rust, `ColdStore` impl) | **NEW**, Phase 3 | implements the existing `ColdStore` trait so the engine can `list_chunks` + `read_chunk`. Sits alongside `LocalFsColdStore` |
| Capability routing | extended, Phase 5 | a new `StorageBackend::GorillaS3` enum variant lets `find_compatible_aggregation` (or its successor) dispatch `Exact` queries against Gorilla-eligible metrics to `GorillaQueryEngine` |
| Cold-fallback raw JSONL path | unchanged — see [`cold_store/format.rs`](#13-references) `parse_jsonl` + the §5.2 forwarding adapters in [`pipeline-query-catalog.md`](pipeline-query-catalog.md) | continues to handle "metric the controller hasn't planned yet" capability-miss queries |
| Controller (5-layer) | extended, Phase 5 | observes inbound queries + workload drift, plans which metrics route via warm path vs Gorilla-S3 (per [memory/feedback_controller_plan_triggers.md](#13-references) — capability misses do NOT trigger replanning, they fall back to cold tier) |

### 3.3 Why this is parallel to, not a replacement for, the warm tier

The warm-tier sketch path optimises for *query rate amortised
over a window* — see
[`pipeline-query-catalog.md`](pipeline-query-catalog.md) §4
(scenario C, multi-stage). The savings are huge when `Q ≫ 1`
queries hit the same window.

The Gorilla-S3 path optimises for *retention cost + exact
recoverability*. It loses to the warm tier on query rate (cold
IO + decode per query) and wins on lossless guarantee + zero
upstream wire traffic. Different Pareto frontier; both are
useful; the controller picks per metric.

The raw-JSONL fallback (already wired) optimises for *zero-loss
capture under capability-miss surprise* — it's the receiver-side
safety net for queries the controller hasn't planned yet. It is
not the same shape as Gorilla-S3 (uncompressed; written by the
gateway-side raw-tee, not by the agent; only read on
capability-miss, not as a primary tier).

---

## 4. Wire format — Gorilla chunk + index

### 4.1 Goals

The wire format is the contract between
`gorillas3processor` (writer) and `GorillaQueryEngine` +
`GorillaS3ColdStore` (readers). It must be:

- **Self-describing.** A chunk standing alone on disk decodes
  into samples without needing the index (the index is for
  pruning, not for correctness).
- **Stable across encoder versions.** A header carries an
  `encoder_version` byte so the decoder can tell which Gorilla
  variant produced it.
- **Cheap to prune.** The hour-bucketed key layout + the
  `index.json` together let a query restrict its read set to
  `O(buckets-touched)` PUTs and at most `O(chunks-in-range)`
  GETs without doing `LIST` against S3.
- **Compatible with the Telegraf-side plugin** in
  [`telegraf-patch/plugins/outputs/gorilla_s3/gorilla_s3.go`](../telegraf-patch/plugins/outputs/gorilla_s3/gorilla_s3.go).
  The OTel processor will write byte-identical chunks so the
  reader doesn't care which agent runtime emitted them.

### 4.2 Block format

Each chunk is one S3 object. Layout (all multi-byte fields
little-endian):

```
┌──────────────────────────────────────────────────────────────┐
│ ChunkHeader (fixed-width)                                    │
│   magic               : 4 bytes  "GORS"                      │
│   schema_version      : u16      (1)                         │
│   encoder_version     : u16      (1 = Facebook 2015 Gorilla) │
│   flags               : u32      (bit 0: SSE-KMS encrypted)  │
│   metric_name_len     : u16                                  │
│   label_set_len       : u32                                  │
│   start_time_unix_ms  : i64                                  │
│   end_time_unix_ms    : i64                                  │
│   sample_count        : u32                                  │
│   payload_len         : u32                                  │
│   header_crc32c       : u32                                  │
├──────────────────────────────────────────────────────────────┤
│ ChunkMetadata (variable-width)                               │
│   metric_name         : utf8 [metric_name_len]               │
│   label_set           : sorted JSON {k:v,k:v,…}              │
│                          [label_set_len]                     │
├──────────────────────────────────────────────────────────────┤
│ ChunkBody                                                    │
│   gorilla_payload     : bytes [payload_len]                  │
│     ─ Gorilla XOR-encoded interleaved (timestamp, value)     │
│       stream: delta-of-delta on i64 timestamps,              │
│       XOR + leading/trailing-zero count on f64 values.       │
│       First sample carries full ts + full f64.               │
│   payload_crc32c      : u32                                  │
└──────────────────────────────────────────────────────────────┘
```

Encoder reference: Facebook 2015 paper "Gorilla: A Fast,
Scalable, In-Memory Time Series Database" (Pelkonen et al.,
VLDB 2015) — the same source the existing Telegraf plugin
hands off to. The encoder lives in the new `asap-gorilla` crate
(§11 Phase 1) wrapping the existing Telegraf-side Go encoder
on the Go side and either a re-implementation or a Rust port
on the backend side. Cross-language byte parity for the chunk
payload IS required (the backend reader must decode chunks
from any agent runtime).

### 4.3 Chunk granularity

Two granularities, layered:

| Granularity | Window | Typical size | Lifecycle |
|---|---|---|---|
| **Small chunks** (`part-NNNNNN.gor`) | 60 s window per metric per labelset (configurable) | a few KB to a few hundred KB depending on rate | written by the agent on every window close; immutable |
| **Large blocks** (`block-YYYYMMDDHH-NNN.gor`) | 1 hour, 64 MB target | up to 64 MB | written by a future hourly compactor (§11 Phase 6+; not in MVP) that consolidates same-metric same-labelset small chunks |

Trade-off:

- Small chunks → low edge memory (60 s buffer); high object
  count; slower large-range queries (more S3 GETs).
- Large blocks → higher per-chunk read cost (must skip on
  decode) but far fewer GETs over a multi-hour query.

MVP ships small chunks only. Compaction is documented but
deferred (§12 Q4).

### 4.4 Index file

Each hour-bucket directory carries one `index.json`:

```json
{
  "schema_version": 1,
  "tenant": "acme",
  "metric": "http_requests_total",
  "hour_bucket": "2026/05/06/14",
  "chunks": [
    {
      "key": "acme/http_requests_total/2026/05/06/14/part-000001.gor",
      "label_hash": "fnv64:0x9a1cf2…",
      "label_set": {"service": "api", "method": "GET"},
      "start_time_unix_ms": 1714999260000,
      "end_time_unix_ms":   1714999320000,
      "sample_count": 60,
      "encoder_version": 1,
      "size_bytes": 412
    },
    …
  ],
  "compacted_blocks": []
}
```

Properties:

- **One index per hour bucket per metric**, mirroring the
  `<tenant>/<metric>/YYYY/MM/DD/HH/` prefix. A query for
  `metric=m` over `[t0, t1)` reads the index files for the
  hour buckets covering `[t0, t1)` (≤ `(t1-t0)/3600s` files),
  filters chunks by time-range overlap, optionally by
  `label_hash` if the query carries label matchers, and reads
  the surviving chunks.
- **Avoids `LIST` calls.** The reader can walk the time range
  by computing prefixes (the same way `cold_store::format::
  hour_prefixes` works for the existing JSONL store) +
  reading the per-bucket `index.json`.
- **Update model.** The agent rewrites `index.json` after each
  successful chunk PUT. The write is `PUT` of the full file;
  S3 PUT is atomic per object so partial-update tearing is
  not possible. Concurrent writers to the same hour bucket
  (multi-agent fleet writing the same metric) are handled by
  per-`(tenant, metric, label_hash)` ownership — see §12 Q7.
- **Reader resilience.** If `index.json` is missing or
  corrupt, the reader falls back to `LIST` on the hour-bucket
  prefix + per-chunk header peek. This is slow but correct.

### 4.5 Object key layout

```
<tenant>/<metric>/YYYY/MM/DD/HH/part-NNNNNN.gor      ← small chunk
<tenant>/<metric>/YYYY/MM/DD/HH/index.json           ← per-hour index
<tenant>/<metric>/YYYY/MM/DD/HH/block-YYYYMMDDHH-NNN.gor   ← compacted block (future)
```

`<tenant>` defaults to `default` for single-tenant
deployments. Multi-tenancy is a configuration decision (§12
Q7), not a wire-format change.

The path mirrors
[`cold_store/format.rs::part_path_prefix`](#13-references),
which uses `raw/<metric>/YYYY/MM/DD/HH/` for the JSONL store.
The Gorilla path drops the `raw/` segment in favor of
`<tenant>/<metric>/...` because the chunk format is
self-describing (no JSONL/Gorilla ambiguity at the prefix level)
and adds the tenant prefix for multi-tenant deployments.

### 4.6 Encryption

- **Default: SSE-KMS.** The Telegraf plugin already wires
  `SSEKMSKeyID` into `s3.PutObjectInput.ServerSideEncryption =
  "aws:kms"` (see lines 256–259 of
  [`gorilla_s3.go`](../telegraf-patch/plugins/outputs/gorilla_s3/gorilla_s3.go)).
  The OTel processor inherits the same default.
- **Opt-out** via `encryption: none` in the processor config
  (for MinIO local dev / tests).
- **Server-side only.** No client-side encryption — the chunk
  bytes themselves carry no encryption metadata, the
  `flags` bit-0 in the header indicates "this object was
  written under SSE-KMS" purely for audit.

### 4.7 Self-describing guarantee

A chunk + the bucket key together are sufficient to fully
decode. Concretely the decoder needs:

- The chunk file (header → body).
- Optionally the metric name + start time which can be
  recovered from the key if the header is unreadable
  (defence in depth).

The index is a *performance* artifact, not a *correctness*
one. A chunk standing on a different key under a different
prefix would still decode, just at the cost of a `LIST` to
find it.

---

## 5. Edge processor `gorillas3processor`

### 5.1 Module location

```
opentelemetry-collector-contrib-patch/processor/gorillas3processor/
├── processor.go         — main consumer + window manager
├── encoder.go           — wraps asap-gorilla encode; same API as Telegraf-side
├── s3_writer.go         — S3 / MinIO PUT + index.json maintenance
├── config.go            — TOML-style config struct
├── factory.go           — OCB-registered factory
└── README.md
```

The processor is a Go OTel `processor.Metrics` that lives in
`opentelemetry-collector-contrib-patch/`, mirroring the layout
of the existing `ddsketchprocessor` / `kllprocessor` /
`countminsketchprocessor` / `countsketchprocessor` /
`hllprocessor`. OCB registration follows the same pattern.

### 5.2 Inputs + outputs

| Direction | Type | Notes |
|---|---|---|
| **Input** | `pmetric.Metrics` (any data type — Gauge / Sum / Histogram / typed sketch variants) | the processor filters to scalar Gauge + Sum + Histogram-derived points; non-scalar inputs (typed sketches) bypass and pass through |
| **Output (downstream OTel)** | `pmetric.Metrics` — empty for Gorilla-routed metrics (when `drop_original: true`), or original passed through (when `drop_original: false`) | the canonical mode is `drop_original: true`: no OTLP forward of these metrics |
| **Output (side-channel)** | S3 PUTs of `.gor` chunks + `index.json` updates | fan-out per (metric, labelset) per window |

### 5.3 Window-based encode

```go
// pseudocode
type GorillaS3Processor struct {
    cfg          *Config
    next         consumer.Metrics
    windowMgr    *windowManager  // tumbling, default 60s
    encoder      *gorilla.Encoder
    s3Writer     *s3Writer
    indexCache   *indexCache     // dirty-tracked per hour bucket
}

func (p *GorillaS3Processor) ConsumeMetrics(ctx, md pmetric.Metrics) error {
    for each ResourceMetrics:
      for each ScopeMetrics:
        for each Metric m:
          if !p.cfg.metricMatcher.Match(m.Name()) {
              continue  // not a Gorilla-routed metric — pass through
          }
          for each scalar DataPoint dp:
              key := (m.Name(), labelsOf(dp))
              p.windowMgr.add(key, dp.Timestamp(), dp.Value())
    if !p.cfg.DropOriginal {
        return p.next.ConsumeMetrics(ctx, md)
    }
    // drop_original: true → md is dropped for this stage
    return nil
}

// timer goroutine, fires per window close
func (p *GorillaS3Processor) onWindowClose(closedWindow window) {
    for each (metric, labelset) batch in closedWindow:
        chunkBytes := p.encoder.Encode(batch.samples, batch.start, batch.end)
        key := buildKey(p.cfg.Tenant, metric, batch.start)
        if err := p.s3Writer.Put(ctx, key, chunkBytes); err != nil {
            p.handleWriteFailure(err, batch)  // see §5.7
            continue
        }
        p.indexCache.recordChunk(metric, batch.start, key, chunkBytes, batch.labelHash)
    p.indexCache.flushDirty(ctx, p.s3Writer)
}
```

Key behaviours:

- **Default window: 60 s tumbling.** Matches the existing
  b5-gorilla baseline window
  ([`deploy/configs/sketchcol-agent-b5-gorilla.yaml`](../deploy/configs/sketchcol-agent-b5-gorilla.yaml)
  line 32). Configurable via `window_interval`.
- **Per-`(metric, labelset)` batching.** One chunk per
  `(metric, labelset)` per window. Per the wire format, a
  chunk's labelset is fixed; samples in a chunk all share
  labels. Multiple chunks per window per metric when the
  metric has multiple labelsets in flight.
- **Encoder is stateful per `(metric, labelset)` over the
  window**, not across windows. Across windows each new
  chunk re-bootstraps with a full first sample.

### 5.4 `drop_original` semantics

The `drop_original: true` config flag is the canonical
production mode: it means "consume the metric, write the
chunk, do **not** forward the metric over the OTLP wire to
the next processor / exporter." The downstream pipeline never
sees this metric.

```
                   metrics (any source)
                          │
                          ▼
         ┌────────────────────────────────┐
         │  gorillas3processor            │
         │   metric m matches matcher?    │
         │     yes → Gorilla encode → S3  │
         │           drop from OTLP       │
         │     no  → pass through         │
         └────────────────────────────────┘
                          │
                          ▼
                 next processor / exporter
```

This is the **wire-bandwidth-zero** property the design
promises. With `drop_original: false` the metric goes both to
S3 and to the OTLP exporter — useful for migration / parity
testing only, not for steady-state production.

### 5.5 Flush triggers

A batch is encoded + PUT to S3 on the earliest of:

| Trigger | Default | Notes |
|---|---|---|
| Window close (tumbling boundary) | every 60 s | the headline trigger |
| Per-batch byte cap | 1 MiB | rare for typical metric rates; protects memory |
| Per-batch sample cap | 10 000 samples | rare; protects memory |
| Graceful shutdown | on `Shutdown` call | drains all open batches into final chunks (best-effort PUT; failed PUTs go to spool, see §5.7) |
| SIGTERM grace window | bounded by host SIGTERM grace | best-effort drain; not a strict guarantee |

### 5.6 Failure modes

| Failure | Behaviour | Configurable | Notes |
|---|---|---|---|
| Transient S3 5xx / network blip | retry with exponential backoff | `max_retries` (default 3), `retry_backoff` (default 1s) | matches Telegraf plugin lines 99–103 |
| Persistent S3 outage (retries exhausted) | one of {block, drop, spool} per `on_s3_failure` config | yes | spool default for production; drop default for benchmarks |
| Local spool overflow | drop oldest | `local_spool_max_bytes` | once spool is full the agent gives up the oldest chunks rather than blocking ingest |
| Encoder error (corrupt input) | drop the offending sample, log, continue | no | encoder errors should never happen with scalar f64+i64 inputs; they would indicate a bug |
| Index update conflict | last-writer-wins under per-`(tenant, metric, labelset)` ownership | yes | see §12 Q7 — multi-agent same-metric writes need per-labelset partitioning |
| `s3_writer.Put` succeeded, `index.json` PUT failed | retry index update independently; on persistent failure, the chunk is still readable via fallback `LIST` (§4.4) | no | correctness is preserved without the index; performance degrades |
| Process crash mid-window | up to one window of data lost | no | matches today's collector — no edge persistence; future PersistentPrecompute trait could close this gap (out of scope) |

#### 5.6.1 Failure-mode picker

| Deployment | Recommended `on_s3_failure` | Rationale |
|---|---|---|
| Production with SLA-bound metric retention | `spool` | local disk absorbs short outages without losing samples |
| Paper benchmark / b5-gorilla baseline | `drop` | matches the paper's wire-bandwidth-zero claim; outage is tester error |
| Compliance-grade archive (no loss tolerance) | `block` | applies backpressure upstream; a bad day ingests slower rather than losing data |

### 5.7 Per-deployment-model variants

Per the runtime fan-out documented in
[`docs/design-asap-edge-framework.md`](design-asap-edge-framework.md)
§7, every host runtime gets its own adapter for the
Gorilla-S3 codec. Status per runtime:

| Runtime | Phase | Status | Output module path | Notes |
|---|---|---|---|---|
| **`sketchcol`** (Go OTel) | Phase 2 | **PROPOSED** (this design lands the spec) | `opentelemetry-collector-contrib-patch/processor/gorillas3processor/` | the canonical implementation; matches the existing sketch-processor layout |
| **`sketchtelegraf`** (Go Telegraf) | Phase 7+ | the **encode + S3-write half is already shipped** in [`telegraf-patch/plugins/outputs/gorilla_s3/gorilla_s3.go`](../telegraf-patch/plugins/outputs/gorilla_s3/gorilla_s3.go) (lines 1–384). Phase 7 needs the matching `aggregators/gorilla` aggregator that windows + encodes; the `output` plugin already exists | `telegraf-patch/plugins/outputs/gorilla_s3/` (output, done) + `telegraf-patch/plugins/aggregators/gorilla/` (aggregator, future) | re-uses the AWS SDK for Go S3 client + the existing PUT path |
| **`sketchotap`** (Rust OTAP) | Phase 7+ | future; reuses the Rust `asap-gorilla` crate (§11 Phase 1) | `otap-patch/processors/gorillas3processor/` | follows the OTAP-specific structural template per [`docs/design-asap-otap-rust-integration.md`](#13-references) |

### 5.8 Configuration shape

```yaml
# in sketchcol's collector config
processors:
  gorilla_s3:
    # Window — same default as b5-gorilla baseline.
    window_interval: 60s

    # Match which metrics route via this processor.
    # Empty matcher = all metrics; explicit matcher recommended.
    metric_matcher:
      include:
        - "audit_*"
        - "compliance_*"

    # Stay at edge, do not forward over OTLP.
    drop_original: true

    # S3 / MinIO config.
    s3:
      bucket: "asap-gorilla-prod"
      region: "us-east-1"
      tenant: "acme"
      sse_kms_key_id: "arn:aws:kms:..."     # default; set to "" to disable
      multipart_threshold_bytes: 8388608    # 8 MiB; matches Telegraf
      multipart_part_bytes:      8388608    # 8 MiB; matches Telegraf
      max_retries: 3
      retry_backoff: 1s
      upload_timeout: 30s

    # Local spool fallback when S3 is unreachable past retry budget.
    on_s3_failure: spool                    # spool | drop | block
    local_spool_dir: /var/spool/asap-gorilla
    local_spool_max_bytes: 1073741824       # 1 GiB
```

Deployments without S3 credentials can set `s3.bucket: ""` +
`s3.local_dir: /var/tmp/asap-gorilla-out` and the processor
falls into local-FS-only mode — exactly the b5-gorilla
baseline shape from
[`deploy/configs/sketchcol-agent-b5-gorilla.yaml`](../deploy/configs/sketchcol-agent-b5-gorilla.yaml).

---

## 6. Backend `GorillaQueryEngine`

### 6.1 Module location

```
ASAPQuery-backend/asap-query-engine/src/engines/gorilla_engine.rs
```

A sibling of `simple_engine.rs`. Same `QueryEngine` trait
surface (the trait is implicit today — see
[`engines/simple_engine.rs`](#13-references) lines 138–174 for
the `SimpleEngine` shape; Phase 4 may extract it explicitly).

### 6.2 Type sketch

```rust
use std::sync::Arc;
use crate::data_model::{InferenceConfig, QueryLanguage};
use crate::engines::query_result::QueryResult;
use crate::drivers::query::fallback::cold_store::gorilla_s3::GorillaS3ColdStore;

pub struct GorillaQueryEngine {
    /// `ColdStore` impl backed by S3 / MinIO. Same trait as
    /// `LocalFsColdStore` so cold-tier infrastructure (s3_adapter
    /// fallback, scan range pruning) is shared.
    cold_store: Arc<GorillaS3ColdStore>,

    /// LRU cache of decoded chunks keyed by S3 object key.
    /// Cap by total decoded-bytes budget (config-driven).
    chunk_cache: ChunkCache,

    /// Per-thread reusable Gorilla decoder pool — encoder /
    /// decoder are CPU-cheap but allocate scratch buffers, so
    /// pooling shaves the steady-state per-query overhead.
    decoder_pool: DecoderPool,

    /// Inference + streaming config, same as SimpleEngine, so
    /// PromQL parsing + capability lookup share infrastructure.
    inference_config: InferenceConfig,
    query_language: QueryLanguage,
}

#[async_trait]
pub trait QueryEngine: Send + Sync {
    async fn execute(&self, q: PromQLQuery, t: QueryTimestamps)
        -> Result<QueryResult, QueryError>;
}

#[async_trait]
impl QueryEngine for GorillaQueryEngine {
    async fn execute(
        &self,
        q: PromQLQuery,
        t: QueryTimestamps,
    ) -> Result<QueryResult, QueryError> {
        // 1. Parse the PromQL into (metric, time_range, statistic, kwargs).
        let plan = self.plan_query(&q, &t)?;

        // 2. List chunks via index.json + time-range pruning.
        let chunk_refs = self.cold_store
            .list_chunks(&plan.metric, plan.start_ms, plan.end_ms)
            .await?;

        // 3. Stream-decode + accumulate per query family. See §6.4.
        let answer = match plan.statistic {
            Statistic::Sum   | Statistic::Count
            | Statistic::Min | Statistic::Max
            | Statistic::Rate | Statistic::Increase => {
                self.streaming_aggregate(&plan, chunk_refs).await?
            }
            Statistic::Quantile | Statistic::Topk
            | Statistic::Cardinality => {
                self.buffered_aggregate(&plan, chunk_refs).await?
            }
        };

        // 4. Wrap with infos.
        let mut result = QueryResult::from(answer);
        result.infos.push("accuracy: ε=0, δ=0, kind=Exact".into());
        result.infos.push("data_source: gorilla-s3".into());
        Ok(result)
    }
}
```

### 6.3 Query execution

Steps:

1. **Parse PromQL.** Reuse `SimpleEngine`'s parsing
   infrastructure (`promql_utilities`, `ast_matching`,
   `parsing::get_metric_and_spatial_filter`,
   `parsing::get_statistics_to_compute`) — these live in
   `promql_utilities` and are not engine-specific. Output: a
   `(metric, time_range_ms, statistic, args, label_matchers)`
   tuple.

2. **Plan.** Determine which chunks cover the time range, by
   asking the cold store. The cold store returns
   `Vec<ChunkRef>` ordered by `start_time_unix_ms`.

3. **Fetch chunks.** Each chunk is fetched via the cache
   (LRU); on miss the cold store reads the chunk bytes from
   S3 and decodes the body. Sample iteration is exposed via
   an iterator so the engine can choose to stream or
   materialise.

4. **Aggregate.** Per query family, the engine accumulates
   results — see §6.4.

5. **Wrap.** Result carries the `accuracy: ε=0, δ=0,
   kind=Exact` info string (matching the `AccuracyProfile::
   exact()` summary in
   [`accuracy.rs`](#13-references)) plus a
   `data_source: gorilla-s3` info string so callers /
   dashboards can distinguish cold-tier exact answers from
   warm-tier sketch answers.

### 6.4 Per-query-family execution

Two strategies, chosen per `Statistic`:

#### 6.4.1 Streaming (additive) statistics

`Sum`, `Count`, `Min`, `Max`, `Rate`, `Increase`, `Avg` (=
sum/count): iterate chunks in time order, decode each chunk,
fold into a running accumulator, **discard** the decoded
samples before fetching the next chunk. Memory cost is
`O(num_groups × accumulator_size)` independent of total
samples in the query range.

```text
running_sum := 0
running_cnt := 0
for chunk in chunk_refs:
    for sample in decode(chunk):
        if sample.ts in [start_ms, end_ms):
            running_sum += sample.value
            running_cnt += 1
return aggregate(running_sum, running_cnt, statistic)
```

#### 6.4.2 Buffered (rank / cardinality / heavy-hitter) statistics

`Quantile`, `TopK`, `Cardinality`: these need all samples in
the time range to compute exactly. Memory cost is
`O(total_samples_in_range × 16 bytes)` (each sample = i64 ts
+ f64 value). For typical 60 s-window-aggregated metrics at
1 sample/window this is small (a 1-day query at 1
sample/minute = 1440 samples × 16 B = ~23 KB per labelset).
For dense sub-second metrics it can grow large; the budget
is enforced via a `max_buffered_samples` config that errors
out (returns "exceeded buffered-aggregate budget; consider a
shorter range") rather than OOM-ing.

Future optimisation (Phase 6+): mergeable exact-quantile
algorithms (e.g. radix-tree for integer values, fully-sorted
streaming merge for f64) can keep the streaming property for
exact quantile when applicable. Out of scope for MVP.

#### 6.4.3 Per-statistic table

| Statistic | Strategy | Memory cost | Notes |
|---|---|---|---|
| `Sum` | streaming | O(groups) | trivial fold |
| `Count` | streaming | O(groups) | counts samples; no value read needed |
| `Avg` | streaming | O(groups) | tracks sum + count |
| `Min` / `Max` | streaming | O(groups) | exact extrema |
| `Rate` / `Increase` | streaming | O(groups) | counter-reset semantics handled via per-group last-value cache |
| `Quantile(φ)` | buffered | O(total samples × 16 B) | sort the buffered values, pick the φ-rank |
| `TopK(k)` | buffered | O(total cardinality × 16 B) | exact heavy-hitters by sum/count |
| `Cardinality` | streaming-with-set | O(distinct labelsets) | exact distinct count via in-memory `BTreeSet<labelset>` |

### 6.5 Caching

```rust
pub struct ChunkCache {
    inner: moka::sync::Cache<ChunkKey, Arc<DecodedChunk>>,
    max_decoded_bytes: u64,
}
```

LRU keyed by `(tenant, metric, key)`; eviction on total
decoded-bytes budget (default: 256 MiB, configurable). Decoded
chunks are immutable so cache hits are zero-copy clones of an
`Arc<DecodedChunk>`. Cache is per-process; future revision can
share a side-car `chunk-cache-server` if multiple backend
processes want to share a hot working set.

### 6.6 Result shape

```rust
QueryResult {
    data: <PromQL vector / matrix as today>,
    infos: vec![
        "accuracy: ε=0, δ=0, kind=Exact",
        "data_source: gorilla-s3",
        "chunks_read: 42",
        "decoded_bytes: 18412",
        "cold_io_ms_p99: 87",
    ],
    warnings: vec![],
}
```

The `infos` array is the same Prometheus convention
`SimpleEngine` already uses (see
[`pipeline-query-catalog.md`](pipeline-query-catalog.md) §5.4
references to it); Grafana 11+ surfaces it inline. The
`accuracy: ε=0, δ=0, kind=Exact` line matches
`AccuracyProfile::exact().summary()` byte-for-byte so the
existing dashboards parse it without change.

---

## 7. `GorillaS3ColdStore` — `ColdStore` trait impl

### 7.1 Module location

```
ASAPQuery-backend/asap-query-engine/src/drivers/query/fallback/cold_store/
├── mod.rs              — ColdStore trait, ColdStoreError
├── format.rs           — JSONL helpers (existing — for the raw fallback)
├── local_fs.rs         — LocalFsColdStore (existing)
└── gorilla_s3.rs       — GorillaS3ColdStore (NEW, Phase 3)
```

Sits alongside `LocalFsColdStore`, sharing the `ColdStore`
trait surface defined in
[`cold_store/mod.rs`](#13-references):

```rust
#[async_trait]
pub trait ColdStore: Send + Sync {
    async fn scan(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RawSample>, ColdStoreError>;
}
```

### 7.2 Trait extension

The current `ColdStore::scan` returns `Vec<RawSample>` — fine
for the raw-JSONL path, lossy for Gorilla because it forces
materialisation of every sample. Phase 3 widens the trait
**additively** with a streaming variant + a chunk-level lookup
so `GorillaQueryEngine` can iterate without materialising:

```rust
#[async_trait]
pub trait ColdStore: Send + Sync {
    /// Existing — kept for raw-JSONL path back-compat.
    async fn scan(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RawSample>, ColdStoreError>;

    /// NEW — list chunk descriptors covering the range, no decode.
    async fn list_chunks(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<ChunkRef>, ColdStoreError> {
        // default impl: not all stores have native chunks; the
        // raw-JSONL store synthesises a single chunk per part.
        Err(ColdStoreError::Unsupported("list_chunks"))
    }

    /// NEW — read + decode a single chunk into a sample iterator.
    async fn read_chunk(
        &self,
        chunk: &ChunkRef,
    ) -> Result<Box<dyn Iterator<Item = RawSample> + Send>, ColdStoreError> {
        Err(ColdStoreError::Unsupported("read_chunk"))
    }
}

pub struct ChunkRef {
    pub key:                String,
    pub metric:             String,
    pub label_set:          BTreeMap<String, String>,
    pub label_hash:         u64,
    pub start_time_unix_ms: i64,
    pub end_time_unix_ms:   i64,
    pub sample_count:       u32,
    pub size_bytes:         u32,
    pub encoder_version:    u16,
}
```

`LocalFsColdStore` keeps its existing `scan()` impl + returns
`ColdStoreError::Unsupported` for the new methods (the raw
fallback path uses `scan()` only).
`GorillaS3ColdStore` implements all three; its `scan()`
materialises by calling `list_chunks() → read_chunk() →
collect()`, so a Gorilla store also satisfies the older
fallback adapter (`s3_adapter.rs`) without change.

### 7.3 `GorillaS3ColdStore` shape

```rust
pub struct GorillaS3ColdStore {
    s3:         Arc<aws_sdk_s3::Client>,
    bucket:     String,
    tenant:     String,
    decoder:    Arc<dyn ChunkDecoder>,
    index_cache: IndexCache,           // per-hour-bucket index.json cache
}

impl GorillaS3ColdStore {
    pub async fn list_chunks(&self, metric: &str, start_ms: i64, end_ms: i64)
        -> Result<Vec<ChunkRef>, ColdStoreError>
    {
        let mut out = Vec::new();
        for prefix in hour_prefixes_gorilla(&self.tenant, metric, start_ms, end_ms) {
            let idx = self.index_cache.fetch(&self.s3, &self.bucket, &prefix).await?;
            for chunk in idx.chunks {
                if chunk.end_time_unix_ms > start_ms && chunk.start_time_unix_ms < end_ms {
                    out.push(chunk.into());
                }
            }
        }
        Ok(out)
    }

    pub async fn read_chunk(&self, c: &ChunkRef)
        -> Result<Box<dyn Iterator<Item = RawSample> + Send>, ColdStoreError>
    {
        let body = self.s3.get_object().bucket(&self.bucket).key(&c.key)
            .send().await?
            .body.collect().await?;
        let decoded = self.decoder.decode(body.into_bytes())?;
        Ok(Box::new(decoded.samples()))
    }
}
```

### 7.4 S3 client choice

- Default: **`aws-sdk-rust`** (the official AWS SDK; supports
  S3 + MinIO via custom endpoint URL). MinIO compatibility
  via `endpoint_url=http://minio:9000` + `force_path_style=
  true`; tested in MVP via the existing MinIO docker-compose
  used by the cold-fallback path.
- Alternative: **`s3` crate** (lighter-weight; if `aws-sdk-
  rust`'s bin size becomes painful). Either client suffices —
  the `GorillaS3ColdStore` API hides the choice.

### 7.5 Caching

Two layers:

| Layer | Object | Eviction | Notes |
|---|---|---|---|
| **Index cache** | `index.json` per `(tenant, metric, hour_bucket)` | TTL (default 60 s) + LRU on file count | hour bucket is "live" when the current wall-clock hour matches; live buckets skip the cache and always re-fetch |
| **Chunk cache** (`ChunkCache` in §6.5) | decoded `DecodedChunk` per `ChunkRef.key` | LRU on total decoded bytes (default 256 MiB) | shared across queries within one backend process |

Both are in-memory. A future Phase 6+ enhancement is a
file-backed disk cache (similar to OS page cache for
S3-Compatible block stores) for very large working sets;
deferred until measured.

### 7.6 Method-signature alignment with `LocalFsColdStore`

The new `ColdStore` trait methods (`list_chunks`,
`read_chunk`) carry default impls returning
`ColdStoreError::Unsupported` so all existing consumers of the
`ColdStore` trait — most importantly the
`s3_adapter::Cold-fallback` query path in
[`asap-query-engine/src/drivers/query/fallback/`](#13-references) —
continue to compile unchanged. `LocalFsColdStore` opts out of
the new methods; `GorillaS3ColdStore` opts in to all three.

---

## 8. Capability routing extension

### 8.1 New `StorageBackend` axis

Today's `find_compatible_aggregation` in
[`asap-common/dependencies/rs/asap_types/src/capability_matching.rs`](#13-references)
matches on `(metric, statistic, sub_type, window_size,
grouping_labels, spatial_filter)` — there is no axis for
"which storage tier is this aggregation served from." A
`Sum` query against a `Sum` `AggregationConfig` and a `Sum`
query against a Gorilla-S3-backed metric look identical.

Phase 5 introduces a **storage-backend axis** to disambiguate:

```rust
// asap-common/dependencies/rs/asap_types/src/capability_matching.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StorageBackend {
    /// Warm-tier sketch DB (today's SimpleMapStore + accumulators).
    /// All aggregations whose `AggregationConfig.aggregation_type`
    /// is in the today's set route here.
    SketchWarmTier,

    /// NEW — Gorilla-S3 cold engine. The aggregation is "exact",
    /// served by GorillaQueryEngine reading chunks from S3.
    GorillaS3,

    /// Reserved for future tiers (e.g. an exact in-memory hot tier).
    #[serde(other)]
    Other,
}
```

`AggregationConfig` (in the same crate) gains a new field:

```rust
pub struct AggregationConfig {
    // … existing fields (aggregation_id, aggregation_type, …)
    /// Which backend serves this aggregation. Defaults to
    /// `StorageBackend::SketchWarmTier` so all today's configs
    /// continue to route to SimpleMapStore.
    #[serde(default = "default_storage_backend")]
    pub storage_backend: StorageBackend,
}

fn default_storage_backend() -> StorageBackend { StorageBackend::SketchWarmTier }
```

### 8.2 `compatible_agg_types` extension

```rust
pub fn compatible_agg_types(stat: Statistic) -> &'static [AggregationType] { /* unchanged */ }

/// NEW — list (statistic, agg_type) pairs that can also be
/// served by a Gorilla-S3 storage backend with `kind=Exact`.
pub fn gorilla_s3_compatible(stat: Statistic) -> bool {
    matches!(
        stat,
        Statistic::Sum   | Statistic::Count
      | Statistic::Min   | Statistic::Max
      | Statistic::Quantile
      | Statistic::Topk
      | Statistic::Cardinality
      | Statistic::Rate  | Statistic::Increase
    )
}
```

Every `Statistic` is Gorilla-S3-compatible because Gorilla's
"sketch" is the raw sample stream, and every statistic over
the raw stream is exact by construction. The current
`compatible_agg_types` (which returns warm-tier-aggregator
candidates) is **untouched**.

### 8.3 Routing in `find_compatible_aggregation`

```rust
pub fn find_compatible_aggregation(
    configs: &HashMap<u64, AggregationConfig>,
    requirements: &QueryRequirements,
) -> Option<AggregationIdInfo> {
    // 1. Try Gorilla-S3 first if the requirement is "exact"
    //    AND the metric has a configured Gorilla aggregation.
    if requirements.accuracy == AccuracyTarget::Exact
        && gorilla_s3_compatible(requirements.statistics[0])
    {
        if let Some(info) = find_gorilla_aggregation(configs, requirements) {
            return Some(info);
        }
    }
    // 2. Fall back to today's warm-tier matching.
    find_warm_aggregation(configs, requirements)
}
```

`AccuracyTarget::Exact` (per
[`controller/docs/design.md`](../controller/docs/design.md)
§6 `core::workload`) is the hint that pushes a query to the
Gorilla path. If the controller's plan has not configured
the metric for Gorilla-S3, the query falls back to warm-tier
matching (which may or may not have an exact aggregator;
typically `Sum` / `MinMax` are exact, others are
approximate).

### 8.4 Inference YAML extension

The deploy-side `backend-inference.yaml` files (and their
sketch-specific variants `backend-inference-{cms,cs,hll,kll}.
yaml`) gain a `data_source` axis:

```yaml
# backend-inference-gorilla.yaml — minimal Gorilla-only deployment
inferences:
  - pattern: "sum_over_time(audit_events[5m])"
    data_source: gorilla_s3      # NEW
    aggregation_id: 100
  - pattern: "quantile_over_time(0.99, latency_critical[5m])"
    data_source: gorilla_s3
    aggregation_id: 101
  # warm-tier inferences continue to omit the field (defaults to sketch_warm_tier)
  - pattern: "sum_over_time(http_requests_total[5m])"
    aggregation_id: 1
```

PR #266 mirrored 33-entry inference sets across the existing
overlays (per
[`pipeline-query-catalog.md`](pipeline-query-catalog.md)
§2.3); a parallel `backend-inference-gorilla.yaml` overlay is
landed alongside them in the deploy follow-up that
accompanies Phase 5.

---

## 9. Controller integration (5-layer fit)

The controller's 5-layer pipeline
([`controller/docs/design.md`](../controller/docs/design.md))
is the spine for the migration. The Gorilla-S3 path slots in
predominantly at L4 (binding) + L5 (stage assignment), with
small additions at L3 + the cost model.

### 9.1 L1 — query_language (no change)

PromQL parsing is identical. The Gorilla-S3 path doesn't
introduce a new query language.

### 9.2 L2 — logical_plan (no change)

PromQL → `PromqlLogicalPlan` → `QueryExpr` lowering is
identical.

### 9.3 L3 — intent_algebra

Minor change: the `AggIntent` already carries an
`AccuracyTarget` enum
([`controller/docs/design.md`](../controller/docs/design.md)
line ~1016: `AccuracyTarget = Exact | Epsilon(f64) |
EpsilonDelta {…}`). For Gorilla-eligible queries the planner
passes `AccuracyTarget::Exact` instead of
`AccuracyTarget::Epsilon(eps)`.

L3 stays sketch-agnostic. The "exact" hint is data — it does
not encode "use Gorilla"; the L4 rule decides that.

### 9.4 L4 — sketch_algebra

The L4 IR is `SketchExpr` (per
[`controller/docs/design.md`](../controller/docs/design.md)
§6 `core::sketch_algebra`). Today's `Bind*` rules map intents
to sketch-bound `SketchExpr::SketchAgg` variants.

For Gorilla-S3 we add a new `Bind*` rule. Two API choices —
the doc records the choice as **option A**, with option B
documented for posterity:

#### Option A (recommended) — Tag the existing `Logical(...)` variant

```rust
// in core::sketch_algebra
pub struct BindGorillaExact;

impl OptimizerRule for BindGorillaExact {
    fn name(&self) -> &'static str { "BindGorillaExact" }
    fn category(&self) -> RuleCategory { RuleCategory::Bind }
    fn priority(&self) -> u16 { 50 }   // before any approximate-binding rule

    fn apply(&self, q: &QueryExpr, c: &DeploymentConstraints) -> Option<SketchExpr> {
        let intent = q.aggregate_intent()?;
        let metric = q.scan_metric()?;
        // Gate: accuracy is Exact AND metric is configured
        // for Gorilla-S3 storage in the controller plan.
        if intent.accuracy != AccuracyTarget::Exact { return None; }
        if !c.is_metric_gorilla_eligible(&metric) { return None; }
        Some(SketchExpr::Logical(q.clone()).with_storage_hint(StorageBackend::GorillaS3))
    }
}
```

Pro: minimal new IR; the `Logical(...)` wrapper already
exists for "no sketch was bound here." Con: the storage hint
is a side-attribute, not visible in the type system.

#### Option B — New `SketchExpr::ExactStorage` variant

```rust
pub enum SketchExpr {
    Logical(QueryExpr),
    SketchAgg { /* … */ },
    /// NEW — explicit-exact, storage-backed.
    ExactStorage {
        backend: StorageBackend,
        child:   Box<SketchExpr>,
    },
    /* … */
}
```

Pro: type-system-visible; rule-engine can match on the
variant cleanly. Con: more IR surface; a new variant for
each storage backend would proliferate.

**Recommendation: option A** for MVP. Re-evaluate at Phase 5
if the side-attribute proves clumsy in the L5 emitter.

### 9.5 L5 — stage_split

[`controller/docs/design.md`](../controller/docs/design.md)
§6 `core::physical` defines today's `StageId` as
`Edge | Gateway | Backend` and `Topology::ThreeStage` as the
canonical 3-stage colouring. The Gorilla-S3 path adds a
**new stage role**: `Storage` — the S3 bucket itself acts as
a stage that holds chunk state between Edge (encode) and
Backend (read + exact query).

Two API choices:

#### Option A (recommended) — New `StageId::Storage` variant

```rust
pub enum StageId {
    Edge,
    Gateway,
    Backend,
    /// NEW — object storage tier (S3 / MinIO) that holds
    /// chunks between Edge encoding and Backend reading.
    /// Distinct from Gateway because no compute happens
    /// here at write time; reads happen on demand from
    /// Backend.
    Storage,
}

pub enum Topology {
    ThreeStage,             // existing — Edge → Gateway → Backend
    /// NEW — Edge → Storage → Backend, no Gateway.
    EdgeStorageBackend,
    /// NEW — Edge → Gateway → Storage → Backend, hybrid.
    /// Used when sketch metrics + Gorilla metrics share a
    /// deployment.
    FourStage,
}
```

Pro: clean separation; `Storage` semantically distinct from
`Gateway` (no compute, just hold). Con: more topology
variants for the allocator to handle.

#### Option B — Extend `Topology::ThreeStage` to `Topology::FourStage`

Keep `StageId` 3-valued; `Storage` is implicit between
`Gateway` and `Backend` as a wire-level construct. The
allocator emits a `Storage` cut-edge marker on the Gateway →
Backend edge for Gorilla-tagged sub-DAGs.

Pro: smaller IR delta. Con: muddles "stage" with "edge type."

**Recommendation: option A** for MVP. `Topology::FourStage`
is the canonical hybrid topology that supports both warm and
Gorilla tiers in the same deployment; `Topology::Edge-
StorageBackend` is the "Gorilla-only" deployment the b5-style
benchmark exercises.

The `StageAllocator` colouring rule for the new variant:

| `SketchExpr` shape | Stage assignment |
|---|---|
| `Logical(Scan)` | `Edge` |
| `Logical(Window) over Logical(Scan)` | `Edge` |
| `SketchExpr::Logical(Aggregate{exact})` w/ `StorageBackend::GorillaS3` hint | **Edge** for the encode side; the read side is implicit on `Backend` (zero-stage on `Storage`) |
| `SketchAgg { … }` (warm tier) | `Edge` (build) → `Gateway` (merge) → `Backend` (read) per existing colouring |
| `SketchEstimate` for Gorilla | `Backend` |
| `SketchEstimate` for warm tier | `Backend` (per existing colouring) |

The `StageConfig` (per Phase E `emitter.rs` —
[`controller/docs/design.md`](../controller/docs/design.md)
lines ~899–911) gains a new variant:

```rust
pub enum StageConfig {
    Edge(EdgeStageConfig),
    Gateway(GatewayStageConfig),
    Backend(BackendStageConfig),
    /// NEW — describes the S3 bucket / prefix layout for
    /// the Storage stage; consumed by the agent (to know
    /// where to PUT) and by the backend (to know where to
    /// GET).
    Storage(StorageStageConfig),
}

pub struct StorageStageConfig {
    pub bucket: String,
    pub region: Option<String>,
    pub tenant: String,
    pub key_layout: KeyLayout,            // mirrors §4.5
    pub encryption: EncryptionPolicy,
}
```

### 9.6 Cost model

`CostModel::workload_cost`
([`controller/docs/design.md`](../controller/docs/design.md)
§6 `core::cost`) gains two new cost terms:

| Cost term | Driven by | Scaling |
|---|---|---|
| **S3 PUT cost** | `(num_metrics × num_labelsets × samples_per_window / window_seconds)` × `$/PUT` | linear in window count; small per-PUT cost ($0.005 / 1000 PUTs on S3 Standard) |
| **S3 GET cost** | per-query: `(chunks_read × $/GET) + (bytes_decoded × $/GB egress if cross-region)` | linear in query rate × chunks-per-query; minor per-GET cost ($0.0004 / 1000 GETs); egress can dominate for cross-region |
| **S3 storage cost** | `chunk_bytes × retention × $/GB-month` | sustained; the headline savings vs raw retention because Gorilla typically achieves 10–20× compression on smooth f64 streams |

The cost objective decides per-metric whether to:

- **Route via warm-tier sketch** — high query rate amortises
  per-window cost; tolerates ε.
- **Route via Gorilla-S3** — exact required, OR retention is
  long-lived and storage cost dominates query cost.
- **Route via raw-JSONL fallback** — capability-miss safety
  net only; controller does not opt metrics into this tier.

The Pareto frontier the paper exercises (per
[`docs/paper-outline.md`](paper-outline.md) §6.4) extends with
Gorilla-S3 as a third Pareto point on the (accuracy × query
latency × $/GB-month × wire bandwidth) grid.

### 9.7 What does and doesn't trigger replanning

Per `memory/feedback_controller_plan_triggers.md`: only
**new queries / workloads** trigger a fresh plan. A capability
miss (a query against a metric with no warm-tier and no
Gorilla aggregation) does **not** replan; it falls back to
the cold-store JSONL path and waits for the next planning
cycle. The same rule applies to the Gorilla path: if a query
requests `accuracy: Exact` for a metric the controller has
not (yet) configured for Gorilla-S3, it falls back to JSONL
cold tier; the controller observes the miss in its workload-
drift telemetry and may add a Gorilla-S3 binding on the next
plan cycle.

---

## 10. Paper / product mapping

For each of the 5 paper claims (per
[`docs/paper-outline.md`](paper-outline.md) §"Measurable
benefits — five evaluation dimensions"), how does the
Gorilla-S3 path compare to the sketch warm-tier path:

### ① Reduced transmission bandwidth

| Path | Wire bandwidth (agent → backend) |
|---|---|
| Raw passthrough (b0a / b0b) | full sample rate |
| Compression baseline (b1 Serf, b5 Gorilla local-only) | compressed sample rate, written locally / forwarded |
| Sketch warm tier (today's main path) | sketch-envelope sample rate, ~10–100× smaller than raw |
| **Gorilla-S3** | **zero off-host** (agent → S3 directly; no wire to backend) |

The Gorilla-S3 path **trades wire bandwidth for storage cost
+ storage IO**. The on-wire claim is the strongest of any
tier.

### ② Low edge collector CPU overhead

Gorilla XOR encoding is CPU-cheap (simple bit-ops on f64
mantissa, no hashing, no comparison-tree). Per the
b5-gorilla baseline numbers tracked in `PROGRESS.md` —
matched-accuracy benchmarks show Gorilla competitive with
raw forwarding on edge CPU. The new processor adds the S3
PUT call on window close; the AWS SDK Go client has been
benchmarked at sub-ms per PUT at the small chunk sizes
typical of one-window batches.

### ③ Low edge collector memory overhead

Per-window buffer = chunk memory budget = `O(samples per
window per (metric, labelset))`. For 60 s windows at 1
sample/s × 1000 labelsets per metric = 60 000 samples × 16 B
= ~1 MB per metric. Across the typical metric mix this is
small relative to the sketch processors' per-series state.

### ④ Backend query accuracy

**Exact** by construction (`AccuracyProfile::exact()`). This
is the headline win for the Gorilla-S3 path: every PromQL
answer is bit-for-bit ground truth, whereas warm-tier sketches
carry the per-sketch-family `(ε, δ)` envelope.

The accuracy reducer in `deploy/scripts/accuracy_reduce.py`
(P8 in `PROGRESS.md`) compares warm-tier query answers against
the cold-tier ground truth (raw JSONL) — the same harness
extends trivially to compare warm-tier vs Gorilla-S3 since
Gorilla-S3 IS the ground truth for the metrics it covers.

### ⑤ Fast query computation / short query latency

Gorilla-S3 is **slower than warm-tier** by construction:
warm-tier reads pre-merged accumulator state out of
`SimpleMapStore` (typically µs to low-ms); Gorilla-S3 reads
chunks out of S3 (10s–100s of ms per GET; ms per decode).

Headline budget per
[`docs/paper-outline.md`](paper-outline.md) §6 cold-fallback
target: **≤ 2× the warm-tier p99 latency**. Concretely, if
the warm tier achieves p99 = 30 ms (typical), the Gorilla
path budget is 60 ms p99. Achievable when:

- Index cache hit (avoid `LIST` + `index.json` GET).
- Chunk cache hit on the working set.
- One S3 GET per chunk (small chunks; no multi-GET fan-out).

The paper's §6.4 latency CDF will show Gorilla-S3 as a
distinct curve, slower than warm-tier but well inside the
"production-usable" envelope (sub-second for typical
ranges).

### Combined Pareto

The Pareto figure (`pareto_acc_vs_thru.png` per
[`docs/paper-outline.md`](paper-outline.md) §6.4) extends with
Gorilla-S3 as a Pareto point on:

```
                     accuracy
                        │
  Gorilla-S3 ●          │            ● raw JSONL fallback
             │          │           /
             │          │          / (slow exact)
             │          │         /
             │   sketch warm tier ●─── (small ε, fast)
             │          │       /
             │          │      / (CMS, KLL, HLL, …)
             │          │     /
             └──────────┴────┴───────── query latency / cost
                                        ─────────────────►
                                        $$/query, ms, GB-month
```

Gorilla-S3 owns the "exact + cheap-storage + cold-IO" corner;
warm tier owns the "approximate + cheap-query + warm-RAM"
corner; raw JSONL owns the "exact + uncompressed + emergency"
corner. The controller picks per metric; a real deployment
mixes all three.

---

## 11. Phased implementation plan

| Phase | Scope | LOC est | Owner |
|---|---|---|---|
| **0** | This doc (`docs/design-gorilla-s3-cold-engine.md`) | doc only | (this PR) |
| **1** | `asap-gorilla` block format crate (Rust + Go bindings); wraps the Telegraf-side encoder; cross-language byte-parity test | ~500 | future |
| **2** | `gorillas3processor` (sketchcol path; Go OTel processor under `opentelemetry-collector-contrib-patch/processor/gorillas3processor/`); reuses §5 spec; small e2e against MinIO local | ~600 | future |
| **3** | `GorillaS3ColdStore` adapter (`asap-query-engine/src/drivers/query/fallback/cold_store/gorilla_s3.rs`); extends `ColdStore` trait additively; in-memory + LRU caches | ~400 | future |
| **4** | `GorillaQueryEngine` (`asap-query-engine/src/engines/gorilla_engine.rs`); dispatches to `GorillaS3ColdStore`; per-statistic-family executor; result wrapping with `accuracy: ε=0,δ=0` info | ~800 | future |
| **5** | Capability routing extension (`StorageBackend` enum in `asap-common`; `find_compatible_aggregation` opt-in dispatch; controller `BindGorillaExact` rule + `StageId::Storage` colouring + `Topology::FourStage`) | ~200 (asap-common) + ~400 (controller) | future |
| **6** | E2E integration test — fake exporter writes to MinIO via Gorilla path; backend serves PromQL queries against the chunks; accuracy reducer joins ground truth | ~300 | future |
| **7+** | `sketchotap` Rust adapter (`otap-patch/processors/gorillas3processor/`); `sketchtelegraf` aggregator-side glue (the output plugin already exists); compaction (hourly small-chunk → 64 MB block); index hot-bucket optimisation; disk-backed chunk cache | follow-up | future |

LoC estimates are best-guess — refine per phase as the work
lands. Each phase is independently shippable + reverts cleanly.

---

## 12. Open questions

Pre-defined questions from the orchestrator + ones discovered
while drafting this doc. Where the doc has formed an opinion,
it's recorded here; truly-open questions are flagged for
orchestrator decision.

### Q1. Is this a product tier or paper-only baseline? Both?

**Both.** §1 use cases enumerate the product tier (compliance,
cost-sensitive metrics, long-tail debug). §10 enumerates the
paper coverage. The doc explicitly does **not** classify it as
"paper-only" — that would scope down the design substantially
(e.g. no compaction, no SSE-KMS, no multi-tenant). The
*minimum viable* phase set (1-6) is paper-grade; phase 7+
turns it into a production tier.

### Q2. Hot/cold split: a metric in Gorilla-S3 — does it ALSO go through sketch warm-tier?

**Strict either-or by default; double-write opt-in.** Default:
a metric configured for Gorilla-S3 has `drop_original: true`
on the agent side, so no warm-tier sketches are built. This
is the wire-bandwidth-zero claim.

Opt-in `drop_original: false` mode supports double-write for
migration / parity testing. The cost model penalises this
(both the wire bytes + the storage cost) so the controller
won't pick it autonomously — only operators set it manually
for testing.

### Q3. Default storage: S3 / MinIO / local-FS-S3-compatible?

**S3-compatible API mandatory; MinIO + local-FS supported in
dev.** The processor + adapter speak S3 API only. MinIO is
the canonical local-dev target (already used by the cold-
JSONL fallback's e2e tests). Pure-local-FS mode (mirroring
the Telegraf plugin's `local_dir` option) is for the
b5-gorilla baseline and benchmarks; it bypasses the index
file (the reader walks the FS tree directly).

### Q4. Compaction strategy?

**Deferred to Phase 7+.** MVP ships small chunks (60 s
windows) only. Compaction (hourly small-chunks → 64 MB
blocks) is **specified** in §4.3 but **not implemented**. The
reader supports both small chunks and large blocks via the
same `index.json`, so adding compaction later is a write-side-
only change.

The compaction trigger is open: time-based (every hour after
the bucket goes "cold") vs size-based (when small-chunk count
in a bucket exceeds N) vs manual. Recommendation: time-based,
24 h after the hour bucket closes (so live queries don't
compete with the compactor for the active hot bucket).

### Q5. Default chunk granularity?

**60 s tumbling, configurable.** Matches the existing b5-
gorilla baseline. Rationale: 60 s gives a useful compression
ratio for typical metric streams (Gorilla compresses better
on longer per-stream batches, up to a point) while keeping
edge memory bounded. Compaction (Q4) addresses the
many-small-chunks-vs-fewer-large-blocks read-side trade-off
without changing the write granularity.

### Q6. Encryption defaults?

**SSE-KMS by default; opt-out via `encryption: none`.** The
Telegraf plugin already has the SSE-KMS code path
([`gorilla_s3.go`](../telegraf-patch/plugins/outputs/gorilla_s3/gorilla_s3.go)
lines 256–259); the OTel processor mirrors the default.
Local-dev / MinIO deployments without KMS opt out via config.

### Q7. Multi-tenant key prefix?

**`<tenant>/...` mandatory; defaults to `default`.** The key
layout in §4.5 always carries the tenant prefix. Single-tenant
deployments use `tenant: "default"`. Cross-tenant access
control is delegated to S3 IAM / bucket policies — the
processor + adapter do not enforce tenancy at the application
layer.

**Open sub-question:** when multiple agents in a fleet write
the same metric, how is the labelset partitioned to avoid
double-write of the same `(tenant, metric, hour, labelset)`
chunk? Recommendation: per-`(tenant, metric)` ownership via
agent-id hash bucketing OR metric-pattern routing pushed by
the controller. **Flagged for orchestrator decision** — the
right answer depends on whether the fleet writes are
sharded (controller-driven) or unsharded (concurrent writes
must merge at read time, which costs an extra index-merge
read per query).

### Q8. Caching strategy (LRU? size-based? time-based?)

**LRU, total-decoded-bytes-budgeted (default 256 MiB) for the
chunk cache; LRU + 60 s TTL for the index cache.** Index
cache TTL forces re-reads of "live" hour buckets (the current
wall-clock hour) so concurrent agents' updates become visible
within 60 s.

Future Phase 7+ enhancement: per-process disk-backed cache
for very large working sets; revisit when measurement justifies
it.

### Q9. What does "ingestion success" mean for the agent — S3 PUT confirmed, or in-flight okay?

**S3 PUT confirmed.** The agent considers a sample
"durably ingested" only after the S3 PUT completes (and the
matching `index.json` PUT completes). Until then the sample
sits in the per-window buffer (memory) or the local spool
(disk, if `on_s3_failure: spool`). A backpressure signal can
propagate upstream when the buffer + spool are at capacity —
upstream handles the signal per its own pipeline conventions.

This is **stronger** than the warm-tier path's "in-flight ok"
shape (the warm-tier processor considers a sketch ingested
when it's enqueued for OTLP export; OTLP delivery success is
not propagated back to the data source). The stronger
guarantee is the cost of the lossless retention promise.

### Q10. Failure modes (S3 unavailable): block / drop / spool?

**Configurable per `on_s3_failure`; spool as production
default.** See §5.6 + §5.6.1 for the picker. The default is
`spool` because production is on-prem MinIO often enough that
short outages are recoverable via a local disk buffer; the
benchmark default is `drop` because tester-induced outages
should not contaminate paper numbers.

---

## 13. References

### Repository docs (read these before extending this design)

- [`docs/pipeline-query-catalog.md`](pipeline-query-catalog.md)
  — current end-to-end pipeline catalog; the Gorilla-S3 path
  adds a new row to §3 (a follow-up PR mirrors this design
  there).
- [`controller/docs/design.md`](../controller/docs/design.md)
  — controller 5-layer architecture; §9 of this doc maps the
  Gorilla path onto its L3-L5 surfaces.
- [`docs/design-asap-edge-framework.md`](design-asap-edge-framework.md)
  — five-layer edge framework + adapter / control-channel
  contract; the Gorilla-S3 path is a Layer-4 codec + Layer-5
  sink combination per §7 of that doc.
- [`docs/paper-outline.md`](paper-outline.md) — the paper §6
  evaluation grid this design extends with the Gorilla
  Pareto point.
- [`docs/design-asap-otap-rust-integration.md`](design-asap-otap-rust-integration.md)
  — the OTAP-Rust integration template the §5.7 sketchotap
  variant follows.
- [`docs/design-asap-telegraf-integration.md`](design-asap-telegraf-integration.md)
  — the Telegraf integration template the §5.7 sketchtelegraf
  variant follows.
- [`PROGRESS.md`](../PROGRESS.md) — "Outstanding follow-ups"
  for the broader project; this design's Phase 1 (asap-
  gorilla crate) ships in parallel with the cold-store
  cleanup work tracked there.
- `memory/project_asap_e2e_goal.md` (user-memory) — the
  controller's e2e goal includes "real PromQL answers per
  sketch, not just green tests"; this design extends that
  promise to "real PromQL exact answers per Gorilla-eligible
  metric."
- `memory/feedback_controller_plan_triggers.md` (user-memory)
  — only new queries / workloads trigger fresh plans;
  capability misses (including missing-Gorilla-binding
  misses) fall back to cold-store, not replan. §9.7 above
  pins this.

### Source files this design lifts from (encode + S3-write +
index format are the source of truth on the agent side)

- [`telegraf-patch/plugins/outputs/gorilla_s3/gorilla_s3.go`](../telegraf-patch/plugins/outputs/gorilla_s3/gorilla_s3.go)
  — existing Telegraf-side `gorilla_s3` output plugin;
  encode + S3 PUT + multipart + retry + SSE-KMS + local-FS
  paths are all built. The OTel processor (§5) mirrors this
  byte-for-byte on the Go side.
- [`deploy/configs/sketchcol-agent-b5-gorilla.yaml`](../deploy/configs/sketchcol-agent-b5-gorilla.yaml)
  — the local-dir b5 baseline config; the production-S3
  variant lands as a sibling deploy config in the
  Phase 2 follow-up.

### Source files this design extends (backend side)

- `ASAPQuery-backend/asap-query-engine/src/drivers/query/fallback/cold_store/mod.rs`
  — `ColdStore` trait surface; §7.2 widens this additively.
- `ASAPQuery-backend/asap-query-engine/src/drivers/query/fallback/cold_store/format.rs`
  — JSONL format + `part_path_prefix` + `hour_prefixes`
  helpers; §4.5 mirrors the hour-bucket layout.
- `ASAPQuery-backend/asap-query-engine/src/drivers/query/fallback/cold_store/local_fs.rs`
  — `LocalFsColdStore` is the trait implementation reference;
  `GorillaS3ColdStore` (§7.3) follows the same shape.
- `ASAPQuery-backend/asap-query-engine/src/engines/simple_engine.rs`
  — `SimpleEngine` is the sibling engine `GorillaQueryEngine`
  (§6) sits next to.
- `ASAPQuery-backend/asap-query-engine/src/stores/sketch_db/accuracy.rs`
  — `AccuracyProfile::exact()` is the result-side contract
  for §6.6's `infos` line.
- `ASAPQuery-backend/asap-common/dependencies/rs/asap_types/src/capability_matching.rs`
  — capability matching surface; §8 extends it with
  `StorageBackend::GorillaS3`.

### External references

- Pelkonen, T. et al. "Gorilla: A Fast, Scalable, In-Memory
  Time Series Database." Proceedings of the VLDB Endowment,
  vol. 8, no. 12, 2015. The encoder algorithm.
- AWS S3 PUT object reference (multipart, SSE-KMS) — the
  `aws-sdk-rust` + `aws-sdk-go` documentation pages.
- MinIO S3 compatibility documentation — for local-dev
  testing.

### Adjacent design docs

- [`docs/design-asap-vector-integration.md`](design-asap-vector-integration.md)
  — Vector-side adapter design, included for symmetry though
  Vector is not in the Phase 1-6 plan.
- [`docs/sketch-algebra-query-mapping.md`](sketch-algebra-query-mapping.md)
  — query → sketch algebra mapping; the Gorilla path is the
  "no sketch binding" trivial mapping for `accuracy: Exact`
  + Gorilla-eligible metrics.
- ADRs under `docs/adr/` — architecture decision records;
  this design will earn a new ADR (`adr-NNNN-gorilla-s3-
  cold-engine.md`) summarising the open-question outcomes
  in §12 once they are settled.

---

*End of design doc. Next step: Phase 1 lands the
`asap-gorilla` block format crate — see §11.*
