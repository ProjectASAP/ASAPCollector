# ASAP archive tier — design

> **Status:** current-state design reference, 2026-05-07. Merged from
> `design-gorilla-s3-cold-engine.md` (original 2-tier framing + custom
> Gorilla chunk wire format) and
> `design-jsonl-deprecation-and-gorilla-promql-completeness.md`
> (post-Path-A2 state — Prometheus TSDB blocks + Thanos serving the
> archive — and the three operational modes).
>
> **Audience:** anyone asking "how does ASAP retain raw samples for
> exact PromQL answers off the wire, and what swaps in / out of the
> archive path under different operating modes?". The companion design
> for the warm sketch tier lives in `docs/design-asap-edge-framework.md`
> + `docs/sketch-algebra-query-mapping.md`; the controller's
> placement / replan rules live in
> `docs/control-plane-design.md`. This doc is the single source of
> truth for the archive tier.
>
> **Last refreshed:** 2026-05-07.

---

## Table of contents

1. [Goal + role of the archive tier](#1-goal--role-of-the-archive-tier)
2. [Wire format — Prometheus TSDB blocks (canonical)](#2-wire-format--prometheus-tsdb-blocks-canonical)
3. [Bucket layout on object storage (MinIO / S3)](#3-bucket-layout-on-object-storage-minio--s3)
4. [Three operational modes](#4-three-operational-modes)
5. [Query path](#5-query-path)
6. [Compaction](#6-compaction)
7. [Cost-model break-even table](#7-cost-model-break-even-table)
8. [Legacy chunk format (historical)](#8-legacy-chunk-format-historical)
9. [What was deleted](#9-what-was-deleted)
10. [Open questions](#10-open-questions)
11. [References](#11-references)

---

## 1. Goal + role of the archive tier

ASAP runs a **two-tier** storage architecture:

- **Warm sketch tier** — `(ε, δ)`-bounded approximate answers from
  pre-merged sketch state. Optimised for high query rate amortised
  over a window. Bandwidth-efficient on the wire (sketch envelopes,
  ~10–100× smaller than raw). Default for everything that fits a
  bounded-error contract.
- **Archive tier** (this doc) — exact answers, lossless retention,
  cold-IO-class query latency. Used when the operator's constraints
  make sketches unsuitable, or when long-range exact replay is
  required.

The archive tier exists for these driving constraints:

| Constraint | Why warm sketches are not enough |
|---|---|
| **Lossless retention** (compliance, audit, regulator-visible counters) | regulators reject `ε`-bounded answers; need the original sample on demand |
| **Long-tail debug / forensic point-in-time** ("per-pod CPU at 03:14:07 yesterday") | sketches summarise — they cannot answer point-in-time per-resource queries |
| **Long retention horizon** (e.g. 90 d Prometheus → 7 y archive) | sketch error compounds when the only retained representation is sketches |
| **Paper baseline** — Pareto comparison of sketch-warm vs archive vs raw on (accuracy × query latency × $/GB-month × wire bandwidth) | rounds out the design space |

The archive tier is **parallel to**, not a replacement for, the warm
tier. The controller picks per metric (or per metric pattern):

- High-rate / approximate-OK metrics → warm sketch tier.
- Compliance / long-retention / exact-required metrics → archive tier.
- Hybrid — both tiers are populated for the same metric (Mode 2 in §4).

The legacy raw-JSONL "cold fallback" tier was deleted in Step 1 of
the JSONL deprecation (backend PR #95, collector PR #312). A
capability miss in the warm tier no longer falls through to JSONL —
it now resolves through the archive tier. See §9.

---

## 2. Wire format — Prometheus TSDB blocks (canonical)

The canonical post-Path-A2 wire format is the **Prometheus TSDB block
format**. `gorillas3processor` writes blocks to S3 / MinIO at this
layout per `<ulid>` block:

```
<ulid>/
  chunks/
    000001
    000002
    …
  index
  meta.json
```

- **`chunks/000001…`** — Prometheus chunk files. Each chunk is
  self-describing (Gorilla XOR encoding for `f64` values + delta-of-
  delta for `i64` timestamps; chunk header carries length + sample
  count + CRC). Standard Prometheus TSDB chunk encoding; readable by
  any Prometheus / Thanos / Mimir / VictoriaMetrics reader without
  ASAP-specific code.
- **`index`** — standard Prometheus TSDB inverted index. Encodes
  `(label_name, label_value) → series_id → chunk_locations`. Enables
  postings-list-based label filtering at query time.
- **`meta.json`** — block-level metadata: ULID, time range, sample
  count, source identifier, compaction level, downsample resolution.
  Uploaded **last** so block visibility is atomic (see §3).

Reference: Prometheus TSDB format documentation —
<https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/README.md>.

The block format is **self-describing** — a block standing alone on
disk is sufficient for any standard reader to decode. No ASAP-specific
sidecars are required for correctness; the only ASAP-specific
artefact in production is a per-tenant prefix on the bucket key (§3).

`gorillas3processor` exposes a `block_format:` config that selects the
emit format:

| `block_format` | Status | Used by |
|---|---|---|
| `prometheus_tsdb` | **canonical** post-Path-A2 (PR #311, mvp step 2.1); demo + production | `thanos store-gateway` + `thanos-query` archive serving path |
| `asap` | **legacy**, still readable | in-process Rust `GorillaQueryEngine` reading buckets written before Path A2 |

The legacy `asap` format is documented in §8.

---

## 3. Bucket layout on object storage (MinIO / S3)

### 3.1 Layout

```
<bucket>/
  <tenant>/
    <ulid-1>/
      chunks/000001
      chunks/000002
      index
      meta.json
    <ulid-2>/
      …
    …
```

- **`<bucket>`** — single bucket per deployment, configurable.
- **`<tenant>`** — per-tenant prefix. Defaults to `default` for
  single-tenant deployments. Multi-tenant deployments separate by
  prefix; cross-tenant access control is delegated to S3 IAM /
  bucket policies. (Per-bucket isolation vs per-prefix isolation is
  flagged in §10 Q1.)
- **`<ulid>`** — block name. ULID gives a lexicographically-sortable
  time-prefixed identifier, so `LIST` over the prefix returns blocks
  in roughly time order without an external index.

### 3.2 Atomic visibility — `meta.json` last

Block writers (`gorillas3processor` + the `thanos compact` sidecar)
upload chunks + index first, **then** `meta.json` last. A reader
treats a block as "visible" only when its `meta.json` is present.
Partial-upload state is invisible to readers, so concurrent
write-and-read does not race.

This is the standard Thanos / Prometheus convention; readers (Thanos
store-gateway, the in-process Rust legacy engine) all honour it.

### 3.3 No `index.json` / no postings sidecar

Pre-Path-A2 buckets carried per-hour-bucket `index.json` and
`postings-v1.json` sidecars (§8). Those are not present in the
canonical Prometheus-TSDB layout; the standard `index` file inside
each block carries postings, time bounds, and chunk-locator metadata.
This eliminates the index-file-write hot path entirely (no per-PUT
re-write of the bucket-level index manifest).

---

## 4. Three operational modes

The controller picks one of three **operational modes** per metric (or
per metric pattern), expressed as a `BackendStorageRouting` plan
emitted at L5 of the 5-layer pipeline. The modes are:

### Mode 1 — sketch at the edge, sketch state on the wire, warm-tier serves

```
[workload] ──raw──► [edge: sketchprocessor] ──sketch envelope──►
  [warm-tier ingest: backend OtlpReceiver] ──sketch state──►
  [warm-tier serve: SimpleEngine]
```

- Edge runs a sketch processor component (`ddsketch` /
  `KLL` / `HLL` / `countmin` / `countsketch`).
- Wire bytes are sketch envelopes only; raw never crosses the wire.
- Warm-tier `SimpleEngine` answers PromQL with `(ε, δ)`-bounded
  accuracy.
- Archive tier is **not** populated for this metric.

This is the bandwidth-optimal mode for high-rate metrics with bounded-
error tolerance.

### Mode 2 — raw at the edge, raw OTLP on the wire, warm-tier builds sketch at ingest

```
[workload] ──raw──► [edge: passthrough] ──raw OTLP──►
  [warm-tier ingest: backend OtlpReceiver builds sketch on receipt] ──►
  [warm-tier serve: SimpleEngine]
                                    └──► [archive tier: gorillas3processor (in backend) writes Prom-TSDB blocks]
```

- Edge does no sketching; raw OTLP traverses the wire.
- Backend ingest builds the sketch and writes raw chunks to the
  archive tier in parallel.
- Warm-tier and archive both populated; queries route by shape.

This is the operating mode the demo uses for "the same metric is
queryable at warm-tier latency for approximate aggregates AND at
archive-tier exactness for forensic / long-tail queries".

### Mode 3 — raw at the edge, raw OTLP directly to Prometheus' native OTLP receiver, archive-tier serves

```
[workload] ──raw──► [edge: passthrough] ──raw OTLP──►
  [Prometheus native OTLP receiver] ──TSDB blocks via WAL/compactor──►
  [archive tier: object storage (MinIO/S3)] ──►
  [archive serve: thanos store-gateway + thanos-query]
```

- Edge does no sketching.
- Raw OTLP goes directly to a Prometheus instance running with the
  native OTLP receiver enabled (Prometheus 2.47+).
- Prometheus' own compactor writes blocks to the archive bucket via
  Thanos sidecar.
- Backend `ThanosForwardEngine` HTTP-forwards archive queries to
  `thanos-query`.

This is the "no warm tier at all" mode — used when the operator
wants the simplest possible deployment (drop sketches; rely on Thanos
for everything). The paper's §RelatedWork uses this as the
"Hydra-style always-streaming" comparison point.

### Mode selection

The controller's L4 cost model (§7) picks the mode per metric based
on:

- query rate (high → Mode 1 amortises sketch build)
- accuracy requirement (`Exact` → Mode 2 or 3)
- retention horizon (long → Mode 2 or 3 to populate archive)
- the per-sketch break-even table (§7) — when raw passthrough is
  cheaper than sketch envelopes on the wire

`Mode 2` is the default for metrics that need both warm-tier latency
and archive-tier exactness. `Mode 1` is the default for everything
else where the archive is not required. `Mode 3` is opt-in for
deployments that want to skip ASAP's sketch path entirely.

---

## 5. Query path

The backend dispatches each PromQL query through `BackendStorageRouting`
to one of three engines:

```
              ┌──────────────────────────────────────────┐
              │ PromQL → BackendStorageRouting           │
              │   per-metric: warm | archive | both      │
              └────┬───────────┬───────────┬─────────────┘
                   │           │           │
                   ▼           ▼           ▼
        ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐
        │ SimpleEngine│  │ ThanosForward│  │ PrometheusForward   │
        │  (warm)     │  │  Engine      │  │  Engine (Mode 3)    │
        │             │  │  (archive,   │  │                     │
        │             │  │   Path A2)   │  │                     │
        └─────────────┘  └──────┬───────┘  └──────────┬──────────┘
                                │                     │
                                ▼                     ▼
                       thanos-query HTTP       Prometheus HTTP
                       (over thanos-store-     /api/v1/query
                       gateway over MinIO)
```

### 5.1 `BackendStorageRouting` dispatch

`BackendStorageRouting` is a per-metric routing map emitted by the
controller's L5 stage emitter (PR #314, mvp phase α; PR #91 added the
multi-target variant so the same metric can map to BOTH warm and
archive). Each entry says:

```yaml
- metric: http_requests_total
  warm_target:
    aggregation_id: 1
  archive_target:
    engine: thanos
    bucket: asap-archive-prod
    tenant: acme
```

A query against the metric is dispatched to the warm target for
shapes the warm tier covers (per `capability_matching`) and to the
archive target otherwise. A metric configured `warm-only` has no
archive target; a metric configured `archive-only` has no warm
target. Both-target metrics are the Mode 2 default.

### 5.2 `ThanosForwardEngine` (canonical Path-A2 archive engine)

For archive queries, the backend forwards the PromQL through HTTP to
the `thanos-query` sidecar running alongside the `thanos
store-gateway` that points at the archive bucket. Thanos uses
Prometheus' reference `promql.Engine` for evaluation, so the archive
tier inherits **PromQL completeness by construction** — no curated
subset, no homegrown evaluator, no silent semantic drift from
Prometheus.

Result wrapping adds the `data_source: archive` info string and the
`accuracy: ε=0, δ=0, kind=Exact` info string so Grafana 11+ surfaces
the tier inline.

`ThanosForwardEngine` is enabled when the env var
`ASAP_ARCHIVE_ENGINE=thanos` is set on the backend (default in the
demo + production deploy).

### 5.3 `PrometheusForwardEngine` (Mode 3)

When `ASAP_ARCHIVE_ENGINE=prometheus` is set, the backend forwards
archive queries to a Prometheus instance directly (no Thanos store-
gateway in front). Used in Mode 3 deployments where Prometheus' own
TSDB + retention is the archive.

### 5.4 Legacy `GorillaQueryEngine` (in-process Rust, fallback only)

When neither `ASAP_ARCHIVE_ENGINE=thanos` nor
`ASAP_ARCHIVE_ENGINE=prometheus` is set, the backend falls back to
the in-process Rust `GorillaQueryEngine`. This engine reads the
**legacy `asap` chunk format** (§8) and answers a **curated PromQL
subset**: `sum / count / avg / min / max / rate / increase /
quantile_over_time / topk`.

Pre-Path-A2 buckets (the original `<tenant>/<metric>/YYYY/MM/DD/HH/
part-NNNNNN.gor` layout with `index.json` + `postings-v1.json`
sidecars) remain readable through this engine for backwards compat.
New deployments should use Path A2 + Thanos; the curated subset is
maintained but is no longer the load-bearing archive engine.

### 5.5 Capability miss → archive

After Step-1 (PR #95) deleted the JSONL fallback, a warm-tier
capability miss falls through directly to the archive engine
(`ThanosForwardEngine` by default) rather than to a JSONL scanner.
The cost model unifies: queries that miss the warm tier are
evaluated through the archive engine as a first-class path, not a
fallback emergency.

A query against a metric **with no routing entry at all** returns a
clear `EngineRouter` error ("no engine configured for metric X") —
not a silent drop, not a JSONL scan. This becomes a deployment-
validation step, not a runtime-behaviour change.

---

## 6. Compaction

### 6.1 `thanos compact` sidecar (canonical, Phase δ.1 PR #321)

Archive-tier compaction is performed by the stock `thanos compact`
binary running as a sidecar (deployed via
`deploy/mvp-singlenode/docker-compose/mvp-thanos-archive.yml`). It performs:

- **Block consolidation** with **decode + re-encode** — small blocks
  in the same time bucket are merged into one larger block, with
  chunks decoded and re-encoded for better compression. (This is
  more than the legacy `gorilla-compactor`'s concat-only merge —
  Thanos achieves higher post-compaction compression on top of
  block consolidation.)
- **Downsampled tiers** — at compaction time, downsampled aggregates
  are emitted as separate blocks at coarser resolutions:

| Tier | Resolution | Default retention |
|---|---|---|
| **raw** | original sample resolution | 30 d |
| **5m** | 5-minute aggregates | 180 d |
| **1h** | 1-hour aggregates | 1 y |

Range queries that span beyond the raw retention window
automatically use the 5m or 1h tier; the trade-off is exactness at
the original resolution vs storage cost over long retention.

Downsampled tiers are pre-aggregated `min / max / sum / count /
counter` per series per window, written as their own TSDB blocks at
`5m` / `1h` resolution. PromQL `_over_time` queries against long
ranges automatically pick the downsampled tier.

### 6.2 Replaced legacy `gorilla-compactor`

The legacy `gorilla-compactor` Rust binary (PR #295) was deleted in
Phase δ.1 PR #321. Its scope was concat-only merge of small chunks
into large blocks within the legacy `asap` chunk format; it had no
downsampling, no decode-re-encode, and required ASAP-specific code.
`thanos compact` covers all of its functionality and more, using a
known and well-supported Prometheus-ecosystem tool.

---

## 7. Cost-model break-even table

The controller's L4 `CostModel::workload_cost` carries a
`WireCostTable` (Phase ε.1 PR #319) that drives the Mode-1-vs-Mode-2-
vs-Mode-3 selection per metric. The table records, per sketch
family, the **break-even sample count** above which raw passthrough
on the wire is cheaper than sketch envelopes:

| Sketch family | State bytes per envelope | Break-even sample count | When raw passthrough wins |
|---|---|---|---|
| **DDSketch** (default α=0.01) | ~2.4 KiB | ~300 samples | metrics with < 300 samples per window per series |
| **KLL** (k=200) | ~3.2 KiB | ~400 samples | low-rate metrics with sparse sample distribution |
| **CountMinSketch** (w=2048, d=4) | ~32 KiB | ~4000 samples | only worthwhile on very high cardinality / rate |
| **CountSketch** (w=2048, d=4) | ~32 KiB | ~4000 samples | as above |
| **HLL** (p=14) | ~16 KiB | ~2000 samples | distinct-cardinality only |

Reading: a metric emitting fewer than `break_even` samples per window
is cheaper to ship raw (Mode 2 or Mode 3) than as a sketch envelope
(Mode 1). The cost model adds storage cost (archive) and S3 PUT/GET
to the comparison so the answer accounts for both bandwidth and
archive overhead.

The full table is generated empirically per release from
microbenchmarks in `asap-precompute-rs`; the numbers above are the
2026-05 calibration. Re-run when sketch parameter defaults change.

The cost model also costs:

| Cost term | Driven by | Notes |
|---|---|---|
| **S3 PUT** | `(blocks-emitted-per-window) × $/PUT` | small per-PUT cost ($0.005 / 1000 PUTs on S3 Standard); driven primarily by edge windowing + Thanos compactor cycles |
| **S3 GET** | per-query: `(blocks-touched × $/GET) + (decoded-bytes × $/GB egress if cross-region)` | for typical archive queries ≤ a few GETs; egress dominates only for cross-region |
| **S3 storage** | `block_bytes × retention × $/GB-month` | the headline savings come from Gorilla XOR (typically 10–20× compression on smooth `f64` streams) |
| **Warm sketch RAM** | `sum(envelope_bytes × n_series)` | proportional to active sketch state |
| **Edge CPU** | sketch-build CPU per sample | per-sketch-family microbench numbers |
| **Cut-edge bandwidth** | bytes-per-window across each (Edge → Backend) edge | the load-bearing term in Mode-selection |

---

## 8. Legacy chunk format (historical)

Pre-Path-A2 deployments wrote a **custom ASAP chunk format** with the
following layout per chunk:

```
┌──────────────────────────────────────────────────────────────┐
│ ChunkHeader (fixed-width)                                    │
│   magic               : 4 bytes  "GORS" (or "GORILLA1")       │
│   schema_version      : u16                                  │
│   encoder_version     : u16  (1 = Facebook 2015 Gorilla)      │
│   flags               : u32  (bit 0: SSE-KMS encrypted)       │
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
├──────────────────────────────────────────────────────────────┤
│ ChunkBody                                                    │
│   gorilla_payload     : Gorilla-XOR-encoded (timestamp, value)│
│   payload_crc32c      : u32                                  │
└──────────────────────────────────────────────────────────────┘
```

Bucket layout for the legacy format:

```
<tenant>/<metric>/YYYY/MM/DD/HH/part-NNNNNN.gor    ← chunk
<tenant>/<metric>/YYYY/MM/DD/HH/index.json         ← per-hour chunk manifest
<tenant>/<metric>/YYYY/MM/DD/HH/postings-v1.json   ← `label=value → series_ids`
```

Properties of the legacy format:

- Self-describing chunks (header → body all required to decode).
- `index.json` carried `byte_offset` + `byte_length` per chunk to
  enable `Range: bytes=` partial reads from S3.
- `postings-v1.json` (added by PR #295) gave label-predicate
  filtering before chunk decode.
- `gorillas3processor` writes this format only when configured with
  `block_format: asap` (the default before mvp step 2.1).
- Read by the in-process Rust `GorillaQueryEngine` (§5.4) — curated
  PromQL subset, not full PromQL.

The legacy format is **still readable**. Buckets written before Path
A2 don't need migration; they remain queryable via
`GorillaQueryEngine`. New deployments should use the Prometheus-TSDB
format (§2) + Thanos (§5.2).

The legacy format is **dead code in the active demo** — the demo
runbook + production runs pin `block_format: prometheus_tsdb` +
`ASAP_ARCHIVE_ENGINE=thanos`. The custom-format spec is preserved
here for posterity + for operators with on-disk pre-Path-A2 corpora.

---

## 9. What was deleted

The path from the original 3-tier framing (warm sketch + custom
Gorilla chunks + JSONL fallback + curated subset engine) to today's
2-tier framing (warm sketch + Prometheus-TSDB blocks served by
Thanos) involved several deletions across collector + backend +
controller:

| Deleted | When | Replaced by |
|---|---|---|
| **JSONL cold-fallback path** — `LocalFsColdStore`, `cold_store::format::parse_jsonl` reader, gateway-side raw-tee exporter, `StorageBackend::ColdJsonlFallback` enum variant, `cold-tier scan bytes` line item in `cost_model` | Step 1 (backend PR #95, collector PR #312) | a warm-tier miss now falls through to the archive engine; capability misses do not replan, they fall back per `memory/feedback_controller_plan_triggers.md` |
| **`asap-planner-rs` library** — Rust planner crate that hosted the original 33-pattern PromQL matchers | Phase γ (PR #99) | controller's L3/L4 (in `controller/`, see PR #315) hosts the patterns natively |
| **`gorilla-compactor`** — Rust binary that did concat-only merge of small `.gor` chunks into 64 MB blocks | Phase δ.1 (PR #321) | stock `thanos compact` sidecar (§6.1) — decode + re-encode, downsampled tiers, ecosystem tool |
| **Prometheus `remote_write` ingest path on the backend** — backend used to expose a `/api/v1/write` endpoint for legacy interop | PR #100 | backend ingests OTLP only; archive ingest is via `gorillas3processor` writing TSDB blocks directly to the bucket |

Net effect: ~2k LOC deleted across the four removals, three operational
surfaces collapsed (JSONL parser, planner crate, compactor binary),
and one Prometheus-ingest API surface removed.

---

## 10. Open questions

1. **Tenant isolation — per-prefix vs per-bucket?** The current
   `<tenant>/<ulid>/...` layout assumes prefix isolation. For strong
   tenant isolation (separate IAM scope per tenant; hard quota
   guarantees) per-tenant **buckets** may be required, which changes
   the controller's plan-target signalling (the `bucket` field
   becomes per-tenant-derived rather than fleet-wide). No decision
   yet; deferred until a multi-tenant deployment forces the call.

2. **Long-range query cost on the archive.** A 30-day range query
   without warm-tier coverage decodes 30 days of chunks (or hits the
   downsampled `5m` / `1h` tier per §6.1). The chunk-LRU cache helps
   for hot queries; cold queries still scan. Worth characterising
   before claiming "exact PromQL on demand at any range" in the
   paper.

3. **Sensitive-metric exemption.** Some metrics (e.g. user-PII
   counters) should never land in the archive at all. The
   `BackendStorageRouting` table can express "warm-only" via single-
   target — verify this surfaces clearly in the controller plan
   emitter so operators can opt metrics out by name or pattern.

---

## 11. References

### In-repo

- `docs/mvp-demo-runbook.md` — current demo wiring, including the
  archive-tier components on `mvp-thanos-archive.yml` and the
  freshness probe coverage matrix.
- `docs/comparison-asap-vs-databricks-pantheon-hydra.md` — comparison
  to Databricks Hydra, Pantheon, and Hydra-style always-streaming
  architectures; references this archive-tier design.
- `docs/spec-mvp-controller-driven-multi-stage-demo.md` — MVP demo
  spec; references this design for the archive-tier portion.
- `docs/control-plane-design.md` — controller 5-layer pipeline; the
  archive tier slots into L4 (binding rule for `Exact` accuracy →
  archive backend) + L5 (`BackendStorageRouting` emission).
- `docs/design-asap-edge-framework.md` — Layer-4 codec / Layer-5 sink
  taxonomy used by `gorillas3processor`.
- `docs/sketch-algebra-query-mapping.md` — sketch algebra mapping;
  the archive path is the "no-sketch-binding" trivial mapping for
  `accuracy: Exact` metrics.
- `controller/docs/design.md` — controller design source.
- `memory/project_asap_e2e_goal.md` (user-memory) — system-level e2e
  goal: "controller plans sketch placement, collector emits, backend
  answers PromQL".
- `memory/feedback_controller_plan_triggers.md` (user-memory) — only
  new queries / workloads trigger fresh plans; capability misses
  fall back to the archive tier, they do not replan.

### Source files for the archive tier

- `opentelemetry-collector-contrib-patch/processor/gorillas3processor/`
  — Go OTel processor that writes blocks to S3. Selects between
  `block_format: prometheus_tsdb` (canonical) and `block_format: asap`
  (legacy) at config time.
- `ASAPQuery-backend/asap-query-engine/src/engines/thanos_forward.rs`
  — `ThanosForwardEngine` HTTP-forward to `thanos-query`.
- `ASAPQuery-backend/asap-query-engine/src/engines/prometheus_forward.rs`
  — `PrometheusForwardEngine` HTTP-forward to a Prometheus
  `/api/v1/query` endpoint (Mode 3).
- `ASAPQuery-backend/asap-query-engine/src/engines/gorilla_engine.rs`
  — legacy in-process Rust `GorillaQueryEngine`; reads the §8 custom
  chunk format; curated PromQL subset.
- `ASAPQuery-backend/asap-query-engine/src/engines/simple_engine.rs`
  — warm-tier `SimpleEngine`; sibling of the archive engines.
- `deploy/mvp-singlenode/docker-compose/mvp-thanos-archive.yml` — Thanos
  store-gateway + thanos-query + thanos compact sidecar wiring.
- `deploy/mvp-singlenode/configs/asap-otel-agent-b5-gorilla.yaml` — local-FS
  baseline config for the archive-tier processor (for the
  b5-gorilla benchmark variant).

### External

- Prometheus TSDB format docs —
  <https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/README.md>
- Thanos store-gateway —
  <https://thanos.io/tip/components/store.md/>
- Thanos compact —
  <https://thanos.io/tip/components/compact.md/>
- Pelkonen, T. et al. "Gorilla: A Fast, Scalable, In-Memory Time
  Series Database." Proceedings of the VLDB Endowment, vol. 8,
  no. 12, 2015. The chunk encoding algorithm.
- Databricks Hydra blog —
  <https://www.databricks.com/blog/10-trillion-samples-day-scaling-beyond-traditional-monitoring-infra-databricks>
  — the always-streaming architecture Mode 3 most resembles.

---

*End of design doc. This doc supersedes
`docs/design-gorilla-s3-cold-engine.md` and
`docs/design-jsonl-deprecation-and-gorilla-promql-completeness.md`
(both deleted in the same change).*
