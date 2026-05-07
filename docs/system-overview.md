# ASAP system overview

> **Audience.** A new engineer, a reviewer, or a paper reader who wants
> the whole picture in one sitting — what the system is, what each
> component does, how the pieces wire up end-to-end, and where the
> design rationale lives.
>
> **Scope.** Current state, post-Phase-α through Phase-ε.1.5. This is
> the canonical "this is the system, today" reference. Per-component
> design docs and the demo runbook stay authoritative on rationale and
> operational steps; this doc points at them.

---

## 1. TL;DR

ASAP is a sketch-aware, controller-planned observability pipeline. It
ingests metrics from a fleet of edge agents and answers PromQL queries
out of a **two-tier serving stack**:

- **Warm sketch tier** (`SimpleEngine` in `ASAPQuery-backend`) —
  bounded-error sketch summaries, sub-millisecond per query.
- **Exact archive tier** (Prometheus-TSDB blocks on MinIO, served by
  `thanos-query` via the backend's `ThanosForwardEngine`) — exact
  PromQL for everything the warm tier can't answer.

A single planner — the **controller** — chooses, per metric, whether
to (a) sketch at the edge, (b) ship raw and sketch at ingest, or
(c) ship raw straight into the Prometheus archive. The choice is
driven by a per-sketch-family wire-cost break-even table
(Phase ε.1's `WireCostTable`) and an accuracy SLA on the query.

```
                 ┌──────────────────────────────────────────────────────┐
                 │                    controller                        │
                 │  L1 query_language → L2 logical_plan → L3 intent     │
                 │  → L4 sketch_algebra → L5 stage_split → emitters     │
                 │  ┌────────────────────────────────────────────────┐  │
                 │  │ WireCostTable + per-metric BindMode selection  │  │
                 │  └────────────────────────────────────────────────┘  │
                 └────────────┬────────────────────────────┬────────────┘
                              │ OpAMP RemoteConfig         │ HTTP push
              ┌───────────────┼───────────────────┐        │
              │               │                   │        ▼
              ▼               ▼                   ▼   ┌──────────────────────┐
        ┌──────────┐   ┌──────────────┐    ┌────────────────┐                │
        │sketchcol │   │ sketchotap   │    │ sketchtelegraf │                │
        │(Go)      │   │ (Rust, OTAP) │    │ (Go, Telegraf) │                │
        └────┬─────┘   └───────┬──────┘    └────────┬───────┘                │
             │                 │                    │                        │
             │ OTLP (modified — five sketch tags 13–17) +                    │
             │ raw OTLP for Mode 2 / Mode 3                                  │
             ▼                 ▼                    ▼                        │
        ┌──────────────────────────────────────────────────┐                 │
        │   gateway (otelcol-contrib + sketch processors,  │                 │
        │   gorillas3processor — Mode 3 splits off here)   │                 │
        └────────┬───────────────────────────────┬─────────┘                 │
                 │ sketch envelopes               │ raw samples (Mode 3)     │
                 ▼                                ▼                          │
        ┌────────────────────┐         ┌─────────────────────────┐           │
        │ ASAPQuery-backend  │         │   Prometheus            │           │
        │   SimpleEngine     │         │   (TSDB; OTLP receiver  │           │
        │   (warm sketch)    │         │   on /api/v1/otlp/...)  │           │
        │                    │         └─────────────────────────┘           │
        │   EngineRouter     │                                               │
        │  ──────────────    │                                               │
        │   ThanosForward    │ ───── HTTP /api/v1/query ─────┐               │
        │   Engine           │                               │               │
        │   PrometheusForward│ ── HTTP /api/v1/query ── (in flight Phase ε.2)│
        │   Engine                                                           │
        └─────┬──────────────┘                               ▼               │
              │                                    ┌──────────────────┐     │
              │ TSDB blocks via                    │  thanos-query    │     │
              │ gorillas3processor                 │  (HTTP :19092)   │     │
              ▼                                    └─────────┬────────┘     │
        ┌────────────────┐  scan blocks    ┌─────────────────┴─────────┐    │
        │     MinIO      │ ◄──────────────┤  thanos-store-gateway      │    │
        │  (S3-compat)   │  bucket index   │  (gRPC :10901)             │    │
        │  TSDB blocks   │ ◄──────────────┤  thanos-compact            │    │
        └────────────────┘  compaction    └────────────────────────────┘    │
                                                                            │
        client PromQL  ─────────────────────────────────────────────────────┘
        (X-ASAP-Engine override / ?engine= param honored)
```

The single-frame point: **edge → gateway → backend** for the warm
sketch path; **edge → gateway → MinIO** (parallel) for the archive
path; **backend forwards** archive PromQL to `thanos-query`. The
controller — sole planner — drives all of this via OpAMP +
HTTP-push, never inline at query time.

---

## 2. The three operational modes

The controller picks one of three `BindMode`s per metric per
deployment. All three speak OTLP on the wire (the fork's "modified
OTLP" with five sketch-typed `Metric.data` variants is a strict
superset of stock OTLP).

| # | Name | Edge | Wire | Backend role | Accuracy |
|---|---|---|---|---|---|
| 1 | `SketchAtEdge` | Sketch processor at edge → flushes envelopes | Modified OTLP (sketch tags 13–17) | `SimpleEngine` over sketch envelopes | Bounded ε > 0 |
| 2 | `RawAtEdgeSketchAtBackend` | No sketch; ship raw OTLP | Stock OTLP raw | Backend's `precompute_engine` builds sketches at ingest | Bounded ε > 0 |
| 3 | `RawAtEdgePrometheusArchive` | No sketch; ship raw OTLP | OTLP HTTP to Prometheus's native receiver `/api/v1/otlp/v1/metrics` | Backend HTTP-forwards to Prometheus's `/api/v1/query` (`PrometheusForwardEngine`, in flight Phase ε.2) | Exact (ε = 0) |

The choice is a cost-model output:

- **Mode 1** wins when the per-window sample count crosses the
  sketch family's break-even threshold (see §10) **and** an
  ε > 0 SLA is tolerable.
- **Mode 2** wins when edge CPU is the binding constraint (sketch
  build cost is centralized, edge ships compact raw OTLP).
- **Mode 3** wins for low-cardinality metrics where the sketch
  envelope overhead never amortizes, **or** when the query needs
  exact answers (`histogram_quantile`, `delta`, `idelta`, `absent`,
  vector matching, etc. — the eleven archive-only intents in §6).

Source of break-even constants: `controller/src/planner/wire_cost.rs`
(`WireCostTable::default_phase_eps_1`) — see §10 for the table.

---

## 3. Three edge runtimes

All three runtimes consume the same precompute libraries (Go and
Rust ports are byte-parity-tested — see §8). The runtime pick is
deployment-shaped: `sketchcol` for OTel-Collector-native deployments,
`sketchotap` for high-throughput OTAP/Arrow pipelines, `sketchtelegraf`
for Telegraf-native sites.

| Runtime | Lang | Library | Status | Mode 1 emit (sketch envelopes) | Mode 3 emit (raw → Prometheus) |
|---|---|---|---|---|---|
| `sketchcol` | Go | `asap-precompute-go` | shipped | Embedded sketch processors → otlphttp exporter | `otlphttp/prometheus` exporter to `/api/v1/otlp/v1/metrics` |
| `sketchotap` | Rust | `asap-precompute-rs` | shipped | OTAP DAG nodes → `urn:otel:exporter:otlp_http` | OTAP passthrough → `exporter:otlp_http` |
| `sketchtelegraf` | Go | `asap-precompute-go` | shipped | Telegraf processor → `outputs.opentelemetry` | `outputs.http` with `data_format = "prometheusremotewrite"` (the shipped Telegraf otel plugin is gRPC-only — HTTP-postable Prometheus remote-write keeps the path lossless) |

Sources:
- `controller/src/config/stage_config.rs` (`emit_edge_yaml` for
  sketchcol)
- `controller/src/config/stage_config_otap.rs` (`emit_otap_dag_yaml`
  for sketchotap)
- `controller/src/config/stage_config_telegraf.rs`
  (`emit_telegraf_toml` for sketchtelegraf)

The sketch-processor logic itself is the same observation-loop in
both libraries (`asap-precompute-go/precompute.go`,
`asap-precompute-rs/src/precompute.rs`); the runtimes differ only
in *how the processor is hosted* (collector pipeline vs. OTAP DAG vs.
Telegraf processor).

---

## 4. The five sketch families

The fork's modified-OTLP wire format extends `Metric.data oneof` with
five new tags (13–17), one per sketch family. All five are typed,
schemaful, and accumulator-symmetric — i.e. the gateway, the backend,
and any downstream merger build identical results from identical
inputs.

| Family | Tag | Use | Backend accumulator | Accuracy envelope (typical) | Break-even (samples/window @ 50 B/sample) |
|---|---|---|---|---|---|
| **DDSketch** | 13 | Quantiles over numeric values (latencies, sizes) | `aggregate_ddsketch` (relative-error merge) | ε ≈ 0.01 (rank error) | **16** (delta-encoded) |
| **KLL** | 14 | Quantiles when relative-error guarantees aren't enough | `aggregate_kll` (compactor-hierarchy merge) | ε ≈ 0.005 (rank error), δ via Hoeffding | **64** (full state — KLL has no delta variant) |
| **HLL** | 15 | Distinct-cardinality (`count(distinct …)`) | `aggregate_hll` (register-max merge) | ε ≈ 1.04 / √m relative | **204** (delta-encoded register array) |
| **Count-Min** | 16 | Heavy-hitters / point-frequency on label values | `aggregate_cms` (row-by-row sum merge) | ε(N), δ(d) bounded | **84** (delta-encoded rows) |
| **Count-Sketch** | 17 | Estimated frequencies that need signed estimators | `aggregate_count_sketch` (cell-sum merge) | ε(N), δ(d) bounded | **5,004** (delta-encoded sparse cells) |

Cross-runtime parity: identical input → byte-identical
`SerializePortable` envelope from any of `sketchcol`,
`sketchotap`, or `sketchtelegraf`. Verified via golden-fixture
parity tests under `integration/cross-host-parity/` and
`integration/parity/`.

Source: `Implementation.tex` impl-components table; cost table
implemented in `controller/src/planner/wire_cost.rs`.

---

## 5. Backend (`ASAPQuery-backend`, post-Phase-γ)

```
asap-query-engine/src/
  engines/
    simple/      ← SimpleEngine — warm sketch tier
    gorilla/     ← ThanosForwardEngine + legacy GorillaQueryEngine
    prometheus/  ← PrometheusForwardEngine (Phase ε.2, in flight)
    physical/    ← lower-level operator nodes
    logical/     ← logical-plan rewrites
  routing/
    backend_storage_routing.rs  ← controller-pushed multi-target table
    engine_router.rs            ← dispatcher + override surface
  precompute_engine/  ← Mode 2 ingest (builds sketches from raw OTLP)
  drivers/            ← HTTP server, ingest endpoints
  stores/             ← sketch_db, gorilla_s3, etc.
```

### Engines

- **`SimpleEngine`** (`engines/simple/`) — the warm sketch tier.
  Pattern-matches incoming PromQL against 33 supported
  PromQL pattern matchers (PR #79 — spatial multi-quantile,
  `quantile_over_time(φ ∈ {0.5, 0.9, 0.95, 0.99}, …[1m|2m|5m])`,
  `sum_over_time` / `count_over_time`, `rate` / `increase`, spatial
  `count` / `sum` / `avg`, `topk(k ∈ {5, 10, 50}, …)`). Misses fall
  through to the archive engine.
- **`ThanosForwardEngine`** (`engines/gorilla/thanos_forward.rs`) —
  archive tier. HTTP-forwards PromQL queries to a co-located
  `thanos-query` sidecar. Registered under both `engine_id =
  "thanos_archive"` (its native id) and `engine_id =
  "gorilla_archive"` (legacy alias, so existing routing tables and
  failover sequences continue to work). Activated when
  `ASAP_THANOS_QUERY_URL` is set.
- **`PrometheusForwardEngine`** (`engines/prometheus/`) — Mode 3
  archive engine. HTTP-forwards to Prometheus's native
  `/api/v1/query`. `engine_id = "prometheus_remote"`. **Status:
  Phase ε.2, in flight.**
- **Legacy `GorillaQueryEngine`** (`engines/gorilla/engine.rs`) — the
  pre-Path-A2 in-process engine over Gorilla-XOR chunks. Curated
  PromQL subset (`sum / count / avg / min / max / rate / increase /
  quantile_over_time / topk`). Kept as a fallback for sites without
  a thanos sidecar.

### Routing

- `BackendStorageRouting` is a **per-metric, multi-target** table
  pushed by the controller (`POST /api/v1/storage_routing`). One
  metric can have entries for the warm tier *and* the archive tier;
  the dispatcher picks one based on the query shape's archive-only
  flag and the `accuracy` SLA.
- `EngineRouter` is the per-query dispatcher. It honors two
  override surfaces:
  - **`X-ASAP-Engine: <id>` HTTP header**
  - **`?engine=<id>` query parameter**
  Both bypass the routing table for the override id when present,
  and fall back to the routing table otherwise. Used by the demo
  driver and operator-side debugging.

### Configuration

| Env var | Purpose | Default |
|---|---|---|
| `ASAP_THANOS_QUERY_URL` | Upstream `thanos-query` URL for `ThanosForwardEngine` | unset → forward engine not registered; fall back to legacy `GorillaQueryEngine` |
| `ASAP_PROMETHEUS_QUERY_URL` | Upstream Prometheus URL for `PrometheusForwardEngine` (Phase ε.2) | unset → engine not registered |
| `ASAP_GORILLA_S3_*` | Legacy in-process `GorillaQueryEngine` config (endpoint, bucket, region, credentials, cache size) | unset → warm-only mode |
| `CONTROLLER_BACKEND_ENDPOINT` | URL the controller pushes plans to (`POST /api/v1/streaming-config`, `POST /api/v1/storage_routing`) | unset → static-config mode |

(See `ASAPQuery-backend/README.md` §"Backend env vars" for the
authoritative table.)

---

## 6. Controller (`ASAPCollector/controller`)

The controller is the **sole planner**. No inline planning happens
at query time, no agent picks its own configuration, no backend
chooses an engine without a routing-table entry. Everything traces
back to a controller plan.

### Five-layer pipeline

| Layer | Module | Role |
|---|---|---|
| L1 query_language | `controller/src/query_language/` | Parse PromQL (and SQL / Elastic DSL where supported) → `language_ast` |
| L2 logical_plan | `controller/src/language_logical_plan/` | AST → language-orthogonal logical operators |
| L3 intent_algebra | `controller/src/intent_algebra/` | Logical → `AggIntent` + `QueryExpr` DAG + typed `Schema`. Each intent has an `archive_only` flag. |
| L4 sketch_algebra | `controller/src/sketch_algebra/` | `QueryExpr` → `SketchExpr` via Bind* rules: `bind_ddsketch_quantile`, `bind_kll_quantile`, `bind_hll_cardinality`, `bind_cms_count`, `bind_cms_topk`, `bind_archive_only` |
| L5 stage_split | `controller/src/stage_split/` | Allocate stages (edge / gateway / backend); colored DAG → per-stage configs |

### Eleven archive-only intents

Phase β lifted these from the legacy planner's "unsupported" branch
into L3 vocabulary so they get a `StreamingConfig` entry (routed
to the archive tier rather than the warm sketch tier):

```
HistogramQuantile, Absent, Present, Delta, Deriv,
PredictLinear, HoltWinters, Idelta, Irate, Resets, Changes
```

Source: `controller/src/intent_algebra/agg_intent.rs` (variant
list + `archive_only()` predicate); module-level docs in
`controller/src/intent_algebra/mod.rs`.

### Cost model

`controller/src/planner/wire_cost.rs` owns the `WireCostTable`
and per-metric `BindMode` selection. The model collapses two
older axes (raw-vs-sketch from `delta_cost_model.rs`,
exact-vs-approximate from `cost_model.rs`) into a single tri-mode
selector that names where the work happens (see §2). All three
modes use OTLP on the wire — the agent's exporter vocabulary is
collapsed to `otlp` / `otlphttp` only (no separate
`prometheusremotewrite` exporter on the OTel-collector path).

### Per-runtime emitters

| Emitter | Output | Consumer |
|---|---|---|
| `emit_edge_yaml` | otelcol-contrib YAML | `sketchcol` agent |
| `emit_otap_dag_yaml` | OTAP DAG YAML | `sketchotap` agent |
| `emit_telegraf_toml` | Telegraf TOML | `sketchtelegraf` agent |
| `emit_gateway_yaml` | otelcol-contrib YAML (gateway role) | gateway |
| `emit_backend_config_json` | StreamingConfig JSON | backend `/api/v1/streaming-config` |
| `emit_backend_storage_routing` (and `…_with_prometheus` for Mode 3) | BackendStorageRouting JSON | backend `/api/v1/storage_routing` |

### Push surfaces

- **OpAMP `RemoteConfig`** — pushed over WebSocket (`GET /v1/opamp`)
  to all registered agents and the gateway. The OpAMP server
  implementation is `controller/src/opamp/mod.rs`.
- **HTTP push** — `POST /api/v1/streaming-config` and
  `POST /api/v1/storage_routing` to the backend, via
  `controller/src/backend_client.rs`.

---

## 7. Archive tier on MinIO + Thanos

The archive tier serves exact PromQL out of standard
Prometheus-TSDB blocks on object storage.

```
edge / gateway → gorillas3processor → MinIO bucket (TSDB blocks)
                                           │
                                           ▼
                            thanos-store-gateway (gRPC :10901)
                                           │
                                           ▼
                            thanos-query (HTTP :19092 in MVP)
                                           ▲
                                           │ HTTP /api/v1/query
                                           │
                            backend ThanosForwardEngine
                                           ▲
                                           │ PromQL with archive-only intent
                                           │
                                       client
```

### Block writer — `gorillas3processor`

Lives in `opentelemetry-collector-contrib-patch/processor/gorillas3processor/`.
Emits Prometheus-TSDB blocks with the standard layout:

```
<bucket>/<ulid>/
  ├── chunks/000001
  ├── index
  └── meta.json
```

Each block is a stock TSDB block readable by any Prometheus-compatible
tool. MVP step 2.1 (PR #311) shipped this writer.

### Thanos stack

| Component | Port | Role |
|---|---|---|
| `thanos-store-gateway` | gRPC :10901 | Scans the MinIO bucket; exposes blocks as a Thanos `StoreAPI` |
| `thanos-query` | HTTP :10903 (host :19092 in MVP demo) | Prometheus-compatible HTTP query server; fans out to store-gateway |
| `thanos-compact` | (no listener) | Compacts and downsamples raw blocks. Retention: raw=30d / 5m=180d / 1h=1y. Phase δ.1 — replaced the deleted `gorilla-compactor` Rust binary. |

### Backend forwarding

Backend's `ThanosForwardEngine` HTTP-forwards archive-tier queries
to `thanos-query`. The `data_source: thanos_archive` info string is
attached to forwarded responses; the legacy alias
`gorilla_archive` is kept so existing routing tables and failover
sequences continue to work.

Path-A2 (Steps 2.1–2.4, 2026-05-07) verified end-to-end:
`histogram_quantile` over the archive matches a hand-computed
reference exactly; 4 TSDB blocks land in MinIO; thanos-query
sidecar healthy; the full Prometheus PromQL surface is now
answered exactly by the archive tier (these query shapes were
rejected by the prior curated-subset `GorillaQueryEngine` and are
the qualitative win of the Path-A2 consolidation).

Source: `docs/mvp-demo-runbook.md` §"Path A2 verification";
design rationale in `docs/design-archive-tier.md`.

---

## 8. Wire format + cross-language byte parity

The wire format is **modified OTLP**: stock OTLP plus five new
typed `Metric.data` variants on tags 13–17. Each variant carries
a `SketchEnvelope` whose `Payload` field is the
language-portable, byte-stable serialization of the sketch
together with its accuracy envelope (ε, δ, kind).

```
SketchEnvelope {
  payload: bytes (SerializePortable output),
  accuracy: AccuracyEnvelope { kind, eps, delta },
  sketch_params: SketchParams { … },
  ...
}
```

### Cross-language byte parity gate

Same input → byte-identical `SerializePortable` envelope from any
of the three runtimes (`sketchcol`, `sketchotap`,
`sketchtelegraf`). This is enforced by:

- `asap-precompute-go/envelope_test.go` — Go-side golden fixtures
- `asap-precompute-rs/src/envelope.rs` (and tests) — Rust-side
  parity tests against the same fixtures
- `integration/parity/` and `integration/cross-host-parity/` —
  end-to-end fixtures running both stacks side by side

The gate is load-bearing: the gateway and the backend assume that
two envelopes for the same logical observation merge to the same
sketch state regardless of which runtime produced them. Any
divergence (compactor RNG drift, label-set ordering, float
canonicalization) breaks the merge invariant.

Source: `Implementation.tex` impl-components §"OTLP wire-format
extension (tags 13–17)".

---

## 9. Deployment shapes

### Single-host MVP demo

The demo (`docs/mvp-demo-runbook.md`) brings up a full stack on
one host:

```
10 producers
  ↓
2 agents (sketchcol)
  ↓
1 gateway (sketchcol with gorillas3processor)
  ↓
1 backend (ASAPQuery-backend)
  ↓
1 MinIO + 3 thanos sidecars (store-gateway, query, compact)
  ↓ (optional, opt-in)
1 Prometheus (raw-baseline path)
```

This is the canonical reproducer. It exercises every layer
(controller plans, OpAMP push, sketch envelopes, archive blocks,
backend routing, both forward engines).

### Production pyramid

```
many SDKs              ← O(100–1,000) series each
  ↓
many collectors        ← O(10K–100K) aggregated series each
  ↓
backend                ← O(millions) series total
```

A controller-emitted plan covers the whole pyramid via OpAMP fan-out.
The collector tier is where most of the wire reduction happens
(sketches at edge for high-cardinality metrics; raw passthrough
for low-cardinality ones).

### Multi-host federation

Not designed today. The controller is single-tenant, single-cluster.
See §12.

---

## 10. Operating-point math (cost-model break-even)

For sketch family `f` with per-flush wire cost
`per_flush_f = state_bytes_f + envelope_bytes_f`, and per-sample raw
OTLP cost `per_sample_bytes` (default 50 — typical observability
counter / gauge with a small label set, after OTLP delta encoding
at the SDK):

```
break_even_samples_per_window = ceil(per_flush_f / per_sample_bytes)
```

The Phase ε.1 default table (`WireCostTable::default_phase_eps_1`):

| Family | `state_bytes` | `envelope_bytes` | `per_flush` | Break-even @ 50 B/sample |
|---|---:|---:|---:|---:|
| DDSketch (delta) | 600 | 200 | 800 | **16** |
| KLL (full — no delta variant) | 3,000 | 200 | 3,200 | **64** |
| HLL (delta) | 10,000 | 200 | 10,200 | **204** |
| Count-Min (delta) | 4,000 | 200 | 4,200 | **84** |
| Count-Sketch (delta) | 250,000 | 200 | 250,200 | **5,004** |

### When raw passthrough wins

When `samples_per_window_per_series < break_even_f` for every
sketch family that could answer the query. At that point
Mode 1 (sketch at edge) costs more per window than just shipping
all the samples — and the controller picks Mode 2 (raw at edge,
sketch at backend) or Mode 3 (raw at edge, exact at archive)
instead.

### Sensitivity

Break-even is linear in `per_sample_bytes` and inverse-linear in
the sketch family's `per_flush` cost, so the dominant scaling
inputs are:

- **Scrape rate × cardinality** (`samples_per_window_per_series`)
  — the workload-side knob.
- **OTLP per-sample size** (`per_sample_bytes`) — affected by label
  set size, delta encoding at the SDK.
- **Sketch family parameters** — DDSketch γ, KLL k, HLL m,
  Count-Min (d, w). These set the wire-cost row.

Source: `controller/src/planner/wire_cost.rs`
(`break_even_samples` + the `break_even_table_at_50_bytes_per_sample`
test snapshot).

---

## 11. What was deleted (architecture is settled, not journey-narrative)

This is the canonical "what is no longer in the system" list.
The architecture today is the result of these subtractions, not
the union of every prototype.

| Deletion | When | Why |
|---|---|---|
| **JSONL cold-fallback path** | Step 1 (PR #95) | Replaced by `GorillaS3ColdStore` (later renamed `GorillaS3Store`) — the JSONL format never made it past the bring-up phase |
| **`asap-planner-rs` library + CLI** | Phase γ (PR #99) | All planning logic absorbed into the controller's L3/L4/L5 pipeline; no remaining caller |
| **`gorilla-compactor` Rust binary** | Phase δ.1 (PR #321) | Replaced by stock `thanos compact` running as a sidecar — same job (decode + re-encode TSDB blocks), zero ASAP-specific code |
| **`prometheus_remote_write` ingest from backend** | PR #100 | The backend no longer accepts Prometheus remote-write directly; ingest is OTLP-only |
| **`LocalFsColdStore` enum variant** | Step 1 | Local-fs cold tier was a pre-MinIO bring-up convenience; obsolete once MinIO became the only target |
| **Cold-tier "scan bytes" line item from cost model** | Step 1 | The JSONL deletion removed the line-item; archive scan cost is now folded into the forward-engine HTTP cost |

These are listed here so a reader doesn't go hunting for them in
older docs and assume they're load-bearing.

---

## 12. What's not yet implemented

Tracked, in flight, or explicitly out of scope today.

| Item | Status |
|---|---|
| **Phase 3.1 warm-tier null-answer fix** | In flight. MVP demo criterion ④ reports UNKNOWN until landed. |
| **Phase 3.2.5 freshness probe routing** | In flight. MVP demo criterion ⑥ reports UNKNOWN until landed. |
| **Phase 3.3 final demo re-run** | Pending Phase 3.1 + 3.2.5. |
| **Phase ε.2 backend `prometheus_remote` engine kind** | In flight. `PrometheusForwardEngine` skeleton lives under `engines/prometheus/`; controller-side BackendStorageRouting emit (`emit_backend_storage_routing_with_prometheus`) is in place. |
| **Per-tenant routing** | `BackendStorageRouting` is global today. Multi-tenant slicing of the table is open work. |
| **Hot-reload signal-driven** | Controller-pushed only today. The backend will accept a `POST /api/v1/streaming-config` at any time but it doesn't watch a filesystem path or react to a SIGHUP. |
| **Multi-host federation** | Not designed. The controller is single-cluster. |

---

## 13. Cross-references

### Operational

- **`docs/mvp-demo-runbook.md`** — how to run the single-host MVP
  demo end-to-end. Authoritative on bring-up sequence, port
  layout, criteria pass/fail, and the in-flight bugs called out
  in §12.

### Design rationale

- **`docs/design-archive-tier.md`** — archive-tier design rationale,
  wire format details, and the JSONL → Path-A2 consolidation history
  (merged from the older `design-jsonl-deprecation-…` and
  `design-gorilla-s3-cold-engine` docs in PR #325).
- **`docs/design-asap-edge-framework.md`** — sketchcol agent design.
- **`docs/design-asap-otap-rust-integration.md`** — sketchotap agent
  design.
- **`docs/design-asap-telegraf-integration.md`** — sketchtelegraf
  agent design.
- **`docs/control-plane-design.md`** — controller architecture; OpAMP
  and HTTP-push plumbing.
- **`docs/sketch-algebra-query-mapping.md`** — L3/L4 query → sketch
  binding rules, from the controller's side.
- **`docs/opamp-config-push.md`** — OpAMP `RemoteConfig` push wire
  format and the WebSocket transport.
- **`controller/docs/design.md`** — controller-internal design notes;
  source for §6's pipeline + cost-model details.

### Comparison

- **`docs/comparison-asap-vs-databricks-pantheon-hydra.md`** — ASAP
  vs. Databricks Pantheon + Hydra, with a column on the
  Gorilla-S3 archive tier vs. Hydra's exact tier.

### Paper

- **`Super_resolution_ingestion_with_sketching_VLDB_or_SIGMOD/main.pdf`**
  (compiled from `Implementation.tex`, `Design.tex`, etc. in the
  same directory). Source-of-truth for the wire-format extension
  (§8) and the impl-components table (§4).

---

## Glossary

- **AggIntent** — the L3 vocabulary: a *what to compute* node
  (`Quantile`, `Cardinality`, `TopK`, `HistogramQuantile`, …).
  Sketch family is *not* picked at L3.
- **Archive-only intent** — an `AggIntent` that the warm tier can't
  serve from any sketch family, so it's pre-flagged for the
  archive tier (`HistogramQuantile`, `Delta`, `Idelta`, `Irate`,
  `Resets`, `Changes`, `Absent`, `Present`, `Deriv`,
  `PredictLinear`, `HoltWinters` — the eleven from §6).
- **BindMode** — one of `SketchAtEdge` / `RawAtEdgeSketchAtBackend` /
  `RawAtEdgePrometheusArchive`. Per-metric controller decision.
- **BackendStorageRouting** — the controller-pushed, per-metric,
  multi-target routing table the `EngineRouter` consults at query
  time.
- **EngineRouter** — backend per-query dispatcher. Consults the
  routing table; honors `X-ASAP-Engine` header / `?engine=` query
  param overrides.
- **Modified OTLP** — stock OTLP plus five new typed `Metric.data`
  variants on tags 13–17, one per sketch family. Strict superset
  of stock OTLP (a stock OTLP receiver ignores unknown tags).
- **Path A2** — the post-Step-2.4 archive-tier shape: edge writes
  Prometheus-TSDB blocks via `gorillas3processor`; backend
  HTTP-forwards PromQL to `thanos-query`. The qualitative win is
  full Prometheus PromQL on the archive tier.
- **WireCostTable** — Phase ε.1 per-sketch-family wire cost model.
  Drives the `BindMode` selection. Lives in
  `controller/src/planner/wire_cost.rs`.

---

*Maintainer note.* When a new operational mode, sketch family, or
engine lands, update §2, §4, or §5 respectively, and add the
deletion (if any) to §11. Per-component design docs stay
authoritative on rationale; this doc is the single-frame
"current-state architecture" reference.
