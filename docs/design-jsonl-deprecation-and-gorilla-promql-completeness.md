# Archive-tier consolidation: delete JSONL cold-fallback + PromQL completeness on `GorillaQueryEngine`

## Status

Active. 2026-05-07. Two outstanding work items targeted at the Gorilla
archive tier:

1. **Delete the JSONL cold-fallback path.** It is now unreachable under
   normal flow because dual-routing (`BackendStorageRouting`) routes every
   metric to either the warm tier or the Gorilla archive based on query
   shape. JSONL is dead code waiting to be removed.
2. **Make `GorillaQueryEngine` PromQL-complete.** Today's engine handles
   a curated subset; reviewers asking "is this an exact-PromQL backend"
   will push back unless the subset is widened or the engine vendors a
   reference implementation.

## What's already on main

The earlier version of this doc proposed three phases. Two of them have
effectively shipped via different mechanisms; only the third remains as
substantial open work:

| Originally proposed | What actually shipped |
|---|---|
| **Phase 1: always-archive every metric at the gateway.** Pitched as a config knob (`archive_all: true`) flipped on per region. | Multi-target `BackendStorageRouting` (PR #91): a single metric routes to BOTH `sketch_warm_tier` AND `gorilla_s3_archive` based on the query shape. Effectively "always-archive" for any metric configured with the archive target. |
| **Phase 2: delete the JSONL path.** Small follow-on once Phase 1 was on. | Not yet done; carried forward as **§"Delete JSONL"** below. |
| **Phase 3: PromQL completeness on `GorillaQueryEngine`.** Three sub-paths (vendor `prometheus/promql`, pure-Rust evaluator, curated subset). | Resolved by Path A2 (Steps 2.1–2.4, 2026-05-07): `gorillas3processor` writes Prometheus-TSDB blocks straight to MinIO, a stock `thanos store-gateway` + `thanos-query` sidecar serves them via Prometheus' reference `promql.Engine`, and the `ThanosForwardEngine` HTTP-forwards archive-tier queries from the backend. The curated `GorillaQueryEngine` subset (postings filtering, partial S3 reads via `byte_offset`/`byte_length`) is no longer the load-bearing archive engine. Phase δ.1 (2026-05-07) followed up by deleting the legacy `gorilla-compactor` Rust binary; archive-tier compaction is now performed by stock `thanos compact` running as a sidecar. |

The architectural validation has been published on issue #46 across
multiple iterations; the current verdict and known gaps are summarised
in `docs/mvp-demo-runbook.md`.

## Goal

Two outstanding items, in order:

### 1. Delete the JSONL cold-fallback path

The `LocalFsColdStore` impl, the `cold_store::format::parse_jsonl`
reader, the gateway-side raw-tee exporter, the
`StorageBackend::ColdJsonlFallback` enum variant, and the
`cold-tier scan bytes` line item in `cost_model` are all reachable by
dead code. Under the current routing scheme (every metric in
`backend-storage-routing.yaml` is mapped to either warm or archive), no
query path ever hits the JSONL reader unless configured deliberately.
Deleting it:

- collapses 3-tier framing to 2-tier in design + paper prose
- removes ~500 LOC of JSONL parsing + tolerance-of-torn-trailing-line
  + pin-tests-guarding-the-tolerance
- removes the gateway raw-tee exporter from the OTel collector patch set
- shrinks the `cost_model`'s axis count by 1

The risk is that a metric *not in the routing table* would have nothing
to fall through to. Mitigations:

- Add a unit test that fails compilation if any controller-emitted plan
  produces a metric without a routing-table entry
- The `EngineRouter` returns a clear error ("no engine configured for
  metric X") rather than silently dropping; this becomes a
  deployment-validation step, not a runtime-behavior change

### 2. PromQL completeness on `GorillaQueryEngine`

Today's engine handles:
- Streaming-additive: `sum / count / avg / min / max / rate / increase`
- Buffered: `quantile_over_time / topk`

For the archive tier to be a full replacement for "raw on a real TSDB",
the engine must cover everything Prometheus or VictoriaMetrics covers:
- `histogram_quantile`
- Range-vector functions: `delta / deriv / predict_linear / holt_winters /
  idelta / irate / resets / changes`
- Aggregations with `by / without`: `group / stddev / stdvar /
  count_values / bottomk` (and the existing aggregations grouped, not
  just whole-set)
- Vector matching: `or / and / unless` with `on / ignoring`,
  `group_left / group_right`
- Scalar/vector coercions: `absent / scalar / vector`
- Time functions: `time / timestamp / month / year / day_of_week / …`
- Subquery expansion: `rate(x[5m])[1h:1m]` syntax
- Lookback-delta semantics matching Prometheus exactly

Three implementation paths:

#### Path A — vendor Prometheus' `promql` package (recommended)

Run a Go sidecar exposing Prometheus' `promql` package as the evaluator;
expose Gorilla-decoded chunks to it via a custom `storage.Queryable`
implementation that decodes chunks from S3 on demand.

- **Cost**: medium. ~2 weeks: the sidecar wrapper, a Rust ↔ Go RPC
  surface (or have the sidecar talk to S3 directly), the `Queryable`
  adapter, integration tests against Prometheus' `promql_test` suite.
- **Pro**: correctness by construction — Prometheus' parser+evaluator is
  the reference implementation. PromQL semantics quirks (lookback-delta,
  subquery expansion, vector matching) come for free.
- **Pro**: changes to the Prometheus query engine flow in by
  version-bumping the dependency.
- **Con**: adds a Go process to the Rust backend deployment.
  Operational surface widens.

#### Path B — pure-Rust evaluator over [`promql-parser`](https://crates.io/crates/promql-parser)

Use the existing Rust `promql-parser` crate for the AST; write the
evaluator in Rust, calling into `asap-gorilla` for chunk decoding.

- **Cost**: high. ~4–6 weeks: the evaluator is the bulk of Prometheus'
  ~12k LOC, with quirks. Need to match Prometheus' lookback-delta and
  subquery semantics exactly to avoid silent wrong answers.
- **Pro**: all-Rust deployment.
- **Con**: correctness risk. Silent semantic divergence from Prometheus
  is the failure mode reviewers care about most; we'd need a thorough
  cross-implementation test corpus to claim parity.

#### Path C — extend the curated subset

Stay with the curated-subset model; add `histogram_quantile`,
`sum/min/max/avg/group by`, `or / and / unless`, and a handful of the
most common range-vector functions; document the unsupported surface
explicitly and have `capability_matching` return a clear error for
queries outside it.

- **Cost**: low. ~1 week.
- **Pro**: ships fast; matches the engine's actual usage today, since
  the warm tier handles most queries.
- **Con**: "exact PromQL on the archive tier" becomes "exact PromQL for
  a subset of queries on the archive tier"; reviewers may push back.

**Recommendation: Path A.** The cost of silent wrong answers from a
homegrown PromQL evaluator is high; vendoring the reference
implementation is the only way to claim parity defensibly. The Go
sidecar adds operational surface but is a known pattern (Thanos itself
embeds Prometheus' query engine). If the deadline forces a short-term
ship, Path C unblocks JSONL deletion + paper framing; Path A lands as a
follow-up — but the paper should say so honestly.

If we adopt Path A, an interesting bonus: ASAP's archive layout could
be made compatible with the **Prometheus block format**, which would
let Thanos' `store gateway` query the archive directly. See the
"Prometheus-block layout" discussion in
`docs/comparison-asap-vs-databricks-pantheon-hydra.md` for that
direction.

## Non-goals

- Replacing the warm sketch tier or its `(ε, δ, kind)` accuracy
  contract.
- Changing the agent → backend wire format. Sketch envelopes remain the
  bandwidth-efficient hot path.
- Federation across regions. Tracked separately.
- Compaction policy. As of Phase δ.1 (2026-05-07) archive-tier
  compaction is delegated to the stock `thanos compact` sidecar
  (deployed via `deploy/docker-compose/mvp-thanos-archive.yml`). The
  legacy concat-only `gorilla-compactor` Rust binary (PR #295) has
  been deleted — Thanos compact already does decode + re-encode for
  better compression on top of block consolidation, plus downsampled
  tiers (raw / 5m / 1h) for free, and is the established
  Prometheus-ecosystem tool for this exact job.

## Storage layout (no change)

Gorilla archive blocks already live at:

```
<tenant>/<metric>/YYYY/MM/DD/HH/part-NNNNNN.gor
<tenant>/<metric>/YYYY/MM/DD/HH/index.json
<tenant>/<metric>/YYYY/MM/DD/HH/postings-v1.json   # added by PR #295
```

Chunks are self-describing (magic + schema_version + encoder_version +
flags + time bounds + sample count + payload length + header CRC32C +
UTF-8 metric name + sorted-JSON labelset + Gorilla body + payload
CRC32C). `index.json` chunk manifest carries `byte_offset` +
`byte_length` per chunk so the engine can issue `Range: bytes=` partial
reads.

Path A would either:
- Have the Go sidecar read the same Gorilla layout (via a shared
  decoder), or
- Promote the layout to Prometheus-block-compatible (chunks file +
  Prometheus index file + meta.json) so off-the-shelf Thanos can read
  it.

The second option is more work but simplifies the integration story
substantially — the sidecar becomes "Thanos store gateway pointing at
our blocks" rather than "custom adapter".

## `cost_model` update

The Layer-4 cost model already costs warm sketch state RAM, Gorilla S3
PUT/GET counts, edge CPU, and bandwidth on each cut edge. After
§"Delete JSONL" the `cold-tier scan bytes` line item is removed;
queries that miss the warm tier are evaluated through the archive
engine. The cost model unifies to "warm sketch share + Gorilla scan
share".

## Migration

JSONL deletion is mechanical; no migration. The Path A integration is
additive — the curated-subset engine continues to serve queries it
knows about, and Path A's Go sidecar handles the rest. If the sidecar
is unreachable (deploy failure), the engine returns a clear error and
queries fail loudly rather than silently.

## Paper impact

`Design.tex`, `Implementation.tex`, `Evaluation.tex`, `abstract.tex`,
`Introduction.tex`:
- Replace 3-tier framing with 2-tier everywhere: warm sketch + exact
  archive.
- Drop the "no data cliff via JSONL" prose; the dual-routing setup
  ensures every metric has a routable answer in bounded time.
- §RelatedWork: lean into the Hydra-style always-streaming parallel
  (already in `docs/comparison-asap-vs-databricks-pantheon-hydra.md`)
  and add a "ASAP archive-tier blocks could be Prometheus-block-format
  for off-the-shelf Thanos compatibility" line.

`docs/design-gorilla-s3-cold-engine.md`:
- Update §"e2e architecture" diagram to reflect the two-tier shape.

`docs/mvp-demo-runbook.md`:
- §"Out of scope for the demo" — drop the JSONL line once §"Delete
  JSONL" lands.

## Open questions

1. **Tenant isolation.** S3 per-tenant prefix or per-tenant bucket? The
   current `<tenant>/<metric>/…` layout assumes prefix isolation; for
   strong tenant isolation we may need per-tenant buckets, which
   changes the controller's plan-target signalling.
2. **Sensitive-metric exemption.** Some metrics (e.g.\ user-PII counters)
   should never land in the archive at all. The
   `BackendStorageRouting` table can express "warm-only" via
   single-target — verify this is surfaced clearly to the controller
   plan emitter.
3. **Long-range query cost on the archive.** A 30-day range query
   without warm-tier coverage means decoding 30 days of chunks. The
   chunk-LRU cache helps for hot queries; cold queries still scan.
   Worth characterising before claiming "exact PromQL on demand" in
   the paper.
4. **Backend image swap (Path A).** If we vendor Prometheus' `promql`,
   the deployment gains a Go process. Operators running the existing
   image need an upgrade path. A companion image
   `asap/query-backend-with-promql:dev` alongside the existing
   `asap/query-backend:dev` for staged rollout.

## Sequencing relative to the paper deadline

Realistic scope:

- **Ship §"Delete JSONL"** (~1-2 days). Paper drops the cold-fallback
  tier description entirely. Cost model simplifies. The architecture
  diagram in §Design becomes 2 tiers.
- **Path C** (~1 week) — extend the curated subset for the queries
  exercised by the demo + the queries the paper explicitly cites.
  Document the unsupported surface in a "future work" note.
- **Path A as post-deadline follow-up.** ~2 weeks of engineering;
  reviewers who push back on Path C get the answer "we have a working
  Path C today and Path A in flight, here's the design doc".

If timeline allows, **Path A directly** is the cleanest end-state — the
paper's archive-tier claim can then say "exact PromQL via Prometheus'
reference query engine over Gorilla-XOR chunks on object storage,
analogous to Thanos store gateway over their block format."

## References

- Comparison doc: `docs/comparison-asap-vs-databricks-pantheon-hydra.md`
- Original Gorilla-S3 design: `docs/design-gorilla-s3-cold-engine.md`
- MVP demo runbook: `docs/mvp-demo-runbook.md`
- Paper §Design tier description:
  `Super_resolution_ingestion_with_sketching_VLDB_or_SIGMOD/Design.tex`
- Prometheus `promql` package:
  https://github.com/prometheus/prometheus/tree/main/promql
- VictoriaMetrics `metricsql`:
  https://github.com/VictoriaMetrics/metricsql
- Rust `promql-parser`:
  https://crates.io/crates/promql-parser
- Thanos store gateway:
  https://thanos.io/tip/components/store.md/
- Databricks Hydra blog:
  https://www.databricks.com/blog/10-trillion-samples-day-scaling-beyond-traditional-monitoring-infra-databricks
