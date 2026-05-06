# JSONL deprecation + always-archive Gorilla-S3 + PromQL-complete query engine

## Status

Design proposal. 2026-05-06.

Not yet scheduled. Targets a single combined work track rather than three
loose follow-ups; Implementation phase 3 (PromQL completeness) is the
multi-week piece — phase 1 + 2 alone are days, not weeks.

## Goal

Collapse ASAP's three-tier serving stack from
`{warm sketch | exact archive | cold-fallback raw JSONL}` down to
`{warm sketch | exact archive}`, where the exact archive tier is a
PromQL-complete query engine over Gorilla-XOR chunks on S3 that subsumes
the role JSONL plays today.

## Non-goals

- Replacing the warm sketch tier or its `(ε, δ, kind)` accuracy contract.
- Changing the agent → backend wire format. Sketch envelopes remain the
  bandwidth-efficient hot path; this work only changes what the *cold*
  half of the stack looks like.
- Federation across regions. Out of scope here; tracked separately.

## Motivation

The current cold-fallback tier is a raw-JSONL store written by a
gateway-side raw-tee exporter. It exists for the genuine surprise case —
a query the controller didn't predict and the archive isn't configured to
hold. In practice it has three problems:

1. **Uncompressed.** JSONL is roughly 5–10× the on-disk size of the same
   data encoded as Gorilla XOR chunks. We pay for a third copy of every
   metric we already keep elsewhere.
2. **Functional duplicate.** The Gorilla-S3 archive tier serves the same
   role — exact retention queryable on demand — for metrics the controller
   has flagged. Two paths, one purpose.
3. **Per-metric opt-in.** Today the archive is opt-in (controller flips
   `StorageBackend::GorillaS3` on a metric); the JSONL fallback is the
   universal default. If we want one tier instead of two, that tier must
   be the universal default.

Databricks' Hydra (see `docs/comparison-asap-vs-databricks-pantheon-hydra.md`)
is a useful reference: they always-stream every raw metric to a lakehouse
on object storage, regardless of any aggregation rule decisions. The
"always-archive" pattern decouples surprise-query coverage from the
archive-opt-in plan.

## Proposal

Three phases, sequential. Each ships independently with its own PR.

### Phase 1 — always-archive at the gateway

Make every metric land in Gorilla-S3 by default, regardless of the
controller's plan.

**What changes:**
- A new gateway-side processor (or extension of the existing
  `gorillas3processor`) writes every scalar metric to S3 as Gorilla chunks,
  not just metrics flagged with `StorageBackend::GorillaS3`.
- The `drop_original` knob remains, but its semantics change: when set, the
  metric is dropped from the OTLP forward path but still written to the
  archive. When unset, it goes to *both* the OTLP forward path and the
  archive (double-write — already a first-class mode in `Design.tex`).
- The controller plan can still mark a metric `SketchWarmTier` (do not
  archive — for transient or sensitive metrics where lossless retention
  is undesirable) as an explicit override, but this becomes the exception
  rather than the rule.

**Why this is small:** the `gorillas3processor` already exists and is
exercised by `b6-gorilla-s3` overlays. The change is broadening which
metrics enter the processor, plus a config knob.

**Estimated cost:** 1–2 days. One small PR in ASAPCollector.

### Phase 2 — delete the JSONL path

Once Phase 1 is in production, the JSONL path becomes unused for any
metric written through the new gateway. We can then delete it cleanly.

**What gets deleted:**
- `asap-query-engine/src/drivers/query/fallback/cold_store/local_fs.rs`
  (the `LocalFsColdStore` impl)
- `asap-query-engine/src/drivers/query/fallback/cold_store/format.rs`
  (`parse_jsonl` + the torn-trailing-line tolerance + the pin tests
  guarding it)
- The gateway raw-tee exporter (`opentelemetry-collector-contrib-patch/exporter/coldstoreexporter` if present;
  audit before deletion)
- `Design.tex` §"Cold-fallback tier" — replaced by a single "exact archive
  tier" subsection
- `Implementation.tex` §Backend query engine cold-fallback prose
- The `cold-tier scan bytes` line item in `cost_model`
- The `StorageBackend::ColdJsonlFallback` enum variant

**What we keep as a graceful-degradation fallback:**
- `capability_matching` should still produce a sensible error when both
  the warm tier and the archive can't answer (e.g., a query against a
  metric that explicitly opted out of archiving). The error should be
  loud, not a silent fall-through to a removed path.

**Estimated cost:** 1–2 days. Small PRs in ASAPQuery-backend (delete
code, update tests, update docs) and ASAPCollector (delete exporter,
update overlays).

### Phase 3 — PromQL completeness on `GorillaQueryEngine`

Today's engine handles a curated subset:
- Streaming-additive: `sum / count / avg / min / max / rate / increase`
- Buffered: `quantile_over_time / topk`

For the archive tier to truly subsume JSONL, the engine must cover the
full PromQL surface: anything Prometheus or VictoriaMetrics can answer,
the engine should answer too. Concretely the missing pieces include:
- `histogram_quantile`
- Range-vector functions: `delta / deriv / predict_linear / holt_winters /
  idelta / irate / resets / changes`
- Aggregations with `by / without`: `group / stddev / stdvar / count_values
  / bottomk` (and the existing aggregations grouped, not just whole-set)
- Vector matching: `or / and / unless` with `on / ignoring`,
  `group_left / group_right`
- Scalar/vector coercions: `absent / scalar / vector`
- Time functions: `time / timestamp / month / year / day_of_week / …`
- Subquery expansion: `rate(x[5m])[1h:1m]` syntax
- Lookback-delta semantics matching Prometheus exactly

This is **a lot**. Three implementation paths, with different cost
profiles:

#### Path A — vendor Prometheus' `promql` package (recommended)

Run a Go sidecar exposing Prometheus' `promql` package as the evaluator;
expose Gorilla-decoded chunks to it via a custom `storage.Queryable`
implementation that decodes chunks from S3 on demand.

- **Cost:** medium. ~2 weeks: the sidecar wrapper, a Rust ↔ Go RPC
  surface, the `Queryable` adapter, integration tests against
  Prometheus' `promql_test` suite.
- **Pro:** correctness by construction — Prometheus' parser+evaluator is
  the reference implementation. PromQL semantics quirks (lookback-delta,
  subquery expansion, vector matching) come for free.
- **Pro:** changes to the Prometheus query engine (new functions,
  performance fixes) flow in by version-bumping the dependency.
- **Con:** adds a Go process to the Rust backend deployment. Operational
  surface widens.

#### Path B — pure-Rust evaluator over [`promql-parser`](https://crates.io/crates/promql-parser)

Use the existing Rust `promql-parser` crate for the AST; write the
evaluator in Rust, calling into `asap-gorilla` for chunk decoding.

- **Cost:** high. ~4–6 weeks: the evaluator is the bulk of Prometheus'
  ~12k LOC, with quirks. Need to match Prometheus' lookback-delta and
  subquery semantics exactly to avoid silent wrong answers.
- **Pro:** all-Rust deployment.
- **Con:** correctness risk. Silent semantic divergence from Prometheus
  is the failure mode reviewers care about most; we'd need a thorough
  cross-implementation test corpus to claim parity.

#### Path C — extend the curated subset

Stay with the curated-subset model; add `histogram_quantile`,
`sum/min/max/avg/group by`, `or / and / unless`, and a handful of the
most common range-vector functions; document the unsupported surface
explicitly and have `capability_matching` return a clear error for
queries outside it.

- **Cost:** low. ~1 week.
- **Pro:** ships fast; matches the engine's actual usage today, since
  warm-tier handles most queries.
- **Con:** "exact PromQL on the archive tier" becomes "exact PromQL for a
  subset of queries on the archive tier"; reviewers may push back.

**Recommendation:** **Path A**. The cost of silent wrong answers from a
homegrown PromQL evaluator is high; vendoring the reference
implementation is the only way to claim parity defensibly. The Go
sidecar adds operational surface but is a known pattern (Thanos itself
embeds Prometheus' query engine). If the deadline forces a short-term
ship, Path C unblocks Phase 1 + 2 deletion while Path A lands as a
follow-up — but the paper should say so honestly.

**Estimated cost:** 1–2 weeks (Path A); 1 week (Path C as bridge).

## Storage layout (no change)

Phase 1's broadening doesn't change the chunk format. Existing layout in
`Design.tex` §"Storage tiers — Gorilla archive tier" stands:

```
<tenant>/<metric>/YYYY/MM/DD/HH/part-NNNNNN.gor
<tenant>/<metric>/YYYY/MM/DD/HH/index.json
```

`index.json` is best-effort pruning; chunks are self-describing
(magic + schema_version + encoder_version + flags + time bounds + sample
count + payload length + header CRC32C + UTF-8 metric name + sorted-JSON
labelset + Gorilla body + payload CRC32C).

## Wire format (no change)

The agent → backend wire stays modified-OTLP with delta-of-sketch
envelopes. The change is gateway → S3 (broadened), not source → backend.

## `cost_model` update

The Layer-4 cost model already has line items for warm sketch state RAM,
Gorilla S3 PUT/GET counts, edge CPU, and bandwidth on each cut edge. The
"cold-tier scan bytes" line item is removed; queries that miss the warm
tier are evaluated through the archive engine, so the cost model unifies
to "warm sketch share + Gorilla scan share".

## Migration path

1. Phase 1 ships behind a config flag (`gorillas3processor: { archive_all: true }`)
   defaulting to `false`. Operators flip it on for a region, observe.
2. After verification, default flips to `true`. JSONL exporter still
   running but its output is increasingly unused.
3. Phase 2 ships: JSONL exporter removed from the gateway, JSONL reader
   removed from the backend, `StorageBackend::ColdJsonlFallback` enum
   variant removed, capability_matching tightened.
4. Phase 3 ships: `GorillaQueryEngine` becomes PromQL-complete.

A rollback escape exists at every step: Phase 1 is a config flip; Phase 2
is a code revert (the patch lives in git history); Phase 3 is additive
(extending coverage doesn't break existing queries).

## Paper impact

`Design.tex`, `Implementation.tex`, `Evaluation.tex`,
`abstract.tex`, `Introduction.tex`:
- Replace "three-tier" framing with "two-tier" everywhere: warm sketch +
  exact archive.
- Drop the "no data cliff via JSONL" prose; replace with "the archive tier
  is the universal fallback — surprise queries get an exact answer in
  bounded time rather than a JSONL scan".
- §RelatedWork: lean into the Hydra-style always-archive parallel; cite the
  comparison doc.

`design-gorilla-s3-cold-engine.md`:
- Update §"e2e architecture" diagram to reflect the two-tier shape.
- Add a §"PromQL completeness" section pointing to this doc.

`docs/comparison-asap-vs-databricks-pantheon-hydra.md`:
- The "Open question — cold-fallback JSONL deprecation" section in that
  doc is satisfied by this doc; cross-link.

## Open questions

1. **Tenant isolation.** S3 per-tenant prefix or per-tenant bucket? The
   current `<tenant>/<metric>/…` layout assumes prefix isolation; for
   strong tenant isolation we may need per-tenant buckets, which changes
   the controller's plan-target signalling.
2. **Sensitive-metric exemption.** Some metrics (e.g., user-PII counters)
   should never land in the archive at all. The controller plan already
   has a `SketchWarmTier`-only mode; we need to surface this clearly to
   the Phase 1 broadcaster.
3. **Long-range query cost on the archive.** A 30-day range query
   without warm-tier coverage means decoding 30 days of chunks. The
   chunk-LRU cache helps for hot queries; cold queries still scan. Worth
   characterising before claiming "exact PromQL on demand" in the paper.
4. **Backend image swap (Path A).** If we vendor Prometheus' `promql`,
   the `precompute_engine` Docker image gains a Go process. Operators
   running the existing image need an upgrade path. Probably worth a
   companion image
   `asap/query-backend-with-promql:dev` alongside the existing
   `asap/query-backend:dev` for staged rollout.

## Sequencing relative to the paper deadline

For the paper deadline (VLDB / SIGMOD), the plausible scope is:
- **Ship Phase 1 + Phase 2 as combined PR** (~2–3 days). Paper drops
  the cold-fallback tier description entirely. Cost model simplifies.
- **Phase 3 lands as Path C** (curated subset extension, ~1 week) so the
  paper can claim "exact PromQL on the archive tier for the queries we
  exercise" honestly, with a "future work: Path A integration with
  Prometheus' query engine" note.
- **Path A as post-deadline follow-up.** Two weeks of engineering;
  reviewers who push back on Path C get the answer "we have a working
  Path C today and Path A in flight, here's the design doc".

## References

- Comparison doc: `docs/comparison-asap-vs-databricks-pantheon-hydra.md`
- Original Gorilla-S3 design: `docs/design-gorilla-s3-cold-engine.md`
- Paper §Design tier description:
  `Super_resolution_ingestion_with_sketching_VLDB_or_SIGMOD/Design.tex`
- Prometheus `promql` package:
  https://github.com/prometheus/prometheus/tree/main/promql
- VictoriaMetrics `metricsql` parser:
  https://github.com/VictoriaMetrics/metricsql
- Rust `promql-parser`:
  https://crates.io/crates/promql-parser
- Databricks Hydra blog:
  https://www.databricks.com/blog/10-trillion-samples-day-scaling-beyond-traditional-monitoring-infra-databricks
