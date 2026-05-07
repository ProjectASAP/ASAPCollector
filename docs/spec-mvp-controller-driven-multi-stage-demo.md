# MVP v6 — controller-driven multi-stage end-to-end demo (issue #46)

## Status

Spec, 2026-05-06. Not yet implemented. Builds on v4 (PR #290 merged) and v5
(PR in flight as `mvp/v5-postings-compactor` + `mvp/v5-postings-aware-engine`).

## Goal

Demonstrate **end-to-end** that ASAP's controller picks sketches and stage
placements from an input PromQL query workload, the runtimes start with that
plan, and the resulting pipeline serves the three canonical query classes
within the six MVP criteria.

## Non-goals

- **Dynamic replanning during the demo run.** The controller computes ONE
  plan based on the input workload at start; runtimes start with that plan
  baked in. Workload-driven replan-while-running is a separate work item.
- **OpAMP-driven hot reconfiguration.** Today the agents/gateways must be
  stopped + restarted to pick up a new sketch config. OpAMP push is wired but
  not yet exercised end-to-end. Out of scope for this MVP.
- **Production-scale cardinality** (1M+ series). Single-host bench at 10K
  total backend cardinality is the target, distributed across the topology
  per the production-pyramid pattern below.
- **Cold/JSONL fallback.** v5's `data_source: gorilla_archive` confirmation
  satisfies criterion ⑤. The cold-fallback JSONL path stays exactly as it is
  today and is exercised but not modified.

## Topology

10K total backend cardinality, spread across the topology following typical
production ratios:

```
10 SDK instances (fake-exporter)
   ~100-500 series each (= 1K-5K total at the SDK layer)
        │
        ▼
2 agent collectors (sketchcol)
   ~5K series each at the agent layer
        │
        ▼
1 gateway collector (sketchcol-gateway)
   ~10K series aggregated at the gateway layer
        │
        ▼
1 backend (precompute_engine)
   ingests sketch envelopes + serves PromQL
```

Distribution rationale (from real-world numbers):
- SDK per-process: hundreds to low thousands. ASAP MVP uses 100-500 per
  fake-exporter instance.
- Agent collector: aggregates from many pods. ASAP MVP gives each agent ~5K.
- Gateway collector: fan-in across agents. ASAP MVP runs ONE gateway at 10K
  to keep the demo container count manageable; production would have 2+ for
  HA.
- Backend: millions in production. ASAP MVP uses 1, sized for 10K.

For the actual fake-exporter knob: each emits e.g. `card_per_agent=500` series
across (`{node, pod, rack, zone}`-style label combos). Ten of them via N=10
is the existing v4 mechanism.

## Three canonical query classes

The MVP must demonstrate **all three** for the controller-plan story to
matter. Each class has different sketch + stage-placement implications:

1. **Window aggregation per series.**
   Examples: `quantile_over_time(0.99, http_requests_total_latency_ms[1m])`,
   `rate(http_requests_total[5m])`, `sum_over_time(http_freshness_probe[10s])`.
   Per-series time-axis reduction.
2. **Label aggregation across multiple series at one timestamp.**
   Examples: `sum by (zone) (http_requests_total)`,
   `topk(10, http_requests_total{status=~"5.."})`,
   `count(http_requests_total{service="api"})`.
   Label-axis reduction at instant.
3. **Combined window + label aggregation.**
   Examples: `sum by (zone) (rate(http_requests_total[5m]))`,
   `topk(5, quantile_over_time(0.99, latency[1m]))`.
   Inner window per-series, outer label aggregation across the per-series
   results.

## Controller responsibility

Input: a list of PromQL queries (a "workload manifest"). For the MVP this is
a static file — `deploy/configs/mvp-v6-workload.yaml` — containing the three
query classes above, mapped to the metrics they reference.

Pipeline (already implemented in `controller/src/`):

```
L1 query_language    parse PromQL strings → AST
L2 logical_plan      AST → logical operators
L3 intent_algebra    AggIntent + QueryExpr DAG + Schema unique_keys
L4 sketch_algebra    SketchExpr + Bind* rules: pick sketch family per metric
L5 stage_split       StageAllocator + ThreeStageEmitter: assign each
                     SketchExpr to a stage (edge / gateway / backend)
                     with per-stage cost
```

**Output for the MVP**: a static plan describing, per metric:
- Sketch family (DDSketch / KLL / HLL / CountSketch / Count-Min Sketch /
  raw / Gorilla-archive)
- Stage of placement (SDK / agent / gateway / backend)
- Window `W`, label projection `L`, encoding triple

Plan is materialised as **per-runtime config files** (mirroring how v4
works): `sketchcol-agent-*.yaml` for agents, `sketchcol-gateway-*.yaml` for
the gateway, `backend-storage-routing.yaml` for the backend's
`EngineRouter`.

For the MVP, we don't need the controller to *push* plans at runtime — we
need it to *generate* the config files that the runtimes start with.

### Open question on controller plumbing

Today the controller's L5 produces a plan struct in memory. **Whether it can
serialise to runtime config YAMLs is unverified.** Three possibilities:

(a) **It already does** — Phase E shipped a "config emitter" that writes the
    YAMLs. Verify before implementation.

(b) **It almost does** — the StageAllocator's output is the right shape but
    needs a thin emitter (~1 day of work) to write YAML.

(c) **It doesn't** — needs a new module. ~3-5 days.

The first implementation step in v6 is to figure out which of (a)/(b)/(c) is
the case, by reading `controller/src/stage_split/` and the existing
`workloads.yaml` codepath.

## What runs where in the MVP plan

Concrete example — for the workload manifest above, a plausible plan output
is:

| Metric | Query class | Sketch family | Stage placement | Why |
|---|---|---|---|---|
| `http_requests_total_latency_ms` | window quantile | DDSketch | edge (per agent) | quantile is sketchable; emit per-agent so backend gets pre-aggregated state |
| `http_requests_total{status=~"5.."}` | label-aggregation count | raw | gateway (sum-merge) | count is exact, gateway aggregates to reduce series count |
| `http_requests_total` | combined: `sum by (zone) (rate(...[5m]))` | raw + agent rollup | edge (rate) → gateway (sum by zone) | inner rate at edge, outer label-sum at gateway; backend just stores the result |
| `http_freshness_probe` | freshness measurement | raw | backend (Gorilla-archive) | exact retention; gateway tees to S3 via `gorillas3processor` |

The actual values depend on the controller's cost model output for the
workload, but the schema-of-output is what runtime config emission needs to
target.

## Six MVP criteria — measurement plan

### ① Bandwidth (X)

Per-stage on-the-wire bytes/s, measured at:
- SDK → agent (per-agent input)
- agent → gateway
- gateway → backend
- (separately) gateway → S3 for the Gorilla-archive metric

Compared against B0 (raw end-to-end via Prometheus remote_write) on the
same workload, same cardinality split. Reported per-edge of the topology so
the user can see which stage saved how much.

Tooling: extend `measure_stages.py` (v4) to label per-edge bytes
(`edge=sdk_to_agent`, `edge=agent_to_gateway`, etc.).

### ② Aggregation query latency (Y)

p50 / p99 of each of the three query classes, compared against B0
Prometheus on the same workload. Measured by `promql_replay.py` (existing).

Reported per query class so we can see which class wins more.

### ③ Combined resource (Z)

Per-stage CPU + RSS + disk, summed across all containers per stage:
- `agent` (×2 containers)
- `gateway` (×1)
- `backend-ingest` + `backend-query` + `backend-storage` (the precompute_engine + MinIO/S3 process)

vs. the same partition for B0 (where `backend-ingest/storage/query` is
Prometheus + its TSDB on disk).

Tooling: `measure_stages.py` (v4 already has stage labelling). Update to
match the new topology (2 agents instead of 1).

### ④ Accuracy

Per-row relative error from the existing `accuracy_reduce.py`:
- Quantile rel-err inside DDSketch's ε envelope
- Sum-over-time exact (Count-Min within εδ)
- Topk recall vs cold-tier truth

Reported per query class. v4 fixed the NaN problem by adding query-side
warm-up; that fix carries forward.

### ⑤ Cold-store fallback for ad-hoc queries

Replay client fires an ad-hoc query that the warm tier cannot answer:
- `count(http_requests_total{service="payments"})` on a metric the controller
  did NOT plan for warm-tier coverage of `service=payments`
- The query routes via `BackendStorageRouting` to `GorillaQueryEngine`
- Response includes `data_source: gorilla_archive` (verified in v4)
- Latency reported

### ⑥ Freshness

For each of the three serving paths (B0 raw / warm-tier sketch /
Gorilla-archive), measure:

```
Δ = (timestamp when query first returns the sample)
  − (timestamp when the sample was generated by fake-exporter)
```

Mechanic (assuming clock sync within the host, which is fine for
single-host bench):

1. Fake-exporter emits a synthetic counter `http_freshness_probe_total{kind=...}` whose
   *value* is the Unix ms epoch at emission time (so the timestamp is encoded
   in the value itself).
2. Replay client polls `last_over_time(http_freshness_probe_total[10s])`
   every 100ms.
3. On first non-empty response: Δ = `poll_response_ts_ms − observed_value`.
4. Repeat for at least 60 samples per path; report p50, p99, count.

Three probes (one per path), each routed to its respective tier:
- `http_freshness_probe_raw` — B0 Prometheus
- `http_freshness_probe_warm` — sketch warm tier (any sketch family — pick
  whichever the controller plans)
- `http_freshness_probe_archive` — Gorilla-archive (controller plan flags
  this metric `StorageBackend::GorillaS3`)

The v4 freshness was UNKNOWN because the probe never reached Prometheus on
B0/B1/B5 (gateway didn't forward it) and ASAP's sketch flush window hadn't
fired in 60s. The v6 fixes:
- Probe routing tested explicitly for all three paths during pre-flight.
- Sketch window for the freshness probe metric pinned at ≤ 1s.
- Soak duration extended only as needed for the slowest path (probably the
  Gorilla-archive path, since chunks aren't queryable until the per-window
  flush lands in S3 — typically ≥ 60s).

## Demo workflow

```
0. user authors mvp-v6-workload.yaml (3 queries, fixed)
1. controller reads workload + cost model
2. controller emits per-runtime config files
3. docker-compose up with the emitted configs
4. fake-exporters emit
5. 60s warm-up
6. 30s query-side warm-up (poll for non-zero on a representative metric)
7. 60s measurement: replay client + freshness probes + measure_stages.py + s3_cost_tracker
8. Cold-fallback ad-hoc query + verify data_source: gorilla_archive
9. mvp_report.py emits MVP_REPORT_v6.md
10. comment posted on issue #46 with v6 results
```

## Phased implementation

### Phase A — verify controller plumbing (~1 day)

- Read `controller/src/stage_split/` and figure out which of (a)/(b)/(c) is
  the case for runtime config emission.
- If (c) — flag as a blocker; spec a separate emitter PR.

### Phase B — controller config emitter (~1-3 days, depends on Phase A)

- Implement / verify the controller's "plan → per-runtime YAML" path.
- Test: given a fixed workload manifest, the controller produces stable,
  diff-able config YAMLs that the runtimes already accept.

### Phase C — multi-agent + gateway-aggregation overlay (~2-3 days)

- New compose overlay `mvp-v6-multi-stage.yml` with 10 fake-exporters,
  2 agents, 1 gateway, 1 backend, 1 MinIO, 1 Prometheus (B0 baseline only).
- Gateway collector with sketch processors enabled (today's gateway is
  pass-through; needs the same processor config the agent uses, scoped to
  per-metric).
- Cardinality split: 100-500 series per fake-exporter, 5K per agent, 10K
  at gateway/backend.

### Phase D — workload + freshness wiring (~1 day)

- `mvp-v6-workload.yaml` defining the three query classes
- Three freshness probe metrics with proper routing (per Phase C overlay)
- Replay client extended to fire all three query classes per measurement
  window

### Phase E — driver + report (~1 day)

- `run_mvp_demo_v6.sh` driving Phases C+D end-to-end
- `mvp_report.py` v6 with sections per criterion + per query class
- Stage-separated bandwidth tooling (`measure_stages.py` extended for
  per-edge labelling)

### Phase F — run + verify + comment (~half day)

- Run end-to-end on the host
- Verify the six criteria each have measured numbers
- Post comment on issue #46 with `MVP_REPORT_v6.md` verbatim

Total: **~1-2 weeks** of focused work, depending on Phase A's verdict on the
controller emitter.

## Dependencies on in-flight work

- **v5-author** (postings + concat-compactor + cost tracker + label-predicate
  ad-hoc queries) — substrate. Must be merged before v6 demo run, since v6's
  ⑤ cold-fallback and ⑥ archive freshness need partial S3 reads + cost
  measurement to be well-instrumented.
- The cold-store JSONL deprecation work — orthogonal to v6; can ship later.

## What v6 does NOT verify (be honest about it)

- ❌ Dynamic plan transitions while the demo runs (one-shot static plan)
- ❌ OpAMP hot reconfig (config baked into runtime startup)
- ❌ 1M+ cardinality (10K bench)
- ❌ Multi-host federation (single-host)
- ❌ PromQL completeness on the archive tier (curated subset only)

These are flagged in the §Non-goals and `MVP_REPORT_v6.md`'s caveats section.
The paper's §Eval discusses them under "future work" or "out of scope at
demo scale".

## Success criteria

- All six MVP criteria report measured numbers (not UNKNOWN, not fabricated)
- Controller's stage-split visibly drives the demo (the per-runtime configs
  are observable as the controller's output, not hand-authored)
- Three query classes all answered, per-class latency reported
- v5 substrate (postings + compactor + cost tracker) actually exercised
- Comment on issue #46 with `MVP_REPORT_v6.md` verbatim
- Both v5 PRs and v6 PRs merged

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Controller's L5 doesn't have a config emitter (Phase A is (c)) | Build a thin emitter; track as a separate PR; Phase B's spec doc precedes implementation |
| Gateway sketch processors don't load cleanly (today's gateway is pass-through) | Mirror the agent's `sketchcol-agent-b6-asap-single-sketch.yaml` config to a `sketchcol-gateway-mvp-v6.yaml`; if the binary doesn't load processor config, this becomes the blocker — escalate before Phase C |
| Sketch window flush timing causes ⑥ freshness UNKNOWN again | Pin window to ≤ 1s for the freshness probe metric; extend soak as needed for archive path |
| Z resource doesn't show savings at 10K cardinality (v4 saw negative) | Honest report; explain in caveats that the savings are at higher cardinality where sketch state amortises better; cross-reference 60-cell sweep numbers |

## References

- Issue #46 (updated): https://github.com/ProjectASAP/ASAPCollector/issues/46
- v4 PR (merged): https://github.com/ProjectASAP/ASAPCollector/pull/290
- v4 issue comment: https://github.com/ProjectASAP/ASAPCollector/issues/46#issuecomment-4391549654
- v5 PRs (in flight): `mvp/v5-postings-compactor`,
  `mvp/v5-postings-aware-engine`
- Comparison doc: `docs/comparison-asap-vs-databricks-pantheon-hydra.md`
- Controller align design: `docs/control-plane-design.md`
- Gorilla-S3 design: `docs/design-gorilla-s3-cold-engine.md`
