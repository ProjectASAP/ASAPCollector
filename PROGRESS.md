# DataCollector progress

_Last updated: 2026-05-05._

## Cross-language byte-format parity, 5/5 sketches (2026-05-05)

Closes [#243](https://github.com/ProjectASAP/ASAPCollector/issues/243).
ADR-0002's bit-identical-wire-format promise is now real across the
Go and Rust runtimes for all five sketch families, so a fleet mixing
`asap-precompute-go` (Go edge runtime + sketchlib-go) and
`asap-precompute-rs` (Rust edge runtime + asap_sketchlib) produces
envelopes the backend can merge cross-source.

### What landed

- **asap_sketchlib**: ports the canonical hash + serialization paths
  to match `sketchlib-go` byte-for-byte.
  - DDSketch ([sketchlib#40](https://github.com/ProjectASAP/asap_sketchlib/pull/40)),
    KLL ([#41](https://github.com/ProjectASAP/asap_sketchlib/pull/41)),
    CountSketch hh_keys + apply_delta topk-rebuild ([#42](https://github.com/ProjectASAP/asap_sketchlib/pull/42))
    landed earlier (2026-05-04 / 2026-05-05 morning).
  - HLL hash-seed alignment ([#43](https://github.com/ProjectASAP/asap_sketchlib/pull/43)).
  - CountSketch HashSpec / `derive_index` / `derive_sign` port
    ([#44](https://github.com/ProjectASAP/asap_sketchlib/pull/44))
    — also extracts the shared `asap_sketchlib::common::hashspec`
    module (20-entry seed table, `HashSpec`, `hash_with_spec`,
    `derive_index`, `derive_sign`) so CMS reuses the same primitives
    bit-identically.
  - CountMinSketch port ([#45](https://github.com/ProjectASAP/asap_sketchlib/pull/45))
    — `CountMinSketch::update` / `::estimate` route through
    `common::hashspec::derive_index` over a power-of-two-rounded
    column mask (`hashLayoutForCols`-equivalent), with `col % cols`
    fold for non-pow2 widths.

- **ASAPCollector** (this repo): un-ignores the `cross_language_parity`
  tests and aligns the wrappers / fixture generators to Go's
  `SerializePortable*` byte layout.
  - DDSketch ([#247](https://github.com/ProjectASAP/ASAPCollector/pull/247)),
    KLL ([#250](https://github.com/ProjectASAP/ASAPCollector/pull/250)),
    HLL ([#252](https://github.com/ProjectASAP/ASAPCollector/pull/252)),
    CountSketch ([#253](https://github.com/ProjectASAP/ASAPCollector/pull/253))
    landed earlier.
  - **CountMinSketch + HLL fixture-generator alignment** ([#254](https://github.com/ProjectASAP/ASAPCollector/pull/254)):
    `CMSWrapper::build_state` now emits the FO payload
    (`counter_type=INT64`, packed sint64 `counts_int`, per-row
    `l1`/`l2`, empty `sum_counts`/`sum2_counts`) matching Go's
    `SerializeProtoBytesFO`. Also fixes a latent inconsistency
    discovered during fixture regen: the HLL fixture generator was
    calling `sk.SerializeProtoBytes()` directly (which embeds
    Producer + HashSpec metadata), while every other sketch uses
    `sk.SerializePortable*()` + strip + `proto.Marshal`. PR #252
    un-ignored the HLL test but missed this strip; the test only
    "passed" because fixtures are gitignored and developers rarely
    regenerate-then-test in one step. Once you do (`GOLDEN_REGEN=1
    go test … && cargo test --include-ignored …`), HLL diverged by
    exactly the 134-byte Producer + HashSpec footprint. #254 brings
    HLL onto the same `SerializePortable` + strip pattern as
    DDSketch / KLL / CountSketch / CMS.

### Verification

```
$ GOLDEN_REGEN=1 go test -run TestGenerateGoldenFixtures \
    ./integration/parity/...
ok  	github.com/ProjectASAP/ASAPCollector/integration/parity

$ cargo test --release --test cross_language_parity -- --include-ignored
running 7 tests
test ddsketch_byte_parity_with_go ... ok
test kll_byte_parity_with_go ... ok
test hll_byte_parity_with_go ... ok
test countsketch_byte_parity_with_go ... ok
test cms_byte_parity_with_go ... ok
test golden_fixtures_when_present_are_nonempty ... ok
test rust_wrappers_produce_nonempty_envelopes_for_same_input ... ok

test result: ok. 7 passed; 0 failed; 0 ignored
```

Fixture sizes (deterministic, byte-stable across regen):
DDSketch 432 B, KLL 423 B, HLL 16398 B, CountSketch 1577 B,
CMS 8275 B.

### Downstream unblocks

`ASAPQuery-backend`'s `edge_runtime_consumes_precompute_rs.rs`
acceptance tests for HLL / CountSketch / CountMinSketch were gated
`#[ignore = "blocked on ASAPCollector#243"]` per
`design-phase3-asap-precompute-rs.md` line 141–144 — they should
now pass without backend code changes. Mechanical un-ignore PR
pending (separate from this work).

## Single-pipeline multi-sketch + delta + queryable warm tier (2026-05-01)

Building on the all-five-sketch wire path from 2026-04-30, this
session closes the loop from agent emit → gateway preserve → backend
decode → store → PromQL answer. After this work, a single
`asap/sketchcol:dev` binary supports any controller-chosen sketch
combination, in any single-pipeline shape, with delta transmission,
end-to-end PromQL queries returning real values.

### What landed

- **DDSketch delta wire fix** ([#210](https://github.com/ProjectASAP/ASAPCollector/pull/210)).
  Three connected agent-side bugs that meant `delta_transmission: true`
  silently behaved like full-state-only after the first window:
  (1) `dp.SetEncoding(pmetric.DDSketchEncodingProto)` ran unconditionally
  — the typed encoding never reflected `proto_delta`, so the backend
  dispatched delta bytes through the proto_full decoder and failed.
  (2) `computeDDSketchDelta` was a stub returning the full-state bytes,
  even after sketchlib-go's `ComputeDelta(snap, current, threshold)`
  was already available. (3) The agent set `ddsketch.encoding` as a
  per-data-point attribute, which made the backend's per-series
  snapshot cache key differ between full and delta frames →
  "delta-sketch arrived before any base snapshot" on every delta.

- **Single-pipeline multi-sketch** ([#211](https://github.com/ProjectASAP/ASAPCollector/pull/211)).
  In window mode, `ddsketch / kll / hll` processors were
  `accumulateIntoWindow(md); return nil` — they accumulated inputs but
  did NOT forward them to the next consumer. So `[ddsketch, hll, batch]`
  silently dropped everything: ddsketch ate the raw inputs (HLL never
  saw them), and HLL had no `case MetricTypeDDSketch:` in its
  `accumulateIntoWindow` switch (so ddsketch's tick emission also got
  dropped). Patched all three to `return p.nextConsumer.ConsumeMetrics(ctx, md)`,
  matching what `countminsketchprocessor / countsketchprocessor` already do.
  Now any chain of windowed sketch processors works in a single
  pipeline, which is what the controller actually generates and what
  the b2/b3/b4 paper baselines were always supposed to test.
  Supersedes the per-pipeline-split workaround in #209 (closed).

- **Companion backend changes:**
  - [ASAPQuery-backend#70](https://github.com/ProjectASAP/ASAPQuery-backend/pull/70):
    bump tonic OTLP gRPC `max_decoding_message_size` from 4 MiB to
    64 MiB. First-window full-state DDSketch at 1k cardinality is
    ~17 MiB, so the gateway's exporter looped forever on the
    `decoded message length too large` error pre-fix.
  - [ASAPQuery-backend#71](https://github.com/ProjectASAP/ASAPQuery-backend/pull/71):
    `range_query_into` switched from "fully-contained" to half-open
    overlap (PromQL queries don't align to the 30s pane grid, so
    the strict filter returned empty even when the data was in the
    store). Plus the engine now picks a single closest pane for
    window queries and annotates the response's `infos` with
    `precompute_window: [start_ms, end_ms) ms (width N ms)` so the
    caller sees exactly which precompute time range produced the
    answer.

### Live e2e verified

```
W1 (full):  OTLP modified-proto sketch ingest: 1000 routed, 0 decode-failed
W2 (delta): OTLP modified-proto sketch ingest: 1000 routed, 0 decode-failed
W3 (delta): OTLP modified-proto sketch ingest: 1000 routed, 0 decode-failed

$ curl '/api/v1/query?query=quantile_over_time(0.5, http_requests_total_latency_ms_quantile[1m])&time=$(now-90s)'
{"data":{"result":[{"metric":{"node":""},
                    "value":[..., "19.493849507395904"]}],
         "resultType":"vector"},
 "infos":["accuracy: ε=0.01, δ=0, kind=relative_quantile",
          "precompute_window: [1777655280000, 1777655310000) ms (width 30000 ms)"]}
```

Pre-fix the same query against b3-delta (`delta_transmission: true`,
`[ddsketch, HLL, batch]` chain) returned `result: []` with
`No precomputed outputs found`.

### Verified live in this round, not yet exhaustively swept

DDSketch was the live-verified path (b3-delta, full + delta, post-fix
window query). The matching backend decoder code paths
(`apply_modified_otlp_delta_bytes` →
`{DDSketch,HLL,CountSketch,CountMinSketch}Accumulator::apply_proto_delta_bytes`)
exist for HLL / CountSketch / CountMinSketch as well, and the agent
processors (HLL `ComputeRegisterDelta`, CMS / CountSketch `ComputeDelta`)
were already correct on the encoding side — only DDSketch had the
three-stack of bugs above. KLL has no delta concept by construction.
Running the full P7 sweep over all five with the accuracy reducer is
the next step (see follow-up #1 below); structurally there is no
known reason it shouldn't pass.

## All-five-sketch runtime e2e verification (2026-04-30)

PromQL → controller → agent (sketchcol) → backend (precompute_engine) →
PromQL response, end-to-end through the modified-OTLP wire format
(typed `Metric.data = {DDSketch | KLLSketch | HLLSketch | CountSketch
| CountMinSketch}` data points, not Gauge-with-payload). One soak
per sketch with a single configuration; this is *the* path that the
sweep harness (P5–P9) drives.

| Sketch | Query (PromQL) | Result | Accuracy envelope | Notes |
|---|---|---|---|---|
| DDSketch | `histogram_quantile(0.5,…)` etc. | q=0.5→21.12, q=0.9→47.95, q=0.99→104.60 | `relative_quantile`, ε=0.01 | Agent uses `sketchlib-go/DDSketch` (replaced DataDog impl); proto envelope encoded via `SerializePortable`. |
| KLLSketch | `histogram_quantile(0.5,…)` | q=0.5→18.26 | `rank_quantile`, ε=0.16 | sketchlib-go KLL `SerializeMsgpack` → backend `DatasketchesKLLAccumulator::from_msgpack_bytes`. |
| HLLSketch | `count(http_requests_total)` | 149.68 distinct | `relative_cardinality`, ε=0.008 | HLL accumulator's `query_statistic` accepts both `Statistic::Cardinality` and `Statistic::Count` (Count alias added). |
| CountSketch | `sum_over_time(http_requests_total[1m])` | 24266 | `additive_frequency`, ε=0.03 | CountSketch query_statistic returns row-mean total when no key is provided. |
| CountMinSketch | `sum_over_time(http_requests_total[1m])` | 145735 | `additive_frequency`, ε≈0.0027, δ=0.03125 | CMS query_statistic now mirrors CountSketch's no-key fallback: returns the min-row sum (canonical CMS total-event estimator). |

### Cross-cutting fixes that made the e2e land

- **sketchlib-go**: `SerializeMsgpack` added to HLL / CountSketch /
  CountMinSketch (parity with KLL); cross-language wire format is
  what `ASAPQuery-backend` consumes through
  `*::from_msgpack_bytes`.
- **Agent processor (`ddsketchprocessor`)**: replaced
  `github.com/DataDog/sketches-go` with `sketchlib-go/DDSketch` so
  the proto envelope is decodable by `asap_sketchlib`'s
  `DDSketchState`.
- **Controller `data_sink`** (`controller/src/types.rs` +
  `config/agent.rs`): generated agent config now picks between
  `Otlp{endpoint,…}` and `PrometheusScrape{…}` exporters via an
  `AgentDataSink` enum, instead of always emitting the
  `prometheus` exporter (architectural fix the user flagged —
  Prometheus exposition is not a controller concern).
- **OpAMP framing** (`controller/src/opamp/mod.rs`): incoming WS
  payloads have their varint header stripped before proto decode;
  outbound `ServerToAgent` frames carry the
  `ReportFullState` flag and the Accept/Offer capability bitmask
  so agents accept and apply config.
- **Backend `query_statistic`**: implemented Quantile / Sum / Count
  / Min / Max for DDSketch; Cardinality (+ Count alias) for HLL;
  Topk / Count / Sum for CountSketch; **Count / Sum (no-key) for
  CMS** with the min-row-sum estimator (this PR).
- **Build glue**: the OCB v0.141.0 builder file is now
  `cmd/sketchcollector/builder-config-sketches.yaml` (renamed from
  `builder-config-ddonly.yaml`); compiles all five sketch
  processors plus `opampextension`.

### Limitations + follow-ups

- **CMS query without a paired key aggregator returns total volume,
  not per-key frequency.** That's the right answer for `sum / count
  / sum_over_time / count_over_time` (every insert increments one
  cell per row, so the min row total is the exact insert count
  modulo CMS hashing collisions — and CMS never *under*-counts). To
  serve `topk(N, …)` over CMS-tracked frequencies the system needs
  a paired `SetAggregator` / `DeltaSetAggregator` running on the
  agent so the backend can enumerate keys in the multi-population
  dual-input path. Out of scope for this verification round.
- **`compatible_agg_types` in `capability_matching.rs` does not list
  CountMinSketch under `Statistic::Sum`** even though
  `query_logics::logics::map_statistic_to_precompute_operator`
  treats CMS as the canonical approximator for both Sum and Count.
  The exact-match `find_query_config` path bypasses
  capability_matching and made the e2e pass; reconciling the two
  tables (so capability matching also picks CMS for Sum) is a
  separate cleanup.

## e2e harness build-out (P1–P9, in progress)

Driven by the user request for a real complete e2e: PromQL →
controller → plan push → agent sketch + backend query → accuracy
+ throughput + latency + plan-transition observability.

| Step | Status | Notes |
|---|---|---|
| P1. Wire `ASAP_COLD_STORE_ROOT` in `asap-query-engine/main.rs` | ✅ 2026-04-30 | `--cold-store-root` flag (env `ASAP_COLD_STORE_ROOT`) selects `prometheus_promql_with_cold`; 4 unit tests |
| P2. Hot-reload View `AttributeFilter` (mid-run projection swap) | ✅ 2026-04-30 | `deploy/fake-exporter/swappable_filter.go` — atomic.Pointer-backed filter wired into `Stream.AttributeFilter`; `POST /control/projection` HTTP endpoint; 5 tests incl. race + e2e through ManualReader. **No SDK patch was needed**: the SDK's `aggregate.Builder.filter` closure dispatches through the function value, so atomic-state inside the filter is observable on the next measurement. |
| P3. Build deploy images + N=1 b3-delta smoke run | ✅ 2026-04-30 | All four images (`asap/{controller,query-backend,fake-exporter,sketchcol}:dev`) build cleanly and `docker compose up` stands up the full stack. Verified: backend logs cold-tier fallback enabled; raw-tee writes ground-truth JSONL with the right path layout; swappable-filter HTTP swap returns `{"applied":"zone"}`. The earlier warm-tier limitation (gateway PRW dropping typed sketches) is now resolved by the OTLP-end-to-end path landed in the deploy follow-up — see follow-up #1 below. |
| P4. Ground-truth tee from fake-exporter to MinIO raw JSONL | ✅ 2026-04-30 | `deploy/fake-exporter/raw_tee.go` — atomic.Pointer-style hour-bucketed JSONL writer matching the Rust `RawSample` wire format byte-for-byte. 8 unit tests incl. concurrent-writer race + format anchor + per-instance file naming. Wired into `runSynthetic` + `runTraceReplay`; controlled by `EXPORTER_RAW_TEE_ROOT` env. e2e overlay mounts a shared `cold-store` Docker volume into both fake-exporter (writer) and backend (reader). |
| P5. PromQL replay client with plan-id tagging | ✅ 2026-04-30 | `deploy/scripts/promql_replay.py` — fires PromQL at backend `:19091` at fixed QPS, captures p50/p99 + result vector per query, tags every line of the JSONL log with the controller's currently-published `plan_id` (1 Hz polling thread). Smoke-tested: 17 queries / 6 s, p50 2.4 ms, p99 1.3 s (cold-fallback dominated). |
| P6. Plan-transition driver + 1 Hz CPU/bandwidth sampler | ✅ 2026-04-30 | `deploy/scripts/plan_transition.py` — fires a query the active plan can't answer; tracks `t_query_in / t_plan_ready / t_first_hit / t_steady` against the controller's plan-id stream; `DockerStatsSampler` dumps 1 Hz cpu/mem/net per container to a separate JSONL. Imports + smoke-tests pass. |
| P7. Sweep runner over sketch × N × scrape × cardinality matrix | ✅ 2026-04-30 | `deploy/scripts/run_e2e_sweep.sh` — drives `{DDSketch, KLL, CS, CMS, HLL} × {N=1, 10} × {scrape=100 ms, 1 s} × {card=1e3, 1e4, 1e5}` (60 cells). Per cell: brings stack up, runs P5 + P6 concurrently for the soak window, snapshots the cold-truth volume into the cell directory, brings stack down with `-v`. |
| P8. Accuracy reducer (truth ⋈ sketch answer) | ✅ 2026-04-30 | `deploy/scripts/accuracy_reduce.py` — parses replay JSONL + the cold-truth tree per cell; computes per-row relative error for quantile / sum / count_unique and per-row top-K recall. Smoke-tested on a real cell: 4.1 M ground-truth samples, 17 query rows, output CSV produced. |
| P9. Plots — Pareto, bandwidth, transition timeline, query CDF | ✅ 2026-04-30 | `deploy/scripts/e2e_plots.py` — four figures + their underlying CSVs. Smoke-tested: produces `pareto_acc_vs_thru.png` and `query_latency_cdf.png` from real data; `bandwidth_vs_n.png` and `transition_timeline.png` skip cleanly when the corresponding sample/transition records aren't in the cell. |

### Operating the e2e harness

```bash
# 1. Pre-reqs: docker, python ≥3.10, matplotlib + pandas in the user
#    env (`pip install --user matplotlib pandas`), the four
#    `asap/*:dev` images built (see deploy/docker/Dockerfile.* —
#    backend uses --build-context backend-src=...).
# 2. Single-cell smoke run:
AGENT_CONFIG=sketchcol-agent-b3-delta.yaml docker compose \
    -f deploy/docker-compose/base.yml \
    -f deploy/docker-compose/agents-N1.yml \
    -f deploy/docker-compose/baseline-b3-delta.yml \
    -f deploy/docker-compose/e2e-overlay.yml \
    up -d

# 3. Drive the workload: replay + plan-transition concurrently:
python3 deploy/scripts/promql_replay.py \
    --target http://localhost:19091 --controller http://localhost:18080 \
    --queries deploy/scripts/queries-e2e.json \
    --qps 5 --duration 60 --out /tmp/replay.jsonl &
python3 deploy/scripts/plan_transition.py \
    --target http://localhost:19091 --controller http://localhost:18080 \
    --transition-query 'histogram_quantile(0.999, sum by (le) (http_requests_total_latency_ms))' \
    --transition-out /tmp/transition.jsonl --sample-out /tmp/sample.jsonl \
    --soak-secs 60 --pre-transition-secs 20

# 4. Snapshot ground truth (volume goes away on -v):
docker cp $(docker compose -f .../base.yml -f .../e2e-overlay.yml ps -q backend):/var/asap/cold/raw /tmp/cell/cold-truth

# 5. Reduce + plot:
python3 deploy/scripts/accuracy_reduce.py --cell-dir /tmp/cell --out /tmp/cell/accuracy.csv
python3 deploy/scripts/e2e_plots.py \
    --sweep-root /tmp --accuracy /tmp/cell/accuracy.csv --out-dir /tmp/cell/plots

# 6. Full sweep (~hours wall):
deploy/scripts/run_e2e_sweep.sh --out-dir /tmp/sweep-$(date +%s) --soak-secs 120
```

### Open follow-ups (not e2e blockers)

1. ~~**Warm-tier sketch ingest is dropped at the gateway.**~~ **Done
   (2026-05-01, patched-gateway PR).** The original PROGRESS note
   blamed the PRW exporter, and #205's yaml-only fix swapped that
   for an OTLP exporter. **That wasn't actually the root cause.**
   The drop happens earlier — in the gateway's pdata *unmarshal*
   step. Stock `otel/opentelemetry-collector-contrib:0.108.0`'s
   pdata only knows about the standard `Metric.data` OneOf
   variants (Gauge / Sum / Histogram / ExponentialHistogram /
   Summary). Tags 13–17 (DDSketch / KLLSketch / HLLSketch /
   CountSketch / CountMinSketch) hit the `default:` arm at
   `opentelemetry-collector/pdata/internal/generated_proto_metric.go:1201`
   which calls `proto.ConsumeUnknown(...)` — that advances past
   the bytes without storing them. There's no `XXX_unrecognized`
   field on the `Metric` struct to catch them. So by the time any
   exporter sees the metric, the typed sketch payload is gone,
   regardless of whether the exporter is PRW or OTLP.

   **Real fix:** run the gateway from the ASAP-patched OTel
   collector (the same build the agents already use). The
   patched build's pdata knows tags 13–17 and round-trips them
   intact. `deploy/docker-compose/base.yml`'s gateway service now
   uses `image: asap/sketchcol:dev` (was stock 0.108). Three
   gateway configs in `deploy/configs/`:
   - `gateway.yaml` — pure forwarder (default).
   - `gateway-aggregate-from-raw.yaml` — gateway runs sketch
     processors and emits typed sketches downstream.
   - `gateway-aggregate-from-sketches.yaml` — gateway merges
     already-sketched payloads. Uses the SAME processors as the
     agent (`ddsketch`, `kll`, etc.); each one's input switch
     handles BOTH raw inputs (Gauge/Sum) AND typed sketch inputs
     (`MetricTypeDDSketch`, etc.) — see e.g.
     `processor/countminsketchprocessor/processor.go:269` and
     `processor/ddsketchprocessor/processor.go:206`. Configured at
     the gateway with the same window the agent used, this gives a
     windowed cross-agent merge for the typed wire format. The
     legacy `countminsketchmerge` / `countsketchmerge` processors
     are only for the OLD Gauge-with-payload wire format — not
     needed for the typed wire today's e2e uses, and similarly
     no separate merge processors are needed for DD / KLL / HLL.

   **e2e verification (2026-05-01):** with the patched gateway
   AND the chain pass-through fix
   ([#211](https://github.com/ProjectASAP/ASAPCollector/pull/211))
   AND the DDSketch delta wire fixes
   ([#210](https://github.com/ProjectASAP/ASAPCollector/pull/210))
   AND the backend's overlap filter + closest-pane fix
   ([ASAPQuery-backend#71](https://github.com/ProjectASAP/ASAPQuery-backend/pull/71))
   in place, the b3-delta config (60s agent window, `delta_transmission: true`,
   chained `[ddsketch, HLL, batch]`) ingests three consecutive
   windows cleanly (1000 routed, 0 decode-failed each) and PromQL
   `quantile_over_time(0.5, http_requests_total_latency_ms_quantile[1m])`
   returns real values from the warm tier with a
   `precompute_window` annotation in the response. See the
   "Single-pipeline multi-sketch + delta" section at the top of
   this file for the captured run. The earlier note about a
   ddsketch→batch wiring quirk was that bug — fixed in #211.

   Switch the gateway mode via `GATEWAY_CONFIG=...` env (mirror
   of `AGENT_CONFIG`). The yaml changes from #205 (otlp/backend
   exporter, `--enable-otel-ingest` on the backend) are still in
   main and are correct in their own right — they just weren't
   sufficient alone.

   `Dockerfile.backend.queryengine` + `queryengine-overlay.yml`
   are still useful when the deploy needs the binary's
   controller-in-loop / query-tracker / backfill / schema-eviction
   features — not directly tied to this fix.
2. **Run the full P7 sweep + accuracy reducer (P8) over all
   five sketch types.** Tooling exists (`run_e2e_sweep.sh`,
   `accuracy_reduce.py`, `e2e_plots.py`) and the wire path was
   verified live for DDSketch in this round. Multi-sketch
   structurally should pass — backend decoder paths
   (`apply_modified_otlp_delta_bytes`) cover all four
   delta-capable sketches; KLL has no delta. What's missing is
   the actual sweep producing a CSV that demonstrates each
   sketch's empirical error stays inside its theoretical
   `AccuracyEnvelope` (ε / δ / kind already surfaced in every
   query response's `infos`). Output: per-cell accuracy CSV +
   the four P9 plots (pareto, bandwidth-vs-N, transition
   timeline, query CDF).
3. **Backend in-memory snapshot cache is lost on backend restart.**
   `IngestState.sketch_snapshots` (the per-series cache that
   delta frames apply against) is RAM-only. After a backend
   bounce, agents continue emitting `proto_delta` against their
   local snapshots, and every delta frame fails decode at the
   backend with "delta-sketch arrived before any base snapshot"
   until the agent itself restarts. Two reasonable fixes: (a)
   persist `sketch_snapshots` to the existing per-key disk
   layer the precompute store already uses, or (b) add an OpAMP
   capability that lets the backend signal agents to send the
   next frame as full state. (a) is local; (b) crosses the
   controller boundary.
4. **Inference config breadth.** Only one PromQL pattern per
   metric currently lands in `backend-inference.yaml`
   (`quantile_over_time(0.5, …[1m])`,
   `histogram_quantile(0.5, …)`, `sum_over_time(…[1m])`). Wider
   ranges (`[2m]`, `[5m]`, `rate(…)`, multi-quantile) fall
   through to capability matching or the cold tier. Adding
   patterns is mechanical but expands what queries the warm
   tier can answer.
5. **~~Cold reader is intolerant of torn last lines.~~**
   **Done 2026-05-05** ([ASAPQuery-backend#80](https://github.com/ProjectASAP/ASAPQuery-backend/pull/80)).
   `parse_jsonl_at` tolerates a torn trailing line + warn-logs
   the part-file path; mid-file corruption still hard-errors.
   Two new pin tests (`parse_jsonl_ignores_torn_trailing_line`,
   `parse_jsonl_errors_on_mid_file_corruption`) lock both
   shapes against future regression.
6. **Reducer runs offline; doesn't need the backend live.** That's
   fine for accuracy claims, but PromQL semantics are easy to
   drift from the engine. Add a self-check that runs the same
   query against the cold truth via the engine itself, where
   feasible.
7. **`build_sketchcollector.sh` env vars are external.** The
   script needs `GOPRIVATE='github.com/ProjectASAP/*'
   GOTOOLCHAIN=auto` to actually build (sketchlib-go's
   private-module + Go toolchain auto-upgrade). Inlining these
   into the script removes a footgun for new contributors.
8. **OTel submodules dirty in working tree.** `opentelemetry-collector`
   and `opentelemetry-go` show as modified content / new
   commits and have been intentionally excluded from PRs since
   #204. Decision still pending — either commit a clean bump as
   its own PR or revert.
9. ~~**`asap/fake-exporter:dev` rebuild broken from upstream drift.**~~
   **Done (2026-05-05).** The patched OTLP proto bindings (mpb.DDSketch /
   KLLSketch / CountSketch / CountMinSketch / HLLSketch) were never
   committed under `opentelemetry-proto-patch/gen/go/...`, so any rebuild
   hit `undefined: mpb.*` symbols. Separately, the patch dir's transform
   files referenced `metricdata.*EncodingGob` enum names that the metric-
   data package had renamed to `*EncodingProto` / `*EncodingDelta`. Fixed:
   (a) regenerated the Go bindings via the upstream Makefile recipe and
   committed them under `opentelemetry-proto-patch/gen/go/`; (b) added a
   `go.opentelemetry.io/proto/otlp` replace to `deploy/fake-exporter/go.mod`
   pointing at the patch's gen tree; (c) updated the http+grpc transform
   files to use the post-rename `*EncodingProto` / `*EncodingDelta`
   metricdata enums and `*_ENCODING_PROTO` / `*_ENCODING_DELTA` mpb enums;
   (d) updated `Dockerfile.fake-exporter` to copy `opentelemetry-proto/`
   into the build context. Regen recipe lives at
   `opentelemetry-proto-patch/REGEN.md`. The companion sketchlib-go PR #53
   rename refactor (Add/Insert/InsertValue/EstimateCardinality/
   GetValueAtQuantile → Update/UpdateValue/Estimate/Quantile) was already
   absorbed into the patch dir before this round; the build verifies it.

---

_Original progress notes follow._

Single source of truth for where DataCollector stands: what's
implemented, what's outstanding, what's out of scope for this
paper. Scoped siblings:

- [`docs/paper-outline.md`](docs/paper-outline.md) — paper
  sections, claims, experiment matrix.
- [`docs/sdk-cost-evaluation.md`](docs/sdk-cost-evaluation.md)
  — SDK-side aggregation knobs + cost-evaluation design.
- [`deploy/README.md`](deploy/README.md) — multi-agent stack
  setup + sweep driver.
- ASAPQuery-backend [`TODO.md`](https://github.com/ProjectASAP/ASAPQuery-backend/blob/main/TODO.md)
  — sketch-DB-side work.

## Implemented

### SDK-side aggregators — `opentelemetry-go-patch/sdk/metric`

Drives the encoding axis of the SDK cost evaluation. Status per
slot:

| `agg_type` | Status | Notes |
|---|---|---|
| `sum`, `last-value`, `explicit-bucket-histogram`, `exponential-histogram` | ✅ upstream | `go.opentelemetry.io/otel/sdk/metric` |
| `dd-full`, `dd-delta` | ✅ | `AggregationDDSketch{DeltaTransmission?: bool}` |
| `kll-full` | ✅ | `AggregationKLLSketch{}` |
| `cs-full`, `cs-delta` | ✅ | `AggregationCountSketch{DeltaTransmission?: bool}` |
| `cms-full`, `cms-delta` | ✅ | `AggregationCountMinSketch{DeltaTransmission?: bool}` |
| `hll-full`, `hll-delta` | ✅ | `AggregationHLLSketch{DeltaTransmission?: bool}` |
| `raw-buffer` | ✅ | `AggregationRawBuffer`; impl `opentelemetry-go-patch/sdk/metric/internal/aggregate/rawbuffer.go`; contract test `deploy/fake-exporter/sdk_emit_test.go` |
| `kll-delta` | no need | KLL's multi-level sample buffers make a byte-diff no smaller than the full sketch. `kll-full` suffices for the encoding-axis comparison. See `docs/sdk-cost-evaluation.md`. |

### Collector-side sketch processors — `opentelemetry-collector-contrib-patch/processor/`

All five sketches use the SDK pre-aggregation path — SDK emits
a typed metric data point (`DDSketch` / `KLLSketch` /
`CountSketch` / `CountMinSketch` / `HLLSketch`), collector
deserializes the sketch bytes, merges into its own per-series
or per-window sketch, emits the aggregated result.

| Processor | Algorithm | Query | Batch | Window |
|---|---|---|---|---|
| `ddsketchprocessor` | DDSketch | Quantiles | ✅ | ✅ |
| `kllprocessor` | KLL | Quantiles | ✅ | ✅ |
| `countsketchprocessor` | CountSketch | Frequency / heavy hitters | ✅ | ✅ |
| `countminsketchprocessor` | Count-Min Sketch | Frequency | ✅ | ✅ |
| `hllprocessor` | HyperLogLog | Cardinality | ✅ | ✅ |
| `countsketchmergeprocessor`, `countminsketchmergeprocessor` | — | Backend-tier merge of per-series sketches | ✅ | ✅ |

Serialization is canonical via `sketchlib-go` (KLL / CS / HLL
use `SerializeToBytes`; CMS uses a per-snapshot gob of
`{Rows, Cols, Count, Sum, Sum2, L1, L2}`).

### Controller — `controller/`

- `/metrics` Prometheus exposer + gRPC `runtime-samples` receiver.
- 5-layer planning pipeline (query → language AST → sketch
  algebra → optimizer → physical plan). See
  [`controller/docs/query-to-sketch-translation.md`](controller/docs/query-to-sketch-translation.md).
- OpAMP config push to agents.

### Multi-agent deploy — `deploy/`

- `deploy/docker-compose/`: `base.yml` (8 services — controller,
  backend, gateway, fake-exporter, minio, minio-setup,
  prometheus, grafana) + `agents-N{1,10,100}.yml` overlays +
  `gen-agents.sh` for arbitrary `N`.
- 7 baseline overlays: `b0a-raw-stream`, `b0b-raw-batched`,
  `b1-serf`, `b2-full`, `b3-delta`, `b4-tunable`, `b5-gorilla`.
- 5 Dockerfiles under `deploy/docker/`: `sketchcol`,
  `sketchcol-stock`, `backend`, `controller`, `fake-exporter`.
- `deploy/helm/asap/`: `Chart.yaml` + `values.yaml` with the
  paper's resource envelope (0.5 CPU / 512 Mi per agent).
  Templates not yet written.

### Evaluation tooling — `deploy/scripts/`

- `run-sdk-cost-grid.sh` — engine: iterates the
  `WINDOWS × PROJECTIONS × AGGS` grid supplied via env.
- `run-sdk-cost-eval.sh` — runs the three canonical
  sub-sweeps (time axis, label axis, encoding axis) via the
  grid engine.
- `run-baseline-sweep.sh` — older per-baseline sweep driver
  (pre-cost-eval, still valid for the baseline matrix).
- `measure-baseline.py` — per-cell scrape of Prometheus + docker
  stats; producer-side columns (CPU / RSS / tx bytes) included.

### Evaluation tooling — `otel_collector_benchmark/` (eval-suite expansion, 2026-04-30)

In-process / single-host benches that don't need the deploy stack;
useful for fast iteration on sketch-internal claims and for paper
plots that only require a producer + collector pair. Landed via
[#197](https://github.com/ProjectASAP/DataCollector/pull/197),
[#198](https://github.com/ProjectASAP/DataCollector/pull/198),
[#199](https://github.com/ProjectASAP/DataCollector/pull/199).

- `matched_accuracy/` (new Go module) — DDSketch / KLL / T-Digest /
  HDR / linhist / raw at the **same** p99 error target. Full sweep
  across Zipf `s ∈ {1.01, 1.5, 2.5}` × 1M samples checked in.
  Headline: DDSketch ~0.5–1% p99 rel-err at **0.9–2 KB** vs HDR
  exact at 123 KB and raw at 8 MB.
- `cardinality_crossover/` (new Go module) — CountSketch and
  CountMinSketch sketch-bytes vs raw-bytes across `N ∈ {100, 1k,
  10k, 100k, 1M, 5M}`, default and narrowed dim configs. Full sweep
  CSVs checked in.
- `bench_delta_sweep.sh` + `delta_sweep_config_template.yaml` —
  window `{1s, 5s, 30s, 5m}` × threshold `{0, 0.1, 1.0}` matrix on
  the existing delta-transmission processors.
- `bench_2node_sim.sh` — single-host simulation of a 2-node
  deployment (port-shifted configs, per-node CPU/RSS, balance
  metric). Lifts to real two-node by swapping the binary launcher
  for ssh-spawn.
- `bench_soak.sh` — long-running steady-state with minute-resolution
  CPU/RSS/heap/fd-count + slope-based leak verdict.
- `telegraf_benchmarks/run_gorilla_local.sh` — wraps existing
  send_firehose.py + summarize_telegraf_metrics.py + 1 Hz ps
  sampler around `max-throughput-gorilla-local.conf`.
- `datasets_eval/debs/benchmark/` — `crosskey` subcommand on
  `run.py` plus `groupings.py` / `compare_crosskey.py` for the
  cross-key merging accuracy plot ([#199](https://github.com/ProjectASAP/DataCollector/pull/199)).

### Evaluation artefacts

- N=1 and N=10 baseline sweep CSVs in `deploy/eval-results/`
  (pre-cost-eval).
- First-pass SDK cost evaluation in
  `deploy/eval-results/sdk-cost/`:
  - `time-axis-20260423.csv` — varies `W`, fixes `L=keep-all, agg=dd-full`.
  - `label-axis-20260423.csv` — varies `L`, fixes `W=60s, agg=dd-full`.
  - `encoding-axis-20260423.csv` — varies `agg`, fixes `W=60s, L=zone,rack`.
  - `FINDINGS-20260423.md` — interpretation + methodology caveat.

## Outstanding — paper blockers

1. **V2 cost sweep with `BYTES_WIN ≥ 2×W`.** First pass used
   `BYTES_WIN=20s < W=60s` on most cells, so absolute bandwidth
   numbers are under-reported. Ratios within a sub-experiment
   are fine; absolute numbers need a rerun (~90 min wall).
2. ~~**Profile the label-axis `AttributeFilter` hot path.** First
   pass showed producer CPU climbing ~4× under label
   projection. Root-cause before that number goes into a
   figure.~~ **Done 2026-05-05.** Root cause: every measurement
   re-allocated the post-filter `attribute.Set` (fresh
   `ToSlice` + `newSet → hashKVs + computeDataFixed`),
   triggering GC pressure ≈ 20 % of CPU. Fix: memoize
   `(input Distinct → filtered Set)` inside `Builder.filter`.
   Bench-level: 5–20 × per-call speedup, 0 allocs/op vs 2
   allocs/op on the cost-eval's `keep-zone-rack` cell. See
   `docs/eval-label-axis-cpu-rootcause.md` + the two
   pprof profiles checked in under
   `deploy/eval-results/sdk-cost/profiles/`. Label-axis CSV
   needs a v2 rerun against the fixed image before going into
   any figure.
3. **~~Fill the `nan` columns in the multi-agent sweep CSV.~~**
   **Done (2026-05-05).** Three fixes landed in
   `deploy/scripts/measure-baseline.py` (PR
   `eval/fill-nan-columns-in-sweep-csv`):
   - **Agent bytes_in/out on raw / Gorilla / Serf baselines.**
     `docker stats` net rx/tx of all `docker-compose-agent-*`
     containers feeds `agent_in_kib_per_s` / `_out_kib_per_s`
     when the patched-processor counter is absent. Captures
     on-the-wire bytes (what claim #1 actually wants); see
     `docs/eval-instrumentation-notes.md` for the
     in-process-vs-wire-bytes caveat.
   - **Gateway / backend zero-vs-NaN.** Added `or vector(0)`
     to gateway PromQL + the backend-samples fallback so B1/B5
     (drop_original=true → no traffic to gateway) report `0`
     rather than `NaN`. Difference between "Prom is gone"
     (still NaN) and "this baseline structurally bypasses the
     gateway" (now 0) preserved.
   - **`backend_query_p99_ms` from client-side replay JSONL.**
     New `--replay-jsonl PATH` flag computes p99 of successful
     `duration_ms` from the replay client's output —
     survives `docker compose down -v`. Wired through
     `run_e2e_sweep.sh` (always-on) and
     `run-baseline-sweep.sh` (`DRIVE_QUERIES=1` opt-in).
   Verified with `deploy/eval-results/sweep-smoke-postfix-20260505.csv`:
   one cell per baseline family, no NaN in any of the 17 columns.
   Grafana dashboards still outstanding (separate work).
4. **Query side of the sweep.** Co-located PromQL replay
   issuing avg / p99 / rate / topK queries over {1m, 5m, 1h}
   windows at steady rate. Capture `query_p50/p99_ms`,
   `cold_bytes_served`, `barrier_drops`.
5. **N-scale sweep rerun** at a representative `(W, L, agg)`
   across `N ∈ {1, 10, 100}`. The 2 k pts/s floor from the
   earlier N=10 sweep is expected under SDK pre-aggregation;
   the question is whether gateway / backend hold up as
   aggregate ingress scales.
6. **Real workload — Google cluster trace.** Fetcher for
   2011 + 2019 traces (`datasets_eval/` has scaffolding, no
   fetcher yet); mapping from trace rows to OTLP series;
   matching PromQL query log.
7. **Controller feedback loop e2e on real workload.** HTTP
   round-trip is done in ASAPQuery-backend
   (`capability_miss_http_e2e_tests.rs`). Cross-process story
   with real ingest still needed: seed plan, inject capability
   miss, measure time-to-plan-ready / time-to-first-hit /
   bw + CPU during transition, assert bounded regression.
8. **Fault injection.** Controller kill, agent kill, network
   partition. Tests under `fault-injection/`:
   - `controller-kill.sh` — `docker kill`; assert queries keep
     serving from the last-known plan.
   - `agent-kill.sh` — `docker kill` one agent; assert the
     controller marks it degraded and replans.
   - `network-partition.sh` — `docker network disconnect`
     agent ⇄ controller; assert the agent runs its last config
     and reconciliation happens at heal.

   ChaosMesh variants on K8s go under the Helm chart.
9. **Reproducibility archive.** `reproduce/` with
   `make reproduce`, `Dockerfile.reproduce`, trace fetcher /
   anonymiser, expected-numbers table with tolerance bands.

## Outstanding — SDK runtime (not a cost-eval blocker)

- ~~**Hot-reload of View `AttributeFilter`.**~~ **Done (P2,
  2026-04-30).** Implemented as an in-process swappable filter in
  `deploy/fake-exporter/swappable_filter.go` rather than a SDK
  patch. The SDK's `Stream.AttributeFilter` is a function value
  that the SDK invokes per measurement; an `atomic.Pointer`-backed
  closure satisfies the same interface and lets the controller
  swap the projection at runtime via `POST /control/projection`.
  Caveat: post-swap, attribute sets that previously hashed to one
  bucket may now hash differently — old buckets keep their data,
  new measurements land in new buckets. The plan-transition
  driver (P6) records the swap timestamp so the accuracy reducer
  (P8) can split before/after.

## Future work (post-paper)

- **ASAPController split.** Consolidate
  `DataCollector/controller/` +
  `ASAPQuery-backend/asap-planner-rs/` + `asap-fusion/` into
  one repo. Deferred — controller keeps evolving here for v1.
- **OpAMP stress test at `N = 100+` agents.** Currently tested
  with ~dozen; proper stress test needed for the scalability
  story.
- **Sketch-processor CPU offload.** Edge sketchcol processors
  update sketches on the data-plane thread. Worker-pool +
  lock-free ring buffer for higher ingest rates on
  resource-constrained edge nodes.
- **Controller HA.** Single controller today. Active-passive
  pair with simple leader election (etcd or similar) so a
  controller kill isn't a "manual restart" event.
- **Helm chart templates.** `deploy/helm/asap/` has
  `values.yaml` + `Chart.yaml` but no templates. Needs an
  initial pass on a real cluster to validate readiness probes /
  resource requests / network policies. Required for the
  reproducibility archive if we promise K8s replay. Landing
  order (one at a time, so each is reviewable):
  `_helpers.tpl` → `controller.yaml` → `backend.yaml` (adds
  PVC for the sketch-DB disk) → `gateway.yaml` → `agents.yaml`
  (replicas = `{{ .Values.agents.count }}` + headless Service
  for Prometheus DNS SD) → `minio.yaml` (StatefulSet + PVC,
  gated on `.Values.minio.enabled`) → `prometheus.yaml` +
  `grafana.yaml`.
- **Compose polish.**
  - Per-agent `AGENT_ID` label. The static enumeration in
    `agents-N*.yml` works for N ≤ 100 but bloats the
    Prometheus target list. Once the agent emits its own
    hostname as a label, the scrape config collapses to a
    single DNS-SD rule.
  - CI check that `base.yml + agents-N<K>.yml + baseline-*.yml`
    merge to a valid combined compose config.
  - `deploy/k8s/` plain manifests as a non-Helm alternative
    for operators who don't want Helm. Lowest priority.

## Architecture

```
Load generator — deploy/fake-exporter/ (OTel-SDK-instrumented app)
        │
        │ OTLP gRPC
        ▼
Agent OTel collector (sketchcol)
  ├── receiver/otlpreceiver
  ├── processor/{dd,kll,cs,cms,hll}sketchprocessor
  ├── processor/batchprocessor
  └── exporter/otlp
        │
        ▼
Gateway OTel collector (contrib 0.108)
        │
        ▼
ASAPQuery backend (sketchDB + PromQL surface)
        ▲
        │ cold fallback
        │
   MinIO (local) / S3 (prod) — raw JSONL parts
                                under raw/<metric>/YYYY/MM/DD/HH/


Controller — DataCollector/controller/
  observes: query workload, SLAs, agent metrics
  decides:  per-metric (W, L, agg_type) triple
  pushes:   OpAMP → agent sketchcol config
            HTTP  → backend streaming config
```
