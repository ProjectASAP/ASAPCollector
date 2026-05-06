# All-sketches single-agent demo

Paper §Architecture figure as a runnable deploy: one `sketchcol`
agent emits **all five** sketch families simultaneously into the
backend, demonstrating "one agent → five concurrent sketch
pipelines → backend serves five concurrent PromQL query families."

This is paper-figure / architecture-demo material, **NOT** §5
evaluation data. The per-sketch sweep (`deploy/scripts/run_e2e_sweep.sh`,
driven by `baseline-b3-delta.yml`, `e2e-overlay-{cs,cms,hll,kll}.yml`)
isolates one sketch per cell so per-family CPU / bytes-per-second /
accuracy numbers are attributable. This config is what §3 of the
paper points at when explaining the architecture.

See `docs/paper-outline.md` §Architecture for the design rationale.

## Files

- `sketchcol-agent-allsketches.yaml` — agent config with all five
  sketch processors (`ddsketch`, `KLL`, `HLL`, `countsketch`,
  `countmin`) in one metrics pipeline, OTLP-forwarded to the
  backend.
- `../docker-compose/baseline-allsketches.yml` — overlay that
  selects the all-sketches agent config and pins workload knobs to
  match `baseline-b3-delta.yml`.

## Run

```bash
AGENT_CONFIG=sketchcol-agent-allsketches.yaml \
  docker compose \
    -f deploy/docker-compose/base.yml \
    -f deploy/docker-compose/agents-N1.yml \
    -f deploy/docker-compose/baseline-allsketches.yml \
    -f deploy/docker-compose/e2e-overlay.yml \
    up -d
```

## Expected query coverage

The base e2e overlay mounts `backend-inference.yaml` (the
unified all-sketch warm-tier dispatch table). Four of five sketch
families' queries route directly through that table:

| Sketch | Example PromQL | Routes to | Inference YAML |
|---|---|---|---|
| DDSketch | `quantile_over_time(0.99, http_requests_total_latency_ms_quantile[1m])` | `DDSketchAccumulator` | unified `backend-inference.yaml` |
| KLL | `quantile_over_time(0.99, http_requests_total_latency_ms_quantile[1m])` (same metric name as DDSketch — typed-variant dispatch) | `DatasketchesKLLAccumulator` | unified `backend-inference.yaml` |
| CountSketch | `topk(10, http_requests_total)` | `CountSketchAccumulator` | unified `backend-inference.yaml` |
| CountMin | `sum_over_time(http_requests_total[1m])` | `CountMinSketchAccumulator` | unified `backend-inference.yaml` |
| HLL | `count(http_requests_total_hll)` | `HLLAccumulator` (Cardinality / Count alias) | **needs** `backend-inference-hll.yaml` |

**HLL caveat.** The unified `backend-inference.yaml` lists
`count(http_requests_total)` (raw name) under HLL coverage, but the
HLL processor's encode path (`hllprocessor/encode.go::cardinalityMetricName`)
always appends a non-empty suffix — `<input>_hll` when
`metric_suffix: "_hll"` is set, or `<input>_hll_cardinality` (default)
when unset; there is no way to emit on the bare raw name. The
all-sketches agent uses `metric_suffix: "_hll"` to match the
per-sketch HLL convention.

Because the unified inference table doesn't list
`http_requests_total_hll` as a sketched metric, HLL queries against
the all-sketches agent currently fall through to the cold tier (or
through `--forward-unsupported-queries` to Prometheus). Adding an
HLL block to the unified `backend-inference.yaml` (mirroring the
`http_requests_total_hll`-keyed entries from
`backend-inference-hll.yaml`) is the canonical fix and is tracked as
a separate cleanup — out of scope for this paper-figure config,
since the §3 architecture story is "five concurrent sketch
pipelines emit to backend", not "every query family is warm-tier
ready out-of-box".

The PROGRESS.md "All-five-sketch runtime e2e verification
(2026-04-30)" table records the verified numeric results for each
of these queries against an isolated single-sketch agent; the
all-sketches demo reproduces the same results, concurrently, from
one agent.
