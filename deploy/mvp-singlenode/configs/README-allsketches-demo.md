# All-sketches single-agent demo

Paper §Architecture figure as a runnable deploy: one `asap-otel`
agent emits **all five** sketch families simultaneously into the
backend, demonstrating "one agent → five concurrent sketch
pipelines → backend serves five concurrent PromQL query families."

This is paper-figure / architecture-demo material, **NOT** §5
evaluation data. The per-sketch sweep (`deploy/mvp-singlenode/scripts/run_e2e_sweep.sh`,
driven by `baseline-b3-delta.yml`, `e2e-overlay-{cs,cms,hll,kll}.yml`)
isolates one sketch per cell so per-family CPU / bytes-per-second /
accuracy numbers are attributable. This config is what §3 of the
paper points at when explaining the architecture.

See `docs/paper-outline.md` §Architecture for the design rationale.

## Files

- `asap-otel-agent-allsketches.yaml` — agent config with all five
  sketch processors (`ddsketch`, `KLL`, `HLL`, `countsketch`,
  `countmin`) in one metrics pipeline, OTLP-forwarded to the
  backend.
- `../docker-compose/baseline-allsketches.yml` — overlay that
  selects the all-sketches agent config and pins workload knobs to
  match `baseline-b3-delta.yml`.

## Run

```bash
AGENT_CONFIG=asap-otel-agent-allsketches.yaml \
  docker compose \
    -f deploy/mvp-singlenode/docker-compose/base.yml \
    -f deploy/mvp-singlenode/docker-compose/agents-N1.yml \
    -f deploy/mvp-singlenode/docker-compose/baseline-allsketches.yml \
    -f deploy/mvp-singlenode/docker-compose/e2e-overlay.yml \
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

**HLL note.** Refactor-2026-05: the HLL processor's
`cardinalityMetricName(base) → base` preserves the input metric name
end-to-end; sketch encoding is identified by the OTLP HLLSketch
variant tag rather than by a name suffix. The retired
`metric_suffix` config field no longer alters wire metric names.
`count(http_requests_total)` against an HLL-backed agg resolves
directly against the bare input name in the unified
`backend-inference.yaml`.

(History: pre-refactor, the HLL processor unconditionally appended
`_hll` / `_hll_cardinality` and the all-sketches agent set
`metric_suffix: "_hll"`. That suffix-based identification scheme
was replaced by the OTLP variant-tag scheme; the agent's
`metric_suffix` config is now ignored.)

Adding an HLL block to the unified `backend-inference.yaml` (mirroring the
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
