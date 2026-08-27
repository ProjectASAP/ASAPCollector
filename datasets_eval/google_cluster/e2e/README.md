# google_cluster E2E validation (backend query path)

Validates the **full warm path** on a real trace:

```
mapped OTLP JSONL --replay--> fused asap_edge agent (:4317)
   --windows close + ship--> data plane
each query.metricsql --query--> data-plane asap_query (:9091)
   --compare--> exact offline ground truth (gt_eval, stdlib-only)
```

Unlike `datasets_eval/debs/benchmark/` (which scrapes the standalone
collectors' Prometheus `/metrics`), this exercises the fused processor
and the backend query engine, and compares to **exact offline GT** over
the same replayed rows — not the archive tier.

## Components

| File | Role | Status |
|---|---|---|
| `gt_eval.py` | exact offline GT from the mapped JSONL (quantile/sum/count_distinct/topk/frequency) | ✅ verified on real data |
| `compare.py` | per-family pass/fail (quantile<2%, sum lossless, HLL<2%, topk overlap≥0.8 & ρ>0.7, CMS one-sided <5%) | ✅ unit-verified |
| `query_client.py` | instant query against `:9091/api/v1/query`, captures `data_source` | ✅ (needs live stack to exercise) |
| `run_e2e.py` | orchestrator: replay → wait → query → gt → compare → report | ✅ offline path verified |
| `workload-google-cluster.yaml` | controller workload (families/grouping/item_label) | ⚠️ starting point — see CONSTRAINT |
| `controller_alloc_eval.py` + `slas.json` | Controller-allocation eval: `{sketch,size,p,ε_cdm}` vs analytical oracle | reusable offline tool |

### Fig 12 — controller-allocation eval (offline, analytical)

```
python3 controller_alloc_eval.py            # text report
python3 controller_alloc_eval.py --json     # machine-readable
python3 controller_alloc_eval.py --use-optimizer http://host:port   # real /api/v1/plan
```

Maps each query + its SLA (`slas.json`, an **explicit input** — PromQL alone
does not fix ε) + documented workload stats → a controller 4-tuple, and compares
to the cost-minimal feasible 4-tuple (analytical oracle). The `{sketch,size}`
allocator faithfully replicates the in-tree `control_plane` bind rules; the
`{p, ε_cdm}` budget-split is the documented extension being evaluated. Reuses
the real `wire.rs`/`tco.rs` cost shape. Generated results are intentionally not committed.
| `../queries.json` | +`id`/`metricsql`/`gt` specs, + CMS `frequency` query | ✅ `run.py validate` green |

## Recipe

```bash
cd datasets_eval/google_cluster
# 1. fetch + map a real subsample (egress to storage.googleapis.com required)
python3 run.py fetch --year 2019 --out-dir /tmp/gct --max-rows 100000
python3 run.py map   --year 2019 --in-dir /tmp/gct --out /tmp/gct-otlp.jsonl --cardinality-cap 1000

# 2. bring up the fused multinode stack with this workload (see CONSTRAINT)
#    deploy/mvp-multinode/scripts/run_demo.sh, CONTROLLER_WORKLOADS=workload-google-cluster.yaml
#    (data-plane FIRST, then control-plane; synthetic producer disabled so :4317 is free)

# 3. run the E2E validation
python3 e2e/run_e2e.py all \
  --jsonl /tmp/gct-otlp.jsonl \
  --otlp-endpoint <agent>:4317 \
  --backend http://<node2>:9091 \
  --warmup-secs 70
```

Pass = every family meets its threshold **and** reports a warm
`data_source` (a `thanos_archive` fall-through is a fail even if the
number matches).

## CONSTRAINT: one family per metric

The fused edge assigns ONE warm family per metric name, and the mapper
emits only two metrics. So a single workload can't run DDSketch +
CountSketch + HLL + CMS on the same metric at once. Either run a
per-family-variant workload (the `workload-google-cluster.yaml` comments
list the variant entries), or extend `otlp_mapper.py` to emit
family-specific metric aliases for a single-pass run. Sum is an
exact-agg, also one-per-metric in the edge config.

## Open items (need the live multinode stack to finalize)

- `run_demo.sh gctrace` arm: mount this workload as `CONTROLLER_WORKLOADS`,
  disable the synthetic otel-app producer so `:4317` is free for replay.
- Confirm the data plane's exact query response shape + `data_source`
  field/header (`query_client.py` parses both body and `X-ASAP-*` headers).
- Validate the `metricsql` spellings resolve warm (adjust per reducer).
- Orchestration gotchas (encoded in the plan): data-plane-before-control-plane,
  restart control-plane after any data-plane restart (dedup), kill stale
  singlenode stack on :19091/:18080, query `[30s]` matching the sealed window.

## Verified locally (offline, real data)

`fetch 4000 → map (cap 200) → gt_eval → compare` produced correct GT for
all 11 queries (p99/p50, by-zone, sums, distinct=173, topk led by
svc-000003, CMS freq=209) and `compare.py` correctly fails a perturbed
lossless-sum and passes a within-band CMS over-estimate.
