# Google Cluster Trace — real-workload evidence for ASAP

Implements the public Google cluster traces (2011 and 2019) as a real
workload feeding the five evaluation claims in `docs/paper-outline.md`.
Without this dataset, every claim rests on synthetic data; this
directory is the workload-credibility hook.

This is the structural sibling of `datasets_eval/debs/` for ASAP's
non-financial workload axis. The DEBS dataset gives us a high-rate
last-trade stream over ~5 k symbols; the Google cluster trace gives
us a per-instance resource-usage stream over millions of (machine,
job, task) tuples — exactly the cardinality regime where SDK-side
sketch aggregation is supposed to win.

## Layout

```
datasets_eval/google_cluster/
├── README.md            ← you are here
├── fetcher.py           ← downloads + caches deterministic subsample
├── otlp_mapper.py       ← trace rows -> OTLP-shaped JSONL
├── queries.json         ← PromQL log, one entry per evaluation claim
├── run.py               ← orchestrator (fetch/map/replay/validate)
└── tests/
    ├── fixtures/                 ← golden 5–10 row inputs
    ├── test_otlp_mapper.py       ← unit-level mapper goldens
    └── test_queries_schema.py    ← queries.json well-formed + matches deploy schema
```

## Subset selection

Both traces are too large to fetch in full on every CI run (2011 is
~40 GB compressed; 2019 is ~2 TB across 8 cells). The fetcher pulls
the **first part-file of the resource-usage table** for each year:

| Year | Table | Source object | Rationale |
|------|-------|---------------|-----------|
| 2011 | `task_usage` | `gs://clusterdata-2011-2/task_usage/part-00000-of-00500.csv.gz` | Per-task 5-min resource samples; canonical for per-(machine, job, task) timeseries. |
| 2019 | `instance_usage` (cell a) | `gs://clusterdata_2019_a/instance_usage-000000000000.csv.gz` | Per-instance Borg resource samples; same shape concern, different scale. |

The fetcher takes `--max-rows N` (default 100 000) and writes a
deterministic, sorted, sha256-checksummed subsample under
`<out-dir>/<year>/<table>.csv`. Subsequent runs are idempotent.

## Cardinality scaling

`otlp_mapper.py --cardinality-cap N` projects the natural
`(machine_id, service, task)` identity tuples onto a hashed N-element
subset using a salted blake2b-8 hash. This:

- matches the synthetic harness's sweep matrix (`N ∈ {1k, 10k, 100k}`),
- is byte-deterministic (same input file + same N -> same output bytes),
- preserves temporal structure (we don't shuffle in time, only in identity).

### Projection bias

For trace cardinality U and cap N:

| Regime | Per-cell collisions | Bias on per-cell sample count | Bias on `count_unique` |
|--------|---------------------|-------------------------------|------------------------|
| U ≤ N  | 0                   | 0                             | 0                      |
| U > N  | ~U/N (Poisson)      | ~√(N/U) RSD                   | saturates at N (recover via U/N rescale) |

For the default N = 1000 and U ≈ 1e7 (2011 task_usage full corpus
cardinality), per-cell RSD is ~1 %. This is acceptable for the
quantile / topk / sum claims; the count_unique claim documents the
saturation in `queries.json` and is rescaled in the accuracy reducer.

## OTLP wire shape (vs `otel-app/`)

The mapper produces JSONL where each row is shaped to align with
otel-app's existing 4-dim attribute schema:

| otel-app attr key | google-cluster attr key | derivation |
|------------------------|-------------------------|------------|
| `zone` (4 vals)        | `zone`                  | `f"z{hash(machine_id) % 4}"` |
| `rack` (10 vals)       | `rack`                  | `f"r{hash(machine_id) % 10:02d}"` |
| `node`                 | `host`                  | `f"host-{machine_id}"` |
| `pod`                  | `service` + `task`      | `f"svc-{job_or_collection_id}"` + `task_index` |

The schema labels (`zone`, `rack`) are synthesized deterministically
from `machine_id` because the trace doesn't expose physical zone /
rack columns directly. This keeps the test harness's group-by and
heavy-hitter queries meaningful without requiring the secondary
`machine_events` join.

The 5 keys `{zone, rack, host, service, task}` are checked by
`run.py validate --jsonl …` to catch schema drift.

## queries.json

Ten queries grouped by ASAP evaluation claim:

| Claim | # queries | kind | What it tests |
|-------|-----------|------|---------------|
| #1 quantile accuracy | 2 | `quantile` | p50/p99 of per-instance CPU/memory rates |
| #2 cross-key roll-up | 2 | `quantile`, `sum` | per-zone p99 / total memory |
| #3 heavy-hitters | 2 | `topk` | top-10 hosts by CPU, top-10 services by sample count |
| #4 counter exactness | 1 | `sum` | cluster-wide CPU rate |
| #5 distinct cardinality | 3 | `count_unique` | distinct services / hosts / instances |

Each entry has `kind`, `promql` (matching the schema of
`deploy/mvp-multinode/harness/queries/e2e.json`), `expected_ground_truth_query`
(for the accuracy reducer), and a `rationale` string explaining why
this query is "natural" for the Google trace.

## One-command smoke

Default subsample, cardinality-cap=1000, dry-run replay:

```bash
python3 datasets_eval/google_cluster/run.py fetch  --year 2019 --out-dir /tmp/gct --max-rows 1000
python3 datasets_eval/google_cluster/run.py map    --year 2019 --in-dir /tmp/gct --out /tmp/gct-otlp.jsonl --cardinality-cap 1000
python3 datasets_eval/google_cluster/run.py replay --jsonl /tmp/gct-otlp.jsonl --dry-run
python3 datasets_eval/google_cluster/run.py validate --queries datasets_eval/google_cluster/queries.json --jsonl /tmp/gct-otlp.jsonl
```

To send for real against a running ASAP agent's OTLP/gRPC port:

```bash
pip install opentelemetry-proto grpcio
python3 datasets_eval/google_cluster/run.py replay \
    --jsonl /tmp/gct-otlp.jsonl \
    --endpoint localhost:4317 \
    --pace-factor 10.0
```

## Disk + wall-time budget

| Run                                | Disk        | Wall time |
|------------------------------------|-------------|-----------|
| fetch 2011 / 1k rows               | <1 MB       | ~5 s      |
| fetch 2019 / 1k rows               | <1 MB       | ~5 s      |
| fetch 2011 / 100k rows (default)   | ~25 MB      | ~30 s     |
| fetch 2019 / 100k rows (default)   | ~30 MB      | ~30 s     |
| fetch 2011 full part-00000-of-00500 | ~80 MB     | ~60 s     |
| fetch 2011 ALL parts (full table)  | ~40 GB      | ~6 h      |
| fetch 2019 cell-a instance_usage   | ~250 GB     | ~24 h     |
| fetch 2019 ALL 8 cells             | ~2 TB       | ~1 wk (NOT recommended) |

The default 100 000 rows is what paper experiments use unless
otherwise stated; it generates enough unique (machine, task) tuples
to exercise the 1k cardinality cap with U ≈ 50–100k.

## Out of scope (by design)

- **No new ingest endpoint** — the replay path uses the existing
  OTLP/gRPC receiver on the agent (`localhost:4317`). Modifying
  `otel-app/` is E2's domain.
- **No sweep-runner integration yet** — Phase B for this dataset is
  fetcher + mapper + queries + smoke; harness integration is a
  follow-up PR.
- **Ground-truth JSONL is not produced here** — otel-app's
  `EXPORTER_RAW_TEE_ROOT` is the canonical ground-truth source and
  should be used by the accuracy reducer when comparing replayed
  Google-trace runs against PromQL ground truth.

## See also

- `PROGRESS.md` "Outstanding — paper blockers" #6 (the goal)
- `docs/paper-outline.md` "Measurable benefits — five evaluation dimensions"
- `docs/sdk-cost-evaluation.md` (the OTLP wire-shape reference)
- `datasets_eval/debs/` (sister dataset: financial / DEBS 2022)
- ClusterData2011_2 schema: <https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md>
- ClusterData2019 schema:   <https://github.com/google/cluster-data/blob/master/ClusterData2019.md>
