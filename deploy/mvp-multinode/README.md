# ASAP MVP paired harness

This is the canonical test-first harness for issue #46. It runs one
codec-matched comparison on four hosts:

```text
node0, node3: identical producers + collectors
node1:        VictoriaMetrics exact baseline / ASAP persistent services
node2:        ASAPQuery data plane + control plane
```

The two arms are:

- `b1`: raw OTLP/gzip metrics forwarded to VictoriaMetrics.
- `asap-gzip`: controller-selected sketches/full or delta over OTLP/gzip to
  ASAPQuery-backend.

## Run

From node0, with `/mydata/ASAPCollector` and `/mydata/ASAPQuery-backend`
available and passwordless SSH to node0–node3:

```bash
cd /mydata/ASAPCollector
bash deploy/mvp-multinode/scripts/run_demo.sh all
```

Override hosts, workload size, warm-up, or duration in
`harness/topology/4node.env` or with environment variables. The driver rebuilds
and distributes images unless `SKIP_BUILD=1` or `SKIP_LOAD=1` is set.

## Layout

```text
mvp-multinode/
├── harness/
│   ├── acceptance.json       predeclared pass/fail thresholds
│   ├── queries/e2e.json      supported MVP query mix
│   └── topology/4node.env    hosts and workload shape
├── configs/
│   ├── b1/                   exact raw-forwarding collector
│   ├── asap-gzip/            ASAP collector bootstrap
│   ├── asap/                 control/data-plane runtime inputs
│   └── shared/               backend service configuration
└── scripts/
    ├── run_demo.sh           one-command paired experiment
    ├── metricsql_replay.py   aligned query replay
    ├── measure_*.{sh,py}     freshness and resource evidence
    ├── mvp_evaluate.py       fail-closed reducer/report writer
    └── tests/                evaluator regression tests
```

Each run preserves its manifest, copied acceptance/query inputs, replay JSONL,
query responses, controller configuration and logs, freshness CSV, per-process
resource CSVs, host-NIC samples, storage measurements, and before/after
Collector self-telemetry counters. The evaluator emits
`MVP_RESULTS.json` and `MVP_REPORT.md`; any missing, stale, empty, or
incomparable required evidence makes the overall verdict `FAIL`.

Replay performs explicit per-query warm-up requests and excludes them from
scoring. Accuracy comparisons require identical label sets and returned sample
timestamps. Freshness is reported per query class and transmission mode.
Resource evidence includes CPU time, steady-state and peak RSS, network traffic,
storage, and every active ASAP/Thanos backend process. The checked-in cost
weights convert only those measured quantities into the normalized comparison.
The Collector runtime gate additionally requires both agents to sustain at
least 90% of the declared base event rate, report no refused or failed-export
metric points, process at least 25k points per CPU-second, stay below four CPU
cores per agent, and keep per-agent peak RSS below 4 GiB. These thresholds are
declared in `harness/acceptance.json`; missing telemetry fails closed.

The MVP excludes Serf comparisons, analytical/projected savings, fleet/figure
sweeps, and archive fallback validation.
