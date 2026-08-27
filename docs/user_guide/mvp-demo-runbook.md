# ASAP MVP demo runbook

The canonical issue-#46 demo is the four-node harness in
[`deploy/mvp-multinode`](../../deploy/mvp-multinode/README.md). It compares raw
OTLP/gzip ingestion into VictoriaMetrics (`b1`) with sketched OTLP/gzip
ingestion into ASAP (`asap-gzip`) using the same deterministic workload.

## Run

Prerequisites are four hosts (`node0`–`node3`) with `/mydata`, Docker, and
passwordless SSH from node0. Adjust hostnames and addresses in
`deploy/mvp-multinode/harness/topology/4node.env` when necessary.

```bash
cd /mydata/ASAPCollector
bash deploy/mvp-multinode/scripts/run_demo.sh all
```

The driver builds and distributes images, starts each paired arm, replays the
checked-in query suite, captures freshness and control-plane evidence, measures
process, NIC, and persistent-storage cost, and then runs the fail-closed
evaluator.

## Verdict

Each run directory contains `MVP_RESULTS.json` and `MVP_REPORT.md`. The command
returns nonzero unless all of these predeclared gates pass:

- every declared query succeeds with an applied controller plan;
- approximate results meet their per-query accuracy contract;
- warm and archive freshness meet their sample, p95, and maximum limits;
- ASAP query p50 and p95 latency beat the exact baseline;
- Collector-only and end-to-end normalized costs beat the raw baseline.

Acceptance thresholds live in
`deploy/mvp-multinode/harness/acceptance.json`. Query definitions live in
`deploy/mvp-multinode/harness/queries/e2e.json`. Change either before a run, never after
observing its results.

## Freshness probe protocol

The synthetic producer emits counters whose values encode emission time in
Unix epoch milliseconds. `measure_freshness.sh` polls the relevant query tier
and computes `poll_time - encoded_emission_time`. Missing tiers, negative
deltas, insufficient samples, stale artifacts, and malformed evidence fail the
evaluation.

## Verify locally

```bash
python3 -m unittest discover \
  -s deploy/mvp-multinode/scripts/tests -p 'test_*.py'
bash -n deploy/mvp-multinode/scripts/run_demo.sh
```

See the deployment README for topology, images, artifact layout, and advanced
environment knobs.
