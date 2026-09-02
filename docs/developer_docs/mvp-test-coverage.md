# MVP test coverage

The canonical MVP claim is accepted only by the four-node run in
[`deploy/mvp-multinode`](../../deploy/mvp-multinode/README.md). Unit and
in-process integration tests are prerequisites; they are not substitutes for
the measured run.

| MVP claim | Required evidence | Pre-merge coverage |
|---|---|---|
| Controller plan is active | Both agents report `Applied`; captured configs agree and contain planned full, delta, and pass-through paths | Controller and evaluator tests |
| Queries use summaries | Every scored ASAP response has a plan id and a warm-tier `data_source`; archive fallback is rejected | Backend query E2E and evaluator tests |
| Accuracy meets the contract | Paired arms use deterministic seeds and explicit evaluation anchors; results match by query, logical sequence, labels, and anchor-relative timestamp | Evaluator tests; four-node run supplies results |
| Freshness meets the contract | Producer value encodes emission epoch milliseconds; the harness queries that value and computes poll time minus emission time | Producer probe tests; four-node run supplies latency samples |
| Latency improves | Warm-up observations are excluded and every query has enough steady-state repetitions | Evaluator tests; four-node run supplies timings |
| Cost improves | All declared processes, storage components, process samples, and host-NIC samples are present | Evaluator tests; four-node run supplies measurements |

Backend unit tests prove individual planner, accumulator, and reducer
contracts. In-process backend E2E tests prove planning, modified-OTLP ingest,
state reconstruction, and query serving without starting the Go Collector.
Cross-language wire tests prove serialization compatibility. None of those
tests alone proves the full MVP because they do not measure the deployed
Collector, network, storage, and exact baseline together.

Any production path covered only by an ignored test is unverified. A known
failure must either be fixed and the test enabled, or the corresponding query
must be removed from the predeclared MVP workload before a run. It must not be
silently served by an unscored fallback.
