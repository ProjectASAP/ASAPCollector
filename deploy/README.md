# ASAP deployment

One canonical MVP demo plus shared deployment infrastructure:

```text
deploy/
├── docker/             image definitions
└── mvp-multinode/      issue-#46 demo
    ├── harness/        immutable run inputs (acceptance, queries, topology)
    ├── configs/        container and controller runtime configuration
    └── scripts/        orchestration, measurement, and evaluation
```

| Path | What | Read |
|---|---|---|
| [`mvp-multinode/`](mvp-multinode/README.md) | Canonical issue-#46 paired harness on a real 10 Gbps LAN | [mvp-multinode/README.md](mvp-multinode/README.md) |
| `docker/` | Collector and load-generator image definitions; ASAPQuery images build from its sibling repository | — |

## Which one should I run?

| Question | Use |
|---|---|
| Does the acceptance evaluator fail closed on missing/stale/invalid evidence? | `python3 deploy/mvp-multinode/scripts/tests/test_mvp_evaluate.py` |
| Are correctness, accuracy, freshness, latency, and cost claims supported on a real LAN? | `deploy/mvp-multinode/scripts/run_demo.sh all` |

## Layout invariants

- `mvp-multinode/scripts/run_demo.sh` rsyncs its utilities and per-arm
  configs to node0–node3 under `/mydata/mvp-multinode/{scripts,configs}/`.
