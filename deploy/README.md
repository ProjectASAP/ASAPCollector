# ASAP deployment

One canonical MVP demo plus shared deployment infrastructure:

```text
deploy/
├── docker/             image definitions
├── helm/               Kubernetes packaging
└── mvp-multinode/      issue-#46 demo
    ├── harness/        immutable run inputs (acceptance, queries, topology)
    ├── configs/        container and controller runtime configuration
    ├── scripts/        orchestration, measurement, and evaluation
    └── cost_model/     analytical cost experiments
```

| Path | What | Read |
|---|---|---|
| [`mvp-multinode/`](mvp-multinode/README.md) | Canonical issue-#46 paired harness on a real 10 Gbps LAN | [mvp-multinode/README.md](mvp-multinode/README.md) |
| `helm/` | Helm charts (K8s / scale path) | — |
| `docker/` | Dockerfiles built once and consumed by both demos (`Dockerfile.asap-otel`, `Dockerfile.otel-app`). The data-plane / control-plane images build from ASAPQuery-backend's `data_plane/Dockerfile` + `control_plane/Dockerfile` (data_plane reorg, 2026-05 — the old combined `Dockerfile.backend` is retired). | — |
| `otel-app/` | Go source for the synthetic producer image | — |

## Which one should I run?

| Question | Use |
|---|---|
| Does the acceptance evaluator fail closed on missing/stale/invalid evidence? | `python3 deploy/mvp-multinode/scripts/tests/test_mvp_evaluate.py` |
| Are correctness, accuracy, freshness, latency, and cost claims supported on a real LAN? | `deploy/mvp-multinode/scripts/run_demo.sh all` |

## Layout invariants

- `mvp-multinode/scripts/run_demo.sh` rsyncs its utilities and per-arm
  configs to node0–node3 under `/mydata/mvp-multinode/{scripts,configs}/`.
