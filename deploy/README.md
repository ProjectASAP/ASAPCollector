# ASAP deployment

Two demos, two packages, plus shared infrastructure pieces:

| Path | What | Read |
|---|---|---|
| [`mvp-singlenode/`](mvp-singlenode/README.md) | Single-host docker-compose harness — N=1 / N=10 / N=100 scale dials, baseline sweeps, sweep driver, MVP cell smoke tests | [mvp-singlenode/README.md](mvp-singlenode/README.md) |
| [`mvp-multinode/`](mvp-multinode/README.md) | 4-node multinode orchestrator — bandwidth claim under a real 10 Gbps LAN, not loopback-flattered | [mvp-multinode/README.md](mvp-multinode/README.md) |
| `helm/` | Helm charts (K8s / scale path) | — |
| `docker/` | Dockerfiles built once and consumed by both demos (`Dockerfile.asap-otel`, `Dockerfile.fake-exporter`). The data-plane / control-plane images build from ASAPQuery-backend's `data_plane/Dockerfile` + `control_plane/Dockerfile` (data_plane reorg, 2026-05 — the old combined `Dockerfile.backend` is retired). | — |
| `fake-exporter/` | Go source for the synthetic producer image | — |

## Which one should I run?

| Question | Use |
|---|---|
| Does the new analyzer route compound PromQL correctly? Does `MVP_REPORT.md` render? Fast PR-time smoke. | `mvp-singlenode/scripts/run_mvp_demo.sh` |
| Are the bandwidth-reduction claims real on a 10 Gbps LAN, not loopback-flattered? | `mvp-multinode/scripts/run_demo.sh` |

## Layout invariants

- `mvp-singlenode/docker-compose/*.yml` reference configs via `../configs/...` — both dirs live under `mvp-singlenode/`, so the relative paths resolve.
- `mvp-multinode/scripts/run_demo.sh` Phase 0 rsyncs `mvp-singlenode/scripts/` (per-node Python utilities) and `mvp-multinode/configs/` (per-arm YAML bundles) to each of node0–node3 under `/mydata/mvp-multinode/{scripts,configs}/`. The Python utilities are shared across the two demos; the configs are not.
