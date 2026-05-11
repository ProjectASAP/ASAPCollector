# 4-node MVP demo orchestrator

CloudLab / multi-host driver for the MVP demo (issue #46). Runs the baseline (B0 Prometheus) and ASAP arms across 4 nodes on a 10 Gbps LAN, so wire-bytes per-edge counters reflect a real NIC instead of loopback.

Single-host driver: `deploy/scripts/run_mvp_demo.sh` (runs all containers on one box; faster smoke iteration, but bandwidth claims are loopback-flattered).

## Topology

```
                   ┌─────────── 10.10.1.0/24 ─ 10 Gbps ───────────┐
                   │                                              │
      node0 ──────►│ producers + agent-a   (data source)          │
   10.10.1.1       │                                              │
                   │                                              │
      node1 ◄──────│ gateway               (ASAP arm only)        │
   10.10.1.2       │                                              │
                   │                                              │
      node2 ◄──────│ backend stack         (asap-query-backend,   │
   10.10.1.3       │                        minio, thanos-*,      │
                   │                        prometheus, embedded  │
                   │                        controller)           │
                   │                                              │
      node3 ──────►│ producers + agent-b   (data source)          │
   10.10.1.4       │                                              │
                   └──────────────────────────────────────────────┘
```

Edits to topology (IPs, hostnames, port mappings) live in `topology.env`.

## Image set

Three images, all built on node0 and `docker save | ssh load`-distributed by `run_demo.sh` Phase 0:

| Image | Built from | Contains |
|---|---|---|
| `asap/asap-otel:dev` | `ASAPCollector` root + `build_asap_otel.sh` | Patched OTel-Collector with sketch processors |
| `asap/fake-exporter:dev` | `deploy/fake-exporter/Dockerfile` | OTLP load generator |
| `asap/query-backend:dev` | `deploy/docker/Dockerfile.backend` (multi-bin) | `asap-query-backend` (port 9091 / 4317 / 4318) **and** `controller` (port 8080 / 4320 / 4321), per the Phase-9 single-binary refactor (#373). The 4-node `run_demo.sh` runs the controller in-process inside the asap-backend container — no separate controller container. |

External images (pulled by each node): `minio/minio:latest`, `minio/mc:latest`, `prom/prometheus:v2.55.0`, `quay.io/thanos/thanos:v0.41.0`.

## How to run

Prereqs:
- 4 CloudLab-style nodes with `/mydata` mounted on each, passwordless SSH from node0, docker buildkit installed.
- Nodes named `node0..node3` (or override via `NODE{0..3}_HOST` in `topology.env`).

From node0:

```bash
# 1) Build the three images on node0.
cd /mydata/ASAPCollector
./build_asap_otel.sh                                       # asap/asap-otel:dev
docker build -t asap/fake-exporter:dev deploy/fake-exporter/
DOCKER_BUILDKIT=1 docker build \
    -f deploy/docker/Dockerfile.backend \
    --build-context backend-src=/mydata/ASAPQuery-backend \
    --build-context asap-precompute-rs=/mydata/ASAPCollector/asap-precompute-rs \
    --build-context asap-sketchlib=/mydata/asap_sketchlib \
    --build-context asap-gorilla-rust=/mydata/ASAPCollector/asap-gorilla-rust \
    -t asap/query-backend:dev .

# 2) Distribute + run the demo. `run_demo.sh` Phase 0 rsyncs configs/scripts,
#    docker-save-distributes the 3 images, and pulls externals on each node.
cd /mydata/ASAPCollector/deploy/mvp-multinode
bash run_demo.sh --mode both
```

Knobs (env-overridable, see `topology.env` for defaults):
- `WARMUP_S` (default 30) — per-arm warm-up after stack-up.
- `SOAK_S` (default 90) — measurement window per arm.
- `MODE` — `baseline` / `asap` / `both`.

## Where artifacts land

- **Per-node** (transient): `/mydata/mvp-multinode/results/`, `/mydata/mvp-multinode/logs/`.
- **Aggregated on node0**: `results/edge-*.csv` (per-edge bandwidth from each node's NIC counters), `results/mvp-report.md`.

`results/` and `logs/` are `.gitignore`d — per the repo's eval-artifact convention (the single-host runbook says the same: never commit historical reports, they get stale fast and look like source of truth).

## Files

| Path | What |
|---|---|
| `topology.env` | Per-node IPs, hostnames, `--add-host` injections, image set, paths, soak knobs |
| `run_demo.sh` | Main driver — per-arm bring-up / soak / teardown across all 4 nodes |
| `autopilot.sh` | Wraps `run_demo.sh` with sweeps (e.g. cardinality grid, sketch-family grid) |
| `validate_arm.sh` | Smoke-check a single arm without running the full demo |
| `snapshot_resources.sh` | Per-container `docker stats` snapshot — used by run_demo.sh Phase 2 |
| `measure_freshness.sh` | Probe-based freshness measurement — used by run_demo.sh Phase 3 |
| `measure_nic_bw.sh` | Per-NIC `cat /sys/class/net/.../statistics` snapshot — fed into per-edge CSV |
| `configs/` | Agent / gateway / backend / controller YAML configs (49 files) — rsync'd to each node at Phase 0 |
| `scripts/` | Replay + reducer Python scripts shared with the single-host driver (rsync'd from `deploy/scripts/` on every run) |

## Differences vs the single-host driver

| | `deploy/scripts/run_mvp_demo.sh` | `deploy/mvp-multinode/run_demo.sh` |
|---|---|---|
| Topology | All containers on one host | 4 nodes on 10.10.1.x |
| Image distribution | Built once, used in place | `docker save | ssh load` to each node |
| Bandwidth measurement | Loopback (flattered) | Real NIC counters per node |
| Compose | `docker-compose` overlays under `deploy/docker-compose/` | Plain `docker run --network host` + `--add-host` (no compose, no overlay merging) |
| `MVP_REPORT.md` | Rendered by `mvp_report.py` (Phase 8) | Generated on node0 from aggregated per-node CSVs |

Pick the single-host driver for fast iteration and PR-time smoke. Use the 4-node driver when bandwidth claims need to land on a real LAN.
