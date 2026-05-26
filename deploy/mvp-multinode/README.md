# 4-node MVP demo orchestrator

CloudLab / multi-host driver for the MVP demo (issue #46). Runs the baseline (B0 Prometheus) and ASAP arms across 4 nodes on a 10 Gbps LAN, so wire-bytes per-edge counters reflect a real NIC instead of loopback.

Single-host driver: `deploy/mvp-singlenode/scripts/run_mvp_demo.sh` (runs all containers on one box; faster smoke iteration, but bandwidth claims are loopback-flattered).

## Topology

```
                   ┌─────────── 10.10.1.0/24 ─ 10 Gbps ───────────┐
                   │                                              │
      node0 ──────►│ producers + agent-a   (data source)          │
   10.10.1.1       │                                              │
                   │                                              │
      node1         │ (unused; asap-gateway retired in #400)       │
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

Five images, all built on node0 and `docker save | ssh load`-distributed by `run_demo.sh` Phase 0:

| Image | Built from | Contains |
|---|---|---|
| `asap/asap-otel:dev` | `ASAPCollector` root + `build_asap_otel.sh` | Patched OTel-Collector with sketch processors |
| `asap/asap-otel-supervised:dev` | `deploy/docker/Dockerfile.asap-otel-supervised` (`FROM asap/asap-otel:dev`) + `build_opamp_supervisor.sh` | The asap-otel collector wrapped by the OpenTelemetry opamp-supervisor (v0.141.0). The ASAP arms run this so the agent APPLIES the controller's pushed remote config (the bare collector's `opampextension` is report-only). |
| `asap/otel-app:dev` | `otel-app/Dockerfile` | OTLP load generator |
| `asap/data-plane:dev` | `ASAPQuery-backend/data_plane/Dockerfile` | `data_plane` (the data plane / query backend, entrypoint `/usr/local/bin/data_plane`, port 9091 / 4317 / 4318). Runs as the `asap-data-plane` container on node2. |
| `asap/control-plane:dev` | `ASAPQuery-backend/control_plane/Dockerfile` | `control_plane` (the control plane / controller, entrypoint `/usr/local/bin/control_plane`, port 8080 / 4320 / 4321). Runs as a separate `asap-control-plane` container on node2 (data_plane reorg, 2026-05 — retires the old combined query-backend image). |
| `asap/gorilla-merger:dev` | `ASAPQuery-backend/gorilla-merger/Dockerfile` (BuildKit secret) | Thanos-Receive-style merger: HTTP fragment ingest (`:10908`), Thanos StoreAPI for the `<2h` pending window (`:10907`), 2h-block shipper → `asap-gorilla-tsdb`. ASAP arms only; runs on node2. `thanos-query` fans out to its StoreAPI alongside the store-gateway. Imports the private `asap-gorilla-go` module, so its build needs a `gh_token` BuildKit secret — see the merger README. |

External images (pulled by each node): `minio/minio:latest`, `minio/mc:latest`, `prom/prometheus:v2.55.0`, `quay.io/thanos/thanos:v0.41.0`.

## How to run

Prereqs:
- 4 CloudLab-style nodes with `/mydata` mounted on each, passwordless SSH from node0, docker buildkit installed.
- Nodes named `node0..node3` (or override via `NODE{0..3}_HOST` in `topology.env`).

From node0:

```bash
# 1) Build the images on node0.
cd /mydata/ASAPCollector
./build_asap_otel.sh                                       # asap/asap-otel:dev
docker build -f deploy/docker/Dockerfile.asap-otel \
    -t asap/asap-otel:dev .
# Wrap the collector with the opamp-supervisor so the ASAP-arm agents APPLY
# the controller's pushed config (instead of running a static config). Builds
# the supervisor from the pinned opentelemetry-collector-contrib submodule
# (cmd/opampsupervisor/v0.141.0 — same release train as the collector).
./build_opamp_supervisor.sh                                # → deploy/docker/opampsupervisor
docker build -f deploy/docker/Dockerfile.asap-otel-supervised \
    -t asap/asap-otel-supervised:dev .                     # asap/asap-otel-supervised:dev
DOCKER_BUILDKIT=1 docker build \
    -f deploy/docker/Dockerfile.otel-app \
    -t asap/otel-app:dev .
# data_plane reorg (2026-05): the data plane and control plane now build
# from two per-crate Dockerfiles in ASAPQuery-backend, producing two
# separate images (the old combined deploy/docker/Dockerfile.backend is
# retired).
DOCKER_BUILDKIT=1 docker build \
    -f /mydata/ASAPQuery-backend/data_plane/Dockerfile \
    -t asap/data-plane:dev /mydata/ASAPQuery-backend
DOCKER_BUILDKIT=1 docker build \
    -f /mydata/ASAPQuery-backend/control_plane/Dockerfile \
    -t asap/control-plane:dev /mydata/ASAPQuery-backend

# 2) Distribute + run the demo. `run_demo.sh` Phase 0 rsyncs configs/scripts,
#    docker-save-distributes the images, and pulls externals on each node.
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
| `scripts/run_demo.sh` | Main driver — per-arm bring-up / soak / teardown across all 4 nodes |
| `scripts/run_demo_sweep.sh` | Wraps `run_demo.sh` with sweeps (e.g. cardinality grid, sketch-family grid) |
| `scripts/validate_arm.sh` | Smoke-check a single arm without running the full demo |
| `scripts/snapshot_resources.sh` | Per-container `docker stats` snapshot — used by run_demo.sh Phase 2 |
| `scripts/measure_freshness.sh` | Probe-based freshness measurement — used by run_demo.sh Phase 3 |
| `scripts/measure_nic_bw.sh` | Per-NIC `cat /sys/class/net/.../statistics` snapshot — fed into per-edge CSV |
| `configs/{b0,b1,asap,shared}/` | Per-arm + shared YAML bundles — rsync'd to each node at Phase 0 |

The per-node Python utilities (`metricsql_replay.py`, `measure_*.py`, `accuracy_reduce.py`, etc.) live in `deploy/mvp-singlenode/scripts/` and `run_demo.sh` rsyncs that directory to each node's `/mydata/mvp-multinode/scripts/` at bring-up. They are shared across the two demos.

## Differences vs the single-host driver

| | `deploy/mvp-singlenode/scripts/run_mvp_demo.sh` | `deploy/mvp-multinode/scripts/run_demo.sh` |
|---|---|---|
| Topology | All containers on one host | 4 nodes on 10.10.1.x |
| Image distribution | Built once, used in place | `docker save | ssh load` to each node |
| Bandwidth measurement | Loopback (flattered) | Real NIC counters per node |
| Compose | `docker-compose` overlays under `deploy/mvp-singlenode/docker-compose/` | Plain `docker run --network host` + `--add-host` (no compose, no overlay merging) |
| `MVP_REPORT.md` | Rendered by `mvp_report.py` (Phase 8) | Generated on node0 from aggregated per-node CSVs |

Pick the single-host driver for fast iteration and PR-time smoke. Use the 4-node driver when bandwidth claims need to land on a real LAN.

## gorilla-merger integration (issues #32 / #24)

The ASAP arms run an `asap-gorilla-merger` container on node2 (Thanos-Receive-style):
HTTP fragment ingest on `:10908` (`POST /ingest/gorilla`), Thanos StoreAPI on
`:10907` for the `<2h` pending window, and a 2h-block shipper into the same
`asap-gorilla-tsdb` bucket the store-gateway watches.

**Wired (#32 — StoreAPI fan-out):** `thanos-query` is launched with a second
`--endpoint=gorilla-merger:10907` alongside `--endpoint=thanos-store-gateway:10901`,
so query unions the merger's recent `<2h` window with the store-gateway's `>=2h`
S3 blocks. The merger ships its own blocks to S3 and drops the local copy once
shipped, so there is no double-count across the boundary. The merger's
distinguishing external label is `cluster=asap-mvp,merger=m1`.

**NOT yet wired (#24 — edge cold-ship to the merger):** the merger's HTTP
ingest expects `asap-gorilla-go` `ASAPFRG1` fragment batches, but **no edge
agent path produces an HTTP fragment ship today.** The current edge cold tier
is the `gorillas3` OTel processor (see
`configs/asap/asap-otel-agent-b6-asap-single-sketch.yaml`), which writes
Prometheus TSDB blocks **directly** to MinIO (`block_format: prometheus_tsdb`,
`tsdb_bucket: asap-gorilla-tsdb`) — there is **no `cold.ship_endpoint` config
key** anywhere in this repo, and the `gorillas3processor` Config struct exposes
no HTTP-ship endpoint (its `agent` role emits fragment *metrics* downstream
through the OTel pipeline, it does not POST them over HTTP). So the
merger ingest port currently has no producer in the multinode deploy.

To close #24, one of the following has to land first (out of scope here):
1. an OTel exporter that serializes the `agent`-role gorillas3 fragment stream
   into `ASAPFRG1` batches and POSTs them to `http://gorilla-merger:10908/ingest/gorilla`
   (gzip optional), replacing the direct-to-S3 `gorillas3` TSDB write; or
2. a `gateway_fragment`-role gorillas3 sidecar that the agents ship fragments
   to over OTLP, which then re-POSTs to the merger.

Until then, the merger container + StoreAPI fan-out are live and queryable, but
ingest is exercised only by a manual `POST /ingest/gorilla` (the merger's own
unit tests cover the wire path). End-to-end edge→merger→query validation is
blocked on the producer side, NOT the merger or query side.
