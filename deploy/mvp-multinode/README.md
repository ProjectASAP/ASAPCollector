# 4-node MVP demo orchestrator

CloudLab / multi-host driver for the MVP demo (issue #46). Runs exact raw-data
and ASAP arms across 4 nodes on a 10 Gbps LAN, so wire-byte measurements use a
real NIC instead of loopback.

This is the canonical issue-#46 harness. The former single-host deployment was
removed because it used legacy per-sketch processors; `mvp-singlenode/scripts`
now contains only shared measurement utilities.

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

From node0, the complete paired experiment is one command:

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
cd /mydata/ASAPCollector
bash deploy/mvp-multinode/scripts/run_demo.sh all
```

Knobs (env-overridable, see `topology.env` for defaults):
- `WARMUP_S` (default 30) — per-arm warm-up after stack-up.
- `SOAK_S` (default 90) — measurement window per arm.
- `MODE` — `baseline` / `asap` / `both`.

## Where artifacts land

- **Per-node** (transient): `/mydata/mvp-multinode/results/`, `/mydata/mvp-multinode/logs/`.
- **Aggregated on node0**: one run-ID directory containing per-arm raw
  observations, `run-manifest.json`, `MVP_RESULTS.json`, and `MVP_REPORT.md`.

`results/` and `logs/` are `.gitignore`d — per the repo's eval-artifact convention (the single-host runbook says the same: never commit historical reports, they get stale fast and look like source of truth).

## Files

| Path | What |
|---|---|
| `topology.env` | Per-node IPs, hostnames, `--add-host` injections, image set, paths, soak knobs |
| `scripts/run_demo.sh` | Main driver — per-arm bring-up / soak / teardown across all 4 nodes |
| `scripts/run_demo_sweep.sh` | Wraps `run_demo.sh` with sweeps (e.g. cardinality grid, sketch-family grid) |
| `scripts/validate_arm.sh` | Smoke-check a single arm without running the full demo |
| `mvp-acceptance.json` | Checked-in, pre-run accuracy/freshness/latency/cost acceptance thresholds |
| `scripts/mvp_evaluate.py` | Fail-closed acceptance evaluator and report generator |
| `scripts/measure_freshness.sh` | Probe-based sample-to-query freshness measurement |
| `scripts/measure_nic_bw.sh` | Per-NIC `cat /sys/class/net/.../statistics` snapshot — fed into per-edge CSV |
| `configs/{b0,b1,asap,shared}/` | Per-arm + shared YAML bundles — rsync'd to each node at Phase 0 |

The per-node Python utilities live in `deploy/mvp-singlenode/scripts/`; the
driver rsyncs them to each node at bring-up.

## Acceptance and exit status

The `all` command finishes by evaluating the primary compression-matched pair:
raw OTLP+gzip (`b1`) versus sketched OTLP+gzip (`asap-gzip`). Both arms use the
same deterministic generator seed and workload shape. The evaluator requires:

- valid, current-run provenance and non-empty query results;
- controller acknowledgement that remote configuration was applied, with both
  full- and delta-sketch decisions visible in the effective configuration;
- per-query accuracy within the checked-in SLA;
- freshness p95 and maximum lag within SLA;
- lower ASAP p50 and p95 query latency;
- lower normalized Collector and end-to-end costs, with per-resource
  regression guardrails.

Missing, malformed, empty, or previous-run artifacts fail the run. The driver
returns non-zero when any category fails. Thresholds must be edited and
reviewed before a measurement run, never after observing its results.

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

**Edge cold-ship to the merger (#24):** the merger's HTTP ingest expects
`asap-gorilla-go` `ASAPFRG1` fragment batches. The current edge cold tier is the
FUSED `asap_edge` processor's `cold:` block (see
`configs/asap/asap-otel-agent-asapedge.yaml`), which Gorilla-XOR-encodes raw
samples into `ASAPFRG1` fragment batches and SHIPS them over HTTP to
`cold.ship_endpoint: http://gorilla-merger:10908/ingest/gorilla` — the edge does
no direct-to-S3 PUT; the merger builds the TSDB block + index and cuts the window
block to MinIO's `asap-gorilla-tsdb` bucket. (This supersedes the old `gorillas3`
OTel processor, which wrote Prometheus TSDB blocks directly to MinIO and had no
HTTP-ship endpoint — that path, and the old per-sketch routing-connector agent
config `asap-otel-agent-b6-asap-single-sketch.yaml`, are retired.)

The fused agent config is delivered to the supervised agent as an OpAMP
RemoteConfig PUSH from the control plane (run_demo starts it with
`ASAP_EDGE_FUSED=1`, gating the controller's `emit_edge_yaml_asap_edge` emitter);
`configs/asap/asap-otel-agent-asapedge.yaml` is the static reference/bootstrap
shape mirroring that emit. The optional `cold.format: intchunk` +
`coldpart_endpoint` opt-in re-encodes the same drained samples as a lossless
intchunk cold-part POSTed to `/ingest/coldpart` instead.

End-to-end edge→merger→query validation against a live stack is the
orchestrator's job (the asap_edge binary actually loading + emitting queryable
sketches cannot be confirmed statically from the config alone).
