# MVP demo runbook — issue #46

Step-by-step instructions for running the ASAP MVP demo on a single host.
The demo exercises the controller-planned, multi-stage,
sketch + Gorilla-S3 pipeline against three canonical PromQL query classes
and emits an `MVP_REPORT.md` with measured numbers per criterion.

The comparison to Databricks Pantheon+Hydra is in
[`docs/comparison-asap-vs-databricks-pantheon-hydra.md`](comparison-asap-vs-databricks-pantheon-hydra.md).

## Current Status

The MVP demo is runnable through `deploy/scripts/run_mvp_demo.sh`. Current runs should be evaluated from the freshly generated `OUT_BASE` report; committed historical artifacts under `deploy/eval-results/` have been removed and that directory is ignored to prevent stale PASS/UNKNOWN reports from being mistaken for source truth.

The accuracy reducer now uses the archive engine as ground truth by reissuing replay PromQL with `X-ASAP-Engine: thanos_archive`. The deleted `/var/asap/cold/raw` JSONL tee and `--use-jsonl` compatibility path are no longer part of the demo.

Cold fallback is considered passing only when `ad-hoc/cold_payments.curlstats` records HTTP 200 and the response carries an archive `data_source` marker (`thanos_archive`, or the legacy `gorilla_archive` alias). The driver-side `POST /api/v1/plan` workaround has been removed; bootstrap GET is the workload/config path exercised by the demo.

## Component status (implemented / tested / planned)

The MVP demo's intent: sketches feed the warm tier in
`ASAPQuery-backend` and are answered by an in-process query engine;
Gorilla-XOR-compressed raw chunks feed an archive tier on MinIO (S3
local) and are answered by a separate query engine that decodes the
chunks on demand. The shared compute lives in two reusable libraries
(`asap-precompute-{go,rs}` and `asap-gorilla`) so OTel / OTAP /
Telegraf edge runtimes can plug into the same wire format and chunk
encoding.

### Edge runtimes (consume the common precompute libraries)

| Runtime | Language | Library | Status |
|---|---|---|---|
| `asap-otel` (OTel collector) | Go | `asap-precompute-go` | ✅ implemented + tested end-to-end (paired sweep) |
| `asap-otap` (OTAP-Dataflow) | Rust | `asap-precompute-rs` | ✅ implemented; cross-host byte-parity test green |
| `asap-telegraf` (Telegraf input) | Go | `asap-precompute-go` | ✅ implemented; envelope byte-parity verified |

### Common precompute / chunk libraries

| Library | Used by | Status |
|---|---|---|
| `asap-precompute-go` | `asap-otel`, `asap-telegraf` | ✅ implemented |
| `asap-precompute-rs` | `asap-otap`, ASAPQuery-backend ingest path | ✅ implemented |
| `asap-gorilla` (Rust encoder/decoder, postings, chunk index) | `gorillas3processor` (via Go-shim), backend `GorillaQueryEngine` | ✅ implemented + tested (39 unit tests pass; cross-language byte parity with the Go gorillas3processor). Phase δ.1 deleted `gorilla-compactor`; archive-tier compaction is handled by stock `thanos-compact` instead. |

### Sketch families (5 supported, byte-parity across runtimes)

| Family | Wire variant | Cross-runtime parity | Backend accumulator | Default `delta_transmission` |
|---|---|---|---|---|
| DDSketch | `Metric.data = DDSketch` | ✅ | ✅ | `true` |
| KLL | `Metric.data = KLLSketch` | ✅ | ✅ | n/a — no delta variant |
| HLL | `Metric.data = HLLSketch` | ✅ | ✅ | `true` |
| CountSketch | `Metric.data = CountSketch` | ✅ | ✅ | `true` |
| Count-Min Sketch | `Metric.data = CountMinSketch` | ✅ | ✅ | `true` |

Operational defaults (factory `createDefaultConfig` in
`opentelemetry-collector-contrib-patch/processor/<family>processor/factory.go`,
plus the controller's stage-emitter
`controller/src/config/stage_config.rs::build_edge_processor_block`) ship
delta-encoded transmission turned on for the four mergeable families:
the per-window wire payload is the bucket / register / cell diff since
the last flush, not the full sketch state. The `_quantile` latency-class
is the only family where the wire payload is always full state — KLL's
randomised compaction means two sketches over the same input history
are not bit-identical and are not additively mergeable, so a delta
variant is undefined (the kllprocessor's `Config.Validate` rejects
`delta_transmission: true` outright; see `Implementation.tex`).

### Backend (`ASAPQuery-backend`)

| Component | Role | Status |
|---|---|---|
| `asap-query-backend` (`query_engine_rust`) binary | Receives sketch envelopes; serves PromQL HTTP | ✅ |
| `SimpleEngine` | Warm-tier query engine over sketch state | ✅ implemented + tested (33 PromQL pattern matchers) |
| `ThanosForwardEngine` | Archive-tier query engine over Prometheus TSDB blocks in MinIO via thanos-query/store-gateway | ✅ full PromQL surface for archive truth and cold fallback |
| `GorillaQueryEngine` | Legacy alias / compatibility path over Gorilla chunks | ⚠️ kept for compatibility; current MVP archive truth defaults to `thanos_archive` |
| `GorillaS3Store` | S3 fetcher with chunk-LRU cache | ✅ (Step-1 of the JSONL deprecation renamed `GorillaS3ColdStore` → `GorillaS3Store` — the only `Store` impl in the archive tier after the JSONL leg was deleted) |
| `BackendStorageRouting` (multi-target) | Per-metric dispatch warm vs archive based on query shape | ✅ implemented + tested |
| `s3_cost.rs` | Counts PUT/GET/HEAD/DELETE + bytes | ✅ exposed at `/internal/s3_cost.csv` |
| Freshness pattern (`http_freshness_probe_*`) | Backend can answer `last_over_time` on probes | ✅ pattern registered |

### Gorilla archive on object storage (MinIO / S3)

| Component | Role | Status |
|---|---|---|
| MinIO container | S3-compatible local object store | ✅ deployed in compose |
| `gorillas3processor` (Go) | Edge processor: encode raw → Gorilla chunks → S3 PUT | ✅ implemented + tested |
| Chunk format (self-describing) | Magic + schema_version + flags + time bounds + sample count + CRC32C + labelset + Gorilla body | ✅ |
| Per-block manifest (`index.json`) | Lists chunks with `byte_offset` + `byte_length` | ✅ enables `Range:` partial S3 reads |
| Postings index (`postings-v1.json`) | `label_name=value → series_ids` | ✅ implemented; consumed by `GorillaQueryEngine` for label-predicate filtering |
| `thanos-compact` sidecar | Archive-tier block compaction (decode + re-encode; downsamples raw / 5m / 1h tiers) | ✅ deployed via `mvp-thanos-archive.yml` (Phase δ.1; replaces the deleted `gorilla-compactor` Rust binary) |

### Controller (5-layer pipeline)

| Layer | Role | Status |
|---|---|---|
| L1 `query_language` | Parse PromQL → AST | ✅ |
| L2 `logical_plan` | AST → logical operators | ✅ |
| L3 `intent_algebra` | `AggIntent` + `QueryExpr` DAG + Schema | ✅ |
| L4 `sketch_algebra` | `SketchExpr` + `Bind*` rules | ✅ |
| L5 `stage_split` | `StageAllocator` + `ThreeStageEmitter` | ✅ |
| Per-runtime config emitter (`emit_edge_yaml` / `emit_gateway_yaml` / `emit_backend_config_json`) | Plan → per-stage YAML | ✅ implemented + tested (gated by `USE_TYPED_STAGE_SPLIT=1`) |
| OpAMP push to agent / gateway / backend | Push emitted configs at startup | ✅ — STATUS = `live` confirmed in demo runs |
| Cost model (`workload_cost`) | Tier-spanning unit cost: warm RAM, S3 PUT/GET, edge CPU, cut-edge bandwidth | ✅ |

### What's NOT working end-to-end (known gaps)

These are the same gaps captured in §"Open gaps" above. Recapping
against the component list:

| Gap | Component touched | Status |
|---|---|---|
| ④ accuracy reducer | query archive truth through `X-ASAP-Engine: thanos_archive` | ✅ implemented in `accuracy_reduce.py` |
| ⑥ freshness probe consumer | `gorillas3processor` archive probe timestamps | ✅ driver/report expect fresh run artifacts; inspect generated freshness CSVs |
| Image-cache stickiness | Backend Docker layer cache | ⚠️  operational gotcha; pass `--no-cache` |

### Planned (not yet implemented)

| Item | Tracking |
|---|---|
| ~~Delete `LocalFsColdStore` + JSONL gateway raw-tee + the `cost_model` cold-tier scan-bytes line item~~ | DONE (Step-1 of the JSONL deprecation — backend PR #95, collector PR #312) |
| Continue deleting legacy `gorilla_archive` naming where it is only an alias and not an API compatibility requirement | `docs/design-archive-tier.md` |

## TL;DR

```bash
# from a clean machine with docker + cargo + Go installed
git clone -b main git@github.com:ProjectASAP/ASAPCollector.git ~/repos/ASAPCollector
git clone -b main git@github.com:ProjectASAP/ASAPQuery-backend.git ~/repos/ASAPQuery-backend
git clone -b main git@github.com:ProjectASAP/asap_sketchlib.git ~/repos/asap_sketchlib

cd ~/repos/ASAPCollector

# 1. Build the four dev images
#    (Phase δ.1: gorilla-compactor binary deleted; thanos-compact runs
#    as a sidecar from mvp-thanos-archive.yml — no separate binary needed)
make -C deploy build-images || bash deploy/scripts/build-all.sh   # see §3 for explicit commands

# 2. Run the demo
USE_TYPED_STAGE_SPLIT=1 \
bash deploy/scripts/run_mvp_demo.sh

# 3. Read the report
cat deploy/eval-results/mvp-current/MVP_REPORT.md
```

Wall time: ~30-45 minutes for the demo run; ~10-15 minutes for the
one-time image builds.

## 1. Prerequisites

### Hardware

- ~16 GB RAM (24 GB if you build all images from scratch).
- ~30 GB free disk for image layers + eval-results outputs.
- x86_64 Linux is the tested target. macOS arm64 should work but isn't
  CI-tested.

### Software dependencies (versions matter — pin where possible)

| Tool | Min version | Used for |
|---|---|---|
| Docker engine | 24.0+ | Container runtime |
| Docker BuildKit | (default in 24.0+) | `DOCKER_BUILDKIT=1` named build contexts |
| Docker Compose v2 | 2.20+ | `docker compose ...` (NOT `docker-compose`) |
| Rust toolchain | 1.90+ | backend image build, controller (Phase δ.1: `gorilla-compactor` Rust binary deleted; replaced by stock `thanos-compact` sidecar) |
| Go toolchain | 1.22+ | `fake-exporter`, `gorillas3processor`, OCB build |
| Python | 3.10+ | Measurement / report scripts |
| protoc | 3.21+ | prost-build in the backend Cargo crates |
| jq | 1.6+ | Driver scripts parse PromQL responses |
| curl | 7.81+ | Driver scripts hit the controller's HTTP API |
| git | 2.34+ | Cloning the sibling repos with worktrees |

#### Linux (Debian / Ubuntu) — one-shot install

```bash
# Docker engine + Compose v2 (Docker's official APT repo)
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) \
       signed-by=/etc/apt/keyrings/docker.gpg] \
       https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
                        docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER   # log out + back in for group to take effect

# Build toolchain + helpers
sudo apt-get install -y build-essential pkg-config libssl-dev \
                        protobuf-compiler git jq curl python3-pip

# Rust (rustup; pin to 1.90+)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    --default-toolchain 1.90.0 --profile minimal
source $HOME/.cargo/env

# Go 1.22+ (skip if already installed)
GO_VERSION=1.22.5
curl -L https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz | \
    sudo tar -C /usr/local -xz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc

# Python deps (the driver scripts expect these)
pip install --user requests pyyaml
```

#### macOS (Homebrew)

```bash
brew install docker docker-compose rust go protobuf jq python@3.11 git
brew install --cask docker  # Docker Desktop, includes engine + Compose
pip3 install --user requests pyyaml
```

#### Verify the toolchain

```bash
docker version | head -3
docker compose version
rustc --version  # ≥ 1.90
go version       # ≥ 1.22
protoc --version
python3 --version
jq --version
```

### Repo layout

The backend's Cargo.toml path-deps `asap-precompute-rs` at
`../../ASAPCollector/asap-precompute-rs`, and that crate path-deps
`asap_sketchlib` at `../../asap_sketchlib`. The MVP demo also expects
`asap-gorilla` at `../../ASAPCollector/asap-gorilla`. Cloning the three
repos as siblings — the canonical dev layout — is REQUIRED:

```
~/repos/
├── ASAPCollector/              ← this repo
├── ASAPQuery-backend/          ← path-dep'd by ASAPCollector for backend builds
└── asap_sketchlib/             ← path-dep'd by both
```

### Eval-results layout

The repository ships with **paper-quality prior runs** in
`deploy/eval-results/`. These are durable evidence pointed at from the
paper — don't delete or commit over them:

```
deploy/eval-results/
├── headline-2026-05-06/        ← 60-cell paired sweep (paper headline)
└── headline-2026-05-06-postfix/ ← post-fix subset re-runs
```

**The MVP demo's own outputs are NOT committed to the repo.** When you
run the demo, the driver writes to
`deploy/eval-results/mvp-current/` (overridable via `OUT_BASE`),
but those files stay local to the runner — they are not pushed back.
For a record of recent runs, read the most recent `MVP_REPORT.md`
posted on the issue-#46 comment thread:

  https://github.com/ProjectASAP/ASAPCollector/issues/46

## 2. Architecture summary (just enough to read the runbook)

The demo runs **two architectures back-to-back on the same workload**
so the issue-#46 criteria (X bandwidth reduction, Y query-latency
reduction, Z combined-resource reduction, accuracy, cold-fallback,
freshness) all fall out as A-vs-B comparisons. Same fake-exporter
producers, same per-agent cardinality, same query classes, same soak
duration — only the pipeline differs.

### Baseline pipeline ("OTel → Prometheus")

The minimal industry-standard observability stack the user would
deploy today: stock OTel agents shipping every raw sample to a
TSDB.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  BASELINE                                                                │
│                                                                          │
│  10 fake-exporter  ──OTLP raw──▶  2 OTel agents  ──remote_write──▶       │
│   (1000 series each)              (no aggregation;                       │
│                                    forward as-is)                        │
│                                          │                               │
│                                          ▼                               │
│                                   Prometheus  ◀─── PromQL queries        │
│                                   (or VictoriaMetrics)                   │
│                                   - TSDB: head block + WAL + chunks      │
│                                   - PromQL evaluator                     │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

   Resource axes measured (per-stage):
     • OTel agent: CPU + RSS + net out
     • Prometheus: CPU + RSS + on-disk chunk bytes + query CPU/RSS
     • Wire bytes (agent → Prometheus)
     • Query latency (PromQL HTTP p50 / p99)
```

Compose overlay: `deploy/docker-compose/mvp-multi-stage.yml` brought
up with the `b0` profile (`docker compose --profile b0 up`) which
adds a Prometheus container with `--web.enable-remote-write-receiver`.
The agents under this profile load `configs/b0/asap-otel-agent-b0-prometheus.yaml`,
which configures a `prometheusremotewrite` exporter with no sketch
processor in the chain.

### ASAP pipeline (current architecture)

The system this paper proposes: controller-planned sketches at the
edge + gateway, exact archive on object storage, single PromQL surface
served by the backend's `EngineRouter` dispatching warm vs. archive
based on the query's shape.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  ASAP                                                                    │
│                                                                          │
│  10 fake-exporter ──OTLP──▶ 2 asap-otel agents ──OTLP──▶ 1 gateway       │
│   (1000 series each)         (sketch processors:        (sketch-merge    │
│                               ddsketch / kll / hll /     processors)     │
│                               cs / cms — picked per             │        │
│                               metric by controller)             │        │
│                                                                  │       │
│                                                                  │       │
│                                              ┌───────────────────┘       │
│                                              ▼                           │
│                                    ASAPQuery-backend                     │
│                                    - SimpleEngine (warm sketch tier)     │
│                                    - ThanosForwardEngine (archive)       │
│                                    - BackendStorageRouting               │
│                                      dispatches per query shape          │
│                                                                          │
│                            ┌───────────────────┘                         │
│                            ▼                                             │
│                     gorillas3processor                                   │
│                     (raw → Gorilla XOR chunks)                           │
│                            │                                             │
│                            ▼                                             │
│                     MinIO (S3 local)  ◀── PromQL queries (count,         │
│                                            topk, ad-hoc post-hoc rate)   │
│                                                                          │
│  controller — plans (sketch family + stage placement) per metric from    │
│   mvp-workload.yaml; emits per-runtime configs via OpAMP.                │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

   Resource axes measured (per-stage; same as baseline):
     • asap-otel agents: CPU + RSS + net in/out
     • gateway: CPU + RSS + net in/out (sketch-merge fan-in)
     • ASAPQuery-backend: CPU + RSS + warm-tier sketch-state RAM
     • MinIO: on-disk Gorilla bytes + S3 PUT/GET counts
     • Wire bytes per cut edge (sdk→agent / agent→gateway / gateway→backend / gateway→S3)
     • Query latency per class (p50 / p99) — PromQL HTTP
```

### Comparison framing — what the demo measures

Same workload runs through both pipelines; the report (`MVP_REPORT.md`,
§1 stage-separated resource table + §2 verdict) reports both as
side-by-side rows for each criterion:

| # | Criterion (per #46) | Baseline figure | ASAP figure | Reduction |
|---|---|---|---|---|
| ① | Bandwidth | bytes/s on `agent → Prometheus` | bytes/s on each cut edge | X% |
| ② | Aggregation query latency | Prometheus PromQL p50 / p99 | ASAPQuery-backend PromQL p50 / p99 (warm + archive) | Y% |
| ③ | Combined e2e resource | Σ(OTel agent + Prometheus CPU/RSS/disk) | Σ(asap-otel + gateway + backend + MinIO) | Z% |
| ④ | Accuracy | exact (raw samples in TSDB) | rel-err per query class within ε/δ envelope | bounded by sketch family |
| ⑤ | Cold-fallback for ad-hoc queries | Prometheus answers anything natively | `data_source: thanos_archive (or legacy gorilla_archive alias)` for ad-hoc / post-hoc / cardinality queries | qualitative PASS |
| ⑥ | Freshness | sample-to-query latency on TSDB ingest path | sample-to-query latency on warm + archive paths | per-path Δ |

### Three query classes the demo exercises

- **Window per series**: `quantile_over_time(0.99, latency[1m])`
- **Label aggregation at instant**: `sum by (zone) (http_requests_total)`
- **Combined**: `sum by (zone) (rate(http_requests_total[5m]))`

Plus an ad-hoc cold-fallback probe:
`count(http_requests_total{service="payments"})` — exercises the
Gorilla-archive engine on the ASAP side via dual-routing; on the
baseline side it's just another query Prometheus answers.

### Freshness probe protocol (criterion ⑥)

Three synthetic counters (`http_freshness_probe_{raw,warm,archive}`)
encode the Unix-epoch-ms emission timestamp as the cumulative
counter value. Each is routed to a different serving tier so the
demo measures Δ = (query response ts) − (sample emission ts) on
the raw / warm / archive paths independently.

Mechanic (assumes single-host clock sync):

1. Fake-exporter emits the counter with cumulative value =
   `now_ms` at each tick (default 1 Hz; `FRESHNESS_PROBE_HZ`).
2. Replay client polls `last_over_time(http_freshness_probe_*[10s])`
   every 100 ms.
3. On first non-empty response: Δ = `poll_response_ts_ms − observed_value`.
4. Repeat for ≥ 60 samples per path; report p50, p99, count.

Pitfalls the demo guards against (each was a real failure mode in
earlier iterations):

- Probe routing must be tested explicitly for all three paths during
  pre-flight — otherwise probes silently never reach Prometheus on
  the raw path or never settle in the warm path.
- Sketch window for the freshness probe metric pinned at ≤ 1 s.
- Soak duration extended only as needed for the slowest path (the
  archive path, since chunks aren't queryable until the per-window
  flush lands in S3 — typically ≥ 60 s).

The generated freshness CSVs are the source of truth for criterion ⑥;
empty files indicate a current run problem, not an expected runbook
exception.

## 3. Compiling and building from source

There are four build artifacts. Build them in this order — the backend
image build context references the Rust crates that the earlier steps
exercise.

```
0. Clone the three sibling repos at ~/repos (one-time)
1. asap-precompute-rs            (cargo, Rust crate)         } compile-time
2. asap-gorilla                  (cargo, Rust crate)         }   sanity
3. controller binary             (cargo)
4. asap/controller:dev           (Docker image)              } runtime
5. asap/asap-otel:dev            (OCB + Docker)              }   images
6. asap/fake-exporter:dev        (Docker image)              }
7. asap/query-backend:dev        (Docker image, multi-context)
```

> **Phase δ.1 (2026-05-07)**: the `gorilla-compactor` Rust binary that
> previous runbook revisions built as Step 4 has been deleted.
> Archive-tier block compaction is now performed by the stock
> `thanos-compact` container (declared in
> `deploy/docker-compose/mvp-thanos-archive.yml`); no separate binary
> needs to be built.

Total wall: ~15-30 min on a clean machine; ~3-5 min on a warm
incremental build.

### Step 0 — clone

```bash
mkdir -p ~/repos && cd ~/repos
git clone -b main git@github.com:ProjectASAP/ASAPCollector.git
git clone -b main git@github.com:ProjectASAP/ASAPQuery-backend.git
git clone -b main git@github.com:ProjectASAP/asap_sketchlib.git
```

### Step 1 — `asap-precompute-rs` (compile sanity)

```bash
cd ~/repos/ASAPCollector/asap-precompute-rs
cargo build --release
cargo test --release  # ~10 unit tests should pass
```

If this fails, `asap_sketchlib` isn't sitting at `../../asap_sketchlib/`
or its Cargo.toml is broken. Re-clone or re-check the layout.

### Step 2 — `asap-gorilla` (compile sanity)

```bash
cd ~/repos/ASAPCollector/asap-gorilla
cargo build --release
cargo test --release  # 39 tests should pass (encoder/decoder + postings)
```

### Step 3 — controller binary (used by `asap/controller:dev`)

```bash
cd ~/repos/ASAPCollector/controller
cargo build --release
cargo test --release  # 482+ pass / ~10 pre-existing fail (out of scope)
ls target/release/controller
```

### Step 4 — `asap/controller:dev` Docker image

```bash
cd ~/repos/ASAPCollector/controller
docker build -t asap/controller:dev .
docker image ls asap/controller:dev
```

### Step 5 — `asap/asap-otel:dev` (agent + gateway use the same image)

This is a two-step build: first OCB compiles the patched OpenTelemetry
Collector binary; then the Dockerfile packages it.

```bash
cd ~/repos/ASAPCollector

# OCB (OpenTelemetry Collector Builder) generates the binary by stitching
# together the patched processors listed in builder-config.yaml.
bash opentelemetry-collector-contrib-patch/cmd/asap-otel/build.sh

# Wrap the binary in the runtime image
docker build \
    -t asap/asap-otel:dev \
    -f opentelemetry-collector-contrib-patch/cmd/asap-otel/Dockerfile \
    .

docker image ls asap/asap-otel:dev
```

See `docs/design-asap-edge-framework.md` for OCB build details and
`builder-config.yaml` semantics.

### Step 6 — `asap/fake-exporter:dev` (with freshness probes)

```bash
cd ~/repos/ASAPCollector/deploy/fake-exporter

# Pure Go build — no extra setup beyond `go` on PATH
docker build -t asap/fake-exporter:dev .
docker image ls asap/fake-exporter:dev

# Verify the freshness probe metrics from PR #299 are baked in:
docker run --rm asap/fake-exporter:dev sh -c \
    'grep -l http_freshness_probe_raw probes.go' || \
    echo "WARNING: image lacks freshness probes — rebuild after PR #299"
```

### Step 7 — `asap/query-backend:dev` (multi-context Docker build)

The backend image stitches together four sibling source trees as
BuildKit named build-contexts. Each context is a separate
`COPY --from=...` in `deploy/docker/Dockerfile.backend`:

```bash
cd ~/repos/ASAPCollector

DOCKER_BUILDKIT=1 docker build \
    -f deploy/docker/Dockerfile.backend \
    --build-context backend-src=$HOME/repos/ASAPQuery-backend \
    --build-context asap-precompute-rs=$PWD/asap-precompute-rs \
    --build-context asap-sketchlib=$HOME/repos/asap_sketchlib \
    --build-context asap-gorilla=$PWD/asap-gorilla \
    -t asap/query-backend:dev \
    .
```

The build runs `cargo build --release --bin asap-query-backend` inside a
`rust:1.90-bookworm` builder stage, then copies the binary into a
`debian:bookworm-slim` runtime stage. Wall: 4-8 min cold, 30-60 s warm.

**Cache stickiness gotcha**: if you've previously built this image and
a backend PR has since merged, Docker's Cargo layer cache may produce
the same image SHA even though the source has changed. Force a clean
rebuild:

```bash
DOCKER_BUILDKIT=1 docker build --no-cache \
    -f deploy/docker/Dockerfile.backend \
    --build-context backend-src=$HOME/repos/ASAPQuery-backend \
    --build-context asap-precompute-rs=$PWD/asap-precompute-rs \
    --build-context asap-sketchlib=$HOME/repos/asap_sketchlib \
    --build-context asap-gorilla=$PWD/asap-gorilla \
    -t asap/query-backend:dev \
    .
```

Verify the freshly-built image has the postings + dual-routing features:

```bash
docker run --rm asap/query-backend:dev sh -c \
    'strings /usr/local/bin/asap-query-backend | \
     grep -E "postings_filtered|thanos_archive|s3_cost" | head -5'
```

If the grep returns nothing, the cache hit on a stale layer; rebuild
with `--no-cache`.

### One-shot build (all 4 images + 1 binary)

Wrap the steps above in a script for repeatability:

```bash
cat > /tmp/asap-build-all.sh <<'BASH'
#!/usr/bin/env bash
set -euxo pipefail
cd ~/repos/ASAPCollector

# Rust crates (compile sanity + cache warm-up)
# Phase δ.1: gorilla-compactor crate deleted (replaced by thanos-compact
# sidecar); only asap-precompute-rs / asap-gorilla / controller remain.
( cd asap-precompute-rs && cargo build --release )
( cd asap-gorilla       && cargo build --release )
( cd controller         && cargo build --release )

# Docker images
docker build -t asap/controller:dev controller/
bash opentelemetry-collector-contrib-patch/cmd/asap-otel/build.sh
docker build -t asap/asap-otel:dev \
    -f opentelemetry-collector-contrib-patch/cmd/asap-otel/Dockerfile .
docker build -t asap/fake-exporter:dev deploy/fake-exporter/
DOCKER_BUILDKIT=1 docker build -f deploy/docker/Dockerfile.backend \
    --build-context backend-src=$HOME/repos/ASAPQuery-backend \
    --build-context asap-precompute-rs=$PWD/asap-precompute-rs \
    --build-context asap-sketchlib=$HOME/repos/asap_sketchlib \
    --build-context asap-gorilla=$PWD/asap-gorilla \
    -t asap/query-backend:dev .

echo "OK — all artifacts built"
BASH
bash /tmp/asap-build-all.sh
```

## 4. Running the demo

```bash
cd ~/repos/ASAPCollector

# Defaults are reasonable; override only if you need to:
export USE_TYPED_STAGE_SPLIT=1                 # fire the typed stage-split path
# Phase δ.1: COMPACTOR_BIN env var was removed — thanos-compact runs as
# a compose sidecar, no host-side binary path is needed.
export OUT_BASE=$PWD/deploy/eval-results       # default
export PER_AGENT_CARDINALITY=500               # 500 × 10 producers = 5K aggregate
export NUM_PRODUCERS=10                         # spread across 2 agents (5 + 5)
export ASAP_SKETCH_FAMILY=ddsketch             # default; overridden per-metric by controller
export EXPORTER_FRESHNESS_PROBES=on            # emit timestamp-encoded probes

# Run the demo SYNCHRONOUSLY (foreground). Wall time: ~30-45 min.
bash deploy/scripts/run_mvp_demo.sh
```

### Essential YAML configs the MVP demo touches

Most files in `deploy/configs/` belong to baseline-sweep / alt-storage / experimental compose overlays. `run_mvp_demo.sh` (single-host) and `deploy/mvp-multinode/run_demo.sh` (4-node) only mount these:

| File | Role | Mounted by |
|---|---|---|
| `configs/asap/mvp-workload.yaml` | Controller workload spec — which metrics, what queries, sketch family per metric | controller |
| `configs/asap/backend-streaming.yaml` | Backend ingest schema | backend |
| `configs/asap/backend-storage-routing.yaml` | Warm-tier ↔ archive routing decisions | backend |
| `configs/asap/asap-otel-gateway-mvp-placeholder.yaml` | Gateway OTLP fan-in (sketches + raw → backend) | gateway (ASAP arm) |
| `configs/b0/asap-otel-agent-b0-prometheus.yaml` | Baseline-B0 agent (raw → Prometheus, no sketches) | agent (B0 arm) |
| `configs/asap/asap-otel-agent-b6-asap-single-sketch.yaml` | ASAP-arm agent (5 sketch processors + OTLP fwd) | agent (ASAP arm) |
| `configs/shared/prometheus-with-remote-write.yml` | B0 Prometheus scrape + remote-write | prometheus-b0 |
| `configs/shared/thanos-objstore.yaml` | Thanos sidecar / store-gateway / compact MinIO endpoint | thanos-* |
| `configs/grafana-datasources.yml` | Grafana pre-wired datasources | grafana (optional) |

For the 4-node demo, the `b1-serf-prometheus` agent config is also mounted when `--mode both`. Other files in `deploy/configs/` (e.g., `backend-streaming-{cms,cs,hll,kll}.yaml`, `asap-otel-agent-b{2,3,4,5}-*.yaml`, `gateway-aggregate-*.yaml`) are referenced by baseline-sweep overlays and ad-hoc experiments — not by the MVP demo itself.

### Image set (single-binary post-#373)

Three images, all built from this repo's root:

| Image | Built from | Contains |
|---|---|---|
| `asap/asap-otel:dev` | `build_asap_otel.sh` | Patched OTel Collector with sketch processors |
| `asap/fake-exporter:dev` | `docker build deploy/fake-exporter/` | OTLP load generator |
| `asap/query-backend:dev` | `Dockerfile.backend` (multi-bin: see deploy/mvp-multinode/README.md for full command) | `asap-query-backend` (port 9091 / 4317 / 4318) AND `controller` (port 8080 / 4320 / 4321). Phase 9 single-binary refactor — no separate `asap/controller:dev` image. |

If you want to run it in the background and watch from another shell:

```bash
nohup bash deploy/scripts/run_mvp_demo.sh > /tmp/mvp-run.log 2>&1 &
echo $! > /tmp/mvp.pid

# poll for completion (blocks until report file appears OR demo PID exits)
until [ -f deploy/eval-results/mvp-current/MVP_REPORT.md ] || \
      ! ps -p $(cat /tmp/mvp.pid) > /dev/null 2>&1; do sleep 30; done
echo "demo done"
```

### What the demo does, phase by phase

The driver runs eight phases (see `deploy/scripts/run_mvp_demo.sh` for
the implementation):

| Phase | Action |
|---|---|
| 0. Pre-flight | Verify images present; clean stale containers (Phase δ.1: gorilla-compactor binary check removed — thanos-compact sidecar comes up with the rest of the stack) |
| 1. Stack-up | `docker compose up` against `base.yml + mvp-multi-stage.yml`; mount `mvp-workload.yaml` into controller; wait for OpAMP push to settle |
| 2. Warm-up | 60 s agent warm-up + 30 s query-side warm-up (poll `count_over_time(http_requests_total[1m])` until non-zero) |
| 3. Measurements | Run `measure_stages.py` + `measure_per_edge_bandwidth.py` + `metricsql_replay.py` over 60 s soak with three query classes |
| 4. Freshness | Run `run_freshness_phase.sh` against three probes (raw / warm / archive) → 3 CSVs |
| 5. Ad-hoc queries | Fire label-predicate queries; capture postings filtering |
| 6. Cold-fallback | Fire `count(http_requests_total{service="payments"})`; verify `data_source: thanos_archive (or legacy gorilla_archive alias)` |
| 7. Compaction | Verify `thanos-compact` sidecar is healthy; capture before/after `mc ls` listing of the MinIO archive bucket; poll `thanos_compact_iterations_total` from `/metrics` to confirm at least one compaction sweep completed (Phase δ.1) |
| 8. Report | Run `mvp_report.py` over the captured CSVs to produce `MVP_REPORT.md` |

## 5. Reading the output

After the demo completes, the output directory tree is:

```
deploy/eval-results/mvp-current/
├── MVP_REPORT.md            ← human-readable report (start here)
├── ad-hoc/                     ← Phase 5 — ad-hoc query responses
│   ├── label-api.json
│   ├── label-status5xx.json
│   └── cold_payments.json      ← criterion ⑤: look for "data_source: thanos_archive (or legacy gorilla_archive alias)"
├── thanos-compact/             ← Phase 7 (Phase δ.1: renamed from compactor/)
│   ├── before.minio.jsonl      ← MinIO bucket listing pre-sweep
│   ├── after.minio.jsonl       ← MinIO bucket listing post-sweep
│   ├── metrics.before.txt      ← thanos-compact /metrics scrape
│   ├── metrics.after.txt       ← thanos-compact /metrics scrape
│   └── health.txt              ← thanos-compact /-/healthy snapshot
├── controller-emitted-configs/ ← Phase 1 — what the controller pushed to each runtime
│   ├── STATUS                  ← live / fallback-placeholder / not-exercised
│   ├── agent.bootstrap.yaml
│   ├── backend.bootstrap.yaml
│   ├── gateway.placeholder.yaml
│   └── per-metric.<name>.json
├── freshness/                  ← Phase 4
│   ├── raw.csv                 ← path,sample_ts_ms,observed_ts_ms,delta_ms
│   ├── warm.csv
│   └── archive.csv
├── measurements/               ← Phase 3
│   ├── replay.jsonl            ← per-query latency log
│   ├── stages.csv              ← per-container CPU / RSS / net / disk
│   ├── per-edge-bandwidth.csv  ← sdk→agent / agent→gateway / gateway→backend / gateway→s3
│   ├── accuracy.csv            ← per-row rel-err vs cold-tier ground truth
│   └── s3_cost.csv             ← PUT/GET/HEAD/DELETE counts during the soak
├── stack-up.log                ← docker compose up output
└── teardown.log                ← docker compose down output
```

`MVP_REPORT.md` itself contains:

- **§1 Stage-separated resource table** — 5 stages × {CPU cores, RSS MiB, net in/out KiB/s, disk MiB}
- **§2 Per-criterion verdict (6 criteria)** — PASS / FAIL / PARTIAL / UNKNOWN with measured numbers
- **§3 Per-query-class breakdown** — sketch + stage chosen by controller × p50/p99 × accuracy
- **§4 Postings filtering effect** — series matched / would have scanned per ad-hoc query
- **§5 Compaction effect** — before/after object count + bytes
- **§6 S3 cost (measured)** — PUT/GET counts per baseline
- **§7 Honest caveats (non-goals)** — what this demo does NOT verify
- **§8 Controller-emitted runtime configs** — STATUS line + captured artifact list
- **Appendix A** — per-edge bandwidth detail

## 6. Verifying the verdict

For an end-to-end PASS picture, expect:

| § | Criterion | Expected on a clean run |
|---|---|---|
| §2 | ① bandwidth (per-edge) | per-edge B/s reported; FAIL acceptable at 5K cardinality |
| §2 | ② query latency | PASS — p99 ≤ 10ms across all three query classes |
| §2 | ③ combined resource | CAPTURED |
| §2 | ④ accuracy | PASS — rel-err inside ε envelope per query class |
| §2 | ⑤ cold-fallback | PASS — `data_source: thanos_archive (or legacy gorilla_archive alias)` in `ad-hoc/cold_payments.json` |
| §2 | ⑥ freshness | PASS — non-zero p50/p99 in `freshness/{raw,warm,archive}.csv` |
| §3 | per-class rel-err | non-empty for all three classes |
| §4 | postings | non-zero `series matched` rows; `would have scanned` ≥ matched |
| §5 | compaction | before > after on object count |
| §8 | controller emitter | STATUS = `live` |

### Bandwidth criterion ① — break-even depends on `samples_per_window`

The bandwidth verdict is NOT a per-sketch property in isolation. Each
sketch family has a per-series wire footprint dominated by a fixed
`state_size` (DDSketch buckets, HLL registers, KLL sample buffer,
Count-Sketch / Count-Min cell matrix); the per-window break-even
versus raw scrape is governed by

```
samples_per_window = scrape_freq_hz × window_seconds
break_even_samples ≈ state_size_bytes / per_sample_raw_bytes
```

For the demo's 60 s flush window:

| `scrape_freq_hz` | `samples_per_window` | DDSketch | HLL | CountSketch | Count-Min |
|---|---|---|---|---|---|
| 1 Hz (legacy) | 60 | LOSE (~1200% inflation; below break-even) | LOSE (~5×) | LOSE (~133×) | LOSE |
| **10 Hz (default)** | **600** | **WIN (delta)** | **WIN (~1.8× — past 330-sample HLL knee)** | LOSE (~13× — better but still loses) | WIN-adjacent |

The MVP demo defaults to `EXPORTER_FREQ_HZ=10` (see `base.yml` and
`mvp-multi-stage.yml`) so all four delta-capable families operate above
their break-even where possible. KLL is omitted from the table (no
delta variant; full-state cost per window).

If a sweep cell ships at 1 Hz (legacy paths, or a host shell that
overrides `EXPORTER_FREQ_HZ=1`), do NOT compare its bandwidth verdict
against the 10 Hz numbers — the operating point is on the wrong side
of every break-even curve.

## 7. Known Issues

No known stale-artifact blockers are documented in this runbook. A clean run should regenerate its own `OUT_BASE` tree, compute accuracy from the archive engine, and require HTTP 200 plus an archive marker for cold fallback.

If a run reports UNKNOWN/FAIL, inspect the freshly generated `MVP_REPORT.md`, `asap/measurements/accuracy.log`, and `asap/ad-hoc/cold_payments.*` files from that same run.

## 8. Cleanup

```bash
cd ~/repos/ASAPCollector
docker compose -f deploy/docker-compose/base.yml \
               -f deploy/docker-compose/mvp-multi-stage.yml \
               down -v
# -v also removes the MinIO data volume; leave it off if you want to inspect
# the Gorilla-S3 archive after the run.
```

Cargo / Docker caches:

```bash
# free the cargo build cache (large)
# Phase δ.1: gorilla-compactor crate deleted; the only Rust target dirs
# left are asap-precompute-rs/, asap-gorilla/, and controller/.
rm -rf ~/repos/ASAPCollector/{asap-precompute-rs,asap-gorilla,controller}/target
# or, if you set CARGO_TARGET_DIR:
rm -rf $CARGO_TARGET_DIR

# free the docker build cache
docker builder prune --all
```

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `no such file or directory: ../../asap_sketchlib/Cargo.toml` during backend build | Repo layout doesn't have the three sibling clones | Re-clone in `~/repos/{ASAPCollector, ASAPQuery-backend, asap_sketchlib}` |
| Backend log: `No matching pattern for http_freshness_probe_warm` | Backend image pre-dates PR #91 freshness pattern registration | `docker build --no-cache ...` per §3 |
| `MVP_REPORT.md` says §8 STATUS = `not-exercised` | `USE_TYPED_STAGE_SPLIT` not propagating | Check `docker exec controller env \| grep USE_TYPED`; re-export at the host shell |
| `freshness/{raw,warm,archive}.csv` empty | Probe exporter/backend path did not produce observations | Rebuild fake-exporter image; verify with the `grep -l` step in §3 and inspect `freshness/run.log` |
| `accuracy.csv` empty | Archive truth queries failed or replay produced no reducible rows | Inspect `asap/measurements/accuracy.log`; verify backend answers with `X-ASAP-Engine: thanos_archive` |
| Demo agent dies at "stack settle" | Controller container not reachable; check `docker ps` and `docker compose logs controller` | Often a port collision; run `docker compose down -v` first |
| OOM kill during the soak | `PER_AGENT_CARDINALITY` too high for the host RAM budget | Lower to 250 or run on a 32 GB host |

## 10. Related runbooks and docs

- **[`docs/system-overview.md`](system-overview.md)** — **canonical current-state architecture reference** (start here if you want the full picture; this runbook is MVP-demo-scoped, system-overview covers all components, all three operational modes, all three edge runtimes, controller pipeline, deployment shapes, and operating-point math)
- `docs/design-archive-tier.md` — archive-tier wire format, bucket layout, three operational modes, and query-path dispatch
- `docs/comparison-asap-vs-databricks-pantheon-hydra.md` — architectural framing vs. Databricks Pantheon + Hydra
- `docs/control-plane-design.md` — controller pipeline (L1 → L5)
- `docs/e2e-test-guide.md` — pytest-style smoke tests (smaller scope than the MVP demo)
- `docs/eval-instrumentation-notes.md` — measurement methodology notes
