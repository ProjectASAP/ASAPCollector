# MVP demo runbook — issue #46

Step-by-step instructions for running the ASAP MVP demo on a single host.
The demo exercises the controller-planned, multi-stage,
sketch + Gorilla-S3 pipeline against three canonical PromQL query classes
and emits an `MVP_REPORT.md` with measured numbers per criterion.

Architectural background lives in
[`docs/spec-mvp-controller-driven-multi-stage-demo.md`](spec-mvp-controller-driven-multi-stage-demo.md);
the comparison to Databricks Pantheon+Hydra is in
[`docs/comparison-asap-vs-databricks-pantheon-hydra.md`](comparison-asap-vs-databricks-pantheon-hydra.md).

## Current status

The MVP demo is **runnable end-to-end and exercises the controller-driven,
multi-stage architecture**, but two ingest-side bugs leave criteria ④ and
⑥ reporting UNKNOWN even when their routing layers are working correctly.

### Verdict

| # | Criterion | Verdict | What works | What doesn't |
|---|---|---|---|---|
| ① | Bandwidth (per-edge) | **FAIL** | per-edge B/s captured for sdk→agent / agent→gateway / gateway→backend / gateway→s3 | absolute reduction over a raw-Prometheus baseline not reported (no apples-to-apples comparison row at this cardinality) |
| ② | Query latency (p50 / p99) | **PASS** | window p99 = 5.2 ms, label p99 = 7.2 ms, combined p99 = 1.8 ms — all three query classes inside the 10 ms envelope | — |
| ③ | Combined resource | **CAPTURED** | per-stage CPU + RSS + net + disk reported | reduction-vs-baseline row depends on a raw-Prometheus baseline cell which is opt-in (port collision) |
| ④ | Accuracy (rel-err per class) | **UNKNOWN** | dispatch + warm-tier eval correct (verified via direct curl) | `accuracy_reduce.py` needs a cold-tier ground-truth stream that the backend doesn't write |
| ⑤ | Cold-fallback (`gorilla_archive`) | **PASS** | `data_source: gorilla_archive` in `cold_payments.json`; chunks land in MinIO; dual-routing dispatches `count` queries to the archive engine | — |
| ⑥ | Freshness (probe Δ) | **UNKNOWN** | freshness pattern registered in backend; routing yaml correct; producer envs propagate | agent's `gorillas3processor.encoder.go` writes chunk-header timestamp at byte offset `[5..9]` instead of `[9..13]`; consumer reads bogus emission timestamps so deltas come up zero |
| §8 | Controller emitter STATUS | **`live`** | typed-stage-split fires; controller writes per-stage configs; `entries=4 multi_target_entries=1` in startup log | — |

The architectural validation is solid (everything to do with planning,
routing, and multi-stage placement demonstrably works); the two
ingest-side bugs above are real follow-ups.

### Open gaps (severity × impact × fix path)

#### Gap 1 — accuracy reducer needs ground-truth dump (criterion ④)

- **Severity**: medium (verdict reads UNKNOWN, not FAIL — the warm engine
  is producing correct answers; we just can't compute relative error
  without ground truth)
- **Impact**: ④ accuracy + §3 per-class rel-err render empty in
  `MVP_REPORT.md`
- **Root cause**: pre-Step-1 the accuracy reducer read ground truth
  from `/var/asap/cold/raw/<metric>/YYYY/MM/DD/HH/part-N.jsonl`,
  written by the gateway-side raw-tee. Step-1 of the JSONL
  deprecation deleted that path; the surviving ground-truth source
  is the Gorilla archive on MinIO/S3 (`gorillas3processor` writes
  Gorilla blocks, `GorillaQueryEngine` reads them back exactly)
- **Fix path**: point `accuracy_reduce.py` at the archive engine
  instead of the deleted JSONL layout. Out of scope for this demo
- **Workaround**: paper-quality accuracy numbers live in the headline
  60-cell sweep at `deploy/eval-results/headline-2026-05-06/accuracy.csv`,
  which uses a different ground-truth path

#### Gap 2 — freshness probe encoder offset (criterion ⑥)

- **Severity**: low-medium (mechanical bug; one-character patch staged
  on a follow-up branch)
- **Impact**: ⑥ freshness reads UNKNOWN; `freshness/*.csv` files contain
  zero rows (or rows with bogus deltas)
- **Root cause**: `opentelemetry-collector-contrib-patch/processor/gorillas3processor/encoder.go`
  writes the chunk-header timestamp at byte offset `[5..9]` instead of
  `[9..13]`. The consumer parses the wrong four bytes and sees zero
- **Fix path**: one-character offset patch staged on a follow-up
  branch; takes effect after `asap/sketchcol:dev` is rebuilt via OCB
- **Workaround**: none — freshness doesn't measure on this demo until
  the image is rebuilt

#### Gap 3 — backend image cache stickiness (operational)

- **Severity**: low (operational gotcha, not a correctness bug)
- **Impact**: rebuilds after backend PRs merge can produce the same
  image SHA, masking that the new code didn't actually land
- **Root cause**: Docker BuildKit caches Cargo build layers
  aggressively; the cache key doesn't always invalidate when a path-dep
  changes
- **Fix path**: pass `--no-cache` to `docker build` after any backend
  PR. Verify via the `strings | grep` snippet in §3
- **Workaround**: documented; users now know to verify

#### Out of scope for the demo (deferred)

- **Dynamic plan transitions** while the demo runs — controller plans
  once at startup. The `ReplannerOpampGateway` covers some of this
  in unit tests but isn't exercised by the MVP demo
- **OpAMP hot reconfig under churn** — not exercised
- **1M+ cardinality** — the demo runs at 5-10K aggregate, single host
- **Multi-host federation** — not designed for; single-host bench only
- **PromQL completeness on the archive tier** — current curated subset
  (Sum / Count / Avg / Min / Max / Rate / Increase + Quantile / TopK)
  covers the demo's queries but not full Prometheus parity. See
  `docs/design-jsonl-deprecation-and-gorilla-promql-completeness.md`
  Path A for the tracking direction. Step-2 of the JSONL deprecation
  promotes the archive blocks to Prometheus-TSDB block format so
  Thanos store-gateway can answer the long tail of PromQL natively

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
| `sketchcol` (OTel collector) | Go | `asap-precompute-go` | ✅ implemented + tested end-to-end (paired sweep) |
| `sketchotap` (OTAP-Dataflow) | Rust | `asap-precompute-rs` | ✅ implemented; cross-host byte-parity test green |
| `sketchtelegraf` (Telegraf input) | Go | `asap-precompute-go` | ✅ implemented; envelope byte-parity verified |

### Common precompute / chunk libraries

| Library | Used by | Status |
|---|---|---|
| `asap-precompute-go` | `sketchcol`, `sketchtelegraf` | ✅ implemented |
| `asap-precompute-rs` | `sketchotap`, ASAPQuery-backend ingest path | ✅ implemented |
| `asap-gorilla` (Rust encoder/decoder, postings, chunk index) | `gorillas3processor` (via Go-shim), backend `GorillaQueryEngine`, `gorilla-compactor` | ✅ implemented + tested (39 unit tests pass; cross-language byte parity with the Go gorillas3processor) |

### Sketch families (5 supported, byte-parity across runtimes)

| Family | Wire variant | Cross-runtime parity | Backend accumulator |
|---|---|---|---|
| DDSketch | `Metric.data = DDSketch` | ✅ | ✅ |
| KLL | `Metric.data = KLLSketch` | ✅ | ✅ |
| HLL | `Metric.data = HLLSketch` | ✅ | ✅ |
| CountSketch | `Metric.data = CountSketch` | ✅ | ✅ |
| Count-Min Sketch | `Metric.data = CountMinSketch` | ✅ | ✅ |

### Backend (`ASAPQuery-backend`)

| Component | Role | Status |
|---|---|---|
| `precompute_engine` binary | Receives sketch envelopes; serves PromQL HTTP | ✅ |
| `SimpleEngine` | Warm-tier query engine over sketch state | ✅ implemented + tested (33 PromQL pattern matchers) |
| `GorillaQueryEngine` | Archive-tier query engine over Gorilla chunks | ⚠️  curated PromQL subset implemented + tested (`sum / count / avg / min / max / rate / increase / quantile_over_time / topk`); full PromQL parity is open work — see `docs/design-jsonl-deprecation-and-gorilla-promql-completeness.md` |
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
| `gorilla-compactor` binary | Concat-only block consolidation (≥6 hourly blocks → 1 day-block) | ✅ implemented + tested (idempotent; partial-read verified) |

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
| ④ accuracy reducer | repoint `accuracy_reduce.py` at the Gorilla archive engine (Step-1 deleted the JSONL ground-truth path it used to read) | ❌ not implemented; ~1d follow-up |
| ⑥ freshness probe consumer | `gorillas3processor` chunk-header offset (`[5..9]` vs `[9..13]`) | ❌ encoder bug; one-character patch staged on a follow-up branch; takes effect after `asap/sketchcol:dev` rebuild |
| Image-cache stickiness | Backend Docker layer cache | ⚠️  operational gotcha; pass `--no-cache` |

### Planned (not yet implemented)

| Item | Tracking |
|---|---|
| ~~Delete `LocalFsColdStore` + JSONL gateway raw-tee + the `cost_model` cold-tier scan-bytes line item~~ | DONE (Step-1 of the JSONL deprecation — backend PR #95, collector PR #312) |
| Full PromQL parity on `GorillaQueryEngine` via the Step-2 promotion to Prometheus-TSDB block format + Thanos store-gateway as the archive query engine | `docs/design-jsonl-deprecation-and-gorilla-promql-completeness.md` |
| Repoint `accuracy_reduce.py` ground-truth lookup at the Gorilla archive engine (Step-1 deleted the JSONL path it used to read) | Same doc §"Open questions" |

## TL;DR

```bash
# from a clean machine with docker + cargo + Go installed
git clone -b main git@github.com:ProjectASAP/ASAPCollector.git ~/repos/ASAPCollector
git clone -b main git@github.com:ProjectASAP/ASAPQuery-backend.git ~/repos/ASAPQuery-backend
git clone -b main git@github.com:ProjectASAP/asap_sketchlib.git ~/repos/asap_sketchlib

cd ~/repos/ASAPCollector

# 1. Build the four dev images and the gorilla-compactor binary
make -C deploy build-images || bash deploy/scripts/build-all.sh   # see §3 for explicit commands
( cd compactor && cargo build --release )

# 2. Run the demo
COMPACTOR_BIN=$PWD/compactor/target/release/gorilla-compactor \
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
| Rust toolchain | 1.90+ | `gorilla-compactor`, backend image build, controller |
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
The agents under this profile load `sketchcol-agent-b0-prometheus.yaml`,
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
│  10 fake-exporter ──OTLP──▶ 2 sketchcol agents ──OTLP──▶ 1 gateway       │
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
│                                    - GorillaQueryEngine (archive)        │
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
     • sketchcol agents: CPU + RSS + net in/out
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
| ③ | Combined e2e resource | Σ(OTel agent + Prometheus CPU/RSS/disk) | Σ(sketchcol + gateway + backend + MinIO) | Z% |
| ④ | Accuracy | exact (raw samples in TSDB) | rel-err per query class within ε/δ envelope | bounded by sketch family |
| ⑤ | Cold-fallback for ad-hoc queries | Prometheus answers anything natively | `data_source: gorilla_archive` for ad-hoc / post-hoc / cardinality queries | qualitative PASS |
| ⑥ | Freshness | sample-to-query latency on TSDB ingest path | sample-to-query latency on warm + archive paths | per-path Δ |

### Three query classes the demo exercises

- **Window per series**: `quantile_over_time(0.99, latency[1m])`
- **Label aggregation at instant**: `sum by (zone) (http_requests_total)`
- **Combined**: `sum by (zone) (rate(http_requests_total[5m]))`

Plus an ad-hoc cold-fallback probe:
`count(http_requests_total{service="payments"})` — exercises the
Gorilla-archive engine on the ASAP side via dual-routing; on the
baseline side it's just another query Prometheus answers.

## 3. Compiling and building from source

There are five build artifacts. Build them in this order — the backend
image build context references the Rust crates that the earlier steps
exercise.

```
0. Clone the three sibling repos at ~/repos (one-time)
1. asap-precompute-rs            (cargo, Rust crate)         } compile-time
2. asap-gorilla                  (cargo, Rust crate)         }   sanity
3. controller binary             (cargo)
4. gorilla-compactor binary      (cargo)
5. asap/controller:dev           (Docker image)              } runtime
6. asap/sketchcol:dev            (OCB + Docker)              }   images
7. asap/fake-exporter:dev        (Docker image)              }
8. asap/query-backend:dev        (Docker image, multi-context)
```

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

### Step 4 — `gorilla-compactor` (used at demo Phase 7)

```bash
cd ~/repos/ASAPCollector/compactor
cargo build --release
cargo test --release  # 11 tests should pass
ls target/release/gorilla-compactor
```

If your `/` partition is small, redirect Cargo's target dir to a larger
volume:

```bash
export CARGO_TARGET_DIR=/data2/$USER/cargo-target
cd ~/repos/ASAPCollector/compactor && cargo build --release
ls $CARGO_TARGET_DIR/release/gorilla-compactor
```

The driver's `COMPACTOR_BIN` env var defaults to
`$REPO_ROOT/compactor/target/release/gorilla-compactor`; export
`COMPACTOR_BIN=$CARGO_TARGET_DIR/release/gorilla-compactor` if you
redirected.

### Step 5 — `asap/controller:dev` Docker image

```bash
cd ~/repos/ASAPCollector/controller
docker build -t asap/controller:dev .
docker image ls asap/controller:dev
```

### Step 6 — `asap/sketchcol:dev` (agent + gateway use the same image)

This is a two-step build: first OCB compiles the patched OpenTelemetry
Collector binary; then the Dockerfile packages it.

```bash
cd ~/repos/ASAPCollector

# OCB (OpenTelemetry Collector Builder) generates the binary by stitching
# together the patched processors listed in builder-config.yaml.
bash opentelemetry-collector-contrib-patch/cmd/sketchcollector/build.sh

# Wrap the binary in the runtime image
docker build \
    -t asap/sketchcol:dev \
    -f opentelemetry-collector-contrib-patch/cmd/sketchcollector/Dockerfile \
    .

docker image ls asap/sketchcol:dev
```

See `docs/design-asap-edge-framework.md` for OCB build details and
`builder-config.yaml` semantics.

### Step 7 — `asap/fake-exporter:dev` (with freshness probes)

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

### Step 8 — `asap/query-backend:dev` (multi-context Docker build)

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

The build runs `cargo build --release --bin precompute_engine` inside a
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
    'strings /usr/local/bin/precompute_engine | \
     grep -E "postings_filtered|gorilla_archive|s3_cost" | head -5'
```

If the grep returns nothing, the cache hit on a stale layer; rebuild
with `--no-cache`.

### One-shot build (all 5 images + 2 binaries)

Wrap the steps above in a script for repeatability:

```bash
cat > /tmp/asap-build-all.sh <<'BASH'
#!/usr/bin/env bash
set -euxo pipefail
cd ~/repos/ASAPCollector

# Rust crates (compile sanity + cache warm-up)
( cd asap-precompute-rs && cargo build --release )
( cd asap-gorilla       && cargo build --release )
( cd controller         && cargo build --release )
( cd compactor          && cargo build --release )

# Docker images
docker build -t asap/controller:dev controller/
bash opentelemetry-collector-contrib-patch/cmd/sketchcollector/build.sh
docker build -t asap/sketchcol:dev \
    -f opentelemetry-collector-contrib-patch/cmd/sketchcollector/Dockerfile .
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
export COMPACTOR_BIN=$PWD/compactor/target/release/gorilla-compactor
export OUT_BASE=$PWD/deploy/eval-results       # default
export PER_AGENT_CARDINALITY=500               # 500 × 10 producers = 5K aggregate
export NUM_PRODUCERS=10                         # spread across 2 agents (5 + 5)
export ASAP_SKETCH_FAMILY=ddsketch             # default; overridden per-metric by controller
export EXPORTER_FRESHNESS_PROBES=on            # emit timestamp-encoded probes

# Run the demo SYNCHRONOUSLY (foreground). Wall time: ~30-45 min.
bash deploy/scripts/run_mvp_demo.sh
```

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
| 0. Pre-flight | Verify images present; verify `gorilla-compactor` binary; clean stale containers |
| 1. Stack-up | `docker compose up` against `base.yml + mvp-multi-stage.yml`; mount `mvp-workload.yaml` into controller; wait for OpAMP push to settle |
| 2. Warm-up | 60 s agent warm-up + 30 s query-side warm-up (poll `count_over_time(http_requests_total[1m])` until non-zero) |
| 3. Measurements | Run `measure_stages.py` + `measure_per_edge_bandwidth.py` + `promql_replay.py` over 60 s soak with three query classes |
| 4. Freshness | Run `run_freshness_phase.sh` against three probes (raw / warm / archive) → 3 CSVs |
| 5. Ad-hoc queries | Fire label-predicate queries; capture postings filtering |
| 6. Cold-fallback | Fire `count(http_requests_total{service="payments"})`; verify `data_source: gorilla_archive` |
| 7. Compaction | `gorilla-compactor --threshold-hours 0 --threshold-count 0 --dry-run` then `--no-dry-run`; capture before/after object count + bytes |
| 8. Report | Run `mvp_report.py` over the captured CSVs to produce `MVP_REPORT.md` |

## 5. Reading the output

After the demo completes, the output directory tree is:

```
deploy/eval-results/mvp-current/
├── MVP_REPORT.md            ← human-readable report (start here)
├── ad-hoc/                     ← Phase 5 — ad-hoc query responses
│   ├── label-api.json
│   ├── label-status5xx.json
│   └── cold_payments.json      ← criterion ⑤: look for "data_source: gorilla_archive"
├── compactor/                  ← Phase 7
│   ├── compactor-plan.log      ← --dry-run output
│   └── compactor.log           ← actual compaction stats
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
| §2 | ⑤ cold-fallback | PASS — `data_source: gorilla_archive` in `ad-hoc/cold_payments.json` |
| §2 | ⑥ freshness | PASS — non-zero p50/p99 in `freshness/{raw,warm,archive}.csv` |
| §3 | per-class rel-err | non-empty for all three classes |
| §4 | postings | non-zero `series matched` rows; `would have scanned` ≥ matched |
| §5 | compaction | before > after on object count |
| §8 | controller emitter | STATUS = `live` |

## 7. Known issues — current state of the demo (2026-05-07)

The architecture works at the dispatch and planning layers. Two ingest-side
bugs surface as UNKNOWN/empty data even when the routing is correct.
These are documented in the §"Current status" verdict above:

### ④ accuracy reducer needs ground-truth dump

`accuracy_reduce.py` joins the replay client's per-query answers against a
ground-truth JSONL stream the backend is supposed to write at
`/var/asap/cold/raw/`. The current backend image doesn't write that stream,
so `accuracy.csv` lands empty even though the warm-tier engine returns
correct answers. **Fix**: backend-side raw-tee writer (out of scope for
the demo's driver/compose layer).

### ⑥ freshness probe encoder offset

The agent's `gorillas3processor.encoder.go` writes the per-chunk timestamp
header at byte offset `[5..9]` instead of `[9..13]`. The `last_over_time`
consumer parses the wrong four bytes and sees zero values, so freshness
deltas are computed against bogus emission timestamps and the CSVs come up
empty. **Fix**: a one-character offset patch staged on a follow-up branch;
takes effect after rebuilding `asap/sketchcol:dev` from the patched binary
via OCB.

### Backend image cache stickiness

`docker build` aggressively caches Cargo build layers. After backend
PRs merge, simple rebuilds can return the same image SHA even though the
source has changed. **Workaround**: pass `--no-cache` to `docker build`
when the postings or dual-routing features are missing from the running image (verify via the
`strings | grep` snippet in §3).

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
rm -rf ~/repos/ASAPCollector/compactor/target
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
| `freshness/{raw,warm,archive}.csv` empty | Probe encoder offset bug (§7) OR fake-exporter image lacks probes | Rebuild fake-exporter image; verify with the `grep -l` step in §3 |
| `accuracy.csv` empty | No ground-truth dump (§7) | Documented; out of demo scope |
| Demo agent dies at "stack settle" | Controller container not reachable; check `docker ps` and `docker compose logs controller` | Often a port collision; run `docker compose down -v` first |
| OOM kill during the soak | `PER_AGENT_CARDINALITY` too high for the host RAM budget | Lower to 250 or run on a 32 GB host |

## 10. Related runbooks and docs

- `docs/spec-mvp-controller-driven-multi-stage-demo.md` — MVP demo spec
- `docs/comparison-asap-vs-databricks-pantheon-hydra.md` — architectural framing
- `docs/design-gorilla-s3-cold-engine.md` — cold-engine wire format + module layout
- `docs/e2e-test-guide.md` — pytest-style smoke tests (smaller scope than the MVP demo)
- `docs/eval-instrumentation-notes.md` — measurement methodology notes
- `docs/control-plane-design.md` — controller pipeline (L1 → L5)
