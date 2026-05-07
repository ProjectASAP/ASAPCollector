# MVP demo runbook — issue #46

Step-by-step instructions for running the ASAP MVP demo on a single host.
The demo exercises the controller-planned, multi-stage, sketch + Gorilla-S3
pipeline against three canonical PromQL query classes and emits a
`MVP_REPORT_v6.md` with measured numbers per criterion.

This runbook describes the **v7-state demo** (the current head of `main` as
of 2026-05-07). Architectural background lives in
[`docs/spec-mvp-v6-controller-driven-multi-stage-demo.md`](spec-mvp-v6-controller-driven-multi-stage-demo.md);
the comparison to Databricks Pantheon+Hydra is in
[`docs/comparison-asap-vs-databricks-pantheon-hydra.md`](comparison-asap-vs-databricks-pantheon-hydra.md).

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
bash deploy/scripts/run_mvp_demo_v6.sh

# 3. Read the report
cat deploy/eval-results/mvp-v6-2026-05-06/MVP_REPORT_v6.md
```

Wall time: ~30-45 minutes for the demo run; ~10-15 minutes for the
one-time image builds.

## 1. Prerequisites

### Software

- **Docker** with BuildKit (`DOCKER_BUILDKIT=1`) and Compose v2
  (`docker compose ...`, not `docker-compose`).
- **Rust** 1.90+ (for `gorilla-compactor` and the backend image build).
- **Go** 1.22+ (only required if you re-build the OTel collector binaries
  via OCB — most users use the pre-built `asap/sketchcol:dev` image).
- **Python** 3.10+ with `requests`, `pyyaml` (for the
  driver's measurement scripts).
- ~16 GB RAM, ~30 GB free disk for the multi-baseline run.

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

### Existing eval-results subtrees

The repository ships with prior runs in `deploy/eval-results/`. Don't
delete or commit over these — they're durable evidence pointed at from
the paper:

```
deploy/eval-results/
├── headline-2026-05-06/        ← 60-cell paired sweep (paper-quality)
├── headline-2026-05-06-postfix/ ← post-fix subset re-runs
├── mvp-2026-05-06/              ← v3 / v4 single-cell MVPs
├── mvp-v4-2026-05-06/           ← v4 4-baseline MVP
├── mvp-v5-2026-05-06/           ← v5 postings + compactor MVP
├── mvp-v6-2026-05-06/           ← v6 controller-driven multi-stage MVP
└── mvp-v6-1-2026-05-06/         ← v6.1 fix-and-rerun
```

The driver writes its output to `deploy/eval-results/mvp-v6-2026-05-06/`
by default; override via `OUT_BASE` env var.

## 2. Architecture summary (just enough to read the runbook)

```
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│  10 fake-exporter ─OTLP─▶ 2 agents ─OTLP─▶ 1 gateway ─OTLP─▶ 1 backend  │
│   (1000 series each)      (sketch processors)  (sketch-merge)            │
│                                                                          │
│                                            └─Gorilla─▶ MinIO/S3          │
│                                                                          │
│  controller — plans (sketch family + stage placement) per metric         │
│   from mvp-v6-workload.yaml; emits per-runtime configs via OpAMP.        │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

Three query classes the demo exercises:
- **Window per series**: `quantile_over_time(0.99, latency[1m])`
- **Label aggregation at instant**: `sum by (zone) (http_requests_total)`
- **Combined**: `sum by (zone) (rate(http_requests_total[5m]))`

Plus an ad-hoc cold-fallback probe: `count(http_requests_total{service="payments"})`
that exercises the Gorilla-archive engine via the dual-routing table.

## 3. Building images and the compactor binary

### `asap/controller:dev`

```bash
cd ~/repos/ASAPCollector/controller
docker build -t asap/controller:dev .
```

### `asap/sketchcol:dev` (agent + gateway use the same image)

```bash
cd ~/repos/ASAPCollector
bash opentelemetry-collector-contrib-patch/cmd/sketchcollector/build.sh
docker build -t asap/sketchcol:dev -f opentelemetry-collector-contrib-patch/cmd/sketchcollector/Dockerfile .
```

(The OCB build produces the binary; the Dockerfile wraps it. See
`docs/design-asap-edge-framework.md` for OCB details.)

### `asap/fake-exporter:dev` (with freshness probes)

```bash
cd ~/repos/ASAPCollector/deploy/fake-exporter
docker build -t asap/fake-exporter:dev .
# Verify the freshness probe metrics are baked in:
docker run --rm asap/fake-exporter:dev grep -l http_freshness_probe_raw probes.go || \
    echo "WARNING: image does not include freshness probes; rebuild after #299"
```

### `asap/query-backend:dev`

The backend image needs all four sibling repos as build contexts:

```bash
cd ~/repos/ASAPCollector
DOCKER_BUILDKIT=1 docker build -f deploy/docker/Dockerfile.backend \
    --build-context backend-src=$HOME/repos/ASAPQuery-backend \
    --build-context asap-precompute-rs=$PWD/asap-precompute-rs \
    --build-context asap-sketchlib=$HOME/repos/asap_sketchlib \
    --build-context asap-gorilla=$PWD/asap-gorilla \
    -t asap/query-backend:dev .
```

If you're seeing stale-image symptoms (e.g.\ no `gorilla_archive` marker
in cold-fallback responses, no `postings_filtered_series_count` field),
add `--no-cache` to bust the layer cache:

```bash
DOCKER_BUILDKIT=1 docker build --no-cache -f deploy/docker/Dockerfile.backend ... -t asap/query-backend:dev .
```

Verify the image has the v5+v7 features:

```bash
docker run --rm asap/query-backend:dev sh -c \
    'strings /usr/local/bin/precompute_engine | grep -E "postings_filtered|gorilla_archive|s3_cost" | head -5'
```

### `gorilla-compactor` (Rust binary, runs outside the Docker stack)

```bash
cd ~/repos/ASAPCollector/compactor
cargo build --release
ls -la target/release/gorilla-compactor
# (or set CARGO_TARGET_DIR=/data2/zeying/cargo-target if your / partition is small)
```

The driver's `COMPACTOR_BIN` env var defaults to
`$REPO_ROOT/compactor/target/release/gorilla-compactor`. Override if you
built elsewhere.

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
bash deploy/scripts/run_mvp_demo_v6.sh
```

If you want to run it in the background and watch from another shell:

```bash
nohup bash deploy/scripts/run_mvp_demo_v6.sh > /tmp/mvp-v6-run.log 2>&1 &
echo $! > /tmp/mvp-v6.pid

# poll for completion (blocks until report file appears OR demo PID exits)
until [ -f deploy/eval-results/mvp-v6-2026-05-06/MVP_REPORT_v6.md ] || \
      ! ps -p $(cat /tmp/mvp-v6.pid) > /dev/null 2>&1; do sleep 30; done
echo "demo done"
```

### What the demo does, phase by phase

The driver runs eight phases (see `deploy/scripts/run_mvp_demo_v6.sh` for
the implementation):

| Phase | Action |
|---|---|
| 0. Pre-flight | Verify images present; verify `gorilla-compactor` binary; clean stale containers |
| 1. Stack-up | `docker compose up` against `base.yml + mvp-v6-multi-stage.yml`; mount `mvp-v6-workload.yaml` into controller; wait for OpAMP push to settle |
| 2. Warm-up | 60 s agent warm-up + 30 s query-side warm-up (poll `count_over_time(http_requests_total[1m])` until non-zero) |
| 3. Measurements | Run `measure_stages.py` + `measure_per_edge_bandwidth.py` + `promql_replay.py` over 60 s soak with three query classes |
| 4. Freshness | Run `run_freshness_phase.sh` against three probes (raw / warm / archive) → 3 CSVs |
| 5. Ad-hoc queries | Fire label-predicate queries; capture postings filtering |
| 6. Cold-fallback | Fire `count(http_requests_total{service="payments"})`; verify `data_source: gorilla_archive` |
| 7. Compaction | `gorilla-compactor --threshold-hours 0 --threshold-count 0 --dry-run` then `--no-dry-run`; capture before/after object count + bytes |
| 8. Report | Run `mvp_report_v6.py` over the captured CSVs to produce `MVP_REPORT_v6.md` |

## 5. Reading the output

After the demo completes, the output directory tree is:

```
deploy/eval-results/mvp-v6-2026-05-06/
├── MVP_REPORT_v6.md            ← human-readable report (start here)
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

`MVP_REPORT_v6.md` itself contains:

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

| § | Criterion | Expected on a clean v7+ run |
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
These are documented honestly in the v7 issue-#46 comment:

### ④ accuracy reducer needs ground-truth dump

`accuracy_reduce.py` joins the replay client's per-query answers against a
ground-truth JSONL stream the backend is supposed to write at
`/var/asap/cold/raw/`. The current backend image doesn't write that stream,
so `accuracy.csv` lands empty even though the warm-tier engine returns
correct answers. **Fix**: backend-side raw-tee writer (out of scope for the
v6 driver/compose layer).

### ⑥ freshness probe encoder offset

The agent's `gorillas3processor.encoder.go` writes the per-chunk timestamp
header at byte offset `[5..9]` instead of `[9..13]`. The `last_over_time`
consumer parses the wrong four bytes and sees zero values, so freshness
deltas are computed against bogus emission timestamps and the CSVs come up
empty. **Fix**: a one-character offset patch staged on a follow-up branch;
takes effect after rebuilding `asap/sketchcol:dev` from the patched binary
via OCB.

### Backend image cache stickiness

`docker build` aggressively caches Cargo build layers. After v5+ backend
PRs merged, simple rebuilds returned the same image SHA even though the
source had changed. **Workaround**: pass `--no-cache` to `docker build`
when the v5/v7 features are missing from the running image (verify via the
`strings | grep` snippet in §3).

## 8. Cleanup

```bash
cd ~/repos/ASAPCollector
docker compose -f deploy/docker-compose/base.yml \
               -f deploy/docker-compose/mvp-v6-multi-stage.yml \
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
| `MVP_REPORT_v6.md` says §8 STATUS = `not-exercised` | `USE_TYPED_STAGE_SPLIT` not propagating | Check `docker exec controller env \| grep USE_TYPED`; re-export at the host shell |
| `freshness/{raw,warm,archive}.csv` empty | Probe encoder offset bug (§7) OR fake-exporter image lacks probes | Rebuild fake-exporter image; verify with the `grep -l` step in §3 |
| `accuracy.csv` empty | No ground-truth dump (§7) | Documented; out of v6/v7 scope |
| Demo agent dies at "stack settle" | Controller container not reachable; check `docker ps` and `docker compose logs controller` | Often a port collision; run `docker compose down -v` first |
| OOM kill during the soak | `PER_AGENT_CARDINALITY` too high for the host RAM budget | Lower to 250 or run on a 32 GB host |

## 10. Reproducing the historical reports

The v3, v4, v5, v6, v6.1, and v7 reports are all preserved in
`deploy/eval-results/`. Each has its own `MVP_REPORT_*.md` and CSVs. To
re-run any historical version, check out the corresponding tag/branch and
run that branch's driver — the topology, knobs, and report format have
shifted across versions:

```bash
git log --oneline --grep "mvp v" deploy/scripts/run_mvp_demo*.sh
```

Or just look at the issue-#46 comment thread on GitHub:
https://github.com/ProjectASAP/ASAPCollector/issues/46

Each comment posts the corresponding `MVP_REPORT_v*.md` verbatim.

## 11. Related runbooks and docs

- `docs/spec-mvp-v6-controller-driven-multi-stage-demo.md` — full v6 spec
- `docs/comparison-asap-vs-databricks-pantheon-hydra.md` — architectural framing
- `docs/design-gorilla-s3-cold-engine.md` — cold-engine wire format + module layout
- `docs/e2e-test-guide.md` — pytest-style smoke tests (smaller scope than the MVP demo)
- `docs/eval-instrumentation-notes.md` — measurement methodology notes
- `docs/control-plane-design.md` — controller pipeline (L1 → L5)
