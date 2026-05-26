# Phase 2.11B — Deployment Performance: pre-shim vs post-shim

Companion to `docs/phase-2-perf-bench-go.md` (Phase 2.11A, PR #236). The
A path closed ADR-0002 §"Performance contract" at the
microbenchmark level (`Precompute.Observe` ns/op). This doc reports the
deployment-level confirmation: what the existing docker-compose
b3-delta harness measures end-to-end, and whether those numbers move
materially between commit `6b3258d` (pre-shim, last commit before the
5 shim PRs landed) and `c86a62c` (post-shim, HEAD of `origin/main`).

The aim is not a fresh measurement framework — it's a sanity check on
the harness we already ship, so a future reader can see that the shim
extraction (PRs #226–#232) didn't blow up the deployed agent's
throughput, RSS, or per-window output bytes.

## Setup

### Hardware / toolchain

- CPU: AMD Ryzen Threadripper PRO 5955WX (32 logical cores)
- RAM: 440 GiB (essentially unconstrained for this stack)
- OS: Linux 5.15 (Ubuntu 20.04 kernel)
- Go: `go1.25.3 linux/amd64`
- Docker: 28.1.1
- Other tenants on the host: an Elasticsearch + Kibana dev stack
  (idle, healthcheck-only). Not isolated, so absolute numbers carry
  some noise.

### Stack

`deploy/mvp-singlenode/docker-compose/baseline-b3-delta.yml` over the shared `base.yml`
+ `agents-N1.yml` overlay. B3-delta is the "delta sketch transmission,
60 s window" baseline; it's the same combination Phase 2.11A's micro
results care about, since the shim sits in the agent processor pipeline
that this baseline exercises.

Workload knobs (defaults from `base.yml`):

- `-freq-hz=10` — 10 Hz event rate from the synthetic producer
- `-cardinality=1000` — 1000 active series
- `-sdk-window=15s` — SDK aggregation window
- `-agg=default` — Sum / LastValue per metric kind

One agent (N1), one gateway, one backend. No load-gen client; the
otel-app is the only writer.

## Methodology

The harness has two relevant scripts:

- `deploy/mvp-singlenode/scripts/measure-baseline.py` — instant Prometheus query for
  per-tier CPU / RSS / point rate / output bytes, plus a
  `docker stats` two-sample window for backend + producer numbers
  (script supplements Prom because cAdvisor isn't in the stack).
- `deploy/mvp-singlenode/scripts/run-baseline-sweep.sh` — orchestrator that brings the
  stack up, soaks for `SOAK_S` seconds, then invokes
  `measure-baseline.py`. We do not use the sweep wrapper here because
  the goal is one stack soak per commit, not the
  baseline × rate × cardinality matrix.

### Commit handling

`6b3258d` predates PR #231 ("wire asap-precompute-go replace
directive") but the pre-shim binary doesn't import `asap-precompute-go`
at all (the shim-extraction PRs are precisely what introduced that
dependency), so no local fix was needed for the OCB build to succeed.
The only environmental fixup was a symlink `/tmp/sketchlib-go ->
/home/zeying/repos/sketchlib-go`, because the OCB-emitted go.mod uses
`../../../../sketchlib-go` from the build dir at
`/tmp/preshim-worktree/.../cmd/asap-otel/`. Both fixups are
build-host-local — nothing was committed.

### Procedure

For each commit:

1. `git worktree add` at the commit, init submodules, run
   `./build_asap_otel.sh` to produce a fresh
   `asap-otel` binary.
2. Copy that binary into the main repo's
   `opentelemetry-collector-contrib-patch/cmd/asap-otel/` and
   `docker build -f deploy/docker/Dockerfile.asap-otel`. Tag
   appropriately, swap onto `:dev`, then
   `docker compose ... up -d --force-recreate agent-1 gateway` so only
   the agent + gateway tier get re-imaged. Producer / backend /
   controller / Prom / MinIO stay continuously up, which removes a
   source of cross-run drift.
3. Soak ≥ 200 s (≥ 3 windows of the 60 s delta cycle, so
   `rate(...[2m])` sees ≥ 2 samples — required by the harness).
4. Take 2 readings ≥ 60 s apart with
   `measure-baseline.py --window 2m --bytes-sample-window 10`; report
   the mean of the two.

## Results

Two readings per commit. Numbers in the table are the **mean** of the
two samples. Raw CSV in `/tmp/perf-2-11b/{preshim,postshim}.csv` on
the build host.

### Single-sample raw values

```
b3-delta-preshim,    agent_cpu=0.003 cores, agent_rss=288.3 MiB, agent_in=26.40 KiB/s, agent_out=4.69 KiB/s, agent_pts=133.3 /s
b3-delta-preshim-2,  agent_cpu=0.002 cores, agent_rss=293.9 MiB, agent_in=25.08 KiB/s, agent_out=2.35 KiB/s, agent_pts=133.3 /s
b3-delta-postshim,   agent_cpu=0.003 cores, agent_rss=302.6 MiB, agent_in=26.41 KiB/s, agent_out=4.70 KiB/s, agent_pts=133.3 /s
b3-delta-postshim-2, agent_cpu=0.002 cores, agent_rss=302.6 MiB, agent_in=25.08 KiB/s, agent_out=2.35 KiB/s, agent_pts=133.3 /s
```

### Comparison table

| Metric                  | Pre-shim (6b3258d) | Post-shim (c86a62c) | Δ (post − pre) | Δ %    | Verdict |
|-------------------------|--------------------|---------------------|---------------:|-------:|---------|
| agent_cpu_cores         | 0.0025             | 0.0025              |       +0.0000  |   0.0% | pass    |
| agent_rss_mib           | 291.1              | 302.6               |        +11.5   |  +4.0% | pass    |
| agent_in_kib_per_s      | 25.74              | 25.74               |        +0.00   |   0.0% | pass    |
| agent_out_kib_per_s     | 3.52               | 3.52                |        +0.00   |   0.0% | pass    |
| agent_points_per_s      | 133.3              | 133.3               |        +0.0    |   0.0% | pass    |

Throughput, input bytes, output bytes, and CPU are essentially
identical — the in/out/points columns match to three significant
figures because the workload is producer-paced (10 Hz × 1000
cardinality) and well below saturation; the agent is so far below
its capacity that the shim's extra method-call hop doesn't show up
as a CPU delta at all.

The 4% RSS bump is the only directional change. It is consistent
with the shim's explicit `Precompute` runtime structure (snapshot
cache, per-source state map) being slightly fatter than the inlined
processor state it replaced. ADR-0002 doesn't gate on RSS, but a
4% bump on a 290 MiB agent footprint is well inside what would be
considered a non-regression — the larger agent_rss drivers
(sketchlib-go DDSketch buffers, OTel runtime) are roughly 10×
larger.

### Producer / backend rows (informational)

| Metric                  | Pre-shim sample mean | Post-shim sample mean | Notes                                    |
|-------------------------|----------------------|-----------------------|------------------------------------------|
| producer_cpu_cores      | 0.061                | 0.071                 | producer container un-restarted; 4-h-old |
| producer_rss_mib        | 56.9                 | 75.5                  | (same; runtime drift, not shim)          |
| backend_cpu_pct         | 0.01                 | 0.94                  | backend never restarted                  |
| backend_rss_mib         | 163.6                | 134.0                 | (same; GC noise, not shim)               |

The producer + backend containers were intentionally **not** restarted
between pre-shim and post-shim measurement — only the agent + gateway
were re-imaged. So these rows compare two snapshots of the *same*
running container hours apart, which is just the runtime's heap / GC
drift over time. They are recorded for completeness but do not say
anything about the shim. Counterintuitively, the post-shim
`backend_rss_mib` is *lower* than pre-shim — that's because the
post-shim row was captured first (after 4 h of soak), the pre-shim
row 9 minutes later; RSS difference between two snapshots of the
unchanged backend container is just GC-cycle noise.

### Gateway + backend Prom rows: NaN

`gateway_cpu_cores`, `gateway_rss_mib`, `gateway_points_per_s`,
`gateway_out_series_per_s`, `backend_samples_per_s`,
`backend_query_p99_ms` are all NaN in the CSV — see "Gaps" below.
Same NaN pattern in pre-shim and post-shim, so the comparison still
holds for the rows that do populate.

## Verdict

**Phase 2.11A** (PR #236) confirmed the per-observation gate at the
microbenchmark level: pre vs post-shim `Precompute.Observe` p99 within
the ADR-0002 10% tolerance for all five sketches.

**Phase 2.11B** (this doc) confirms the deployment-level non-regression:
on the b3-delta harness, every shim-affected metric — agent CPU, in /
out KiB/s, throughput — is within run-to-run noise of pre-shim. RSS
moves +4% which is well inside any reasonable tolerance and explained
by the explicit shim runtime structure replacing inlined state.

Together Phase 2.11A and 2.11B close ADR-0002 §"Performance contract"
with both micro and deployment-level confirmation.

### Caveats

- **Single host, single-machine docker noise.** Two-sample mean for
  each metric, but only one stack soak per commit. Run-to-run variance
  in `agent_out_kib_per_s` is intrinsic to the 60 s delta window —
  `rate()` over 2 m sees 2–3 emissions, so 30–50% jitter on that
  column within a single stable run is normal (4.7 → 2.4 KiB/s
  between samples 60 s apart, identical between commits).
- **Shared host.** A separate Elasticsearch dev stack ran during
  measurement (idle but resident); not isolated to a cgroup boundary.
- **Producer-paced workload.** At 1000 cardinality × 10 Hz the agent
  CPU is ~ 0.0025 cores — three orders of magnitude below saturation.
  This deployment audit confirms there's no *new* overhead, but does
  not stress the shim. A higher-cardinality stress test (e.g. 1e5
  cardinality × 100 Hz) would be more discriminating; see "Gaps"
  for why we don't run it here.
- **No cold-store or query traffic.** This soak measured ingest only;
  `backend_samples_per_s` and `backend_query_p99_ms` are NaN because
  no PromQL replay client ran. The end-to-end query path is exercised
  by `run_e2e_sweep.sh` which is much more expensive (5 sketch
  families × 12 cells × ≥ 2 min each ≥ 2 h wall-clock) and out of
  scope for this audit.
- **No `-race`, no profiling overhead.** Plain release build via
  `Dockerfile.asap-otel`.

## Gaps in the existing harness

The four gaps that came up while running the Phase 2.11B audit have
been triaged below. Each is annotated with the resolution from PR
#246 (`fix(perf-harness): close 4 gaps from Phase 2.11B deployment
perf run`); two more harness gaps that surfaced separately are
listed at the end as standing follow-ups.

1. **Gateway metric-name skew — FIXED in PR #246.**
   `measure-baseline.py` was written when the gateway was on otelcol
   v0.108 (no `_total` suffix on process counters). The current
   gateway image is v0.141 (matches the agent), so
   `gateway_cpu_cores` / `gateway_rss_mib` / `gateway_points_per_s` /
   `gateway_out_series_per_s` all returned NaN against today's
   stack. Fix: each gateway query is now `<v0.141 name> or <v0.108
   name>`, so the script keeps producing rows whether the gateway
   image is current or a legacy worktree replay.

2. **Backend `/metrics` is empty under ingest-only — DOCUMENTED in
   PR #246, deferred as a design-level concern.** The backend's
   `asap_ingest_samples_total` and `asap_query_duration_seconds_bucket`
   only get populated when PromQL query traffic flows; an ingest-only
   soak (this audit, `run-baseline-sweep.sh`'s default) leaves both
   at NaN. This is *not* a query-string bug — the metrics genuinely
   don't exist under ingest-only operation, so editing
   `measure-baseline.py` won't help.

   Closing the gap properly requires either:

   - **Replay path on the harness side.** Add an opt-in MetricsQL
     replay client (the existing `deploy/mvp-singlenode/scripts/metricsql_replay.py`
     primitives are a starting point) that the sweep wrapper drives
     before the measurement window. This is its own feature with
     its own design questions (which queries to replay, at what
     rate, on which sketch families) and is out of scope for a
     harness-fixes PR.
   - **Synthetic ingest-side counter on the backend.** The backend
     could expose an `asap_ingest_envelopes_total` counter that
     fires regardless of whether query traffic ran. That's a
     backend code change, also out of scope for a Collector-side
     harness PR.

   Because the harness can't synthesize these metrics by itself,
   PR #246 only updates the docstring on the `backend_samples_per_s`
   / `backend_query_p99_ms` query templates to mark them as
   "requires query traffic"; the operator now sees in-script why
   the column is blank. The deeper "ingest-only vs ingest+query
   soak" mode distinction is tracked as a follow-up item; it
   belongs in a `run-baseline-sweep.sh` redesign, not a one-shot
   query-template fix.

3. **No per-observation latency emission from the deployed shim —
   FIXED in PR #246 (DDSketch only) + follow-up.** ADR-0002's
   binding metric is per-observation `Observe` p99, which the
   deployed asap-otel previously didn't expose as a Prom
   histogram (Phase 2.11A measured it in `testing.B` only).

   Resolution:

   - **Runtime.** `asap-precompute-go` now exposes a
     `LatencyObserver func(d time.Duration)` hook installed via
     `Precompute.SetLatencyObserver`. The hook fires once per
     `Observe` call (success, ErrSeriesCapExceeded, ErrLateData,
     and matcher-miss all time), giving the deployed shim the same
     envelope `testing.B` measures. Nil-safe at the hot path
     (atomic-pointer load + nil check).
   - **DDSketch shim wiring.** `ddsketchprocessor.enableSelfMonitoring`
     constructs a `Float64Histogram` named
     `asap_processor_observe_seconds` with bucket boundaries
     spanning 50 ns – 10 ms (covers the 80–500 ns/op post-shim
     micro envelope plus tail). Each per-metric Precompute spawned
     via `getOrCreate` picks up the histogram via
     `proc.recordObserveLatency`. The histogram appears on the
     gateway / agent `/metrics` endpoint when
     `EnableSelfMonitoring=true` (the production default).
   - **Other 4 shims (KLL, HLL, CountSketch, CountMin) — follow-up.**
     The runtime change is fully backwards-compatible: shims that
     don't call `SetLatencyObserver` lose nothing. Wiring the
     histogram into the remaining four processors is a mechanical
     copy of the DDSketch monitor.go diff; pulled out of this PR
     to keep the diff focused per the PR-scope constraint. Tracked
     as **Phase 2.11C**.

4. **No direct sketch-payload-bytes metric — STANDING.**
   `agent_out_kib_per_s` is the OTel-collector-level processor
   output bytes, which conflates delta-encoded sketch payload bytes
   with envelope metadata. The B3-delta savings claim requires
   distinguishing the two; this is visible in
   `gateway_out_series_per_s` minus a B0a (raw stream) reference,
   but the delta isn't a single column. A
   `asap_sketch_payload_bytes_per_window` counter on the processor
   would close this gap. Not addressed in PR #246.

5. **Legacy rate knob removed.** The old per-second rate knob was a
   no-op under SDK aggregation and has been dropped entirely; the
   workload is paced by `-freq-hz` and flushed by `-sdk-window`.
   The sweeps no longer carry the dead dimension.

6. **Producer-paced workload caps the discriminating power — FIXED
   in PR #246.** At cardinality 1000 × 10 Hz the agent ran at
   ~0.25% of one core so CPU diffs were dominated by measurement
   noise. `baseline-b3-delta.yml` now overrides
   `-cardinality` and `-freq-hz` to 1e5 × 100 Hz,
   chosen to land the agent in the 50–70% one-core band on
   reference hardware (Threadripper PRO 5955WX as described in
   the "Hardware" section). The override still honours host-env
   shadowing — set `OTELAPP_CARDINALITY=1000`
   on the host to recover the legacy quiet profile for ad-hoc work.

   Re-running the pre-shim vs post-shim comparison under the new
   profile is its own measurement and is **not** included in this
   PR; the PR only updates the harness so the next operator who
   runs the sweep sees CPU-cores deltas instead of measurement
   noise. The numerical re-baselining belongs in a Phase 2.11C
   "saturating-load comparison" doc.

## Reproduction

The recipe used to produce the numbers above:

```
# build pre-shim binary in a worktree (sibling sketchlib-go must exist)
git worktree add /tmp/preshim-worktree 6b3258d
cd /tmp/preshim-worktree
git submodule update --init --recursive opentelemetry-collector \
  opentelemetry-collector-contrib opentelemetry-proto
ln -sfn /home/zeying/repos/sketchlib-go /tmp/sketchlib-go
GOPRIVATE='github.com/ProjectASAP/*' bash build_asap_otel.sh

# build pre-shim docker image
cp /tmp/preshim-worktree/opentelemetry-collector-contrib-patch/cmd/asap-otel/asap-otel \
   $REPO/opentelemetry-collector-contrib-patch/cmd/asap-otel/
cd $REPO
docker build -f deploy/docker/Dockerfile.asap-otel -t asap/asap-otel:preshim .

# swap onto :dev tag, recreate just agent + gateway, soak, measure
docker tag asap/asap-otel:dev asap/asap-otel:postshim-saved
docker tag asap/asap-otel:preshim asap/asap-otel:dev
cd $REPO/deploy/docker-compose
AGENT_CONFIG=asap-otel-agent-b3-delta.yaml docker compose \
  -f base.yml -f agents-N1.yml -f baseline-b3-delta.yml \
  up -d --no-deps --force-recreate agent-1 gateway
sleep 200  # 2 m for rate window + 80 s margin
python3 $REPO/deploy/mvp-singlenode/scripts/measure-baseline.py \
  --baseline b3-delta-preshim --scale N1 --rate 1000 --cardinality 1000 \
  --window 2m --bytes-sample-window 10
sleep 60
python3 $REPO/deploy/mvp-singlenode/scripts/measure-baseline.py \
  --baseline b3-delta-preshim-2 --scale N1 --rate 1000 --cardinality 1000 \
  --window 2m --bytes-sample-window 10

# restore post-shim and re-measure (or just keep the prior post-shim numbers)
docker tag asap/asap-otel:postshim-saved asap/asap-otel:dev
docker compose ... up -d --no-deps --force-recreate agent-1 gateway
# (etc.)
```
