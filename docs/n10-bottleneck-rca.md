# RCA: "N=10 throughput collapse" (PR #185) — not a bottleneck

_Written: 2026-04-23. Supersedes the bottleneck hypothesis in
the PR #185 commit message._

> **Status (2026-04-23, later the same day):** the diagnosis
> in this doc stands. The **Options A–D recommendation section
> below is superseded** by
> [`sdk-aggregation-cost.md`](sdk-aggregation-cost.md),
> which makes `raw-buffer` / `*-full` / `*-delta` distinct SDK
> aggregators (encoding axis), independent of time window `W`
> and label projection `L`. Options A / B of this doc reduce to
> "revert to a per-tick interval", which isn't a paper strategy —
> it's an accidental way to get near-per-sample emission.
> Option C (bypass SDK) and Option D (reframe as bytes-per-window)
> are both partially realized in the three-axis design:
> bytes-per-window is the metric; `raw-buffer` is the SDK-native
> "every sample" encoding (doesn't bypass the SDK, just picks a
> different aggregator). Read the diagnosis, skip the
> recommendation.

## TL;DR

The ~2,000 pts/s per-agent floor observed across all baselines
in `sweep-N10-20260422.csv` is **not** a scale bottleneck.
It is the correct export rate of the fake-exporter's OTel SDK
setup after PR #182 changed the `PeriodicReader` interval from
`1s / rate` (variable) to `1s` (fixed). At `cardinality=1000`
and two instruments (Counter + Gauge), `1000 × 2 × (1/1s) =
2000 data points/s` — independent of `EXPORTER_RATE`.

Fix is not a kernel / Docker / SDK tuning exercise. Fix is a
paper-story decision: what _should_ the fake-exporter emit, and
what does the §6.2 "bandwidth reduction" claim actually compare?

## Evidence

Rate-invariance test — N=1, b0a-raw-stream, held cardinality
constant, varied `EXPORTER_RATE` by 10×:

| EXPORTER_RATE | EXPORTER_CARDINALITY | `gateway_points_per_s` | `backend_samples_per_s` |
|---:|---:|---:|---:|
| 1,000 | 1,000 | 2,000 | 2,001 |
| 10,000 | 1,000 | 2,000 | 2,001 |

Both runs: single producer, 60s batch processor, 150s soak,
Prometheus `rate()` over 2m window. Gateway aggregate flat at
2k/s; 10× input-rate delta produces **zero** throughput delta.

Raw CSV: [`deploy/eval-results/n10-diagnosis/rate-invariance-20260423.csv`](../deploy/eval-results/n10-diagnosis/rate-invariance-20260423.csv).

## Root cause

`deploy/fake-exporter/main.go` — PeriodicReader interval.

Before PR #182 (git blame: #171/#175 era):

```go
reader := sdkmetric.NewPeriodicReader(exp,
    sdkmetric.WithInterval(time.Second/time.Duration(rate)))
```

After PR #182 (current):

```go
reader := sdkmetric.NewPeriodicReader(exp,
    sdkmetric.WithInterval(time.Second))
```

The "before" form set interval = `1s / rate`. At `rate=1000`
the SDK exported every 1 ms, so each tick carried ≤1 ms of
accumulated `counter.Add` / `gauge.Record` calls — effectively
one data point per attribute set per ms → ~2 M pts/s nominal,
bandwidth-bottlenecked down to the ~325 k observed in
`sweep-N1-20260422.csv`.

The "after" form set interval = `1s` fixed. Within each 1 s
tick, the OTel SDK pre-aggregates:

- `Counter.Add(…)` calls per attribute set are summed into a
  single `Sum` data point per attribute set per tick.
- `Gauge.Record(…)` calls per attribute set are reduced to the
  last-recorded value per attribute set per tick.

So regardless of how many `Add` / `Record` calls the ticker
fires per second, each tick emits exactly `cardinality ×
#instruments` data points (1000 × 2 = 2000). `1 tick/s × 2000
= 2000 pts/s`. This is the floor PR #185 saw.

The N=10 observation of `10 × 2k = 20k` aggregate is not
"coordinated throttling" — it is ten concurrent, independent
producers each emitting the correct pre-aggregated 2k each.

## Why PR #185's ruled-out causes were ruled out correctly

PR #185 correctly ruled out:

- **Agent saturation** (agents at 0.01c, 220 MiB) — right
  conclusion. The agents genuinely had nothing to do because
  the producer was pre-aggregating.
- **Per-baseline effect** (identical across raw and sketch) —
  right conclusion. The bottleneck was upstream of the pipeline.

The mislabel is in the _remaining_ candidates: OTLP SDK
backpressure, Docker userland-proxy, kernel socket buffers.
None of these are the cause. All three would manifest as
rate-sensitive degradation; the new rate-invariance test rules
all three out simultaneously.

## Implications for the paper

The §6.2 "M× bandwidth reduction (raw vs sketch)" claim is more
fragile than it looks. With the current fake-exporter the raw
baselines (B0a / B0b / B1) are **already SDK pre-aggregated**
at 2000 pts/s — they are not emitting raw samples. The
bandwidth difference between raw and sketch baselines in the
existing CSVs therefore measures:

- payload shape per exported data point (raw value vs
  serialized sketch),

**not** the paper's intended story of "raw emits every sample,
sketch emits one summary per window".

Concretely, in `sweep-N1-20260422.csv`:

- `b3-delta` @ N=1: `agent_in_kib_per_s = 16,462`, i.e. the
  sketch processor receives 16 MiB/s of pre-aggregated input
  from the SDK.
- `b2-full` @ N=1: `agent_out_kib_per_s = 167,701`, i.e. after
  the sketch processor serializes a full sketch per window.

Both are off the "truly raw sample stream" story that the
paper wants to tell. The "raw" side is not emitting every
sample — it's emitting a pre-aggregated sum per attribute set
per second.

## Options

Pick one before re-running any §6.2 sweep. Each has paper
implications.

### A. Revert PR #182's interval change, accept the old number

```go
reader := sdkmetric.NewPeriodicReader(exp,
    sdkmetric.WithInterval(time.Second/time.Duration(rate)))
```

- Pros: Brings back the 325 k N=1 number, restores the
  rate-sensitive throughput axis.
- Cons: At rate=1000 the SDK still pre-aggregates within each
  1 ms tick, but tick is small enough that each accumulated
  `Counter.Add` typically fires just once per tick per
  attribute set → emits ~1 data point per attribute set per
  ms → approximates one exported point per `Add`. Works for
  the paper but only accidentally. The trace-replay code path
  in PR #182 still wants the fixed 1 s interval, so this
  revert is a synthetic-only fix.

### B. Dual-interval: synthetic keeps `1s/rate`, trace-replay keeps `1s`

Guard with an `if traceFile != "" { … } else { … }` in
`main()`. Minimal behavioural change vs. pre-#182; restores
synthetic's rate-proportional emit; leaves trace replay
operating on its own tick.

- Pros: All existing N=1 baseline CSVs become reproducible
  again. Trace replay (the PR #182 feature) unaffected.
- Cons: Two code paths to maintain. Paper story still has the
  structural issue in (C) below when cardinality grows.

### C. Abandon OTel SDK aggregation for the "raw" baseline

The cleanest paper framing is: "B0 / B1 emit every sample;
B2 / B3 emit one summary per window." The OTel SDK does not
give us "every sample" for Counters or Gauges — the SDK
fundamentally aggregates. Options:

1. Emit raw samples via a plain OTLP gRPC client
   (`otlpmetricgrpc` under the hood), bypassing the SDK
   `MeterProvider` for the raw baseline only.
2. Use an OTel `Exponential Histogram` or per-sample
   counter-with-exemplars path; exemplars survive SDK
   aggregation.
3. Write samples as OTel _logs_ with structured fields and
   treat the raw baseline as a log-shipping workload. (Honest
   to production behaviour; breaks PromQL compatibility.)

- Pros: Paper story matches code. "M× reduction" claim is
  defensible.
- Cons: Bigger code change. Raw-baseline bandwidth numbers
  will shift materially — likely up, maybe toward the
  paper-motivating direction.

### D. Accept current behaviour, rewrite the paper framing

Reframe B0 / B1 as "SDK-aggregated raw" (one data point per
attribute set per scrape interval). This matches real-world
Prometheus behaviour (scrape at 15 s → one data point per
series per 15 s), so it is defensible.

- Pros: No code change. Matches production patterns.
- Cons: "M× reduction" claim loses most of its force because
  both sides are already aggregated; bandwidth difference
  reduces to "raw value + labels" vs "sketch blob". Need to
  frame §6.2 as "bytes per aggregated sample", not "throughput
  reduction". Changes the §6.7 N-scale story: per-agent rate
  is fundamentally `cardinality × (1 / scrape_interval)`, not
  load-driven.

## Recommendation

**Option B + Option D combined**:

- Land B as a fake-exporter code change (dual-interval) so the
  pre-#182 N=1 sweep CSVs are reproducible for backward
  comparison.
- Adopt D for the paper framing. Measure **bytes per window**
  rather than points/s. The §6.2 story becomes "compressed
  aggregated bytes" (sketch) vs "raw aggregated bytes" (B0 /
  B1), which matches the real Prometheus pattern.
- Do not pursue C right now — the code churn isn't worth the
  paper-claim upgrade if D is acceptable, and C needs to be
  weighed against the reviewer-facing "why is your raw baseline
  not the standard Prometheus scrape baseline".

This means the remaining `nan` columns in
`sweep-N10-20260422.csv` still need filling (instrumentation
gap, independent of this RCA), but the "N=10 throughput
collapse" line in `DataCollector/TODO.md §1` can be closed.

## Follow-up actions

1. ~~Diagnose with variant sweeps (host network, SDK knobs,
   kernel buffers)~~ — skipped; rate-invariance test proved
   none of those are the cause.
2. Decide on Options A–D above with the user.
3. Depending on decision: implement B, run one N=1 + one N=10
   sweep to re-establish baselines.
4. Update `DataCollector/TODO.md §1` — close the "N=10
   throughput collapse" bullet, replace with "decide §6.2
   framing" bullet referencing this doc.
5. Update `paper-outline.md` experiment table — "throughput"
   claim becomes "bandwidth (bytes/window)" if going with D.
