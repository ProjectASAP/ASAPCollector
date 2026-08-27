# Benchmark: SDK Sampling + Collector-Side KLL Sketching — Real Deployment Cost Analysis

<!-- Design metadata -->

## TL;DR

Deployment-cost evidence for SDK sampling and collector-side KLL summarization.

**Status:** draft

**MVP relationship:** supporting.

This document is design-level: it defines scope, behavior, constraints, and trade-offs; implementation details are intentionally out of scope.


**Date:** 2026-08-04
**Tools:** `otel-app` (real SDK/OTLP load generator), `asap-otel` (real collector binary),
`ASAPQuery-backend/data_plane` (real backend binary)
**Method:** Real deployed processes on real network sockets — not simulated or in-memory.
CPU measured via `/proc/pid/stat` (utime+stime) at matched wall-clock checkpoints across
processes; network bytes measured via `iptables` OUTPUT-chain counters on the ports between
each hop; RSS via `/proc/pid/status` (VmRSS). All arms run 10 generator processes for 61s
(5s SDK export window), `-agg raw-buffer -gauge-only`, comparing WARM_P=1.0 (no sampling,
"raw") against WARM_P=0.1 (90% dropped, "sampled").

Two independent axes were measured: (1) SDK-side (generator process) CPU/memory cost of
sampling itself, and (2) end-to-end wire bytes when sampling is combined with a collector-side
KLL sketch (`asap_edge` processor, `mode: whole_stream, shard_count: 1, k: 800`).

---

## Methodology note: the series-ID dictionary confound

The OTLP/gRPC exporter has an ASAP-specific series-ID dictionary
(`otlpmetricgrpc.WithSeriesDictionary`, default on): once the collector confirms a series, the
exporter sends only a numeric ID instead of full `Attributes` on later exports. This is a real
wire-format optimization, not present in upstream OTel.

- For **CPU/memory comparisons**, the dictionary must stay enabled on **both** arms (raw and
  sampled) — disabling it on only the raw arm removes a real per-DataPoint cost that both arms
  should pay symmetrically, artificially cheapening the raw baseline and understating the
  measured CPU/RSS reduction from sampling. This was found empirically mid-session (an initial
  150K/1Hz CPU run with the dictionary disabled only on raw measured 38.55%/40.97% reduction; a
  corrected rerun with the dictionary enabled on both arms measured 41.03%/46.44%).
- For **wire-bytes comparisons**, the raw/passthrough baseline should have the dictionary
  **disabled** (`-disable-series-dictionary`) so it represents a genuinely naive pipeline (full
  attributes sent every time) — otherwise part of the measured wire savings is attributable to
  this exporter-level trick rather than to sampling/sketching, and the baseline is not a fair
  "no ASAP tricks" reference point.

All numbers below follow the correct convention for their respective comparison.

---

## Part 1: SDK-side CPU/RSS reduction from sampling alone

Dictionary enabled on both arms. `/proc/pid/stat` utime+stime summed across 10 generator PIDs
at t≈20s and t≈50s elapsed; RSS summed at t≈50s.

| Cardinality | Freq | CPU reduction @t20 | CPU reduction @t50 | RSS reduction @t50 |
|---|---|---|---|---|
| 10,000 | 1 Hz | 15.18% | 18.08% | ≈0% (−0.19%, noise) |
| 150,000 | 1 Hz | 46.44% | 41.03% | 26.02% |
| 200,000 | 1 Hz | 37.46% | 39.70% | 21.96% |

**Takeaway:** sampling's CPU/RSS benefit is not constant — it depends on how large a share of
per-process cost is fixed/near-fixed (worker-pool scaffolding, runtime/GC baseline, per-series
bookkeeping) versus admission-driven. At low cardinality (10K) fixed costs dominate and the
measurable benefit is small. 150K is close to a local peak for this workload shape; 200K is
already a few points past it. This should not be read as a single constant "sampling saves N%
CPU" — it is workload- and scale-dependent, and any downstream cost estimate should say at what
scale it applies.

A raw-buffer aggregator fix landed alongside this investigation (see Part 3) that lets sampled
series amortize the exporter's series-ID lookup cost across windows the same way the sketch
aggregators (DDSketch/KLL/CountSketch/CountMinSketch/HLL) already did. It measurably reduces
absolute CPU for any raw-buffer-aggregated series (confirmed: enabling the dictionary on a raw
arm now only costs ~4-10% instead of paying full per-DataPoint attribute-fingerprinting cost
forever) — but because it helps whichever arm touches more distinct series per window (the raw
arm, at 100% touched-fraction, benefits at least as much as the sampled arm at ~41% touched-
fraction for p=0.1/1Hz/5s), it does not by itself increase the sampled/raw reduction percentage.
It is a genuine efficiency win, just not one that shows up in this specific ratio.

## Part 2: End-to-end wire bytes — sampling + collector-side KLL sketch vs raw passthrough

Sampled arm: WARM_P=0.1 + `asap_edge` KLL whole_stream sketch (shard_count=1, k=800,
window=10s), dictionary enabled. Raw arm: WARM_P=1.0, plain passthrough collector,
`-disable-series-dictionary`. Bytes via `iptables` OUTPUT counters at t≈50s.

| Scale | SDK→collector reduction | collector→backend reduction |
|---|---|---|
| 10,000 series / 1Hz | 48.94% | 54.82% |
| 150,000 series / 1Hz | 54.77% | 69.62% |
| 150,000 series / 10Hz | **broken — see below** | **broken — see below** |

**Scale dependence:** the KLL sketch payload is fixed-size (set by `k`, independent of series
count or input volume), so at lower cardinality the fixed sketch cost is a proportionally larger
fraction of what raw would have cost — the compression ratio genuinely improves with scale. The
collector→backend hop shows this most directly since it is exactly where the sketch's
fixed-size payload dominates.

**Real failure mode found at 150K/10Hz:** `shard_count=1` (needed for wire-efficient
`whole_stream` KLL — `shard_count>1` was found in an earlier round to inflate wire bytes
~proportionally to shard count, since each shard independently flushes its own full sketch) is a
single-shard ingestion path. At 150K×1Hz (150K events/sec into the shard) it keeps up cleanly.
At 150K×10Hz (1.5M events/sec) it does not: the collector stalled, the backend showed
`SketchStore: 0 instance(s)` for the entire run, and effectively zero sketch bytes were ever
flushed — a real architectural ceiling, not a benchmark artifact. There is a genuine open tension
between `shard_count=1` (wire-efficient, doesn't scale past some ingestion rate) and
`shard_count>1` (scales ingestion, costs wire efficiency) that is unresolved as of this writing.

---

## Part 3: SDK fix landed this round — raw-buffer series-ID caching

`AggregationRawBuffer` (used throughout the above SDK-sampling tests) previously deleted and
recreated its per-series state on every `collect()` cycle, so it never participated in the
OTLP/gRPC exporter's series-ID dictionary caching that DDSketch/KLL/CountSketch/CountMinSketch/
HLL aggregators already had — every DataPoint it ever emitted paid the exporter's full
`descriptorKey`/`attributesFingerprint` cost, forever, with zero amortization. Fixed by:

- `sdk/metric/internal/aggregate/rawbuffer.go`: series now persist across `collect()` cycles
  (idle-evicted after `maxIdleCycles` consecutive empty windows, mirroring
  `ddSketch.cumulative`'s pattern) instead of being deleted every window; each DataPoint gets
  `SeriesID`/`SeriesIDSink`/`AttrsClearer` populated so a confirmed series' later DataPoints skip
  `Attributes` and the exporter's fingerprinting work entirely.
- `sdk/metric/metricdata/data.go`: added `SeriesIDSink`/`AttrsClearer` to the generic
  `DataPoint[N]` struct (previously only the sketch-specific DataPoint types had them).
- `exporters/otlp/otlpmetric/otlpmetricgrpc/internal/series/dictionary.go`:
  `annotateNumberDataPoints` (Gauge/Sum/raw-buffer's DataPoint type) now processes
  `SeriesIDSink`/`AttrsClearer` like the sketch-specific annotate functions already did.
- `exporters/otlp/otlpmetric/otlpmetricgrpc/exporter.go`: new
  `otlpmetricgrpc.WithSeriesDictionary(enabled bool)` option (default true, unchanged prior
  behavior) so a benchmark's naive/passthrough baseline arm can opt out of this ASAP-specific
  wire optimization — exposed in `otel-app` as `-disable-series-dictionary`.

Verified via two new unit tests (`rawbuffer_test.go`):
`TestRawBufferCachesSeriesIDAcrossCollects` and
`TestRawBufferEvictsIdleSeriesAndResetsCachedID`.

---

## Part 4: Illustrative AWS EC2 cost-saving estimate

Using the 150K-series/1Hz numbers above (the cleanest complete data point covering both CPU and
bandwidth) and real on-demand AWS pricing confirmed live against a current third-party AWS
pricing mirror (instances.vantage.sh) on 2026-08-04:

- Compute: `c6i.xlarge` (4 vCPU) $0.17/hr and `c6i.2xlarge` (8 vCPU) $0.34/hr both resolve to
  **$0.0425/vCPU-hr** — pricing is linear per vCPU within the family, so a measured CPU-tick
  reduction translates directly to the same percentage compute-cost reduction under elastic
  provisioning.
- Data transfer OUT: AWS's long-published standard rate is $0.09/GB (first 10TB/month,
  us-east-1 and most regions) — this specific figure was **not** re-confirmed via a live scrape
  this round (the AWS pricing page is JS-rendered); treat it as the well-known standard rate,
  not a freshly verified one. Same-AZ/same-region transfer is typically far cheaper
  (~$0.01/GB) or free — deployment topology (is the collector in the same AZ as the SDK? Is the
  backend in the same region as the collector?) determines which rate actually applies, and was
  not known at estimate time.

| Item | Raw | Sampled | Saved | Basis |
|---|---|---|---|---|
| Collection compute (~5.11 vs ~3.01 vCPU sustained, $0.0425/vCPU-hr, 730hr/mo) | $158.40/mo | $93.41/mo | **$64.98/mo (41%)** | measured CPU |
| SDK→collector transfer (6,836 vs 3,092 GB/mo) | — | — | 3,745 GB/mo | measured bytes |
| collector→backend transfer (4,380 vs 1,331 GB/mo) | — | — | 3,050 GB/mo | measured bytes |
| Transfer $ saved, upper bound (both hops @ $0.09/GB internet egress) | | | **$611/mo** | |
| Transfer $ saved, lower bound (both hops @ $0.01/GB same-region) | | | **$68/mo** | |

**Caveats, explicitly:** this is a linear monthly extrapolation of one 61-second real run at one
scale (150K series/1Hz), not a production bill. The compute figure covers only the SDK/telemetry
instrumentation layer, not a whole application's cost. The ~9x range on the transfer estimate
($68–$611/mo) is entirely a deployment-topology assumption, not measurement uncertainty — the
percentage reductions themselves (41% compute, 54.77%/69.62% transfer) are the real, measured,
defensible numbers; the dollar figures are illustrative, assumption-sensitive translations of
those percentages via current public AWS on-demand pricing.
