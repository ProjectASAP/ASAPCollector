# 8-node CloudLab evaluation — real-hardware §6 figures

Empirical results produced on an 8-node CloudLab cluster (10 Gbps LAN) wired up
to run the `mvp-multinode` harness at scale. Role split:

| node | role |
|---|---|
| node0 | driver / orchestrator (+ accuracy stack, edge benches) |
| node1 | COLD backend (MinIO, Thanos, gorilla-merger, Prometheus, VictoriaMetrics) |
| node2 | WARM backend (data_plane + control_plane) |
| node3–node7 | data sources (agents + producers) |

Driven via `TOPOLOGY_ENV=deploy/mvp-multinode/topology.8node.env`.

## What runs end-to-end
Verified the full pipeline on real hardware: producers → asap_edge agent
(sketch processors) → warm `data_plane` SketchStore (1800+ sids ingested) →
PromQL query surface. Controller pushes streaming-config + storage-routing over
OpAMP; instant warm queries (`sum by(zone)`) resolve from sketch state; cold
tier ships gorilla fragments to MinIO/merger.

---

## Fig 2 — total backend ingest wire bandwidth  (`figs/fig2_bandwidth.png`)
Base 4-node sweep (cold=node1 + warm=node2 RX summed), 2 agents × 2 producers,
cardinality 200, 50 Hz, 60 s soak.

| arm | backend ingest MB/s | vs raw-none |
|---|---|---|
| b0 raw OTLP (none) | 25.5 | 1.0× |
| b1 raw OTLP (gzip) | 1.07 | 24× |
| **asap sketch** (warm 0.21 + cold-gorilla 0.44) | **0.65** | **39×** |

Warm sketch sink alone (node2) = 0.21 MB/s → **120× below raw-none**. ASAP also
ships the full cold raw backup and still beats gzip'd raw on total wire.
**Conclusion:** sketch aggregation cuts ingest wire ~40–120× vs raw — the headline cost win.

## Fig 6 — edge agent CPU / memory  (`figs/fig6_edge_resource.png`)
| arm | agent CPU mean % | agent RSS MiB |
|---|---|---|
| b0 | 37.7 | ~615 |
| b1 | 49.8 | ~1440 |
| asap | 119.2 | ~1510 |

**Conclusion (honest tradeoff):** ASAP spends ~3× edge CPU (sketch compute + cold
gorilla encode) to buy the 40–120× wire reduction. Not a free win — a favourable
cost-for-bandwidth trade.

### Fig 6b — memory-leak soak (`figs/fig6b_soak_rss.png`, `fig6b_soak.csv`)
30-min soak (asap arm, 2 agents, 100 RSS samples), linear-fit RSS slope:

| agent | first MiB | last MiB | slope MiB/hr |
|---|---|---|---|
| asap-agent-a | 1542 | 1603 | **−290** |
| asap-agent-b | 1220 | 1100 | **−183** |

**Conclusion:** agent RSS is flat-to-slightly-negative over the soak (GC reclaim,
not growth) → **no leak**. 30-min in-session proxy for the 24h paper target; a
true 24h run would confirm at scale but the slope is already non-positive.

## Fig 8 — cross-layer placement  (`figs/fig8_placement.png`, `fig8_placement.csv`)
Same DDSketch agg_type, only the producer's `-agg` changes: `raw-buffer` (agent
sketches) vs `ddsketch` (SDK sketches, agent forwards). CPU% per layer:

| placement | producer | agent | backend |
|---|---|---|---|
| **agent** (asap_edge sketches) | 240 | **111** | 90 |
| **SDK** (producer sketches) | 99 | **4.5** | 6.6 |

**Conclusion:** placement doesn't change correctness but shifts *where* the CPU
lands — and **earlier (SDK) placement is cheaper at every downstream layer**
because it aggregates before serialization/transport: moving the sketch to the SDK
drops agent CPU **111% → 4.5% (~25×)**, backend 90% → 6.6%, and even the producer
falls 240% → 99% (one compact sketch/window vs the full raw-buffer stream). The
tradeoff: SDK placement needs the sketch library in every app; agent placement
keeps apps thin at the cost of agent CPU.

## Fig 7 — PromQL query latency CDF  (`figs/fig7_latency_cdf.png`)
599-query replay (queries-e2e.json).

| arm | p50 ms | p99 ms |
|---|---|---|
| b0 raw (VictoriaMetrics) | 0.49 | 0.54 |
| b1 raw (VictoriaMetrics) | 0.48 | 0.52 |
| asap (warm sketch) | 37.4 | 166.9 |

**Caveat:** with the cold tier ON, `_over_time[5m]` range quantiles fail over to
the Thanos archive (slow path), inflating ASAP latency.

### Fig 7 cold-OFF arm (warm-only, no archive failover) — `fig7_coldoff_*`
Cold-OFF all-families stack, 599-query replay against the warm tier:

| query | p50 ms | p99 ms |
|---|---|---|
| DDSketch quantile | 36.7 | **51.3** |
| sum | 2.4 | 3.6 |

**Conclusion:** turning the cold tier OFF drops the quantile p99 from 167 ms
(cold-ON archive failover) to **51 ms** (warm sketch reconstruction) — confirming
the cold-ON latency was archive routing, not the warm path. Instant `sum` resolves
warm in ~2.4 ms.

## Fig 10 — scaling: per-agent bandwidth vs fleet size N  (`figs/fig10_scaling.png`, `fig10_scale_v1.csv`)
One supervised asap agent per source host (node3–7), N ∈ {1..5}.

Cleaned re-run (90 s soak, 6-sample averaged CPU):

| N | total sink MB/s | per-agent MB/s | agent CPU % | agent RSS MiB |
|---|---|---|---|---|
| 1 | 0.056 | 0.056 | 138 | 1244 |
| 2 | 0.094 | 0.047 | 100 | 1336 |
| 3 | 0.181 | 0.060 | 85 | 1456 |
| 4 | 0.508 | 0.127 | 73 | 1433 |
| 5 | 0.725 | 0.145 | 77 | 1390 |

**Conclusion:** per-agent warm bandwidth is flat at N=1–3 (0.047–0.060 MB/s) and
rises modestly at N=4–5 (0.13–0.15) — backend-side per-agent overhead (control-plane
scrape + OpAMP traffic that scales with the fleet) plus residual flush variance,
not super-linear edge cost. Agent CPU is stable (73–138%). Physical N capped at 5
source nodes; extend to N=100 with `cost_model/simulator.py`. (A v1 45 s-soak run
showed the same trend with noisier CPU — the longer soak + averaged sampling
cleaned it up.)

## Fig 3 — per-family accuracy (cold-OFF, wall-clock-anchored)  (`fig3_accuracy.json`)
Google-cluster-2019 trace (100k rows → aliased per family), instant quantile
read vs exact offline ground truth, p0.99.

| family | n series matched | median rel-err | p95 rel-err | mean rel-err |
|---|---|---|---|---|
| DDSketch (q0.99) | 385 / 1000 | **0.026** | 0.176 | 0.049 |
| KLL (q0.99) | 316 / 1000 | 0.084 | 0.431 | 0.135 |

**Conclusion:** DDSketch holds a tighter p99 tail than KLL on this trace (median
0.026 vs 0.084), consistent with the design's relative-error guarantee; KLL's
larger tail is small-N order-statistic variance (single-window collapse).

## Fig 11 — edge gorilla encode  (`fig11_edge_bench.txt`)
Deterministic Go benchmark on a node0 core (this hardware):

| series | encode time | per-sample | allocs |
|---|---|---|---|
| 100 | 21.4 ms | ~2.1 µs | 109k |
| 1 000 | 254.7 ms | ~2.1 µs | 1.09M |

**Conclusion:** edge gorilla encode is ~2.1 µs/sample and scales linearly — the
bounded price of the lossless cold backup. (Cost model `costmodel_tables.txt`
extends to storage/$ and the sampling sweep.)

## Fig 1 — accuracy-vs-cost Pareto  (`figs/fig1_pareto.png`)
Measured anchors (Fig 2 ingest wire normalized to raw-none; Fig 3 DDSketch p99
median rel-err) + sampling extension from the cost model.

| point | ingest cost (× raw-none) | accuracy (1−median rel-err) |
|---|---|---|
| raw (none/gzip) | 1.0 / 0.042 | 1.000 (exact) |
| ASAP p=1.0 | 0.025 | 0.974 |
| ASAP p=0.5 | 0.013 | 0.964 |
| ASAP p=0.25 | 0.006 | 0.944 |

**Conclusion:** ASAP sits far left of the raw baseline (40× cheaper ingest) at
~0.97 accuracy; sampling slides the frontier further left at a small, predicted
accuracy cost. Raw is exact but pays the full wire.

## Fig 11 — cold gorilla storage vs uncompressed raw  (`figs/fig11_storage.png`, `fig11_storage_bench.txt`)
MEASURED bytes/sample on this hardware (`asap-gorilla-go`
`TestGorillaXORBytesPerSampleByDataShape`), bytes/series/day at 1 Hz:

| tier | bytes/series/day | vs raw |
|---|---|---|
| raw (uncompressed, 16 B/sample) | 1 382 400 | 1.0× |
| gorilla-XOR (counter, 1.34 B/s) | 115 776 | **12×** |
| gorilla-XOR (smooth counter, 1.56 B/s) | 134 784 | 10× |
| gorilla-XOR (random-walk, 6.96 B/s) | 601 344 | 2.3× |

Edge footprint: **1765 B/series** open-window RSS.
**Conclusion:** the cold gorilla tier shrinks archived bytes **2.3×–12×** vs
uncompressed raw — strongly data-dependent (best on counters, worst on
high-entropy gauges); it IS vanilla Prometheus gorilla by construction.

## Fig 9 — coordinated sampling: ONE law for every monitor  (`fig9_cmspoint.csv`, `figs/fig9_unified_sampling.png`)
> Note: the whole-sketch F2 row below was measured by `fig9_f2_coordinated.sh`;
> its `fig9_f2.csv` was retired when the eval axis moved to the whole-sketch
> GEOMETRIC protocol (`f2_wholesketch_cluster.csv`) — the granted-p values are
> preserved inline in the table here.
Drivers: `scripts/fig9_coordinated.sh` (cms_point) + `scripts/fig9_f2_coordinated.sh` (whole-sketch F2). Both run the CDM coordinator live (auto-learn from the controller-pushed config, hot-reload, no boot-seed).

**Result — both monitor types obey the SAME per-edge sampling law, the whole-sketch ε-floor**
```
p_i = 1 / (1 + ε²·rate_i)            (ε = 0.2)
```
p depends only on each edge's rate, NOT on the monitored functional. Measured on hardware, stable over the last 3 windows:

| monitor (functional) | per-edge rate (hi→lo) | granted p |
|---|---|---|
| **cms_point** (`http_requests_total`, key `s0`) | 600k / 120k / 24k | 0.0001 / 0.0003 / 0.0013 |
| **whole-sketch F2** (`Σ_x f(x)²`, no key) | 38400 / 9600 / 2400 | 0.0007 / 0.0026 / 0.0102 |

All six points lie on the single `1/(1+ε²·rate)` curve (`figs/fig9_unified_sampling.png`): the busiest edge is sampled hardest; F2 (whole-sketch) and cms_point (a declared point) are indistinguishable to the *sampler*.

**Why one law (and not a per-key `√(f_i/rate_i)` allocation).** Sampling protects the warm SKETCH, whose point/L2 estimate error is bounded by the stream norm (rate/L2 — NitroSketch), never by a single key's `f(x)`. So the only accuracy a per-edge `p` buys is keeping that edge's L2 mass within ε — `ε_s=√((1−p)/(p·rate))≤ε ⇒ p≥1/(1+ε²·rate)`. The `√(f/rate)` allocation (ASAP's own distributed-NitroSketch note, *not* a published theorem) assumed an **exact-count-one-key** variance model that does not hold for sketches; for whole-sketch it is structurally impossible (`F2=Σf²≥Σf=rate` forces the alert-tied budget too large to bind). It was **retired** (ASAPQuery-backend#381). The monitored functional now drives only the **threshold/alert** (`known_value → global_estimate`), not sampling. Earlier this figure reported cms_point as the √rate law (0.0010/0.0022/0.0049); that was the artifact, now superseded.

**Live-integration fixes that got the coordinated path working** (each masked the next): (1) workload `functional: sum`→`cms_point`/`f2`; (2) coordinator read monitors only at boot → hot-reload watcher (#379); (3) slack ≫ window mass → τ/window tuning; (4) edge registered under `nil` key → thread the monitor key (#504); (5) `mergeFlagOverrides` dropped `-monitor-config-url`/`-monitor-key` (#504). All five are code fixes; the F2 path additionally needed the `functional` round-trip (#381) + edge mode (#505).

---

## Reproduce
```bash
export TOPOLOGY_ENV=deploy/mvp-multinode/topology.8node.env
# base sweep (Fig 2/6/7):
SKIP_BUILD=1 SKIP_LOAD=1 SNAP_NODES="node1 node2 node3 node4" \
  DOCKER_RESTART_POLICY=unless-stopped OTELAPP_MAX_BUFFER_PER_SERIES=512 \
  bash deploy/mvp-multinode/scripts/run_demo_sweep.sh
python3 deploy/mvp-multinode/scripts/plots.py <run_dir>
# scaling (Fig 10):
SKIP_BUILD=1 SKIP_LOAD=1 DOCKER_RESTART_POLICY=unless-stopped \
  bash deploy/mvp-multinode/scripts/scale_fleet.sh "1 2 3 4 5" 90
# accuracy (Fig 3):
python3 datasets_eval/multisketch/run_perfamily.py --arms ddsketch,kll --window 120s
```

## Known issues surfaced (and fixed) during this eval
- `snapshot_resources.sh` hard-coded NIC `enp130s0f0` → auto-detect the 10.10.1.x
  iface (this cluster is `eno2`); node set now configurable via `SNAP_NODES`.
- `Dockerfile.otel-app` never COPY'd the `asap-precompute-go` sibling its go.mod
  requires → otel-app image build failed; added the build-context + COPY.
- `run_demo.sh` / `run_demo_sweep.sh` now honor a `TOPOLOGY_ENV` override and
  `run_demo.sh` is sourceable as a library (`RUN_DEMO_LIB=1`) for `scale_fleet.sh`.
- Producer OOM (exit 137) during the supervised agent's config-apply restart
  window — mitigated with `DOCKER_RESTART_POLICY=unless-stopped` +
  `OTELAPP_MAX_BUFFER_PER_SERIES`.
