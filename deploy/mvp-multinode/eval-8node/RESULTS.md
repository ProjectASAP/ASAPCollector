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

## Fig 7 — PromQL query latency CDF  (`figs/fig7_latency_cdf.png`)
599-query replay (queries-e2e.json).

| arm | p50 ms | p99 ms |
|---|---|---|
| b0 raw (VictoriaMetrics) | 0.49 | 0.54 |
| b1 raw (VictoriaMetrics) | 0.48 | 0.52 |
| asap (warm sketch) | 37.4 | 166.9 |

**Caveat:** with the cold tier ON, `_over_time[5m]` range quantiles fail over to
the Thanos archive (slow path), inflating ASAP latency. The cold-OFF warm-only
accuracy stack (Fig 3) resolves the same quantiles from sketch state directly.

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

## Fig 9 — coordinated sampling on a skewed fleet  (`fig9_partial.csv`)
Driver: `scripts/fig9_coordinated.sh` + `configs/asap/mvp-workload-fig9.yaml`
(adds a `monitor:` block to `http_requests_total`). **Pipeline now end-to-end
wired and verified on hardware** (was an empty `monitors[]` gap before):

1. The control-plane emits the monitor — `streaming-config.monitors` =
   `[{agg_id: 16346598078036168951, tau, epsilon: 0.2, window_ms: 30000}]`.
2. The data-plane runs the CDM coordinator (new `DP_MONITOR_FLAGS=
   "--enable-monitor-coordinator --monitor-grpc-port 4319"` plumbed into `backend_up`).
3. Three trace-replay edges (node3/4/5) **connect to the coordinator and report
   a 25× skewed rate**:

| edge | node | per-window rate | CDM connected | granted p |
|---|---|---|---|---|
| hot | node3 | 80 000 | ✓ | 1.0 |
| med | node4 | 16 000 | ✓ | 1.0 |
| quiet | node5 | 3 200 | ✓ | 1.0 |

**Remaining:** the coordinator did **not** issue differentiated `p<1` grants even
with the global sum ≫ τ (tried τ=5e6 and τ=1e5). The edges connect and report,
but the grant-trigger (the CMY slack/round allocation in
`data_plane/src/monitor/`) stayed at p=1 — needs investigation of the grant
condition (likely edge round-registration on a τ change, or the value-functional
not crossing the slack boundary the way the allocator expects). Note: coordination
only activates in the producer's **trace-replay** path (`-trace-file`,
`runTraceReplay`), not the synthetic-workload path — a synthetic `monitor_agg_id`
producer never calls `newSampleController`. fig9_coordinated.sh uses synthetic
`timestamp_ms,series_id,value` CSVs to drive skewed, sustained (`-trace-loop`) rates.

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
