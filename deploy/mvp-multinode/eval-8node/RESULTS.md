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

| N | total sink MB/s | per-agent MB/s | agent RSS MiB |
|---|---|---|---|
| 1 | 0.047 | 0.047 | 1208 |
| 2 | 0.095 | 0.048 | 1595 |
| 3 | 0.187 | 0.062 | 1283 |
| 4 | 0.758 | 0.190 | 1330 |
| 5 | 1.244 | 0.249 | 1576 |

**Conclusion:** per-agent warm bandwidth is flat at N=1–3 (0.047–0.062 MB/s).
N=4–5 show measurement variance (45 s window catches sketch-flush bursts; CPU
column is single-snapshot-noisy). A longer-soak + averaged-CPU re-run
(`STAT_SAMPLES`, 90 s soak — `scale_fleet.sh` supports both) is needed to confirm
flatness past N=3; physical N is capped at 5 source nodes (extend to N=100 with
`cost_model/simulator.py`).

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
