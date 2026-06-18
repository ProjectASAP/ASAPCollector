# Phase-2 integrated cluster eval — real freshness + per-component + cold-tier

Single-node (Phase-1) gave accuracy/latency/CPU/wire/RSS-vs-ε but could NOT give
real **freshness** (compressed replay sealed windows instantly) or **per-component**
resources or **cold-tier** routing. Phase-2 runs the full warm+cold stack on the
8-node cluster, wall-clock paced, and measures all of it.

Driver: `scripts/epsilon_cluster_sweep.sh` — per admission p (= the ε-floor the
coordinator sets), brings up the asap arm and captures freshness (per tier),
per-container CPU/mem, per-node NIC bandwidth, and warm query latency.
The `-warm-sample-p` knob was threaded through `run_demo.sh` producers.

## Baseline (p=1.0), light stable workload (card=300, 20Hz, 2 prod/node)

### Real freshness (wall-clock gen→queryable)
| tier | p50 | p99 |
|---|---|---|
| warm (sketch) | 1.0 s | 2.1 s |
| archive (gorilla/S3) | 0.66 s | 1.45 s |

(valid-epoch probe subset; measure_freshness probe read is multi-series-noisy —
aggregation filters observed_value_ms>1e12.)

### Per-component resources
| tier | component | CPU mean / max | mem |
|---|---|---|---|
| edge | agent-a / agent-b | 137% / 110% (261% peak) | 0.5–1.3 GiB |
| edge | producers ×4 | 63–71% | 0.3–2.0 GiB |
| warm | data-plane (sketch backend) | 2.1% / 3.2% | 117 MiB |
| warm | control-plane | ~0% | 2 MiB |
| cold | gorilla-merger (archiver) | 17.6% (147% burst) | 432 MiB |
| cold | minio / thanos×3 / prometheus | ~0% | 11–80 MiB |

Headline: the warm sketch backend holds 5,000+ sketches for 2% of one core and
117 MiB; cost lives at the edge (collection) and the cold archiver; thanos/minio
idle until a cold query hits.

## ε-sweep (p ∈ {1.0,0.5,0.25,0.1,0.05}) — populated by epsilon_cluster_sweep.sh
(freshness / CPU / mem / NIC-bandwidth / latency vs ε — table appended after run.)
