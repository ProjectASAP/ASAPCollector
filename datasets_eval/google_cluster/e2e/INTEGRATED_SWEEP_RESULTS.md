# Integrated ε-sweep — end-to-end metrics under coordinated sampling

`epsilon_e2e_sweep.py` extends `pareto_sweep.py` to emit **6 metric classes in one
run**, swept over the admission p (= the ε-floor `p=1/(1+ε²·rate)` the autonomous
coordinator sets for the corresponding ε): query **accuracy**, query **latency**
(p50/p99), data **freshness** (sample-generation → backend-queryable), and
per-process **CPU / RSS / wire bandwidth**.

## Phase-1 (single-node, google-cluster-2019 cpu_rate, pooled, GT p99=0.0494)

| arm | ε | p | accuracy | lat p99 | freshness | bk RSS | pr RSS | cpu cores | wire kbps |
|---|---|---|---|---|---|---|---|---|---|
| raw    | 0     | 1.00 | 1.000  | 2.5 ms | —     | 47 MB | 71 MB | 0.115 | 14.7 |
| dd     | 0     | 1.00 | 0.867* | 2.6 ms | 19 ms | 15 MB | 60 MB | 0.100 | 0.4 |
| dd     | 0.005 | 0.50 | 0.867  | 2.6 ms | 19 ms | 15 MB | 60 MB | 0.099 | 0.4 |
| dd     | 0.008 | 0.25 | 0.850  | 2.6 ms | 19 ms | 15 MB | 58 MB | 0.091 | 0.4 |
| dd     | 0.014 | 0.10 | 0.850  | 2.6 ms | 19 ms | 15 MB | 55 MB | 0.081 | 0.3 |
| dd     | 0.020 | 0.05 | 0.850  | 2.6 ms | 20 ms | 16 MB | 51 MB | 0.093 | 0.3 |

**Takeaways:** sampling to p=0.05 keeps accuracy flat (high-N pooled is
sampling-robust), drops edge CPU 0.10→0.08, keeps wire ~40× below raw, backend RSS
bounded ~15 MB. Warm latency ~2.6 ms (vs cold-tier ~51 ms, Fig 7 / #501).

**Caveats (single-node):** (1) `*`accuracy ~13% low — the regenerated `[30s]`
window reads a sub-pool; the *calibrated* accuracy-vs-p is in
`docs/evaluation-plan-figures.md` Fig 3. (2) freshness ~19 ms is a single-node
artifact — the **compressed** replay (31 d → ~20 s) seals windows mid-run, so
emit→queryable collapses to ~0. Real freshness + per-component (edge/dp/cp) +
cold-tier routing are the **Phase-2 cluster run** (wall-clock-paced,
`measure_freshness.sh` + the probe; per-container `snapshot_resources.sh`).
