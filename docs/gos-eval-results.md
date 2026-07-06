# GOS Evaluation Results

Measured results for the GOS framework (design-gos-unified-edge-telemetry.md).
Three axes: (1) anisotropic-vs-isotropic delta communication `ρ`; (2) geometric
vs distributed F2 monitoring; (3) the Woodruff–Zhang `k/ε²` reference.

Reproduce: `deploy/mvp-multinode/scripts/f2_monitor_eval.sh` (F2), and
`go test ./sketches/ -run TestGosAnisoSavingsRatio -v` (ρ).

---

## 1. Anisotropic vs isotropic per-cell thresholds (`ρ`)

At equal accuracy budget `B`, the per-cell water-filling gives communication
`Σ V_j/T_j` never worse than a uniform threshold (Cauchy–Schwarz), by the factor
`ρ = (Σ√(|g_j|·V_j))² / [(ΣV_j)(Σ|g_j|)] ≤ 1`. Sweeping the cell-magnitude skew
(Zipf(s) → `g_j=2|Ĉ_j|`, uniform activity), `n = d·w = 1280`, `k = 4`:

| skew `s` | measured `ρ` | predicted `ρ` | comm saving |
|---|---|---|---|
| 0.0 (uniform) | 1.0000 | 1.0000 | 0% |
| 0.5 | 0.9026 | 0.9026 | 9.7% |
| 1.0 (Zipf) | 0.4966 | 0.4966 | **50.3%** |
| 1.5 | 0.1283 | 0.1283 | 87.2% |
| 2.0 (heavy) | 0.0284 | 0.0284 | 97.2% |

The `ρ` above is a **closed-form check**: both columns evaluate the same
Cauchy–Schwarz factor on a synthetic cell vector, so the exact match confirms
only that `AllocateThresholds` realizes the water-filling optimum — it is **not**
a measurement of communication on a real stream (a caveat first raised in the
code review). The cost of anisotropic mode is the `O(d·w)` edge-memory threshold
vector; the `GosAnisotropic` toggle exposes the tradeoff.

### Cluster measurement (real agents, iptables byte count)

Replaces the closed-form check with an end-to-end measurement:
`deploy/mvp-multinode/scripts/gos_aniso_cluster.sh` runs a real asap-otel agent
(CountSketch `top_endpoint_qps`, `gos_delta_epsilon=0.1`, iso vs `gos_anisotropic:
true`) fed by the otel-app five-sketch producer with Zipf(`s`) endpoint labels,
and counts the delta bytes that reach the sink node's `:4317` with an iptables
counter (kernel-side, exact), over 70 s of steady state per arm after a warmup:

| Zipf `s` | iso bytes | aniso bytes | measured `ρ` |
|---|---|---|---|
| 1.1 | 503,230 | 303,901 | **0.60** (40% saving) |
| 1.5 | 301,851 | 302,328 | 1.00 |
| 2.0 | 303,693 | 300,198 | 0.99 |

**This is the honest result, and it does NOT match the closed-form story.**
Anisotropic saves ~40% at *low* input skew (`s=1.1`) but is a wash at higher
skew — the OPPOSITE of the closed-form prediction that savings grow with skew.
Two likely confounds, still to be run down: (a) a ~303 KB floor across most
cells suggests a fixed per-window cost (full keyframe / re-fill after
window-reset) dominating the *delta* the gate controls; (b) the CountSketch hash
SPREADS a heavy Zipf endpoint across `d` random-sign cells, so high *input* skew
does not straightforwardly produce high *cell-magnitude* skew — the quantity the
water-filling actually exploits. Treat the anisotropic per-cell win as
**unconfirmed on real workloads** pending this investigation; the mechanism
(config parse → `applyGosMode` → per-cell `{T_j}`) is verified live end-to-end,
the payoff is not.

---

## 2. Geometric vs distributed F2 monitoring

Cross-language (Rust coordinator + Go multi-edge driver), `k=4` edges, 20
sub-window steps, `d=5`, `w=256`, `τ=10⁶`, `ε=0.1`. Total bytes = edge→coord
sketch bytes + coord→edge `C_ref` broadcast bytes.

**Cluster (real NIC):** the same matrix reproduced across the 8-node CloudLab
fabric — edges on a source node, coordinator on the WARM node, every ship and
broadcast crossing the 10 GbE LAN — with byte-identical totals (the protocol is
deterministic) and the same alert decisions. Reproduce:
`deploy/mvp-multinode/scripts/f2_wholesketch_cluster.sh` → recorded at
`deploy/mvp-multinode/eval-8node/f2_wholesketch_cluster.csv`.

| Workload | Mode | Alert | Total bytes | vs distributed |
|---|---|---|---|---|
| **stable** (F₂ < τ) | raw (no aggregation) | none ✓ | 3,956 | — |
| **stable** | distributed | none ✓ | 923,280 | 1.0× |
| **stable** | **geometric** | none ✓ | **230,820** | **0.25× (4.0× less)** |
| **ramp** (F₂ crosses τ) | raw (no aggregation) | fired (exact) ✓ | 7,120 | — |
| **ramp** | distributed | fired @921,600 ✓ | 923,280 | 1.0× |
| **ramp** | **geometric** | fired @921,600 ✓ | **531,132** | **0.58× (1.74× less)** |

Both sketch modes fire the **same** alert (observed 921,600, inside the
`[(1−ε)τ, τ) = [900k, 10⁶)` band). Geometric wins in **both** regimes vs
distributed.

**Raw baseline honesty.** The raw rows ship every sample as a msgpack
`[ts, key, value]` record (real serialized frames, exact-F2 alert ground
truth — `f2driver … raw`). On THIS workload raw is far cheaper than the
sketches — **by construction**: the protocol-stress workload has only `H=4`
distinct keys (~9 samples/step total), while a sketch ship costs a fixed
`d·w ≈ 11.5 KB` regardless of `H`. Raw bytes scale `∝ H` (~1,957 B/key over
the 20-step run at `H=2048`: 4,007,440 B measured via `F2_KEYS=2048`), sketch
bytes do not — the crossover on this run shape is `H* ≈ 470` keys vs
distributed and `H* ≈ 270` vs ramp-geometric; at `H=2048` the sketch wins
**4.3×** (distributed) / **7.5×** (geometric). For realistic
cardinality/rate the gap is the C1-wire result (33.8×/65.9× on the
Google-cluster trace). Sweep `F2_KEYS` to reproduce the crossover.

### Effect of the `C_ref` delta broadcast (design §12 open-problem #1)

The geometric coordinator→edge `C_ref` was originally a full `~11.5 KB` matrix
per resync × `k` edges (O(k) amplification), which made geometric *lose* the ramp
regime. Delta-encoding the broadcast (sparse changed cells only):

| ramp geometric | egress (`bytes_out`) | total |
|---|---|---|
| full-matrix broadcast | 1,107,936 | 1,620,000 (loses) |
| **sparse delta broadcast** | **207,984** | **531,132 (wins)** |

Egress dropped **5.3×**, flipping geometric from a loss to a `1.74×` win.

### Delta-loss resilience (safety under a dropped `C_ref` delta)

The sparse delta is a dependency chain, so a lost/corrupt `ΔC_ref` could leave an
edge running its safe-zone test against a diverged reference — a *silent missed
violation*. Reproduce: `deploy/mvp-multinode/scripts/f2_deltaloss_demo.sh`
(injects loss on edge-0's 2nd delta via the f2driver `F2_INJECT` knob), same
ramp/geometric scenario three ways:

| scenario | alert | total bytes | ref_errs | behavior |
|---|---|---|---|---|
| no loss | **1** ✓ | 531,132 | 0 | baseline |
| corrupt delta | **1** ✓ | 531,132 | 1 | edge-0 **detects** the bad delta → `needFull` → force-ship |
| dropped delta | **1** ✓ | 506,106 | 0 | silent loss undetected by that edge; recovered by the periodic keyframe + the other edges' true sketches in the global merge |

**The alert fires in all three cases** — safety is preserved under delta loss. A
*corrupt* delta is caught at the edge (`ref_errs=1`, force-ship); a *silently
dropped* delta has no sequence gap for that edge to detect (`ref_errs=0`), so its
recovery rests on the coordinator's periodic Full keyframe (`F2_KEYFRAME_INTERVAL`)
and the fact that the global `mean_f2` still sums the other edges' exact sketches.
A per-broadcast sequence number (edge-detected gap → on-demand resync request)
would close the silent-drop detection gap; it is noted as future work.

---

## 3. Woodruff–Zhang `k/ε²` reference

WZ (STOC'12) proves continuous `(1±ε)` F₂ monitoring over `k` sites needs
`Θ̃(k/ε²)` communication — a **hard lower bound** for any protocol. Normalizing by
the "one round" unit `k·S` (`S ∝ d·w ∝ 1/ε²` = one sketch), here
`k·S = 4 × 11,541 ≈ 46 KB`:

| Mode / workload | total / (k·S) | reading |
|---|---|---|
| distributed | **20×** | ships every step → `W=20` rounds |
| geometric, ramp | 11.5× | near-threshold ships |
| geometric, stable | **5.0×** | within a small constant of the `k/ε²` floor |

Distributed pays the full `W×` (re-ships every step); geometric approaches the
`Θ̃(k/ε²)` floor in the stable regime (few resyncs). No protocol beats `k/ε²`
worst-case — geometric's win is the data-dependent constant, exactly as the
theory predicts.

---

## 4. Per-row sampling (unbiasedness)

`CountSketch.UpdateStringSampledPerRow` (per-row geometric admission, `1/p`
weighting): inserting a heavy key `N=20,000` times at `p=0.5` estimates within
`<15%` relative error (`sketchlib-go .../sampled_test.go`), confirming the
inverse-probability weighting keeps the estimator unbiased under per-row
admission. Per §3.2 the per-row (vs per-item) form additionally decorrelates the
`d` row estimates so the median concentrates the sampling error into the `(1−δ)`
guarantee — realized at equal edge CPU.
