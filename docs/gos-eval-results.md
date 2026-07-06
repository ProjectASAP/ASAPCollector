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

**Measured `ρ` matches the predicted Cauchy–Schwarz factor exactly** — confirming
both the analysis and that `AllocateThresholds` realizes the water-filling
optimum. Savings scale with skew: for a heavy-tailed sketch (a few cells carry
the F₂ mass) anisotropic uses a **small fraction** of the uniform communication;
for a flat sketch there is (correctly) no difference. The cost is the `O(d·w)`
edge-memory threshold vector — the `GosAnisotropic` toggle exposes the tradeoff.

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
| **stable** (F₂ < τ) | distributed | none ✓ | 923,280 | 1.0× |
| **stable** | **geometric** | none ✓ | **230,820** | **0.25× (4.0× less)** |
| **ramp** (F₂ crosses τ) | distributed | fired @921,600 ✓ | 923,280 | 1.0× |
| **ramp** | **geometric** | fired @921,600 ✓ | **531,132** | **0.58× (1.74× less)** |

Both modes fire the **same** alert (observed 921,600, inside the
`[(1−ε)τ, τ) = [900k, 10⁶)` band). Geometric wins in **both** regimes.

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
