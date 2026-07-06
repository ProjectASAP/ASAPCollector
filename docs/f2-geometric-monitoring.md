# F2 / Geometric Safe-Zone Distributed Monitoring

Wires the previously-dormant whole-sketch **F2 (`‖f‖₂²`)** monitor end-to-end
across the Go edge and the Rust coordinator. F2 (second frequency moment =
self-join size / variance / heavy-hitter energy) is **non-linear** across edges —
`F2(Σfᵢ) = ΣF2(fᵢ) + 2Σ⟨fᵢ,fⱼ⟩` — so it cannot be reduced to the per-edge scalar
that the existing CMY slack-countdown thresholds. It answers the "not a single
key" monitoring case (`FunctionalCMSPoint` watches one `f(x)`; F2 watches the
whole sketch).

## What was built

| Layer | File(s) |
|---|---|
| Algorithm (Rust) | `ASAPQuery-backend/data_plane/src/monitor/f2.rs` — `CountSketchF2` (now `f64`, `from_matrix`/`to_matrix`), `DistributedF2Monitor`, `GeometricF2Monitor` |
| Coordinator state (Rust) | `.../monitor/f2_coord.rs` — `F2CoordMonitor` (both modes, lazy geometric resync) |
| Coordinator wiring (Rust) | `.../monitor/server.rs` — F2 routing, msgpack decode, `RefBroadcast`, comm accounting; `.../monitor/coordinator.rs` — `Functional`/`F2Mode` + `MonitorConfig` fields |
| Wire (proto) | `.../proto/monitor/monitor.proto` (+ Go vendored copy) — `MonitorReport.sketch`, `RefBroadcast`, `CoordToEdge.ref` |
| Config | `crates/asap_types/src/streaming_config.rs` (`MonitorSpec.d/w/mode`); `data_plane/src/main.rs` mapping |
| Edge (Go) | `asap-precompute-go/monitor/f2engine.go` — `F2Engine` (local safe-zone test, ship/silent), `types.go` (`FunctionalF2`, `F2Mode`), `grpcclient/client.go` (sketch + `RefBroadcast`), `sketches/countsketch.go` (`CellMatrix()`) |
| Eval | `data_plane/src/bin/f2_monitor_harness.rs`, `asap-precompute-go/.../cmd/f2driver/main.go`, `deploy/mvp-multinode/scripts/f2_monitor_eval.sh` |

## Protocol

- **Geometric** — the real, communication-efficient protocol. Each edge runs the
  Sharfman–Schuster–Keren safe-zone test **locally** against the broadcast
  reference `C_ref` (`‖C_ref + (k/2)ΔCᵢ‖ + (k/2)‖ΔCᵢ‖ ≤ R`) and ships **only on a
  local violation**. On a ship the coordinator refreshes that edge's reference
  and re-broadcasts `C_ref` (a lazy resync — no `PollLocal` gather; safety-correct
  because all-sites-locally-safe ⇒ global `‖C‖ < R`). **The safe radius is
  `R = √(d·(1−ε)τ)`, not `√(d·τ)`** — it monitors the *alert* threshold `(1−ε)τ`
  so that all-safe ⇒ `F2 < (1−ε)τ`; using the raw `τ` (the original bug) let
  edges stay silent through the whole `[(1−ε)τ, τ)` band and fire the alert late.
- **Distributed** — an **eval-only baseline** (≈ centralization): every edge ships
  its Count-Sketch every window; the coordinator merges and estimates F2 with the
  **mean-of-rows** estimator `F̂₂ = ‖ΣCᵢ‖²/d`, alerting at `F̂₂ ≥ (1−ε)τ`. It is
  *not* a separate algorithm — it is geometric with an empty safe zone, kept only
  as the communication upper bound and the accuracy ground truth. Production uses
  the geometric protocol. **Both modes use the same mean-of-rows estimator**
  (`F2CoordMonitor::mean_f2`), which is exactly the functional the geometric ball
  `‖C‖ ≤ √(d(1−ε)τ) ⇔ ‖C‖²/d ≤ (1−ε)τ` bounds — so edge silence and coordinator
  alert test one identical quantity (design doc §7). This is deliberately *not*
  the median-of-rows point-query estimator (`estimate_f2`); the two must not be
  conflated — median for individual-key location, mean for the aggregate energy
  the ball bounds.

The alert decision is identical in both modes (verified: both fire at
`observed=921,600`, inside `[900000, 1000000)`); only the communication differs.

## Cross-language correctness

- Full sketches cross the wire as the 3-element msgpack array `[rows, cols, matrix]`
  (`asapmsgpack.MarshalCountSketch` ↔ Rust `rmp_serde` 3-tuple). **Note:** Rust
  `portable::CountSketch::to_msgpack` writes a *4-element* array (adds `topk`),
  which the Go decoder rejects — so both directions use the explicit 3-tuple.
- Sparse `C_ref` deltas (coordinator→edge) cross as a **5-element** msgpack array
  `[rows, cols, rowIdx[], colIdx[], vals[]]` of changed cells (`CRefUpdate::Delta`
  ↔ `asapmsgpack` sparse codec); the edge applies it to its cached `C_ref`.
- The safe-zone verdict is guarded by a **golden-vector parity test** evaluated
  in both languages on identical matrices: Go
  `monitor.TestF2LocallySafeGolden` and Rust `f2::tests::is_locally_safe_matches_go_golden`.

## Eval results

`deploy/mvp-multinode/scripts/f2_monitor_eval.sh` (4 edges, 20 sub-window steps,
d=5, w=256, τ=1e6, ε=0.1):

| Workload | Mode | Alert | Total bytes | Ships | Silent |
|---|---|---|---|---|---|
| **stable** (F2 < τ) | distributed | none ✓ | 923,280 | 80 | 0 |
| **stable** | **geometric** | none ✓ | **230,820** | 4 | 76 |
| **ramp** (F2 crosses τ) | distributed | fired ✓ | 923,280 | 80 | 0 |
| **ramp** | **geometric** | fired ✓ | **531,132** | 28 | 52 |

- **Stable regime (the realistic monitoring case): geometric uses ~4× less
  communication** while reaching the same (correct) no-alert decision — sites
  stay locally safe and go silent, so the coordinator receives almost nothing.
- **Ramp regime**: geometric still detects the crossing, ships fewer sketches,
  **and now wins overall (1.74× less)** — the `C_ref` re-broadcast is
  delta-encoded (see below), so it no longer dominates cost.

See [`gos-eval-results.md`](gos-eval-results.md) §2 for the full breakdown
(including the delta-broadcast ablation) and the Woodruff–Zhang `k/ε²` reference.

## Delta broadcast (done) + remaining productionization

The geometric `C_ref` re-broadcast originally sent the **full** reference matrix
to every edge on each resync (`O(k)` amplification), which made geometric *lose*
the ramp regime. It is now **delta-encoded** (`CRefUpdate::Delta`, sparse changed
cells only — the 5-element msgpack frame above), which cut egress ~5.3× and
flipped ramp geometric from a loss to a 1.74× win. The gate is still isotropic
(ships every `Δ ≠ 0` cell); giving the broadcast *anisotropic per-cell
thresholds* is design §12 open-problem #1.

**Delta-loss resilience.** A sparse delta creates a dependency chain: an edge
that misses or fails to decode one `ΔC_ref` runs the rest of the epoch against a
diverged reference, and later deltas compound onto a wrong base — a corrupt
safe-zone test could then wrongly pass (a *silent missed violation*). Two guards:
- **Edge (safety):** when a delta cannot be applied (`f2engine.go` marks the
  reference `needFull`), the edge stops trusting it and **force-ships every
  window** until a Full keyframe resyncs it. The coordinator's global `mean_f2`
  therefore always sees that edge's true sketch, so the alert fires correctly —
  the divergence can no longer hide a crossing.
- **Coordinator (recovery):** the broadcast is a Full keyframe at epoch start and
  every `F2_KEYFRAME_INTERVAL` rounds (I-frame style), bounding how long a
  diverged edge stays in the force-ship state; it also self-heals at the epoch
  boundary. The interval is set above the eval's per-mode broadcast count, so the
  measured communication figures are unchanged. A faster on-demand resync (an
  explicit edge→coordinator keyframe request) is a possible future refinement.

**Remaining productionization.** The `F2Engine` is wired to the coordinator over
real gRPC and driven by the eval, but it is **not reachable from the production
edge runtime**: `precompute.go`'s `monitorValue` switch handles only
Sum/CMSPoint/LinearBuckets, so a `FunctionalF2` spec falls through to `ok=false`
and silently disables monitoring for that series. Hooking `F2Engine` into the
per-window flush path (alongside the scalar `monitorValue` hook) is the remaining
step before "production uses the geometric protocol" is true of the edge as well
as the coordinator.
