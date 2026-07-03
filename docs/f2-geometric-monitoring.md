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
  its Count-Sketch every window; the coordinator merges and estimates
  `F2̂ = estimate_f2(Σ latest_i)`, alerting at `F2̂ ≥ (1−ε)τ`. It is *not* a
  separate algorithm — it is geometric with an empty safe zone, kept only as the
  communication upper bound and the accuracy ground truth. Production uses the
  geometric protocol.

The alert decision is identical in both modes (verified: both fire at
`observed=921,600`, inside `[900000, 1000000)`); only the communication differs.

## Cross-language correctness

- Sketches cross the wire as the 3-element msgpack array `[rows, cols, matrix]`
  (`asapmsgpack.MarshalCountSketch` ↔ Rust `rmp_serde` 3-tuple). **Note:** Rust
  `portable::CountSketch::to_msgpack` writes a *4-element* array (adds `topk`),
  which the Go decoder rejects — so both directions use the explicit 3-tuple.
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
| **ramp** | geometric | fired ✓ | 1,384,920 | 24 | 56 |

- **Stable regime (the realistic monitoring case): geometric uses ~4× less
  communication** while reaching the same (correct) no-alert decision — sites
  stay locally safe and go silent, so the coordinator receives almost nothing.
- **Ramp regime**: geometric still detects the crossing and ships 3× fewer
  sketches, but loses overall because the coordinator re-broadcasts the full
  ~11.5 KB `C_ref` to every edge on each ship.

## Known limitation / next step

The geometric `C_ref` re-broadcast sends the **full** reference matrix to every
edge on each resync, which dominates cost in high-drift regimes. Delta-encoding
the broadcast (OctoSketch-style — only changed cells) would cut it sharply and
let geometric win the ramp regime too. The `F2Engine` is wired to the coordinator
over real gRPC and driven by the eval; hooking it into the main runtime's
per-window flush path in `precompute.go` (alongside the scalar `monitorValue`
hook) is the remaining productionization step.
