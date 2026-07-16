package sketches

import "math"

// GOS per-cell threshold allocation — the edge-side twin of the controller's
// Rust `control_plane/src/threshold_alloc.rs` (see
// docs/design-gos-unified-edge-telemetry.md §7). The edge computes its own
// per-cell delta thresholds from local sketch state and a small set of
// controller-pushed scalars (ε, k), rather than receiving a full threshold
// vector over the wire.
//
// The F2 case is ISOTROPIC (uniform T = ε‖Ĉ‖/(2k√(dw))), so the scalar it
// returns feeds the existing per-cell delta path (`ComputeDeltaAgainst`, which
// already gates each cell by `|ΔS[r][c]| ≥ threshold`) with no serialization
// change.
//
// The anisotropic (gradient-weighted per-cell {T_j}) water-filling variant
// that used to live here (`AllocateThresholds`/`GosCell`/`GosParams`) has been
// removed pending a redesign of its `Activity_j` input: the water-filling
// solve needs Activity measured against a single, uniformly-timed reference
// point across all cells, but under the insert-time reset-on-send model
// (docs/design-gos-unified-edge-telemetry.md §11) different cells can reset
// at different times, so the old `Activity_j = |current-prev|` definition is
// no longer well-defined without reintroducing a periodic full-matrix
// snapshot. See §11's "Open items" for the leading candidate (a per-cell EMA
// of |Δ|) — not yet implemented or verified against the §7 error guarantee.

// F2IsotropicThreshold is the closed form T = ε·‖Ĉ‖ / (2·k·√(d·w)) — the uniform
// relative-error delta threshold for F2. It scales with the current norm so
// relative error stays bounded as the sketch grows. Returns +Inf for degenerate
// dims (no gating).
func F2IsotropicThreshold(epsilon, norm float64, k uint32, d, w int) float64 {
	kk := math.Max(1, float64(k))
	n := float64(d * w)
	if n <= 0 {
		return math.Inf(1)
	}
	return epsilon * norm / (2.0 * kk * math.Sqrt(n))
}
