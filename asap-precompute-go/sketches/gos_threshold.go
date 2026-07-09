package sketches

import "math"

// GOS per-cell threshold allocation — the edge-side twin of the controller's
// Rust `control_plane/src/threshold_alloc.rs` (see
// docs/design-gos-unified-edge-telemetry.md §7). The edge computes its own
// per-cell delta thresholds from local sketch state and a small set of
// controller-pushed scalars (ε, k, sampling p, freshness), rather than
// receiving a full threshold vector over the wire.
//
// The F2 case is ISOTROPIC (uniform T = ε‖Ĉ‖/(2k√(dw))), so the scalar it
// returns feeds the existing per-cell delta path (`ComputeDeltaAgainst`, which
// already gates each cell by `|ΔS[r][c]| ≥ threshold`) with no serialization
// change. The anisotropic (gradient-weighted) `AllocateThresholds` variant is
// provided for the future per-cell-threshold delta.

// GosCell is one Count-Sketch cell's inputs to the allocator.
type GosCell struct {
	// Grad = |g_j| = |∂f/∂x_j| (function sensitivity; for F2, 2|Ĉ_j|). Non-positive
	// ⇒ the cell is irrelevant to the function, bounded only by the caps.
	Grad float64
	// Activity = V_j (per-window change mass on the cell).
	Activity float64
}

// GosParams are the relative accuracy budget and box bounds.
type GosParams struct {
	Budget     float64 // B (relative: ε‖Ĉ‖² for F2)
	K          uint32  // sites
	TQueryCap  float64 // OctoSketch query cap (math.Inf(1) for none)
	SampleP    float64 // sampling rate p (floor √(V(1-p)/p)); ≥1 ⇒ no floor
	FreshDelta float64 // freshness bound Δ* (cap V_j·Δ*); math.Inf(1) for none
}

func (p GosParams) floor(activity float64) float64 {
	if p.SampleP >= 1.0 || p.SampleP <= 0.0 || activity <= 0.0 {
		return 0.0
	}
	return math.Sqrt(activity * (1.0 - p.SampleP) / p.SampleP)
}

func (p GosParams) cap(activity float64) float64 {
	fresh := math.Inf(1)
	if !math.IsInf(p.FreshDelta, 1) {
		fresh = activity * p.FreshDelta
	}
	return math.Min(p.TQueryCap, fresh)
}

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

// AllocateThresholds is the Go mirror of Rust `threshold_alloc::allocate_thresholds`:
// box-constrained water-filling of per-cell thresholds T_j ∝ √(V_j/|g_j|) subject
// to the relative budget k·Σ|g_j|T_j ≤ B, clamped to [floor, min(cap)].
func AllocateThresholds(cells []GosCell, p GosParams) []float64 {
	k := math.Max(1, float64(p.K))
	out := make([]float64, len(cells))

	free := make([]int, 0, len(cells))
	for j, c := range cells {
		if c.Grad > 0 && c.Activity > 0 {
			free = append(free, j)
		} else {
			out[j] = p.cap(c.Activity)
		}
	}

	budgetLeft := p.Budget
	for {
		var denom float64
		for _, j := range free {
			denom += math.Sqrt(k * cells[j].Grad * cells[j].Activity)
		}
		if denom <= 0 || budgetLeft <= 0 {
			for _, j := range free {
				out[j] = p.floor(cells[j].Activity)
			}
			break
		}
		s := budgetLeft / denom
		clampedAny := false
		next := free[:0:0] // fresh slice, don't alias
		for _, j := range free {
			cj := k * cells[j].Grad
			t := s * math.Sqrt(cells[j].Activity/cj)
			cap := p.cap(cells[j].Activity)
			floor := p.floor(cells[j].Activity)
			switch {
			case t > cap:
				out[j] = cap
				budgetLeft -= cj * cap
				clampedAny = true
			case t < floor:
				out[j] = floor
				budgetLeft -= cj * floor
				clampedAny = true
			default:
				next = append(next, j)
			}
		}
		if !clampedAny {
			for _, j := range next {
				cj := k * cells[j].Grad
				out[j] = s * math.Sqrt(cells[j].Activity/cj)
			}
			break
		}
		free = next
		if len(free) == 0 {
			break
		}
	}
	return out
}
