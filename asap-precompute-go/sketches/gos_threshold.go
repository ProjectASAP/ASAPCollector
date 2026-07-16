// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import "math"

// F2IsotropicThreshold is the GOS closed form for the F2 (‖f‖₂²) functional's
// uniform per-cell insert-time delta threshold — see
// docs/design-gos-unified-edge-telemetry.md §7/§11 and
// docs/sampling-cdm-gos-derivations.md §8: a cell is worth sending once its
// accumulated-since-last-sync magnitude reaches
//
//	T = ε·‖Ĉ‖ / (2·k·√(d·w))
//
// where ‖Ĉ‖ is the sketch's current whole-matrix Frobenius norm, k is the
// number of coordinating sites (edges) sharing the accuracy budget, and d·w
// is the cell count (rows×cols). The threshold scales with the current norm
// so relative error stays bounded as the sketch grows (cold start: norm≈0 ⇒
// T≈0 ⇒ the first few inserts cross almost immediately, which is intentional
// — see design doc §11 "Cold start is a feature, not a bug").
//
// Returns +Inf for degenerate dims (d·w<=0), which callers treat as "no
// gating" (never crosses).
func F2IsotropicThreshold(epsilon, norm float64, k uint32, d, w int) float64 {
	kk := math.Max(1, float64(k))
	n := float64(d * w)
	if n <= 0 {
		return math.Inf(1)
	}
	return epsilon * norm / (2.0 * kk * math.Sqrt(n))
}

// CMSIsotropicThreshold is the GOS closed form for CountMinSketch's per-cell
// insert-time delta threshold — see
// docs/sampling-cdm-gos-derivations.md §8.2 "CDM isotropic threshold (L1,
// max-composition)":
//
//	T = ε·N / k
//
// where N is the sketch's current total (nonnegative) mass — the L1 scale,
// as opposed to CountSketch's F2IsotropicThreshold, which scales with the
// L2/Frobenius norm. There is deliberately no √(d·w) factor here: CMS's
// point query is a min over d cells, so its staleness is bounded by the
// single worst stale cell (max-composition), not combined per-row mass —
// the water-filling/Frobenius derivation that produces CountSketch's √(dw)
// term does not apply to a min-composed estimator.
func CMSIsotropicThreshold(epsilon, n float64, k uint32) float64 {
	kk := math.Max(1, float64(k))
	return epsilon * n / kk
}
