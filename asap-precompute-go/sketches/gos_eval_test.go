package sketches

import (
	"math"
	"testing"
)

// TestGosAnisoSavingsRatio validates the GOS anisotropic-vs-isotropic
// communication ratio ρ (design-gos §7): at equal accuracy budget B, the
// per-cell water-filling gives communication Σ V_j/T_j that is never worse than
// the uniform threshold (Cauchy–Schwarz), by the factor
//
//	ρ = (Σ_j √(|g_j| V_j))² / [ (Σ_j V_j)(Σ_j |g_j|) ]  ≤ 1
//
// with equality iff V_j/|g_j| is constant. We sweep the sketch-cell skew
// (Zipf(s) magnitudes → g_j = 2|Ĉ_j|, uniform activity V_j=1) and confirm the
// MEASURED ratio (from AllocateThresholds) matches the predicted ρ and ρ ≤ 1.
func TestGosAnisoSavingsRatio(t *testing.T) {
	const (
		n = 1280 // d·w = 5·256
		k = 4
		B = 0.1 // budget; ρ is scale-invariant in B
	)
	t.Logf("%-6s  %-10s  %-12s  %-8s", "skew", "measured ρ", "predicted ρ", "saving")
	for _, skew := range []float64{0.0, 0.5, 1.0, 1.5, 2.0} {
		g := make([]float64, n)
		V := make([]float64, n)
		var sumG float64
		for j := 0; j < n; j++ {
			g[j] = math.Pow(1.0/float64(j+1), skew) // Zipf(skew) cell magnitudes
			V[j] = 1.0                              // uniform per-window activity
			sumG += g[j]
		}
		cells := make([]GosCell, n)
		for j := range cells {
			cells[j] = GosCell{Grad: g[j], Activity: V[j]}
		}
		Taniso := AllocateThresholds(cells, GosParams{
			Budget: B, K: k, TQueryCap: math.Inf(1), SampleP: 1.0, FreshDelta: math.Inf(1),
		})
		Tiso := B / (float64(k) * sumG) // uniform: k·T·Σg = B

		var commA, commU float64
		for j := 0; j < n; j++ {
			commA += V[j] / Taniso[j]
			commU += V[j] / Tiso
		}
		measured := commA / commU

		var sSqrt, sV float64
		for j := 0; j < n; j++ {
			sSqrt += math.Sqrt(g[j] * V[j])
			sV += V[j]
		}
		predicted := sSqrt * sSqrt / (sV * sumG)

		t.Logf("%-6.1f  %-10.4f  %-12.4f  %5.1f%%", skew, measured, predicted, 100*(1-measured))

		if measured > 1.0+1e-6 {
			t.Errorf("skew=%.1f: anisotropic must not exceed uniform (Cauchy–Schwarz), ρ=%.4f", skew, measured)
		}
		if math.Abs(measured-predicted) > 0.01 {
			t.Errorf("skew=%.1f: measured ρ=%.4f should match predicted %.4f", skew, measured, predicted)
		}
	}
}
