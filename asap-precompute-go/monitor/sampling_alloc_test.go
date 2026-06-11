package monitor

import (
	"math"
	"testing"
)

// mergedVariance = Σ f_i·(1−p_i)/p_i (the additive sampling variance the
// allocation must keep ≤ budget).
func mergedVariance(freqs, p []float64) float64 {
	v := 0.0
	for i := range freqs {
		if p[i] > 0 && p[i] < 1 {
			v += freqs[i] * (1 - p[i]) / p[i]
		}
	}
	return v
}
func totalCPU(rates, p []float64) float64 {
	c := 0.0
	for i := range rates {
		c += rates[i] * p[i]
	}
	return c
}

// TestAllocateSampleRates_SkewedFleetBeatsUniform: on a skewed fleet (one hot
// edge, a quiet tail), the coordinated p_i ∝ √(f_i/rate_i) allocation hits the
// same merged-variance budget at strictly LESS total edge update work than the
// per-edge-uniform NitroSketch baseline — the whole point of coordination.
func TestAllocateSampleRates_SkewedFleetBeatsUniform(t *testing.T) {
	// Skewed TOTAL rates (one hot edge) but the queried key is spread ~evenly,
	// so f_i/rate_i varies — exactly when coordination helps (when f_i ∝ rate_i
	// the optimum is uniform; see the flat case). The hot edge does 100k updates
	// but holds little of the key, so sampling it hard cuts most of the CPU for
	// little accuracy loss on the key.
	rates := []float64{100000, 1000, 1000, 1000, 1000} // 1 hot edge + 4 quiet
	freqs := []float64{100, 100, 100, 100, 100}         // key ~uniform across edges
	V := 2000.0

	pCoord := AllocateSampleRates(rates, freqs, V)
	pUni := UniformSampleRate(freqs, V)
	uni := make([]float64, len(rates))
	for i := range uni {
		uni[i] = pUni
	}

	vc, vu := mergedVariance(freqs, pCoord), mergedVariance(freqs, uni)
	if vc > V*1.02 {
		t.Fatalf("coordinated variance %.1f exceeds budget %.1f", vc, V)
	}
	if math.Abs(vu-V) > V*0.02 {
		t.Fatalf("uniform variance %.1f should ≈ budget %.1f", vu, V)
	}
	cc, cu := totalCPU(rates, pCoord), totalCPU(rates, uni)
	t.Logf("coordinated p=%v  CPU=%.0f  var=%.0f", round3(pCoord), cc, vc)
	t.Logf("uniform     p=%.4f      CPU=%.0f  var=%.0f", pUni, cu, vu)
	if cc >= cu {
		t.Fatalf("coordinated CPU %.0f should be < uniform CPU %.0f at equal variance", cc, cu)
	}
	t.Logf("coordination saves %.1f%% edge update work at equal accuracy", 100*(cu-cc)/cu)
	// Hot edge sampled hard; quiet edges near p=1.
	if pCoord[0] >= pCoord[1] {
		t.Fatalf("hot edge should get smaller p than quiet edges: %v", pCoord)
	}
}

// TestAllocateSampleRates_FlatFleetNoWin: honest negative — on a uniform fleet
// the coordinated allocation collapses to ≈ the uniform rate (no skew to exploit).
func TestAllocateSampleRates_FlatFleetNoWin(t *testing.T) {
	rates := []float64{2000, 2000, 2000, 2000}
	V := 3000.0
	pCoord := AllocateSampleRates(rates, rates, V)
	pUni := UniformSampleRate(rates, V)
	cc := totalCPU(rates, pCoord)
	cu := pUni * (2000 * 4)
	if math.Abs(cc-cu) > 0.05*cu {
		t.Fatalf("flat fleet: coordinated CPU %.0f should ≈ uniform %.0f (no win)", cc, cu)
	}
	t.Logf("flat fleet: coordinated≈uniform (CPU %.0f vs %.0f), as expected", cc, cu)
}

func round3(xs []float64) []float64 {
	o := make([]float64, len(xs))
	for i, x := range xs {
		o[i] = math.Round(x*1000) / 1000
	}
	return o
}
