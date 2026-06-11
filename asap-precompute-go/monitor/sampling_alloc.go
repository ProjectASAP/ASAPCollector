package monitor

import "math"

// AllocateSampleRates computes the distributed-NitroSketch per-edge update-
// sampling probabilities p_i that MINIMIZE total edge update work Σ rate_i·p_i
// subject to a merged sampling-variance budget
//
//	V  ≥  Σ_i f_i·(1−p_i)/p_i
//
// (the additive merged variance for a linear sketch; see
// docs/distributed-nitrosketch-coordinated-sampling.md). The KKT solution is
//
//	p_i  =  clamp( √λ · √(f_i / rate_i),  0, 1 )
//
// i.e. sample harder (smaller p) on high-rate edges where local accuracy is
// cheap, and keep p≈1 on low-rate edges. √λ is the single scale that makes the
// variance constraint bind; we solve it by bisection (variance is monotone
// decreasing in the scale). This is the coordinator's allocation step — it would
// run in the data-plane coordinator and ship each p_i in a Grant.SampleP; this
// Go copy is the reference implementation + the thing the unit test exercises.
//
// rates[i] = edge i's items/window; freqs[i] = edge i's mass of the queried
// quantity (pass rates as a proxy when the per-key split is unknown — then
// p_i ∝ 1/√rate_i). varBudget = V in the bound above (≈ (ε·‖f‖)²-derived). A
// rate_i ≤ 0 yields p_i = 1 (nothing to sample).
func AllocateSampleRates(rates, freqs []float64, varBudget float64) []float64 {
	n := len(rates)
	p := make([]float64, n)
	if n == 0 {
		return p
	}
	base := make([]float64, n) // √(f_i/rate_i)
	maxBase := 0.0
	for i := range rates {
		if rates[i] <= 0 || freqs[i] <= 0 {
			base[i] = 0 // forces p_i = 1 (no rate ⇒ no sampling benefit)
			continue
		}
		base[i] = math.Sqrt(freqs[i] / rates[i])
		if base[i] > maxBase {
			maxBase = base[i]
		}
	}
	if maxBase == 0 || varBudget <= 0 {
		for i := range p {
			p[i] = 1
		}
		return p
	}

	// variance(scale) = Σ f_i·(1−p_i)/p_i with p_i = min(1, scale·base_i).
	// Monotone decreasing in scale: scale→0 ⇒ p→0 ⇒ variance→∞; at
	// scale = 1/min(positive base_i) all p_i hit 1 ⇒ variance = 0.
	variance := func(scale float64) float64 {
		v := 0.0
		for i := range rates {
			if base[i] == 0 {
				continue // p_i = 1, contributes 0
			}
			pi := scale * base[i]
			if pi >= 1 {
				continue
			}
			v += freqs[i] * (1 - pi) / pi
		}
		return v
	}

	// scaleHi: every positive-base edge clamped to p_i = 1 (variance 0 ≤ V).
	minBase := math.Inf(1)
	for i := range base {
		if base[i] > 0 && base[i] < minBase {
			minBase = base[i]
		}
	}
	scaleHi := 1.0 / minBase
	if variance(scaleHi) >= varBudget {
		// Even no sampling (all p=1) can't meet V → don't sample at all.
		for i := range p {
			p[i] = 1
		}
		return p
	}
	// Bisect for the smallest scale (= most sampling, least CPU) with
	// variance(scale) ≤ varBudget.
	lo, hi := 0.0, scaleHi
	for iter := 0; iter < 100; iter++ {
		mid := 0.5 * (lo + hi)
		if mid <= 0 {
			lo = mid
			continue
		}
		if variance(mid) > varBudget {
			lo = mid // too much sampling (variance too high) → raise scale
		} else {
			hi = mid
		}
	}
	scale := hi
	for i := range rates {
		if base[i] == 0 {
			p[i] = 1
			continue
		}
		p[i] = math.Min(1, scale*base[i])
	}
	return p
}

// UniformSampleRate returns the single sampling probability p that meets the
// same variance budget V with one rate everywhere (the per-edge-independent
// NitroSketch baseline): (1−p)/p·Σf = V ⇒ p = Σf/(Σf+V). Used to quantify the
// coordination win vs uniform p.
func UniformSampleRate(freqs []float64, varBudget float64) float64 {
	sumF := 0.0
	for _, f := range freqs {
		if f > 0 {
			sumF += f
		}
	}
	if sumF <= 0 || varBudget <= 0 {
		return 1
	}
	p := sumF / (sumF + varBudget)
	if p > 1 {
		return 1
	}
	return p
}
