// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"math"
	"testing"
)

// TestCountSketch_UpdateSampling_Unbiased verifies the NitroSketch update-
// sampling on the Count-Sketch wrapper: with p<1 each item is admitted w.p. p
// and upweighted by 1/p, so the frequency estimate stays ~unbiased while ~(1−p)
// of the per-item d-row counter work is skipped (the distributed-NitroSketch CPU
// lever). The admitted subset is deterministic (fixed seed), so the test is
// stable.
func TestCountSketch_UpdateSampling_Unbiased(t *testing.T) {
	const n = 40000
	mk := func(p float64) *CountSketchWrapper {
		w, err := NewCountSketchWrapper(5, 2048)
		if err != nil {
			t.Fatalf("new: %v", err)
		}
		w.WithSampleP(p)
		for i := 0; i < n; i++ {
			w.UpdateString("k", 1)
		}
		return w
	}
	exact := mk(1.0) // no sampling
	sampled := mk(0.25)

	if exact.SampleP() != 1.0 {
		t.Fatalf("WithSampleP(1.0) should disable sampling, got %v", exact.SampleP())
	}
	if sampled.SampleP() != 0.25 {
		t.Fatalf("SampleP() = %v, want 0.25", sampled.SampleP())
	}
	eExact := exact.EstimateCount([]byte("k"))
	eSampled := sampled.EstimateCount([]byte("k"))
	t.Logf("exact estimate=%.0f  sampled(p=0.25) estimate=%.0f  (true=%d)", eExact, eSampled, n)

	// Unbiasedness: the sampled estimate should be within a few % of the true
	// count despite touching the counters only ~1/4 as often.
	if rel := math.Abs(eSampled-float64(n)) / float64(n); rel > 0.10 {
		t.Fatalf("sampled estimate %.0f is %.1f%% off true %d — should be ~unbiased", eSampled, 100*rel, n)
	}
}
