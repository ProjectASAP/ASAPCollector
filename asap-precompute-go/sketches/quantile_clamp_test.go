// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"math"
	"testing"
)

// TestQuantileClamp verifies KLL and DDSketch wrappers clamp out-of-range
// quantile inputs to [0,1] (the QuantileSketch contract) rather than
// passing them straight through to the underlying sketch.
func TestQuantileClamp(t *testing.T) {
	t.Parallel()

	t.Run("clampQuantile_helper", func(t *testing.T) {
		cases := []struct{ in, want float64 }{
			{-1, 0},
			{-0.0001, 0},
			{0, 0},
			{0.5, 0.5},
			{1, 1},
			{1.5, 1},
			{math.Inf(1), 1},
			{math.Inf(-1), 0},
			{math.NaN(), 0},
		}
		for _, c := range cases {
			if got := clampQuantile(c.in); got != c.want {
				t.Errorf("clampQuantile(%v) = %v, want %v", c.in, got, c.want)
			}
		}
	})

	t.Run("kll_out_of_range_equals_clamped", func(t *testing.T) {
		seed := int64(42)
		w := NewKLLWrapper(200, &seed)
		for i := 0; i < 1000; i++ {
			w.Update(float64(i))
		}
		// q below 0 must equal q==0; q above 1 must equal q==1.
		if got, want := w.Quantile(-5), w.Quantile(0); got != want {
			t.Errorf("KLL Quantile(-5)=%v, want Quantile(0)=%v", got, want)
		}
		if got, want := w.Quantile(9), w.Quantile(1); got != want {
			t.Errorf("KLL Quantile(9)=%v, want Quantile(1)=%v", got, want)
		}
		// NaN must be finite (clamped to 0), not NaN.
		if got := w.Quantile(math.NaN()); math.IsNaN(got) {
			t.Errorf("KLL Quantile(NaN) returned NaN, want clamped/finite")
		}
	})

	t.Run("ddsketch_out_of_range_equals_clamped", func(t *testing.T) {
		w := NewDDSketchWrapper(0.01)
		for i := 1; i <= 1000; i++ {
			w.Update(float64(i))
		}
		if got, want := w.Quantile(-5), w.Quantile(0); got != want {
			t.Errorf("DDSketch Quantile(-5)=%v, want Quantile(0)=%v", got, want)
		}
		if got, want := w.Quantile(9), w.Quantile(1); got != want {
			t.Errorf("DDSketch Quantile(9)=%v, want Quantile(1)=%v", got, want)
		}
		if got := w.Quantile(math.NaN()); math.IsNaN(got) {
			t.Errorf("DDSketch Quantile(NaN) returned NaN, want clamped/finite")
		}
	})
}
