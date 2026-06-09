package sketches

import (
	"math"
	"testing"
)

// TestDDSketchWrapper_LinearReadout_RangeCount verifies the value-range count
// realization of monitor.FunctionalLinearBuckets: count of samples whose bucket
// value falls in [lo, hi], and that it is monotone non-decreasing.
func TestDDSketchWrapper_LinearReadout_RangeCount(t *testing.T) {
	w := NewDDSketchWrapper(0.01) // 1% relative accuracy
	for i := 0; i < 10; i++ {
		w.Update(1.0)
	}
	for i := 0; i < 5; i++ {
		w.Update(100.0)
	}
	for i := 0; i < 3; i++ {
		w.Update(1000.0)
	}

	// Whole stream (>= 0): all 18 samples.
	if got := w.LinearReadout([]float64{0}); got != 18 {
		t.Fatalf("count(>=0) = %v, want 18", got)
	}
	// >= 50: the 100s and 1000s = 8.
	if got := w.LinearReadout([]float64{50}); got != 8 {
		t.Fatalf("count(>=50) = %v, want 8", got)
	}
	// [50, 500]: just the 100s = 5 (DDSketch bucket value within 1% of 100).
	if got := w.LinearReadout([]float64{50, 500}); got != 5 {
		t.Fatalf("count([50,500]) = %v, want 5", got)
	}
	// Empty coeffs → zero (disabled).
	if got := w.LinearReadout(nil); got != 0 {
		t.Fatalf("count(nil) = %v, want 0", got)
	}

	// Monotonicity: another in-range sample only increases the readout.
	before := w.LinearReadout([]float64{50, 500})
	w.Update(100.0)
	if after := w.LinearReadout([]float64{50, 500}); after <= before {
		t.Fatalf("readout not monotone: before=%v after=%v", before, after)
	}
}

// TestDDSketchWrapper_SatisfiesLinearReader confirms the wrapper satisfies the
// exact interface the runtime's monitorValue type-asserts, so a
// FunctionalLinearBuckets spec actually activates instead of silently
// disabling.
func TestDDSketchWrapper_SatisfiesLinearReader(t *testing.T) {
	var _ interface {
		LinearReadout([]float64) float64
	} = NewDDSketchWrapper(0.01)
}

// TestDDSketchWrapper_LinearReadout_OpenUpperBound checks the single-bound form
// (coeffs=[lo]) treats hi as +Inf.
func TestDDSketchWrapper_LinearReadout_OpenUpperBound(t *testing.T) {
	w := NewDDSketchWrapper(0.01)
	w.Update(10)
	w.Update(1e6)
	got := w.LinearReadout([]float64{5})
	if got != 2 {
		t.Fatalf("count(>=5) = %v, want 2 (open upper bound should include 1e6)", got)
	}
	// Sanity: an explicit finite hi below 1e6 excludes it.
	if got := w.LinearReadout([]float64{5, 100}); got != 1 || math.IsNaN(got) {
		t.Fatalf("count([5,100]) = %v, want 1", got)
	}
}
