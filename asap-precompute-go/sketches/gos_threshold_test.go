package sketches

import (
	"fmt"
	"math"
	"testing"
)

// TestGosModeToggle exercises the isotropic GOS delta gate end-to-end: it
// produces a valid delta against the empty base on skewed data, and GOS-off
// preserves the passed-threshold path.
func TestGosModeToggle(t *testing.T) {
	w, err := NewCountSketchWrapper(5, 256)
	if err != nil {
		t.Fatalf("new wrapper: %v", err)
	}
	w.UpdateString("hot", 1000) // one heavy key
	for i := 0; i < 50; i++ {
		w.UpdateString(fmt.Sprintf("k%d", i), 1) // many light keys
	}
	base, err := w.DeltaAgainstEmptyBase()
	if err != nil {
		t.Fatalf("empty base: %v", err)
	}

	w.SetGosMode(0.1, 4)
	payload, _, err := w.ComputeDeltaAgainst(base, 1)
	if err != nil {
		t.Fatalf("SetGosMode(0.1, 4): %v", err)
	}
	if len(payload) == 0 {
		t.Fatal("empty payload")
	}

	// GOS off: the passed threshold path is used, no error.
	w.SetGosMode(0, 4)
	if _, _, err := w.ComputeDeltaAgainst(base, 1); err != nil {
		t.Fatalf("gos off: %v", err)
	}
}

// Mirrors Rust threshold_alloc::tests::f2_closed_form_matches_formula.
func TestF2IsotropicClosedForm(t *testing.T) {
	got := F2IsotropicThreshold(0.1, 1000.0, 4, 5, 256)
	want := 0.1 * 1000.0 / (2.0 * 4.0 * math.Sqrt(float64(5*256)))
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("got %v want %v", got, want)
	}
}
