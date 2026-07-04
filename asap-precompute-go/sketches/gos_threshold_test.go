package sketches

import (
	"fmt"
	"math"
	"testing"
)

// TestGosModeToggle exercises the isotropic/anisotropic delta toggle end-to-end:
// both modes produce a valid delta against the empty base on skewed data, and
// GOS-off preserves the passed-threshold path.
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

	for _, aniso := range []bool{false, true} {
		w.SetGosMode(0.1, aniso, 4)
		payload, _, err := w.ComputeDeltaAgainst(base, 1)
		if err != nil {
			t.Fatalf("aniso=%v: %v", aniso, err)
		}
		if len(payload) == 0 {
			t.Fatalf("aniso=%v: empty payload", aniso)
		}
	}

	// GOS off: the passed threshold path is used, no error.
	w.SetGosMode(0, false, 4)
	if _, _, err := w.ComputeDeltaAgainst(base, 1); err != nil {
		t.Fatalf("gos off: %v", err)
	}
}

func params(budget float64, k uint32) GosParams {
	return GosParams{Budget: budget, K: k, TQueryCap: math.Inf(1), SampleP: 1.0, FreshDelta: math.Inf(1)}
}

// Mirrors Rust threshold_alloc::tests::f2_closed_form_matches_formula.
func TestF2IsotropicClosedForm(t *testing.T) {
	got := F2IsotropicThreshold(0.1, 1000.0, 4, 5, 256)
	want := 0.1 * 1000.0 / (2.0 * 4.0 * math.Sqrt(float64(5*256)))
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestAllocateUniformBindsBudget(t *testing.T) {
	cells := make([]GosCell, 8)
	for i := range cells {
		cells[i] = GosCell{Grad: 2.0, Activity: 100.0}
	}
	th := AllocateThresholds(cells, params(500.0, 3))
	for i := 1; i < len(th); i++ {
		if math.Abs(th[i]-th[0]) > 1e-9 {
			t.Fatalf("thresholds must be uniform: %v", th)
		}
	}
	var used float64
	for i, c := range cells {
		used += c.Grad * th[i]
	}
	used *= 3.0
	if math.Abs(used-500.0) > 1e-6 {
		t.Fatalf("budget must bind, used %v", used)
	}
}

func TestAllocateAnisotropicShape(t *testing.T) {
	// T_j ∝ √(V_j/|g_j|): 4× activity ⇒ 2× T; 4× grad ⇒ 0.5× T.
	cells := []GosCell{
		{Grad: 1, Activity: 100},
		{Grad: 1, Activity: 400},
		{Grad: 4, Activity: 100},
	}
	th := AllocateThresholds(cells, params(1000.0, 1))
	if math.Abs(th[1]/th[0]-2.0) > 1e-6 {
		t.Errorf("activity ratio: %v / %v", th[1], th[0])
	}
	if math.Abs(th[2]/th[0]-0.5) > 1e-6 {
		t.Errorf("grad ratio: %v / %v", th[2], th[0])
	}
}

func TestSamplingFloorLifts(t *testing.T) {
	cells := make([]GosCell, 4)
	for i := range cells {
		cells[i] = GosCell{Grad: 1, Activity: 100}
	}
	p := params(1e-6, 1)
	p.SampleP = 0.5 // floor = √(100·0.5/0.5) = 10
	th := AllocateThresholds(cells, p)
	for _, tj := range th {
		if math.Abs(tj-10.0) > 1e-9 {
			t.Fatalf("floor 10, got %v", tj)
		}
	}
}

func TestQueryCapClamps(t *testing.T) {
	cells := make([]GosCell, 4)
	for i := range cells {
		cells[i] = GosCell{Grad: 1, Activity: 100}
	}
	p := params(1e9, 1)
	p.TQueryCap = 5.0
	th := AllocateThresholds(cells, p)
	for _, tj := range th {
		if tj > 5.0+1e-9 {
			t.Fatalf("capped at 5, got %v", tj)
		}
	}
}
