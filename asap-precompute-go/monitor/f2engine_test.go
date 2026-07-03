package monitor

import (
	"math"
	"testing"

	"github.com/ProjectASAP/sketchlib-go/wire/asapmsgpack"
)

// countingReporter records Register/Report calls for the ship/silent assertions.
type countingReporter struct {
	registers int
	reports   int
	lastBytes int
}

func (c *countingReporter) Register(Registration) { c.registers++ }
func (c *countingReporter) Report(r Report) {
	c.reports++
	c.lastBytes = len(r.Sketch)
}

// The golden matrices below are ALSO evaluated by the Rust side
// (data_plane/src/monitor/f2.rs test `is_locally_safe_matches_go_golden`).
// Both languages must return the SAME safe/unsafe verdict for the same inputs —
// any divergence silently breaks the geometric no-missed-crossing guarantee.
//
//	d=2, w=3   ref = 0   current = cRef = [[10,0,0],[0,10,0]]   k = 1
//	delta = current;  scale = k/2 = 0.5
//	centre = cRef + 0.5*delta = 1.5*current  ->  ||centre|| = 1.5*sqrt(200) = 21.213
//	ball   = 0.5*||delta||    = 0.5*sqrt(200)          = 7.071
//	sum    = 28.284
//	R = sqrt(d*tau):  tau=500 -> R=31.62 (SAFE)   tau=300 -> R=24.49 (UNSAFE)
func TestF2LocallySafeGolden(t *testing.T) {
	current := [][]float64{{10, 0, 0}, {0, 10, 0}}
	ref := [][]float64{{0, 0, 0}, {0, 0, 0}}
	cRef := [][]float64{{10, 0, 0}, {0, 10, 0}}
	const d = 2

	safe500 := f2LocallySafe(current, ref, cRef, 1, math.Sqrt(d*500.0))
	if !safe500 {
		t.Errorf("tau=500 must be SAFE (R=%.3f > 28.284)", math.Sqrt(d*500.0))
	}
	safe300 := f2LocallySafe(current, ref, cRef, 1, math.Sqrt(d*300.0))
	if safe300 {
		t.Errorf("tau=300 must be UNSAFE (R=%.3f < 28.284)", math.Sqrt(d*300.0))
	}
}

func TestF2EngineDistributedShipsEveryWindow(t *testing.T) {
	rep := &countingReporter{}
	eng := NewF2Engine("e0", 60000, rep)
	eng.Configure(1, nil, Spec{
		Enabled: true, Functional: FunctionalF2, Mode: F2ModeDistributed,
		Tau: 1e9, Epsilon: 0.1, SketchRows: 2, SketchCols: 3,
	})
	m := [][]float64{{1, 0, 0}, {0, 1, 0}}
	for step := 0; step < 5; step++ {
		eng.OnWindow(1, nil, m, 60000) // same epoch across steps
	}
	if rep.registers != 1 {
		t.Errorf("expected 1 register, got %d", rep.registers)
	}
	if rep.reports != 5 {
		t.Errorf("distributed must ship every step: want 5, got %d", rep.reports)
	}
	ships, silent, _, _ := eng.Stats()
	if ships != 5 || silent != 0 {
		t.Errorf("want ships=5 silent=0, got ships=%d silent=%d", ships, silent)
	}
}

func TestF2EngineGeometricStaysSilentUnderSmallDrift(t *testing.T) {
	rep := &countingReporter{}
	eng := NewF2Engine("e0", 60000, rep)
	eng.Configure(1, nil, Spec{
		Enabled: true, Functional: FunctionalF2, Mode: F2ModeGeometric,
		Tau: 1e6, Epsilon: 0.1, SketchRows: 2, SketchCols: 3,
	})
	base := [][]float64{{100, 0, 0}, {0, 100, 0}}
	// Step 0: no reference yet -> bootstrap ship + register.
	eng.OnWindow(1, nil, base, 60000)
	if rep.reports != 1 {
		t.Fatalf("bootstrap must ship once, got %d", rep.reports)
	}
	// Coordinator would broadcast C_ref = base (single site). Feed it back.
	cref, err := asapmsgpack.MarshalCountSketch(2, 3, base)
	if err != nil {
		t.Fatalf("marshal C_ref: %v", err)
	}
	eng.OnRef(RefBroadcast{AggID: 1, K: 1, CRef: cref})

	// A tiny perturbation stays inside the safe zone -> silent.
	small := [][]float64{{101, 0, 0}, {0, 100, 0}}
	eng.OnWindow(1, nil, small, 60000)
	if rep.reports != 1 {
		t.Errorf("small drift must stay silent: reports=%d", rep.reports)
	}
	// A large jump (per-row F2 = 1500² = 2.25e6 > τ) trips the safe zone -> ships.
	big := [][]float64{{1500, 0, 0}, {0, 1500, 0}}
	eng.OnWindow(1, nil, big, 60000)
	if rep.reports != 2 {
		t.Errorf("large drift must ship: reports=%d", rep.reports)
	}
	_, silent, refs, errs := eng.Stats()
	if silent != 1 || refs != 1 || errs != 0 {
		t.Errorf("want silent=1 refs=1 errs=0, got silent=%d refs=%d errs=%d", silent, refs, errs)
	}
}
