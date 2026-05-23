package precompute

import "testing"

// TestObserveKeyedMatchesObserve verifies the shared-key entry point:
// ObserveKeyed fed the key cfg.SeriesKeyFor(obs) produces the same windowed
// state (same series partition + counts) as Observe, so asap_edge can build
// the precompute key once and share the decode.
func TestObserveKeyedMatchesObserve(t *testing.T) {
	cfg := &PrecomputeConfig{
		AggID:       7,
		SketchType:  SketchTypeDDSketch,
		Mode:        Tumbling,
		Window:      WindowSpec{Size: 60_000_000_000}, // 60s in ns
		AggregateBy: []string{"zone"},
	}
	mkObs := func(zone string, ts uint64, v float64) *Observation {
		return &Observation{
			TimestampMs: ts,
			Labels:      []KeyValue{{Key: "zone", Value: zone}, {Key: "method", Value: "GET"}},
			Value:       ObservationValue{Kind: KindFloat, Float: v},
		}
	}
	newP := func() Precompute {
		return New(cfg, func() Sketch { return &countSketchStub{} }, observeAdder{})
	}

	pu := newP() // unkeyed
	pk := newP() // keyed
	for i := 0; i < 6; i++ {
		ts := uint64(1000 + i)
		for _, z := range []string{"z0", "z1"} {
			o := mkObs(z, ts, float64(i))
			if err := pu.Observe(o); err != nil {
				t.Fatalf("Observe: %v", err)
			}
			o2 := mkObs(z, ts, float64(i))
			if err := pk.ObserveKeyed(cfg.SeriesKeyFor(o2), o2); err != nil {
				t.Fatalf("ObserveKeyed: %v", err)
			}
		}
	}
	su, sk := pu.Stats().Snapshot(), pk.Stats().Snapshot()
	if su.ActiveSeries != sk.ActiveSeries {
		t.Fatalf("ActiveSeries: unkeyed=%d keyed=%d", su.ActiveSeries, sk.ActiveSeries)
	}
	if su.ActiveSeries != 2 {
		t.Fatalf("expected 2 series (z0,z1 collapsed by aggregate_by=[zone]), got %d", su.ActiveSeries)
	}
	if su.InputObservations != sk.InputObservations {
		t.Fatalf("InputObservations: unkeyed=%d keyed=%d", su.InputObservations, sk.InputObservations)
	}
}

// minimal Sketch + observer stubs (sum the values; only counts matter here).
type countSketchStub struct{ n uint64 }

func (s *countSketchStub) Snapshot() ([]byte, error)                  { return nil, nil }
func (s *countSketchStub) ComputeDeltaAgainst([]byte, uint64) ([]byte, bool, error) { return nil, true, nil }
func (s *countSketchStub) ApplyDelta([]byte) error                    { return nil }
func (s *countSketchStub) Merge(Sketch) error                         { return nil }
func (s *countSketchStub) Reset()                                     { s.n = 0 }

type observeAdder struct{}

func (observeAdder) Observe(sk Sketch, _ ObservationValue) error {
	sk.(*countSketchStub).n++
	return nil
}
