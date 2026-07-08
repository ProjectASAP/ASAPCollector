package otlpfilter

import (
	"testing"
)

// TestUpsert_GrantLifecycle drives the SampleState the way the asap_edge
// grant hook does — install, update, and withdraw one metric's sampling —
// and checks the filter's behavior tracks each step.
func TestUpsert_GrantLifecycle(t *testing.T) {
	const (
		name = "warm.granted"
		n    = 400
	)
	s := NewSampleState()
	in := marshal(t, buildGaugeMetric(name, n))

	// No grant yet: passthrough.
	if out := s.FilterRequest(in); string(out) != string(in) {
		t.Fatalf("un-granted metric must pass through byte-identically")
	}

	// Grant p=0.2, d=2 → thinned.
	s.Upsert(name, SampleParams{P: 0.2, Rows: 2})
	kept := gaugeDataPointCount(unmarshal(t, s.FilterRequest(in)), name)
	if kept <= 0 || kept >= n {
		t.Fatalf("granted metric not thinned: kept=%d of %d", kept, n)
	}

	// Re-grant identical params: decisions unchanged (stateless — same
	// survivors, not merely the same count).
	kept2 := gaugeDataPointCount(unmarshal(t, s.FilterRequest(in)), name)
	if kept2 != kept {
		t.Fatalf("identical re-grant changed survivors: %d vs %d", kept2, kept)
	}

	// Grant p=1 (coordinator withdraws sampling) → entry removed, passthrough.
	s.Upsert(name, SampleParams{P: 1.0, Rows: 2})
	if out := s.FilterRequest(in); string(out) != string(in) {
		t.Fatalf("withdrawn grant must restore byte-identical passthrough")
	}

	// Withdrawing an absent entry is a no-op (no panic, still passthrough).
	s.Upsert(name, SampleParams{P: 0, Rows: 0})
	if out := s.FilterRequest(in); string(out) != string(in) {
		t.Fatalf("no-op withdraw must keep passthrough")
	}

	// A different metric's grant does not affect this one.
	s.Upsert("other.metric", SampleParams{P: 0.1, Rows: 4})
	if out := s.FilterRequest(in); string(out) != string(in) {
		t.Fatalf("unrelated grant must not thin this metric")
	}
}

// TestDefault_SharedInstance: Default() is one process-wide state — the
// asap_edge grant hook (writer) and the asap_otlp receiver (reader) see the
// same map. Cleaned up afterwards to keep tests isolated.
func TestDefault_SharedInstance(t *testing.T) {
	const name = "warm.shared"
	if Default() != Default() {
		t.Fatal("Default must return a stable singleton")
	}
	Default().Upsert(name, SampleParams{P: 0.5, Rows: 3})
	defer Default().Upsert(name, SampleParams{P: 1, Rows: 0}) // cleanup

	p, seed, sampled := Default().paramsFor(name)
	if !sampled || p.P != 0.5 || p.Rows != 3 || seed == 0 {
		t.Fatalf("shared state did not observe the upsert: %+v sampled=%v", p, sampled)
	}
}
