package precompute

import "testing"

// TestSketchEnvelope_ZeroValueDefaults pins the documented zero-value
// behavior of the new in-process fields added in step 2.4b. A
// freshly-allocated SketchEnvelope must report Count=0 and
// Temporality=0 (unspecified) so adapters that don't run the
// runtime's flush path (e.g. test fixtures hand-building envelopes)
// see deterministic defaults.
func TestSketchEnvelope_ZeroValueDefaults(t *testing.T) {
	t.Parallel()
	var env SketchEnvelope
	if env.MetricName != "" {
		t.Errorf("MetricName: want empty, got %q", env.MetricName)
	}
	if env.Count != 0 {
		t.Errorf("Count: want 0, got %d", env.Count)
	}
	if env.AggregationTemporality != 0 {
		t.Errorf("AggregationTemporality: want 0 (unspecified), got %d", env.AggregationTemporality)
	}
}

// TestSketchEnvelope_FieldsAssignable confirms the typed fields are
// settable as plain values — guards against an accidental switch to
// pointer types during refactors that would break the host-neutral
// invariant from ADR-0002.
func TestSketchEnvelope_FieldsAssignable(t *testing.T) {
	t.Parallel()
	env := SketchEnvelope{
		MetricName:             "http.requests",
		Count:                  17,
		AggregationTemporality: 1, // delta
	}
	if env.MetricName != "http.requests" {
		t.Errorf("MetricName: %q", env.MetricName)
	}
	if env.Count != 17 {
		t.Errorf("Count: %d", env.Count)
	}
	if env.AggregationTemporality != 1 {
		t.Errorf("AggregationTemporality: %d", env.AggregationTemporality)
	}
}
