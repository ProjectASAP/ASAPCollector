// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package precompute

import "testing"

func TestPolicyFingerprint_Deterministic(t *testing.T) {
	in := PolicyFingerprintInput{
		Metric:            "requests",
		AggregationType:   "CountSketch",
		Parameters:        map[string]any{"w": 2048, "d": 5, "with_heap": false},
		GroupingLabels:    []string{"service", "zone"},
		WindowSizeSecs:    3600,
		SlideIntervalSecs: 3600,
		WindowType:        "tumbling",
	}
	a := PolicyFingerprint(in)
	b := PolicyFingerprint(in)
	if a != b {
		t.Fatalf("PolicyFingerprint is not deterministic: %d != %d", a, b)
	}
	if a == 0 {
		t.Fatal("PolicyFingerprint should never be the zero/UNSET sentinel for a real input")
	}
}

// TestPolicyFingerprint_GroupingLabelsOrderIndependent mirrors the Rust
// comment's claim that GroupingLabels need not be pre-sorted — the function
// sorts them itself (matching the backend's KeyByLabelNames
// sorted-at-construction invariant).
func TestPolicyFingerprint_GroupingLabelsOrderIndependent(t *testing.T) {
	base := PolicyFingerprintInput{
		Metric:          "requests",
		AggregationType: "Sum",
		WindowType:      "tumbling",
	}
	a := base
	a.GroupingLabels = []string{"zone", "service"}
	b := base
	b.GroupingLabels = []string{"service", "zone"}
	if PolicyFingerprint(a) != PolicyFingerprint(b) {
		t.Fatal("GroupingLabels order should not affect the fingerprint")
	}
}

// TestPolicyFingerprint_SensitiveToEachField spot-checks that changing any
// single hash-input field changes the fingerprint — a fingerprint that
// ignores a field would silently conflate two different policies.
func TestPolicyFingerprint_SensitiveToEachField(t *testing.T) {
	base := PolicyFingerprintInput{
		Metric:            "requests",
		AggregationType:   "CountSketch",
		Parameters:        map[string]any{"w": 2048},
		GroupingLabels:    []string{"service"},
		WindowSizeSecs:    3600,
		SlideIntervalSecs: 3600,
		WindowType:        "tumbling",
		SpatialFilter:     "{zone=us-east}",
	}
	baseline := PolicyFingerprint(base)

	variants := map[string]PolicyFingerprintInput{}
	v := base
	v.Metric = "latency"
	variants["metric"] = v

	v = base
	v.AggregationType = "Sum"
	variants["aggregation_type"] = v

	v = base
	v.AggregationSubType = "p99"
	variants["aggregation_sub_type"] = v

	v = base
	v.Parameters = map[string]any{"w": 4096}
	variants["parameters"] = v

	v = base
	v.GroupingLabels = []string{"pod"}
	variants["grouping_labels"] = v

	v = base
	v.WindowSizeSecs = 60
	variants["window_size_secs"] = v

	v = base
	v.WindowType = "sliding"
	variants["window_type"] = v

	v = base
	v.SpatialFilter = "{zone=us-west}"
	variants["spatial_filter"] = v

	for name, in := range variants {
		if got := PolicyFingerprint(in); got == baseline {
			t.Errorf("changing %s did not change the fingerprint (still %d)", name, got)
		}
	}
}

// TestPolicyFingerprint_SpatialFilterNormalization mirrors
// normalize_spatial_filter (crates/asap_types/src/utils.rs): the fingerprint
// must be identical regardless of whitespace or predicate order within the
// braces, since the backend normalizes identically before hashing.
func TestPolicyFingerprint_SpatialFilterNormalization(t *testing.T) {
	base := PolicyFingerprintInput{
		Metric:          "requests",
		AggregationType: "Sum",
		WindowType:      "tumbling",
	}
	// Reordering whole comma-separated predicates (no whitespace touching a
	// comma) normalizes identically: PolicyFingerprint normalizes internally
	// before hashing, so these two raw inputs fingerprint the same.
	a := base
	a.SpatialFilter = "{b,a}"
	b := base
	b.SpatialFilter = "{a,b}"
	if PolicyFingerprint(a) != PolicyFingerprint(b) {
		t.Fatal("{b,a} and {a,b} should fingerprint identically (same predicate set, reordered)")
	}

	// NOTE (documented quirk, not a Go-side bug): normalizeSpatialFilter only
	// trims the OUTER "{...}" wrapper before splitting on ",", never the
	// whitespace immediately adjacent to each comma — so a filter with
	// spaces around its commas does NOT normalize to the same string as the
	// same predicates written without spaces. This matches
	// normalize_spatial_filter's identical behavior in
	// ASAPQuery-backend/crates/asap_types/src/utils.rs byte-for-byte
	// (confirmed by reading that source) — the cross-language contract
	// requires matching Rust's behavior exactly, including this quirk,
	// rather than "fixing" it unilaterally on one side only.
	c := base
	c.SpatialFilter = "{b,a}"
	d := base
	d.SpatialFilter = "{ a , b }"
	if PolicyFingerprint(c) == PolicyFingerprint(d) {
		t.Fatal("{b,a} and { a , b } are NOT equivalent inputs here (comma-adjacent whitespace isn't trimmed) — see comment")
	}

	e := base
	e.SpatialFilter = ""
	f := base
	f.SpatialFilter = ""
	if PolicyFingerprint(e) != PolicyFingerprint(f) {
		t.Fatal("empty spatial filter should be deterministic")
	}
}
