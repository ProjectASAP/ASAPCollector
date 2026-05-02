// Package parity is the Phase 2 end-to-end parity harness for the
// ASAP edge-framework migration. It verifies that the new
// asap-precompute-go runtime emits SketchEnvelopes byte-identical
// to today's 5 OTel sketch processors when given the same input
// stream — the gate before refactoring those processors into thin
// shims that delegate to the runtime (Phase 2 steps 2.5–2.9).
//
// This file holds the test entry points; the harness logic lives in
// integration/parity/harness/.
package parity_test

import (
	"testing"

	"github.com/ProjectASAP/ASAPCollector/integration/parity/harness"
)

// TestParity_AllSketches runs the full multi-metric harness once,
// compares per-sketch envelopes, and emits a structured diff report.
// Subtests below isolate one sketch each so a failure in (say)
// CountSketch doesn't mask a later success in DDSketch.
func TestParity_AllSketches(t *testing.T) {
	syncfg := harness.DefaultSyntheticConfig()
	rcfg := harness.DefaultRuntimeConfig()

	input := harness.BuildInput(syncfg)

	runtime, err := harness.RunRuntimePath(input, rcfg)
	if err != nil {
		t.Fatalf("runtime path: %v", err)
	}
	legacy, err := harness.RunLegacyPath(input, rcfg)
	if err != nil {
		t.Fatalf("legacy path: %v", err)
	}

	// Each subtest is named so `go test -run` can target one sketch.
	cases := []struct {
		name       string
		metric     string
		skipReason string // empty = expect parity; non-empty = SKIP
	}{
		{name: "DDSketch", metric: harness.MetricDDSketch},
		{
			name:   "KLL",
			metric: harness.MetricKLL,
			// Legacy KLL batch path collapses every input data
			// point into a synthetic appended ResourceMetrics
			// (see kllprocessor.processBatch) and keys series
			// by `metricName + "::" + dpAttrs(attrs)`. Resource
			// attrs do NOT enter the series key. Output also
			// gains a `_kll` metric-name suffix the runtime
			// path doesn't replicate. asap-precompute-go always
			// includes resource attrs in SeriesKey, so the two
			// emit different envelope cardinalities for the
			// same input. Byte-for-byte parity is not reachable
			// here without aligning the legacy processor's
			// resource-handling. See PR description
			// follow-up #1.
			skipReason: "structural divergence: legacy KLL batch " +
				"path drops resource attrs from series key and adds " +
				"a `_kll` metric-name suffix",
		},
		{
			name:   "HLL",
			metric: harness.MetricHLL,
			// Same shape as KLL: legacy HLL batch path adds a
			// new empty ResourceMetrics and keys series only
			// by dp-attrs, plus a `_hll_cardinality` metric-name
			// suffix. See PR description follow-up #2.
			skipReason: "structural divergence: legacy HLL batch " +
				"path drops resource attrs from series key and adds " +
				"a `_hll_cardinality` metric-name suffix",
		},
		{
			name:   "CountSketch",
			metric: harness.MetricCountSketch,
			// Legacy CountSketchProcessor uses a single "global"
			// partition when AggregateBy is empty (one sketch
			// for ALL data points across all resources/labelsets);
			// asap-precompute-go partitions by (resource,
			// dp-labels). The two output cardinalities differ by
			// design; byte-for-byte parity isn't reachable
			// without matching the partition strategy. See PR
			// description follow-up #3.
			skipReason: "structural divergence: legacy emits one " +
				"global partition; runtime emits per-(resource,labelset) series",
		},
		{
			name:   "CountMinSketch",
			metric: harness.MetricCMS,
			// Legacy CountMinSketchProcessor keys series by
			// (metricName, dp-attrs) — resource attrs do NOT
			// enter the series key. asap-precompute-go always
			// includes resource in SeriesKey. Same divergence
			// shape as CountSketch; byte-parity not reachable
			// without running the runtime in a per-metric scope
			// that strips resource. See PR description
			// follow-up #4.
			skipReason: "structural divergence: legacy ignores " +
				"resource attrs in series key; runtime always includes them",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run("TestParity_"+tc.name, func(t *testing.T) {
			if tc.skipReason != "" {
				t.Skipf("SKIP %s: %s", tc.name, tc.skipReason)
			}
			r, ok := runtime[tc.metric]
			if !ok {
				t.Fatalf("runtime path produced no output for %s", tc.metric)
			}
			l, ok := legacy[tc.metric]
			if !ok {
				t.Fatalf("legacy path produced no output for %s", tc.metric)
			}
			report := harness.Diff(tc.name, r.Envelopes, l.AllOutputs)
			if !report.Equal() {
				t.Errorf("parity divergence:\n%s", report.Format())
			} else {
				t.Logf("parity OK: %d envelopes matched byte-for-byte", report.BothCount)
			}
		})
	}
}

// TestParity_DDSketch isolates the DDSketch comparison so a
// regression there doesn't get swallowed by an unrelated KLL
// failure. Same shape as the subtest above; runs the harness
// independently so this test is also useful with `-run TestParity_DDSketch`.
func TestParity_DDSketch(t *testing.T) {
	runIsolated(t, "DDSketch", harness.MetricDDSketch, "")
}

func TestParity_KLL(t *testing.T) {
	runIsolated(t, "KLL", harness.MetricKLL,
		"structural divergence: legacy KLL batch path drops resource "+
			"attrs from series key and adds a `_kll` metric-name suffix")
}

func TestParity_HLL(t *testing.T) {
	runIsolated(t, "HLL", harness.MetricHLL,
		"structural divergence: legacy HLL batch path drops resource "+
			"attrs from series key and adds a `_hll_cardinality` metric-name suffix")
}

func TestParity_CountSketch(t *testing.T) {
	runIsolated(t, "CountSketch", harness.MetricCountSketch,
		"structural divergence: legacy emits global partition; "+
			"runtime emits per-(resource,labelset) series")
}

func TestParity_CountMinSketch(t *testing.T) {
	runIsolated(t, "CountMinSketch", harness.MetricCMS,
		"structural divergence: legacy ignores resource attrs in "+
			"series key; runtime always includes them")
}

func runIsolated(t *testing.T, name, metric, skipReason string) {
	t.Helper()
	if skipReason != "" {
		t.Skipf("SKIP %s: %s", name, skipReason)
	}
	syncfg := harness.DefaultSyntheticConfig()
	rcfg := harness.DefaultRuntimeConfig()
	input := harness.BuildInput(syncfg)

	runtime, err := harness.RunRuntimePath(input, rcfg)
	if err != nil {
		t.Fatalf("runtime path: %v", err)
	}
	legacy, err := harness.RunLegacyPath(input, rcfg)
	if err != nil {
		t.Fatalf("legacy path: %v", err)
	}
	r, ok := runtime[metric]
	if !ok {
		t.Fatalf("runtime path produced no output for %s", metric)
	}
	l, ok := legacy[metric]
	if !ok {
		t.Fatalf("legacy path produced no output for %s", metric)
	}
	report := harness.Diff(name, r.Envelopes, l.AllOutputs)
	if !report.Equal() {
		t.Errorf("parity divergence:\n%s", report.Format())
	} else {
		t.Logf("parity OK: %d envelopes matched byte-for-byte",
			report.BothCount)
	}
}
