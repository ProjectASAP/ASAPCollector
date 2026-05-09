// Package parity is the Phase 2 end-to-end parity harness for the
// ASAP edge-framework migration. It verifies that the new
// asap-precompute-go runtime emits SketchEnvelopes byte-identical
// to today's 5 OTel sketch processors when given the same input
// stream — the gate before refactoring those processors into thin
// shims that delegate to the runtime (Phase 2 steps 2.5–2.9).
//
// This file holds the test entry points; the harness logic lives in
// integration/parity/runtime-impl/harness/.
package parity_test

import (
	"testing"

	"github.com/ProjectASAP/ASAPCollector/integration/parity/runtime-impl/harness"
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
			// KLL byte-parity is now achieved end-to-end:
			// sketchlib-go's NewKLLSketchWithSeed plus the
			// kllprocessor's Seed config knob make compaction
			// deterministic when both pipelines use the same
			// seed. The harness pins HarnessKLLSeed=42 on both
			// sides — see harness/runtime.go and harness/legacy.go.
		},
		{
			name:   "HLL",
			metric: harness.MetricHLL,
		},
		{
			name:   "CountSketch",
			metric: harness.MetricCountSketch,
		},
		{
			name:   "CountMinSketch",
			metric: harness.MetricCMS,
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
	runIsolated(t, "KLL", harness.MetricKLL, "")
}

func TestParity_HLL(t *testing.T) {
	runIsolated(t, "HLL", harness.MetricHLL, "")
}

func TestParity_CountSketch(t *testing.T) {
	runIsolated(t, "CountSketch", harness.MetricCountSketch, "")
}

func TestParity_CountMinSketch(t *testing.T) {
	runIsolated(t, "CountMinSketch", harness.MetricCMS, "")
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
