// Package crosshostparity_test is the Phase 4 step E cross-host
// envelope byte-parity gate. It exercises asap-precompute-go through
// both the OTel adapter (fed pmetric.Metrics) and the Telegraf adapter
// (fed telegraf.Metric) on a deterministic, semantically-equivalent
// input and asserts that the resulting SketchEnvelope.Payload bytes
// match per sketch.
//
// Because the runtime is host-neutral, a divergence here means one of
// the codecs is leaking host-specific shape into the runtime — the
// exact regression Phase 4's "asap-precompute-go is single source of
// truth" objective forbids.
package crosshostparity_test

import (
	"testing"

	"github.com/ProjectASAP/ASAPCollector/integration/parity/codec/harness"
)

// TestCrossHostParity_AllSketches runs the full multi-sketch harness
// once, comparing the OTel-driven and Telegraf-driven envelopes per
// sketch. Each sketch is a subtest so a divergence in one doesn't
// mask a later success in another.
func TestCrossHostParity_AllSketches(t *testing.T) {
	syncfg := harness.DefaultSyntheticConfig()
	rcfg := harness.DefaultRuntimeConfig()

	otelInput := harness.BuildOTelInput(syncfg)
	tgInput := harness.BuildTelegrafInput(syncfg)

	otelEnvs, err := harness.RunOTel(otelInput, rcfg)
	if err != nil {
		t.Fatalf("RunOTel: %v", err)
	}
	tgEnvs, err := harness.RunTelegraf(tgInput, rcfg)
	if err != nil {
		t.Fatalf("RunTelegraf: %v", err)
	}

	cases := []struct {
		name       string
		metric     string
		skipReason string // empty = expect parity; non-empty = SKIP
	}{
		{name: "DDSketch", metric: harness.MetricDDSketch},
		{name: "KLL", metric: harness.MetricKLL},
		{name: "HLL", metric: harness.MetricHLL},
		{name: "CountSketch", metric: harness.MetricCountSketch},
		{name: "CountMinSketch", metric: harness.MetricCMS},
	}

	for _, tc := range cases {
		tc := tc
		t.Run("TestCrossHostParity_"+tc.name, func(t *testing.T) {
			if tc.skipReason != "" {
				t.Skipf("SKIP %s: %s", tc.name, tc.skipReason)
			}
			oe, ok := otelEnvs[tc.metric]
			if !ok {
				t.Fatalf("OTel path produced no output for %s", tc.metric)
			}
			te, ok := tgEnvs[tc.metric]
			if !ok {
				t.Fatalf("Telegraf path produced no output for %s", tc.metric)
			}
			harness.AssertByteParity(t, tc.name, oe, te)
		})
	}
}

// Per-sketch standalone tests so a single failure can be reproduced
// with `go test -run TestCrossHostParity_DDSketch` etc. Each spins up
// its own RunOTel / RunTelegraf so the failure is independent.

func TestCrossHostParity_DDSketch(t *testing.T) {
	runIsolatedCrossHost(t, "DDSketch", harness.MetricDDSketch, "")
}

func TestCrossHostParity_KLL(t *testing.T) {
	runIsolatedCrossHost(t, "KLL", harness.MetricKLL, "")
}

func TestCrossHostParity_HLL(t *testing.T) {
	runIsolatedCrossHost(t, "HLL", harness.MetricHLL, "")
}

func TestCrossHostParity_CountSketch(t *testing.T) {
	runIsolatedCrossHost(t, "CountSketch", harness.MetricCountSketch, "")
}

func TestCrossHostParity_CountMinSketch(t *testing.T) {
	runIsolatedCrossHost(t, "CountMinSketch", harness.MetricCMS, "")
}

func runIsolatedCrossHost(t *testing.T, name, metric, skipReason string) {
	t.Helper()
	if skipReason != "" {
		t.Skipf("SKIP %s: %s", name, skipReason)
	}
	syncfg := harness.DefaultSyntheticConfig()
	rcfg := harness.DefaultRuntimeConfig()

	otelInput := harness.BuildOTelInput(syncfg)
	tgInput := harness.BuildTelegrafInput(syncfg)

	otelEnvs, err := harness.RunOTel(otelInput, rcfg)
	if err != nil {
		t.Fatalf("RunOTel: %v", err)
	}
	tgEnvs, err := harness.RunTelegraf(tgInput, rcfg)
	if err != nil {
		t.Fatalf("RunTelegraf: %v", err)
	}
	oe, ok := otelEnvs[metric]
	if !ok {
		t.Fatalf("OTel path produced no output for %s", metric)
	}
	te, ok := tgEnvs[metric]
	if !ok {
		t.Fatalf("Telegraf path produced no output for %s", metric)
	}
	harness.AssertByteParity(t, name, oe, te)
}
