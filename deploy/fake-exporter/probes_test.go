// Tests for the MVP v6 freshness probes.
//
// Contract under test (probes.go):
//
//   - startFreshnessProbes wires three Float64Counters with the
//     canonical names http_freshness_probe_{raw,warm,archive}.
//   - Each probe ticks at FRESHNESS_PROBE_HZ. After N ticks the
//     cumulative counter value equals the wall-clock UnixMilli of
//     the most recent tick (within a few ms of test wall-clock).
//   - EXPORTER_FRESHNESS_PROBES=off short-circuits the wiring so the
//     ManualReader sees no probe metrics at all.
//
// We use a ManualReader so we can deterministically Collect() the
// SDK output without any OTLP round-trip.

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// newProbeTestProvider returns a MeterProvider wired with a
// ManualReader so the test can Collect() on demand.
func newProbeTestProvider(t *testing.T) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource.Default()),
	)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider, reader
}

// collectCumulativeByName returns probe-name → cumulative counter
// value across all DataPoints (probes carry no labels, so each name
// has exactly one DP). Names not present return 0 / false.
func collectCumulativeByName(
	t *testing.T,
	reader *sdkmetric.ManualReader,
) map[string]float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if !strings.HasPrefix(m.Name, "http_freshness_probe_") {
				continue
			}
			switch v := m.Data.(type) {
			case metricdata.Sum[float64]:
				for _, dp := range v.DataPoints {
					out[m.Name] = dp.Value
				}
			case metricdata.Sum[int64]:
				for _, dp := range v.DataPoints {
					out[m.Name] = float64(dp.Value)
				}
			default:
				t.Logf("metric=%s unexpected data type %T", m.Name, m.Data)
			}
		}
	}
	return out
}

// TestFreshnessProbesEmitTimestamp asserts that after a few ticks the
// cumulative counter value for each of the three probe metrics is
// within a small slack of the current UnixMilli. This is the core
// freshness invariant the replay client relies on.
func TestFreshnessProbesEmitTimestamp(t *testing.T) {
	t.Setenv("EXPORTER_FRESHNESS_PROBES", "on")
	t.Setenv("EXPORTER_FRESHNESS_PROBE_HZ", "20") // 50ms period — fast test

	provider, reader := newProbeTestProvider(t)
	meter := provider.Meter("probe-test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startFreshnessProbes(ctx, meter)
	defer stop()

	// Allow several ticks at 50ms each.
	time.Sleep(250 * time.Millisecond)

	now := time.Now().UnixMilli()
	got := collectCumulativeByName(t, reader)

	for _, name := range freshnessProbeNames {
		v, ok := got[name]
		if !ok {
			t.Errorf("probe %s missing from collected metrics; got=%v", name, got)
			continue
		}
		// Cumulative value should be the unix_ms at the most recent
		// emission — close to now, never in the future. Allow a
		// generous slack for scheduler jitter on busy CI hosts.
		const slackMs = 500
		if v <= 0 {
			t.Errorf("probe %s cumulative=%.0f, want >0 (timestamp)", name, v)
			continue
		}
		if delta := now - int64(v); delta < -10 || delta > slackMs {
			t.Errorf(
				"probe %s cumulative=%.0f, now=%d, delta=%d ms; "+
					"want within [-10, +%d]",
				name, v, now, delta, slackMs,
			)
		}
	}
}

// TestFreshnessProbesDisabled verifies the EXPORTER_FRESHNESS_PROBES=off
// kill switch — when disabled, no probe metrics make it to the
// reader.
func TestFreshnessProbesDisabled(t *testing.T) {
	t.Setenv("EXPORTER_FRESHNESS_PROBES", "off")

	provider, reader := newProbeTestProvider(t)
	meter := provider.Meter("probe-test-off")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startFreshnessProbes(ctx, meter)
	defer stop()

	time.Sleep(150 * time.Millisecond)

	got := collectCumulativeByName(t, reader)
	if len(got) != 0 {
		t.Errorf("EXPORTER_FRESHNESS_PROBES=off but reader saw probes: %v", got)
	}
}

// TestFreshnessProbeNamesMatchSpec is a guardrail against accidental
// renames: the three probe names are the contract that
// deploy/configs/mvp-v6-freshness-probes.yaml + Phase E config
// emitters route on. If this list changes the YAML routing breaks
// silently.
func TestFreshnessProbeNamesMatchSpec(t *testing.T) {
	want := []string{
		"http_freshness_probe_raw",
		"http_freshness_probe_warm",
		"http_freshness_probe_archive",
	}
	if len(freshnessProbeNames) != len(want) {
		t.Fatalf("freshnessProbeNames=%v, want %v", freshnessProbeNames, want)
	}
	for i, n := range want {
		if freshnessProbeNames[i] != n {
			t.Errorf("freshnessProbeNames[%d]=%q, want %q", i, freshnessProbeNames[i], n)
		}
	}
}

// TestFreshnessProbeMonotonic verifies the cumulative value is
// non-decreasing across two Collect() calls — the SDK rejects
// non-monotonic Adds and we want to fail fast if a future refactor
// breaks that.
func TestFreshnessProbeMonotonic(t *testing.T) {
	t.Setenv("EXPORTER_FRESHNESS_PROBES", "on")
	t.Setenv("EXPORTER_FRESHNESS_PROBE_HZ", "20")

	provider, reader := newProbeTestProvider(t)
	meter := provider.Meter("probe-test-mono")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startFreshnessProbes(ctx, meter)
	defer stop()

	time.Sleep(120 * time.Millisecond)
	first := collectCumulativeByName(t, reader)
	time.Sleep(120 * time.Millisecond)
	second := collectCumulativeByName(t, reader)

	for _, name := range freshnessProbeNames {
		a, ok1 := first[name]
		b, ok2 := second[name]
		if !ok1 || !ok2 {
			t.Errorf("probe %s missing in collect (first=%v second=%v)", name, ok1, ok2)
			continue
		}
		if b < a {
			t.Errorf("probe %s cumulative went backwards: %.0f → %.0f", name, a, b)
		}
	}
}
