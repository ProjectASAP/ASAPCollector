// Tests for the five-sketch MVP workload (issue #46).
//
// Contract under test (five_sketch_workload.go):
//
//   - startFiveSketchWorkload wires the four new instruments under
//     the contract names from mvp-workload.yaml:
//
//       request_size_bytes      (Gauge,   KLL family)
//       unique_users_per_min    (Counter, HLL family)
//       top_endpoint_qps        (Counter, CountSketch family)
//       endpoint_request_freq   (Counter, CountMinSketch family)
//
//   - The HLL counter carries a `user_id` attribute drawn from a
//     bounded pool (so the planner's HLL sizing has an actual
//     cardinality to estimate).
//
//   - The CountSketch / CountMinSketch counters carry an `endpoint`
//     attribute drawn Zipfian (so top-K is actually meaningful).
//
//   - The five-sketch metrics always emit (the old five-sketch on/off
//     gate was removed; the controller now decides storage tier).
//
// We use a ManualReader so the test is OTLP-free and deterministic.
//
// IMPORTANT: this test runs the workload at high freq for a short
// time, so it's bounded under a second.

package main

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// fiveSketchTestProvider returns a ManualReader-backed provider so
// the test can synchronously Collect() the metrics.
func fiveSketchTestProvider(t *testing.T) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource.Default()),
	)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return provider, reader
}

// collectFiveSketchMetrics returns a map of metric name → DataPoint
// count across all DataPoints, for the four MVP metrics. It also
// records (per metric) whether the inner attribute (user_id /
// endpoint) was present on any DataPoint — that's how we verify the
// label schema without enumerating every series.
type fiveSketchObserved struct {
	dataPoints int
	hasInner   bool
}

func collectFiveSketch(t *testing.T, reader *sdkmetric.ManualReader) map[string]fiveSketchObserved {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	want := map[string]string{
		"request_size_bytes":    "", // gauge — no inner label required beyond outer
		"unique_users_per_min":  "user_id",
		"top_endpoint_qps":      "endpoint",
		"endpoint_request_freq": "endpoint",
	}
	out := map[string]fiveSketchObserved{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			innerKey, ok := want[m.Name]
			if !ok {
				continue
			}
			obs := out[m.Name]
			switch v := m.Data.(type) {
			case metricdata.Sum[int64]:
				obs.dataPoints += len(v.DataPoints)
				if innerKey != "" {
					for _, dp := range v.DataPoints {
						if hasAttr(dp.Attributes, innerKey) {
							obs.hasInner = true
							break
						}
					}
				}
			case metricdata.Sum[float64]:
				obs.dataPoints += len(v.DataPoints)
				if innerKey != "" {
					for _, dp := range v.DataPoints {
						if hasAttr(dp.Attributes, innerKey) {
							obs.hasInner = true
							break
						}
					}
				}
			case metricdata.Gauge[float64]:
				obs.dataPoints += len(v.DataPoints)
				if innerKey != "" {
					for _, dp := range v.DataPoints {
						if hasAttr(dp.Attributes, innerKey) {
							obs.hasInner = true
							break
						}
					}
				} else {
					// Gauge with no required inner label still counts as "ok".
					obs.hasInner = true
				}
			default:
				t.Logf("metric=%s unexpected data type %T", m.Name, m.Data)
			}
			out[m.Name] = obs
		}
	}
	return out
}

func hasAttr(set attribute.Set, key string) bool {
	for _, kv := range set.ToSlice() {
		if string(kv.Key) == key {
			return true
		}
	}
	return false
}

// TestFiveSketchWorkloadEmitsAllMetrics drives a small label set at
// high frequency for a short window and verifies all four metrics
// appear in the collected scope, with the documented inner labels.
func TestFiveSketchWorkloadEmitsAllMetrics(t *testing.T) {
	c := defaultConfig()
	c.FiveSketchUserPool = 500
	c.FiveSketchEndpoints = 20

	provider, reader := fiveSketchTestProvider(t)
	meter := provider.Meter("five-sketch-test")

	// Two outer label sets so each per-series goroutine kicks in.
	outer := [][]attribute.KeyValue{
		{attribute.String("zone", "z0"), attribute.String("pod", "pod-000")},
		{attribute.String("zone", "z1"), attribute.String("pod", "pod-001")},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startFiveSketchWorkload(ctx, meter, outer, 100.0, c) // 10ms period
	defer stop()

	// Allow several ticks per series — at 100Hz, 200ms = ~20 ticks.
	time.Sleep(200 * time.Millisecond)

	got := collectFiveSketch(t, reader)
	for _, name := range []string{
		"request_size_bytes",
		"unique_users_per_min",
		"top_endpoint_qps",
		"endpoint_request_freq",
	} {
		obs, ok := got[name]
		if !ok {
			t.Errorf("metric %s missing from collected scope; got=%v", name, got)
			continue
		}
		if obs.dataPoints == 0 {
			t.Errorf("metric %s has no data points after 200ms soak", name)
		}
		if !obs.hasInner {
			t.Errorf("metric %s missing expected inner-label datapoint", name)
		}
	}
}

