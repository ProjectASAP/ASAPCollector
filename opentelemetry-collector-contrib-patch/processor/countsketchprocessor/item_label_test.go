// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countsketchprocessor

import (
	"context"
	"testing"

	countsketch "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// makeEndpointQPS builds a Gauge whose data points each carry an
// `endpoint` label; counts is a map endpoint -> number of observations
// (each observation increments by 1, like top_endpoint_qps).
func makeEndpointQPS(metricName string, counts map[string]int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	m.SetEmptyGauge()
	for ep, n := range counts {
		for i := 0; i < n; i++ {
			dp := m.Gauge().DataPoints().AppendEmpty()
			dp.SetDoubleValue(1.0)
			dp.Attributes().PutStr("endpoint", ep)
		}
	}
	return md
}

// firstCSPayload deserializes the single emitted CountSketch payload.
func firstCSPayload(t *testing.T, md pmetric.Metrics) *countsketch.CountSketch {
	t.Helper()
	dps := getCSOutputDPs(md)
	require.Len(t, dps, 1, "expected exactly one output data point")
	raw := dps[0].Attributes().AsRaw()["sketch_payload"].([]byte)
	cs, err := countsketch.DeserializeCountSketchFromProtoBytes(raw)
	require.NoError(t, err)
	return cs
}

// TestItemLabel_KeysByLabelValue verifies that with item_label="endpoint"
// distinct endpoints are counted as distinct items (estimates track each
// endpoint's own count) and TopK ranks them — instead of collapsing every
// observation under the metric name.
func TestItemLabel_KeysByLabelValue(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		Epsilon:        0.001,
		Delta:          0.99,
		TransmitSketch: true,
		DropOriginal:   true,
		ItemLabel:      "endpoint",
	}
	require.NoError(t, cfg.Validate())
	proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))

	md := makeEndpointQPS("top_endpoint_qps", map[string]int{
		"/checkout": 100,
		"/cart":     40,
		"/home":     10,
	})
	out, err := proc.ProcessMetrics(context.Background(), md)
	require.NoError(t, err)
	cs := firstCSPayload(t, out)

	// Each endpoint's estimate should track its own count (within
	// CountSketch error), NOT the total (150) and NOT zero.
	assert.InDelta(t, 100, cs.EstimateStringCount("/checkout"), 15)
	assert.InDelta(t, 40, cs.EstimateStringCount("/cart"), 15)
	assert.InDelta(t, 10, cs.EstimateStringCount("/home"), 15)

	// The metric name must NOT be a key anymore (the C2 bug counted
	// everything under "top_endpoint_qps").
	assert.InDelta(t, 0, cs.EstimateStringCount("top_endpoint_qps"), 15)

	// TopK must rank the heavy hitter first.
	require.NotNil(t, cs.TopK)
	require.NotEmpty(t, cs.TopK.Heap, "TopK heap must track distinct endpoints")
	var top string
	var topCount int64
	for _, e := range cs.TopK.Heap {
		if e.Count > topCount {
			topCount = e.Count
			top = e.Key
		}
	}
	assert.Equal(t, "/checkout", top, "heaviest endpoint must rank first")
}

// TestItemLabel_FallbackToMetricNameByteParity verifies that leaving
// item_label empty preserves the legacy metric-name keying byte-for-byte:
// every observation is counted under the metric name, so the emitted
// sketch is identical to one whose only key is the metric name.
func TestItemLabel_FallbackToMetricNameByteParity(t *testing.T) {
	mk := func(itemLabel string) []byte {
		cfg := &Config{
			Mode:           ModeBatch,
			Epsilon:        0.001,
			Delta:          0.99,
			TransmitSketch: true,
			DropOriginal:   true,
			ItemLabel:      itemLabel,
		}
		require.NoError(t, cfg.Validate())
		proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))
		md := makeEndpointQPS("top_endpoint_qps", map[string]int{"/checkout": 100})
		out, err := proc.ProcessMetrics(context.Background(), md)
		require.NoError(t, err)
		dps := getCSOutputDPs(out)
		require.Len(t, dps, 1)
		return dps[0].Attributes().AsRaw()["sketch_payload"].([]byte)
	}

	// With item_label unset, everything is keyed by the metric name.
	legacy := mk("")
	cs, err := countsketch.DeserializeCountSketchFromProtoBytes(legacy)
	require.NoError(t, err)
	// The metric name carries all 100 observations; the endpoint value does not.
	assert.InDelta(t, 100, cs.EstimateStringCount("top_endpoint_qps"), 15)
	assert.InDelta(t, 0, cs.EstimateStringCount("/checkout"), 15)
}

// TestItemLabel_MissingLabelFallsBackToMetricName verifies that when
// item_label is configured but the named label is absent on a data point,
// the key falls back to the metric name (no panic, no empty key).
func TestItemLabel_MissingLabelFallsBackToMetricName(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		Epsilon:        0.001,
		Delta:          0.99,
		TransmitSketch: true,
		DropOriginal:   true,
		ItemLabel:      "endpoint",
	}
	require.NoError(t, cfg.Validate())
	proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))

	// makeCSGaugeMetrics emits an http_requests Gauge with a service.name
	// label but NO endpoint label.
	out, err := proc.ProcessMetrics(context.Background(), makeCSGaugeMetrics("svc", 30))
	require.NoError(t, err)
	cs := firstCSPayload(t, out)
	// Falls back to keying by the metric name.
	assert.InDelta(t, 30, cs.EstimateStringCount("http_requests"), 10)
}
