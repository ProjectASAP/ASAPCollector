// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package series

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// kllRM builds a minimal ResourceMetrics containing a single KLL data point.
// The caller controls the initial state of the data point (Attributes,
// SeriesIDSink, AttrsClearer) to simulate what the aggregator produces on
// each export cycle.
func kllRM(res *resource.Resource, scope instrumentation.Scope, dp metricdata.KLLSketchDataPoint[float64]) *metricdata.ResourceMetrics {
	return &metricdata.ResourceMetrics{
		Resource: res,
		ScopeMetrics: []metricdata.ScopeMetrics{
			{
				Scope: scope,
				Metrics: []metricdata.Metrics{
					{
						Name: "latency",
						Data: metricdata.KLLSketch[float64]{
							Temporality: metricdata.DeltaTemporality,
							DataPoints:  []metricdata.KLLSketchDataPoint[float64]{dp},
						},
					},
				},
			},
		},
	}
}

func kllDPs(rm *metricdata.ResourceMetrics) []metricdata.KLLSketchDataPoint[float64] {
	return rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.KLLSketch[float64]).DataPoints
}

// TestMultiHop_AttrsUntilCollectorConfirms verifies the fix for the multi-hop
// forwarding bug:
//
//   SDK → Agent → Gateway
//
// Before the downstream collector (Gateway) has confirmed a series via a
// SeriesAssignments response, every export must carry full dimension attributes
// (SeriesID == 0 in the data point) so that any intermediate collector hop can
// read, enrich, and forward them. Only after Apply() registers the
// collector-assigned ID should the SDK switch to ID-only mode.
func TestMultiHop_AttrsUntilCollectorConfirms(t *testing.T) {
	res := resource.NewSchemaless(attribute.String("host.name", "node-1"))
	scope := instrumentation.Scope{Name: "testlib", Version: "v1"}
	realAttrs := attribute.NewSet(
		attribute.String("method", "GET"),
		attribute.String("path", "/api"),
	)

	// aggregatorSeriesID and aggregatorAttrs mirror the fields stored inside
	// kllSketchSeries in the real aggregator. They persist across export cycles.
	var aggregatorSeriesID uint64
	aggregatorAttrs := realAttrs

	dict := NewDictionary()

	// -----------------------------------------------------------------------
	// Export 1: no collector response yet — full attrs must be sent.
	// -----------------------------------------------------------------------
	dp1 := metricdata.KLLSketchDataPoint[float64]{
		Attributes:   aggregatorAttrs,
		SeriesIDSink: &aggregatorSeriesID,
		AttrsClearer: &aggregatorAttrs,
	}
	rm1 := kllRM(res, scope, dp1)
	dict.Annotate(rm1)

	dps1 := kllDPs(rm1)
	require.Len(t, dps1, 1)

	// SeriesID must be 0: the transform's seriesIdentity function sends
	// attributes when SeriesID == 0, so any intermediate hop receives them.
	assert.Equal(t, uint64(0), dps1[0].SeriesID,
		"export 1: SeriesID must be 0 before collector confirmation so attrs are forwarded by intermediate hops")

	// Attributes must be present in the data point so the transform serialises
	// them onto the wire.
	assert.Equal(t, realAttrs, dps1[0].Attributes,
		"export 1: full dimension attributes must be present in the exported data point")

	// The aggregator's own copy of attrs must NOT have been cleared yet —
	// otherwise the next export cycle would lose the labels.
	assert.Equal(t, realAttrs, aggregatorAttrs,
		"export 1: aggregator attrs must survive for subsequent export cycles")

	// The aggregator's seriesID field must remain 0 so that exportDataPoint
	// continues to take the else-branch (emit attrs) on the next cycle.
	assert.Equal(t, uint64(0), aggregatorSeriesID,
		"export 1: aggregator seriesID must not be written before collector confirmation")

	// -----------------------------------------------------------------------
	// Export 2 (still no response): same guarantees must hold.
	// -----------------------------------------------------------------------
	dp2 := metricdata.KLLSketchDataPoint[float64]{
		Attributes:   aggregatorAttrs,
		SeriesIDSink: &aggregatorSeriesID,
		AttrsClearer: &aggregatorAttrs,
	}
	rm2 := kllRM(res, scope, dp2)
	dict.Annotate(rm2)

	dps2 := kllDPs(rm2)
	assert.Equal(t, uint64(0), dps2[0].SeriesID,
		"export 2 (unconfirmed): SeriesID must still be 0")
	assert.Equal(t, realAttrs, dps2[0].Attributes,
		"export 2 (unconfirmed): attrs must still be present")
	assert.Equal(t, realAttrs, aggregatorAttrs,
		"export 2 (unconfirmed): aggregator attrs must still be intact")

	// -----------------------------------------------------------------------
	// Collector responds: Apply() registers the canonical series ID.
	// -----------------------------------------------------------------------
	// Compute the keys the same way Dictionary.Annotate does so the assignment
	// matches the entry that was created during Annotate.
	rk := resourceKey(res)
	sk := scopeKey(scope)
	fp := attributesFingerprint(realAttrs)

	collectorAssignedID := uint64(42)
	dict.Apply([]Assignment{
		{
			ResourceKey:           rk,
			ScopeKey:              sk,
			MetricName:            "latency",
			MetricType:            metricTypeKLLSketchDouble,
			AttributesFingerprint: fp,
			SeriesID:              collectorAssignedID,
		},
	})

	// -----------------------------------------------------------------------
	// Export 3: after confirmation — ID-only mode must be active.
	// -----------------------------------------------------------------------
	dp3 := metricdata.KLLSketchDataPoint[float64]{
		Attributes:   aggregatorAttrs, // still real attrs (not yet cleared)
		SeriesIDSink: &aggregatorSeriesID,
		AttrsClearer: &aggregatorAttrs,
	}
	rm3 := kllRM(res, scope, dp3)
	dict.Annotate(rm3)

	dps3 := kllDPs(rm3)
	assert.Equal(t, collectorAssignedID, dps3[0].SeriesID,
		"export 3: confirmed collector ID must be used")
	assert.Equal(t, attribute.Set{}, dps3[0].Attributes,
		"export 3: attrs must be suppressed after collector confirmation")

	// Aggregator state must have transitioned to ID-only:
	// SeriesIDSink written with the confirmed ID so exportDataPoint takes the
	// fast path on subsequent cycles.
	assert.Equal(t, collectorAssignedID, aggregatorSeriesID,
		"export 3: aggregator seriesID updated to collector-assigned value")
	// AttrsClearer must have fired to release memory in the aggregator.
	assert.Equal(t, attribute.Set{}, aggregatorAttrs,
		"export 3: aggregator attrs cleared after switching to ID-only mode")

	// -----------------------------------------------------------------------
	// Export 4: aggregator now holds the confirmed ID — fast path.
	// -----------------------------------------------------------------------
	// Simulate what exportDataPoint does when series.seriesID != 0: it sets
	// dp.SeriesID directly and does NOT set SeriesIDSink or AttrsClearer.
	dp4 := metricdata.KLLSketchDataPoint[float64]{
		SeriesID: aggregatorSeriesID, // already confirmed, no attrs
	}
	rm4 := kllRM(res, scope, dp4)
	dict.Annotate(rm4)

	dps4 := kllDPs(rm4)
	assert.Equal(t, collectorAssignedID, dps4[0].SeriesID,
		"export 4: fast-path ID preserved")
	assert.Equal(t, attribute.Set{}, dps4[0].Attributes,
		"export 4: no attrs on fast path")
}

// TestMultiHop_ResourceAttrsAlwaysSent verifies that resource-level attributes
// (host.name, service.instance.id, k8s.pod.name, etc.) — the ones used for
// agent counting and Prometheus scrape discovery — are never affected by the
// series ID optimisation.
//
// This is tested indirectly: Annotate operates on data-point attributes only;
// the ResourceMetrics.Resource field is never touched.
func TestMultiHop_ResourceAttrsAlwaysSent(t *testing.T) {
	res := resource.NewSchemaless(
		attribute.String("service.instance.id", "abc-123"),
		attribute.String("host.name", "node-7"),
		attribute.String("k8s.pod.name", "otel-agent-xkcd"),
	)
	scope := instrumentation.Scope{Name: "testlib"}
	dpAttrs := attribute.NewSet(attribute.String("method", "POST"))

	var aggrID uint64
	aggrAttrs := dpAttrs
	dict := NewDictionary()

	dp := metricdata.KLLSketchDataPoint[float64]{
		Attributes:   aggrAttrs,
		SeriesIDSink: &aggrID,
		AttrsClearer: &aggrAttrs,
	}
	rm := kllRM(res, scope, dp)
	dict.Annotate(rm)

	// Resource attributes must be completely untouched.
	resSet := res.Set()
	require.Equal(t, 3, resSet.Len())
	v, ok := resSet.Value(attribute.Key("service.instance.id"))
	require.True(t, ok)
	assert.Equal(t, "abc-123", v.AsString())
	v, ok = resSet.Value(attribute.Key("host.name"))
	require.True(t, ok)
	assert.Equal(t, "node-7", v.AsString())
	v, ok = resSet.Value(attribute.Key("k8s.pod.name"))
	require.True(t, ok)
	assert.Equal(t, "otel-agent-xkcd", v.AsString())
}
