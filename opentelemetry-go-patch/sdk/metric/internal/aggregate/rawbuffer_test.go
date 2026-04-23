// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestRawBufferEmitsEverySample verifies the core contract: every
// Add/Record call within a collection interval becomes its own
// DataPoint on the emitted Gauge.
func TestRawBufferEmitsEverySample(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	ctx := t.Context()
	meas, comp := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 10,
	}.RawBuffer(0) // default max

	alice := attribute.NewSet(userAlice, adminTrue)
	bob := attribute.NewSet(userBob, adminFalse)

	// Emit several distinct observations on two attribute sets,
	// advancing the clock between each so timestamps differ.
	for _, v := range []float64{1, 2, 3} {
		meas(ctx, v, alice)
		c.Now() // advance
	}
	for _, v := range []float64{10, 20} {
		meas(ctx, v, bob)
		c.Now()
	}

	var got metricdata.Aggregation
	n := comp(&got)

	// 3 (alice) + 2 (bob) = 5 raw data points.
	require.Equal(t, 5, n, "expected one DataPoint per raw sample")

	gauge, ok := got.(metricdata.Gauge[float64])
	require.True(t, ok, "expected Gauge[float64]")
	require.Len(t, gauge.DataPoints, 5)

	// Sum of values should equal 1+2+3+10+20 = 36 — every sample
	// survives, none pre-aggregated by the SDK.
	var sum float64
	for _, dp := range gauge.DataPoints {
		sum += dp.Value
	}
	assert.InDelta(t, 36.0, sum, 1e-9)
}

// TestRawBufferSecondCollectIsEmpty verifies that collect clears
// internal buffers even when called with cumulative temporality —
// raw-buffer has no meaningful cumulative semantics, so the second
// collect after no new observations should emit zero DataPoints.
func TestRawBufferSecondCollectIsEmpty(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	ctx := t.Context()
	meas, comp := Builder[float64]{
		Temporality:      metricdata.CumulativeTemporality,
		Filter:           attrFltr,
		AggregationLimit: 10,
	}.RawBuffer(0)

	alice := attribute.NewSet(userAlice, adminTrue)
	meas(ctx, 1.0, alice)
	meas(ctx, 2.0, alice)

	var first metricdata.Aggregation
	require.Equal(t, 2, comp(&first))

	var second metricdata.Aggregation
	require.Equal(t, 0, comp(&second), "cumulative raw-buffer resets after each collect")
}

// TestRawBufferRespectsMaxEventsPerSeries verifies overflow behaviour:
// when a single attribute set exceeds the configured cap, further
// measurements on that set are silently dropped (and counted in
// series.drops, though we don't yet expose that externally).
func TestRawBufferRespectsMaxEventsPerSeries(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	ctx := t.Context()
	const cap = 4
	meas, comp := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 10,
	}.RawBuffer(cap)

	alice := attribute.NewSet(userAlice, adminTrue)
	for i := 0; i < 10; i++ {
		meas(ctx, float64(i), alice)
	}

	var got metricdata.Aggregation
	n := comp(&got)
	assert.Equal(t, cap, n,
		"buffer capped at MaxEventsPerSeries; extra %d measurements should be dropped", 10-cap)
}

// TestRawBufferHandlesManyAttributeSets verifies the aggregator
// doesn't lose per-attribute isolation when many series coexist.
func TestRawBufferHandlesManyAttributeSets(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	ctx := t.Context()
	// No Filter — test series isolation across arbitrary attribute
	// keys (attrFltr only keeps "user", which would collapse all
	// series_id-differentiated sets into one).
	meas, comp := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		AggregationLimit: 0, // unlimited
	}.RawBuffer(100)

	const cardinality = 50
	const samplesPerSeries = 3
	for i := 0; i < cardinality; i++ {
		attrs := attribute.NewSet(
			attribute.Int("series_id", i),
			adminTrue,
		)
		for s := 0; s < samplesPerSeries; s++ {
			meas(ctx, float64(i*10+s), attrs)
		}
	}

	var got metricdata.Aggregation
	n := comp(&got)
	assert.Equal(t, cardinality*samplesPerSeries, n)
}
