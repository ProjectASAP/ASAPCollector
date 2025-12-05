// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
)

const testDDSketchAccuracy = 0.01

func TestDDSketchDelta(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	ctx := context.Background()
	meas, comp := Builder[float64]{
		Temporality:      metricdata.DeltaTemporality,
		Filter:           attrFltr,
		AggregationLimit: 3,
	}.DDSketch(testDDSketchAccuracy, false, false)

	got := new(metricdata.Aggregation)

	require.Equal(t, 0, comp(got))

	record(meas, []arg[float64]{
		{ctx, 2, alice},
		{ctx, 10, bob},
		{ctx, 2, alice},
		{ctx, 2, alice},
		{ctx, 10, bob},
	})
	require.Equal(t, 2, comp(got))
	agg := (*got).(metricdata.DDSketch[float64])
	require.Equal(t, metricdata.DeltaTemporality, agg.Temporality)
	dpAlice := findDDSketchDP(t, agg.DataPoints, fltrAlice)
	require.Equal(t, uint64(3), dpAlice.Count)
	require.InDelta(t, 6, dpAlice.Sum, 1e-9)
	assertExtremaEqual(t, dpAlice.Min, 2)
	assertExtremaEqual(t, dpAlice.Max, 2)
	require.NotEmpty(t, dpAlice.Sketch)

	dpBob := findDDSketchDP(t, agg.DataPoints, fltrBob)
	require.Equal(t, uint64(2), dpBob.Count)
	require.InDelta(t, 20, dpBob.Sum, 1e-9)
	assertExtremaEqual(t, dpBob.Min, 10)
	assertExtremaEqual(t, dpBob.Max, 10)

	record(meas, []arg[float64]{
		{ctx, 10, alice},
		{ctx, 3, bob},
	})
	require.Equal(t, 2, comp(got))
	agg = (*got).(metricdata.DDSketch[float64])
	dpAlice = findDDSketchDP(t, agg.DataPoints, fltrAlice)
	require.Equal(t, uint64(1), dpAlice.Count)
	require.InDelta(t, 10, dpAlice.Sum, 1e-9)
	assertExtremaEqual(t, dpAlice.Min, 10)
	assertExtremaEqual(t, dpAlice.Max, 10)

	dpBob = findDDSketchDP(t, agg.DataPoints, fltrBob)
	require.Equal(t, uint64(1), dpBob.Count)
	require.InDelta(t, 3, dpBob.Sum, 1e-9)
	assertExtremaEqual(t, dpBob.Min, 3)
	assertExtremaEqual(t, dpBob.Max, 3)

	require.Equal(t, 0, comp(got))

	record(meas, []arg[float64]{
		{ctx, 1, alice},
		{ctx, 1, bob},
		{ctx, 1, carol},
		{ctx, 1, dave},
	})
	require.Equal(t, 3, comp(got))
	agg = (*got).(metricdata.DDSketch[float64])

	findDDSketchDP(t, agg.DataPoints, fltrAlice)
	findDDSketchDP(t, agg.DataPoints, fltrBob)
	dpOverflow := findDDSketchDP(t, agg.DataPoints, overflowSet)
	require.Equal(t, uint64(2), dpOverflow.Count)
	require.InDelta(t, 2, dpOverflow.Sum, 1e-9)
	assertExtremaEqual(t, dpOverflow.Min, 1)
	assertExtremaEqual(t, dpOverflow.Max, 1)
}

func TestDDSketchCumulativeNoMinMax(t *testing.T) {
	c := new(clock)
	t.Cleanup(c.Register())

	ctx := context.Background()
	meas, comp := Builder[int64]{
		Temporality:      metricdata.CumulativeTemporality,
		Filter:           attrFltr,
		AggregationLimit: 3,
	}.DDSketch(testDDSketchAccuracy, true, true)

	got := new(metricdata.Aggregation)

	record(meas, []arg[int64]{
		{ctx, 2, alice},
		{ctx, 10, bob},
	})
	require.Equal(t, 2, comp(got))
	agg := (*got).(metricdata.DDSketch[int64])
	require.Equal(t, metricdata.CumulativeTemporality, agg.Temporality)
	dpAlice := findDDSketchDP(t, agg.DataPoints, fltrAlice)
	require.Equal(t, uint64(1), dpAlice.Count)
	require.Zero(t, dpAlice.Sum)
	assertExtremaMissing(t, dpAlice.Min)
	assertExtremaMissing(t, dpAlice.Max)

	dpBob := findDDSketchDP(t, agg.DataPoints, fltrBob)
	require.Equal(t, uint64(1), dpBob.Count)
	require.Zero(t, dpBob.Sum)

	record(meas, []arg[int64]{
		{ctx, 4, alice},
		{ctx, 1, bob},
	})
	require.Equal(t, 2, comp(got))
	agg = (*got).(metricdata.DDSketch[int64])
	dpAlice = findDDSketchDP(t, agg.DataPoints, fltrAlice)
	require.Equal(t, uint64(2), dpAlice.Count)
	require.Zero(t, dpAlice.Sum)

	dpBob = findDDSketchDP(t, agg.DataPoints, fltrBob)
	require.Equal(t, uint64(2), dpBob.Count)
	require.Zero(t, dpBob.Sum)
}

func TestDDSketchAggregationEquality(t *testing.T) {
	dp := metricdata.DDSketchDataPoint[float64]{
		Attributes: attribute.NewSet(attribute.String("key", "value")),
		Count:      1,
		Sum:        1,
		Encoding:   metricdata.DDSketchEncodingProto,
		Sketch:     []byte{0x1, 0x2, 0x3},
	}
	agg := metricdata.DDSketch[float64]{
		Temporality: metricdata.DeltaTemporality,
		DataPoints:  []metricdata.DDSketchDataPoint[float64]{dp},
	}
	metricdatatest.AssertAggregationsEqual(t, agg, agg)
}

func record[N int64 | float64](meas Measure[N], inputs []arg[N]) {
	for _, in := range inputs {
		meas(in.ctx, in.value, in.attr)
	}
}

func findDDSketchDP[N int64 | float64](t *testing.T, dps []metricdata.DDSketchDataPoint[N], attrs attribute.Set) metricdata.DDSketchDataPoint[N] {
	t.Helper()
	for _, dp := range dps {
		if dp.Attributes.Equals(&attrs) {
			return dp
		}
	}
	t.Fatalf("did not find datapoint for %v", attrs)
	return metricdata.DDSketchDataPoint[N]{}
}

func assertExtremaEqual[N int64 | float64](t *testing.T, extrema metricdata.Extrema[N], expected N) {
	t.Helper()
	value, ok := extrema.Value()
	require.True(t, ok, "expected extrema value")
	require.Equal(t, expected, value)
}

func assertExtremaMissing[N int64 | float64](t *testing.T, extrema metricdata.Extrema[N]) {
	t.Helper()
	_, ok := extrema.Value()
	require.False(t, ok, "expected extrema to be unset")
}
