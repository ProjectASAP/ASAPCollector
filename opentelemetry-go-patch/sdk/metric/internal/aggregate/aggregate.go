// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// now is used to return the current local time while allowing tests to
// override the default time.Now function.
var now = time.Now

// Measure receives measurements to be aggregated.
type Measure[N int64 | float64] func(context.Context, N, attribute.Set)

// ComputeAggregation stores the aggregate of measurements into dest and
// returns the number of aggregate data-points output.
type ComputeAggregation func(dest *metricdata.Aggregation) int

// Builder builds an aggregate function.
type Builder[N int64 | float64] struct {
	// Temporality is the temporality used for the returned aggregate function.
	//
	// If this is not provided a default of cumulative will be used (except for
	// the last-value aggregate function where delta is the only appropriate
	// temporality).
	Temporality metricdata.Temporality
	// Filter is the attribute filter the aggregate function will use on the
	// input of measurements.
	Filter attribute.Filter
	// ReservoirFunc is the factory function used by aggregate functions to
	// create new exemplar reservoirs for a new seen attribute set.
	//
	// If this is not provided a default factory function that returns an
	// dropReservoir reservoir will be used.
	ReservoirFunc func(attribute.Set) FilteredExemplarReservoir[N]
	// AggregationLimit is the cardinality limit of measurement attributes. Any
	// measurement for new attributes once the limit has been reached will be
	// aggregated into a single aggregate for the "otel.metric.overflow"
	// attribute.
	//
	// If AggregationLimit is less than or equal to zero there will not be an
	// aggregation limit imposed (i.e. unlimited attribute sets).
	AggregationLimit int
}

func (b Builder[N]) resFunc() func(attribute.Set) FilteredExemplarReservoir[N] {
	if b.ReservoirFunc != nil {
		return b.ReservoirFunc
	}

	return dropReservoir
}

type fltrMeasure[N int64 | float64] func(ctx context.Context, value N, fltrAttr attribute.Set, droppedAttr []attribute.KeyValue)

func (b Builder[N]) filter(f fltrMeasure[N]) Measure[N] {
	if b.Filter != nil {
		fltr := b.Filter // Copy to make it immutable after assignment.
		return func(ctx context.Context, n N, a attribute.Set) {
			fAttr, dropped := a.Filter(fltr)
			f(ctx, n, fAttr, dropped)
		}
	}
	return func(ctx context.Context, n N, a attribute.Set) {
		f(ctx, n, a, nil)
	}
}

// LastValue returns a last-value aggregate function input and output.
func (b Builder[N]) LastValue() (Measure[N], ComputeAggregation) {
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		lv := newDeltaLastValue[N](b.AggregationLimit, b.resFunc())
		return b.filter(lv.measure), lv.collect
	default:
		lv := newCumulativeLastValue[N](b.AggregationLimit, b.resFunc())
		return b.filter(lv.measure), lv.collect
	}
}

// PrecomputedLastValue returns a last-value aggregate function input and
// output. The aggregation returned from the returned ComputeAggregation
// function will always only return values from the previous collection cycle.
func (b Builder[N]) PrecomputedLastValue() (Measure[N], ComputeAggregation) {
	lv := newPrecomputedLastValue[N](b.AggregationLimit, b.resFunc())
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(lv.measure), lv.delta
	default:
		return b.filter(lv.measure), lv.cumulative
	}
}

// PrecomputedSum returns a sum aggregate function input and output. The
// arguments passed to the input are expected to be the precomputed sum values.
func (b Builder[N]) PrecomputedSum(monotonic bool) (Measure[N], ComputeAggregation) {
	s := newPrecomputedSum[N](monotonic, b.AggregationLimit, b.resFunc())
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(s.measure), s.delta
	default:
		return b.filter(s.measure), s.cumulative
	}
}

// Sum returns a sum aggregate function input and output.
func (b Builder[N]) Sum(monotonic bool) (Measure[N], ComputeAggregation) {
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		s := newDeltaSum[N](monotonic, b.AggregationLimit, b.resFunc())
		return b.filter(s.measure), s.collect
	default:
		s := newCumulativeSum[N](monotonic, b.AggregationLimit, b.resFunc())
		return b.filter(s.measure), s.collect
	}
}

// ExplicitBucketHistogram returns a histogram aggregate function input and
// output.
func (b Builder[N]) ExplicitBucketHistogram(
	boundaries []float64,
	noMinMax, noSum bool,
) (Measure[N], ComputeAggregation) {
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		h := newDeltaHistogram[N](boundaries, noMinMax, noSum, b.AggregationLimit, b.resFunc())
		return b.filter(h.measure), h.collect
	default:
		h := newCumulativeHistogram[N](boundaries, noMinMax, noSum, b.AggregationLimit, b.resFunc())
		return b.filter(h.measure), h.collect
	}
}

// ExponentialBucketHistogram returns a histogram aggregate function input and
// output.
func (b Builder[N]) ExponentialBucketHistogram(
	maxSize, maxScale int32,
	noMinMax, noSum bool,
) (Measure[N], ComputeAggregation) {
	h := newExponentialHistogram[N](maxSize, maxScale, noMinMax, noSum, b.AggregationLimit, b.resFunc())
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(h.measure), h.delta
	default:
		return b.filter(h.measure), h.cumulative
	}
}

// DDSketch returns a DDSketch aggregate function input and output.
// deltaTransmission enables sparse delta encoding for cumulative exports;
// deltaThreshold is the minimum absolute bucket count change to include in a delta.
func (b Builder[N]) DDSketch(relativeAccuracy float64, noMinMax, noSum bool, deltaTransmission bool, deltaThreshold uint64) (Measure[N], ComputeAggregation) {
	agg := newDDSketch[N](relativeAccuracy, noMinMax, noSum, b.AggregationLimit, b.resFunc(), deltaTransmission, deltaThreshold)
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(agg.measure), agg.delta
	default:
		return b.filter(agg.measure), agg.cumulative
	}
}

// Noop returns a no-op aggregate function input and output that tracks
// attribute sets but always emits zero-valued data points. Used when
// transmit_sketch is false or no aggregation is needed.
func (b Builder[N]) Noop() (Measure[N], ComputeAggregation) {
	agg := newNoopAggregate[N](b.AggregationLimit)
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(agg.measure), agg.delta
	default:
		return b.filter(agg.measure), agg.cumulative
	}
}

// KLLSketch returns a KLL sketch aggregate function input and output.
func (b Builder[N]) KLLSketch(k int) (Measure[N], ComputeAggregation) {
	agg := newKLLSketch[N](k, false, b.AggregationLimit)
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(agg.measure), agg.delta
	default:
		return b.filter(agg.measure), agg.cumulative
	}
}

// CountSketch returns a CountSketch aggregate function input and output.
// deltaTransmission enables sparse delta encoding for cumulative exports;
// deltaThreshold is the minimum absolute cell change to include in a delta.
func (b Builder[N]) CountSketch(rows, cols int, epsilon, delta float64, dimension string, deltaTransmission bool, deltaThreshold float64) (Measure[N], ComputeAggregation) {
	agg := newCountSketchAgg[N](rows, cols, epsilon, delta, dimension, b.AggregationLimit, deltaTransmission, deltaThreshold)
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(agg.measure), agg.delta
	default:
		return b.filter(agg.measure), agg.cumulative
	}
}

// CountMinSketch returns a Count-Min Sketch aggregate function input and output.
// deltaTransmission enables sparse delta encoding for cumulative exports;
// deltaThreshold is the minimum absolute cell change to include in a delta.
func (b Builder[N]) CountMinSketch(rows, cols int, deltaTransmission bool, deltaThreshold float64) (Measure[N], ComputeAggregation) {
	agg := newCountMinSketchAgg[N](rows, cols, b.AggregationLimit, deltaTransmission, deltaThreshold)
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(agg.measure), agg.delta
	default:
		return b.filter(agg.measure), agg.cumulative
	}
}

// HLLSketch returns a HyperLogLog sketch aggregate function input and output.
// deltaTransmission enables sparse delta encoding for cumulative exports.
func (b Builder[N]) HLLSketch(deltaTransmission bool) (Measure[N], ComputeAggregation) {
	agg := newHLLSketch[N](b.AggregationLimit, deltaTransmission)
	switch b.Temporality {
	case metricdata.DeltaTemporality:
		return b.filter(agg.measure), agg.delta
	default:
		return b.filter(agg.measure), agg.cumulative
	}
}

// reset ensures s has capacity and sets it length. If the capacity of s too
// small, a new slice is returned with the specified capacity and length.
func reset[T any](s []T, length, capacity int) []T {
	if cap(s) < capacity {
		return make([]T, length, capacity)
	}
	return s[:length]
}
