// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metric

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAggregationErr(t *testing.T) {
	t.Run("DropOperation", func(t *testing.T) {
		assert.NoError(t, AggregationDrop{}.err())
	})

	t.Run("SumOperation", func(t *testing.T) {
		assert.NoError(t, AggregationSum{}.err())
	})

	t.Run("LastValueOperation", func(t *testing.T) {
		assert.NoError(t, AggregationLastValue{}.err())
	})

	t.Run("ExplicitBucketHistogramOperation", func(t *testing.T) {
		assert.NoError(t, AggregationExplicitBucketHistogram{}.err())

		assert.NoError(t, AggregationExplicitBucketHistogram{
			Boundaries: []float64{0},
			NoMinMax:   true,
		}.err())

		assert.NoError(t, AggregationExplicitBucketHistogram{
			Boundaries: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 1000},
		}.err())
	})

	t.Run("NonmonotonicHistogramBoundaries", func(t *testing.T) {
		assert.ErrorIs(t, AggregationExplicitBucketHistogram{
			Boundaries: []float64{2, 1},
		}.err(), errAgg)

		assert.ErrorIs(t, AggregationExplicitBucketHistogram{
			Boundaries: []float64{0, 1, 2, 1, 3, 4},
		}.err(), errAgg)
	})

	t.Run("ExponentialHistogramOperation", func(t *testing.T) {
		assert.NoError(t, AggregationBase2ExponentialHistogram{
			MaxSize:  160,
			MaxScale: 20,
		}.err())

		assert.NoError(t, AggregationBase2ExponentialHistogram{
			MaxSize:  1,
			NoMinMax: true,
		}.err())

		assert.NoError(t, AggregationBase2ExponentialHistogram{
			MaxSize:  1024,
			MaxScale: -3,
		}.err())
	})

	t.Run("InvalidExponentialHistogramOperation", func(t *testing.T) {
		// MazSize must be greater than 0
		assert.ErrorIs(t, AggregationBase2ExponentialHistogram{}.err(), errAgg)

		// MaxScale Must be <=20
		assert.ErrorIs(t, AggregationBase2ExponentialHistogram{
			MaxSize:  1,
			MaxScale: 30,
		}.err(), errAgg)
	})

	t.Run("DDSketchOperation", func(t *testing.T) {
		assert.NoError(t, AggregationDDSketch{}.err())
		assert.NoError(t, AggregationDDSketch{RelativeAccuracy: 0.02}.err())
	})

	t.Run("InvalidDDSketchOperation", func(t *testing.T) {
		assert.ErrorIs(t, AggregationDDSketch{RelativeAccuracy: -0.1}.err(), errAgg)
		assert.ErrorIs(t, AggregationDDSketch{RelativeAccuracy: 1}.err(), errAgg)
	})

	t.Run("KLLSketchOperation", func(t *testing.T) {
		assert.NoError(t, AggregationKLLSketch{}.err())
		assert.NoError(t, AggregationKLLSketch{K: 200}.err())
		assert.ErrorIs(t, AggregationKLLSketch{K: -1}.err(), errAgg)
	})

	t.Run("CountSketchOperation", func(t *testing.T) {
		assert.NoError(t, AggregationCountSketch{}.err())
		assert.NoError(t, AggregationCountSketch{
			Rows:      4,
			Cols:      2048,
			Epsilon:   0.01,
			Delta:     0.99,
			Dimension: "value",
		}.err())
		assert.ErrorIs(t, AggregationCountSketch{Rows: -1}.err(), errAgg)
		assert.ErrorIs(t, AggregationCountSketch{Cols: -1}.err(), errAgg)
		assert.ErrorIs(t, AggregationCountSketch{Epsilon: 1}.err(), errAgg)
		assert.ErrorIs(t, AggregationCountSketch{Delta: -0.1}.err(), errAgg)
	})

	t.Run("CountMinSketchOperation", func(t *testing.T) {
		assert.NoError(t, AggregationCountMinSketch{}.err())
		assert.NoError(t, AggregationCountMinSketch{Rows: 4, Cols: 2048}.err())
		assert.ErrorIs(t, AggregationCountMinSketch{Rows: -1}.err(), errAgg)
		assert.ErrorIs(t, AggregationCountMinSketch{Cols: -1}.err(), errAgg)
	})

	t.Run("HLLSketchOperation", func(t *testing.T) {
		assert.NoError(t, AggregationHLLSketch{}.err())
	})
}

func TestExplicitBucketHistogramDeepCopy(t *testing.T) {
	const orig = 0.0
	b := []float64{orig}
	h := AggregationExplicitBucketHistogram{Boundaries: b}
	cpH := h.copy().(AggregationExplicitBucketHistogram)
	b[0] = orig + 1
	assert.Equal(t, orig, cpH.Boundaries[0], "changing the underlying slice data should not affect the copy")
}

func TestDDSketchAggCopy(t *testing.T) {
	a := AggregationDDSketch{RelativeAccuracy: 0.02, NoMinMax: true}
	assert.Equal(t, a, a.copy())
}

func TestSketchAggCopy(t *testing.T) {
	assert.Equal(t, AggregationKLLSketch{K: 200}, AggregationKLLSketch{K: 200}.copy())
	assert.Equal(t, AggregationCountSketch{
		Rows:      4,
		Cols:      2048,
		Epsilon:   0.01,
		Delta:     0.99,
		Dimension: "value",
	}, AggregationCountSketch{
		Rows:      4,
		Cols:      2048,
		Epsilon:   0.01,
		Delta:     0.99,
		Dimension: "value",
	}.copy())
	assert.Equal(t, AggregationCountMinSketch{Rows: 4, Cols: 2048}, AggregationCountMinSketch{Rows: 4, Cols: 2048}.copy())
	assert.Equal(t, AggregationHLLSketch{}, AggregationHLLSketch{}.copy())
}
