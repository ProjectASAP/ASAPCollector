// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"sync"
	"time"

	countsketch "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	defaultCountSketchRows = countsketch.DefaultRows
	defaultCountSketchCols = countsketch.DefaultCols
)

type countSketchSeries[N int64 | float64] struct {
	attrs     attribute.Set
	sketch    *countsketch.CountSketch
	dimension string
	epsilon   float64
	delta     float64
}

type countSketchValues[N int64 | float64] struct {
	rows      int
	cols      int
	epsilon   float64
	delta     float64
	dimension string

	limit    limiter[countSketchSeries[N]]
	values   map[attribute.Distinct]*countSketchSeries[N]
	valuesMu sync.Mutex
}

func newCountSketchValues[N int64 | float64](rows, cols int, epsilon, delta float64, dimension string, limit int) *countSketchValues[N] {
	if rows <= 0 {
		rows = defaultCountSketchRows
	}
	if cols <= 0 {
		cols = defaultCountSketchCols
	}
	return &countSketchValues[N]{
		rows:      rows,
		cols:      cols,
		epsilon:   epsilon,
		delta:     delta,
		dimension: dimension,
		limit:     newLimiter[countSketchSeries[N]](limit),
		values:    make(map[attribute.Distinct]*countSketchSeries[N]),
	}
}

func (d *countSketchValues[N]) newSeries(attr attribute.Set) *countSketchSeries[N] {
	sk, err := countsketch.NewCountSketch(d.rows, d.cols)
	if err != nil {
		otel.Handle(err)
		return nil
	}
	return &countSketchSeries[N]{
		attrs:     attr,
		sketch:    sk,
		dimension: d.dimension,
		epsilon:   d.epsilon,
		delta:     d.delta,
	}
}

func (d *countSketchValues[N]) measure(
	ctx context.Context,
	value N,
	fltrAttr attribute.Set,
	droppedAttr []attribute.KeyValue,
) {
	d.valuesMu.Lock()
	defer d.valuesMu.Unlock()

	series, ok := d.values[fltrAttr.Equivalent()]
	if !ok {
		fltrAttr = d.limit.Attributes(fltrAttr, d.values)
		series, ok = d.values[fltrAttr.Equivalent()]
		if !ok {
			series = d.newSeries(fltrAttr)
			if series == nil {
				return
			}
			d.values[fltrAttr.Equivalent()] = series
		}
	}

	// Use attribute set as the tracked key
	key := fltrAttr.Encoded(attribute.DefaultEncoder())
	series.sketch.UpdateString(key, float64(value))
}

type countSketchAgg[N int64 | float64] struct {
	*countSketchValues[N]
	start time.Time
}

func newCountSketchAgg[N int64 | float64](rows, cols int, epsilon, delta float64, dimension string, limit int) *countSketchAgg[N] {
	return &countSketchAgg[N]{
		countSketchValues: newCountSketchValues[N](rows, cols, epsilon, delta, dimension, limit),
		start:             now(),
	}
}

func (d *countSketchAgg[N]) measure(
	ctx context.Context,
	value N,
	fltrAttr attribute.Set,
	droppedAttr []attribute.KeyValue,
) {
	d.countSketchValues.measure(ctx, value, fltrAttr, droppedAttr)
}

func (d *countSketchAgg[N]) delta(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.CountSketch[N])
	data.Temporality = metricdata.DeltaTemporality

	d.valuesMu.Lock()
	defer d.valuesMu.Unlock()

	n := len(d.values)
	dPts := reset(data.DataPoints, n, n)

	var i int
	for _, series := range d.values {
		if d.exportDataPoint(series, t, &dPts[i]) {
			i++
		}
	}

	dPts = dPts[:i]
	clear(d.values)
	d.start = t

	data.DataPoints = dPts
	*dest = data
	return len(dPts)
}

func (d *countSketchAgg[N]) cumulative(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.CountSketch[N])
	data.Temporality = metricdata.CumulativeTemporality

	d.valuesMu.Lock()
	defer d.valuesMu.Unlock()

	n := len(d.values)
	dPts := reset(data.DataPoints, n, n)

	var i int
	for _, series := range d.values {
		if d.exportDataPoint(series, t, &dPts[i]) {
			i++
		}
	}
	dPts = dPts[:i]

	data.DataPoints = dPts
	*dest = data
	return len(dPts)
}

func (d *countSketchAgg[N]) exportDataPoint(
	series *countSketchSeries[N],
	t time.Time,
	dest *metricdata.CountSketchDataPoint[N],
) bool {
	bytes, err := series.sketch.SerializeToBytes()
	if err != nil {
		otel.Handle(err)
		return false
	}

	dp := dest
	dp.Attributes = series.attrs
	dp.StartTime = d.start
	dp.Time = t
	dp.Dimension = series.dimension
	dp.Epsilon = series.epsilon
	dp.Delta = series.delta
	dp.Encoding = metricdata.CountSketchEncodingGob
	dp.Sketch = bytes
	return true
}
