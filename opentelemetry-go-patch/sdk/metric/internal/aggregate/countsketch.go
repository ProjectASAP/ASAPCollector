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
	seriesID  uint64
	sketch    *countsketch.CountSketch
	dimension string
	epsilon   float64
	delta     float64

	measuredSince bool
	idleCycles    uint8
}

type countSketchValues[N int64 | float64] struct {
	rows      int
	cols      int
	epsilon   float64
	delta     float64
	dimension string

	limit      limiter[countSketchSeries[N]]
	values     map[attribute.Distinct]*countSketchSeries[N]
	valuesMu   sync.Mutex
	seriesPool sync.Pool
}

func newCountSketchValues[N int64 | float64](rows, cols int, epsilon, delta float64, dimension string, limit int) *countSketchValues[N] {
	if rows <= 0 {
		rows = defaultCountSketchRows
	}
	if cols <= 0 {
		cols = defaultCountSketchCols
	}
	v := &countSketchValues[N]{
		rows:      rows,
		cols:      cols,
		epsilon:   epsilon,
		delta:     delta,
		dimension: dimension,
		limit:     newLimiter[countSketchSeries[N]](limit),
		values:    make(map[attribute.Distinct]*countSketchSeries[N]),
	}
	v.seriesPool.New = func() any { return new(countSketchSeries[N]) }
	return v
}

func (d *countSketchValues[N]) newSeries(attr attribute.Set) *countSketchSeries[N] {
	series := d.seriesPool.Get().(*countSketchSeries[N])
	if series.sketch != nil {
		series.sketch.Reset() // reuse Count rows and L2 backing arrays
	} else {
		sk, err := countsketch.NewCountSketch(d.rows, d.cols)
		if err != nil {
			otel.Handle(err)
			return nil
		}
		series.sketch = sk
	}
	series.attrs = attr
	series.seriesID = 0
	series.dimension = d.dimension
	series.epsilon = d.epsilon
	series.delta = d.delta
	series.measuredSince = true
	series.idleCycles = 0
	return series
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

	series.measuredSince = true
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

	for _, series := range d.values {
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.dimension = ""
		series.epsilon = 0
		series.delta = 0
		series.measuredSince = false
		series.idleCycles = 0
		d.seriesPool.Put(series)
	}
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
	var toEvict []attribute.Distinct
	for key, series := range d.values {
		if !series.measuredSince {
			series.idleCycles++
			if series.idleCycles >= maxIdleCycles {
				toEvict = append(toEvict, key)
			}
			continue
		}
		series.measuredSince = false
		series.idleCycles = 0

		if d.exportDataPoint(series, t, &dPts[i]) {
			i++
		}
	}
	dPts = dPts[:i]

	for _, key := range toEvict {
		series := d.values[key]
		delete(d.values, key)
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.dimension = ""
		series.epsilon = 0
		series.delta = 0
		series.measuredSince = false
		series.idleCycles = 0
		d.seriesPool.Put(series)
	}

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
	if series.seriesID != 0 {
		dp.SeriesID = series.seriesID
	} else {
		dp.Attributes = series.attrs
		dp.SeriesIDSink = &series.seriesID
		dp.AttrsClearer = &series.attrs
	}
	dp.StartTime = d.start
	dp.Time = t
	dp.Dimension = series.dimension
	dp.Epsilon = series.epsilon
	dp.Delta = series.delta
	dp.Encoding = metricdata.CountSketchEncodingGob
	dp.Sketch = bytes
	return true
}
