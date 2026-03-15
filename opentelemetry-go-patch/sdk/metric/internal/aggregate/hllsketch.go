// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"sync"
	"time"

	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type hllSketchSeries struct {
	attrs  attribute.Set
	sketch *hll.HyperLogLog
	count  uint64
}

type hllSketchValues[N int64 | float64] struct {
	limit    limiter[hllSketchSeries]
	values   map[attribute.Distinct]*hllSketchSeries
	valuesMu sync.Mutex
}

func newHLLSketchValues[N int64 | float64](limit int) *hllSketchValues[N] {
	return &hllSketchValues[N]{
		limit:  newLimiter[hllSketchSeries](limit),
		values: make(map[attribute.Distinct]*hllSketchSeries),
	}
}

func (d *hllSketchValues[N]) measure(
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
			series = &hllSketchSeries{
				attrs:  fltrAttr,
				sketch: hll.NewHyperLogLog(),
			}
			d.values[fltrAttr.Equivalent()] = series
		}
	}

	series.sketch.Insert(float64(value))
	series.count++
}

type hllSketch[N int64 | float64] struct {
	*hllSketchValues[N]
	start time.Time
}

func newHLLSketch[N int64 | float64](limit int) *hllSketch[N] {
	return &hllSketch[N]{
		hllSketchValues: newHLLSketchValues[N](limit),
		start:           now(),
	}
}

func (d *hllSketch[N]) measure(
	ctx context.Context,
	value N,
	fltrAttr attribute.Set,
	droppedAttr []attribute.KeyValue,
) {
	d.hllSketchValues.measure(ctx, value, fltrAttr, droppedAttr)
}

func (d *hllSketch[N]) delta(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.HLLSketch)
	data.Temporality = metricdata.DeltaTemporality

	d.valuesMu.Lock()
	defer d.valuesMu.Unlock()

	n := len(d.values)
	dPts := reset(data.DataPoints, n, n)

	var i int
	for _, series := range d.values {
		if series.count == 0 {
			continue
		}
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

func (d *hllSketch[N]) cumulative(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.HLLSketch)
	data.Temporality = metricdata.CumulativeTemporality

	d.valuesMu.Lock()
	defer d.valuesMu.Unlock()

	n := len(d.values)
	dPts := reset(data.DataPoints, n, n)

	var i int
	for _, series := range d.values {
		if series.count == 0 {
			continue
		}
		if d.exportDataPoint(series, t, &dPts[i]) {
			i++
		}
	}
	dPts = dPts[:i]

	data.DataPoints = dPts
	*dest = data
	return len(dPts)
}

func (d *hllSketch[N]) exportDataPoint(
	series *hllSketchSeries,
	t time.Time,
	dest *metricdata.HLLSketchDataPoint,
) bool {
	bytes, err := series.sketch.SerializeToBytes()
	if err != nil {
		otel.Handle(err)
		return false
	}

	cardinality := uint64(series.sketch.Estimate())

	dp := dest
	dp.Attributes = series.attrs
	dp.StartTime = d.start
	dp.Time = t
	dp.Count = series.count
	dp.Cardinality = cardinality
	dp.Precision = hll.HLLPrecision
	dp.Encoding = metricdata.HLLSketchEncodingBinary
	dp.Sketch = bytes
	return true
}
