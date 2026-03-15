// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"sync"
	"time"

	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const defaultKLLK = 200

type kllSketchSeries[N int64 | float64] struct {
	attrs  attribute.Set
	sketch *kll.KLLSketch

	count uint64
	sum   float64
	min   float64
	max   float64
	first bool
}

type kllSketchValues[N int64 | float64] struct {
	k      int
	noStats bool

	limit    limiter[kllSketchSeries[N]]
	values   map[attribute.Distinct]*kllSketchSeries[N]
	valuesMu sync.Mutex
}

func newKLLSketchValues[N int64 | float64](k int, noStats bool, limit int) *kllSketchValues[N] {
	if k <= 0 {
		k = defaultKLLK
	}
	return &kllSketchValues[N]{
		k:       k,
		noStats: noStats,
		limit:   newLimiter[kllSketchSeries[N]](limit),
		values:  make(map[attribute.Distinct]*kllSketchSeries[N]),
	}
}

func (d *kllSketchValues[N]) newSeries(attr attribute.Set, value N) *kllSketchSeries[N] {
	sk, err := kll.NewKLLSketch(d.k)
	if err != nil {
		otel.Handle(err)
		return nil
	}
	series := &kllSketchSeries[N]{
		attrs:  attr,
		sketch: sk,
		first:  true,
	}
	return series
}

func (d *kllSketchValues[N]) measure(
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
			series = d.newSeries(fltrAttr, value)
			if series == nil {
				return
			}
			d.values[fltrAttr.Equivalent()] = series
		}
	}

	series.sketch.Insert(float64(value))
	series.count++
	fv := float64(value)
	series.sum += fv
	if series.first {
		series.min = fv
		series.max = fv
		series.first = false
	} else {
		if fv < series.min {
			series.min = fv
		}
		if fv > series.max {
			series.max = fv
		}
	}
}

type kllSketch[N int64 | float64] struct {
	*kllSketchValues[N]
	start time.Time
}

func newKLLSketch[N int64 | float64](k int, noStats bool, limit int) *kllSketch[N] {
	return &kllSketch[N]{
		kllSketchValues: newKLLSketchValues[N](k, noStats, limit),
		start:           now(),
	}
}

func (d *kllSketch[N]) measure(
	ctx context.Context,
	value N,
	fltrAttr attribute.Set,
	droppedAttr []attribute.KeyValue,
) {
	d.kllSketchValues.measure(ctx, value, fltrAttr, droppedAttr)
}

func (d *kllSketch[N]) delta(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.KLLSketch[N])
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

func (d *kllSketch[N]) cumulative(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.KLLSketch[N])
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

func (d *kllSketch[N]) exportDataPoint(
	series *kllSketchSeries[N],
	t time.Time,
	dest *metricdata.KLLSketchDataPoint[N],
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
	dp.Count = series.count
	dp.Sum = series.sum
	dp.Min = series.min
	dp.Max = series.max
	dp.Encoding = metricdata.KLLSketchEncodingGob
	dp.Sketch = bytes
	return true
}
