// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// countMinSketchSnapshot is the serialization DTO.
type countMinSketchSnapshot struct {
	Rows  int
	Cols  int
	Count [][]float64
	Sum   [][]float64
	Sum2  [][]float64
	L1    []float64
	L2    []float64
}

type countMinSketchSeries[N int64 | float64] struct {
	attrs       attribute.Set
	seriesID    uint64
	sketch      *cms.CountMinSketch
	sampleCount uint64

	measuredSince bool
	idleCycles    uint8
}

type countMinSketchValues[N int64 | float64] struct {
	rows int
	cols int

	limit      limiter[countMinSketchSeries[N]]
	values     map[attribute.Distinct]*countMinSketchSeries[N]
	valuesMu   sync.Mutex
	seriesPool sync.Pool
}

func newCountMinSketchValues[N int64 | float64](rows, cols, limit int) *countMinSketchValues[N] {
	if rows <= 0 {
		rows = 4
	}
	if cols <= 0 {
		cols = 2048
	}
	v := &countMinSketchValues[N]{
		rows:   rows,
		cols:   cols,
		limit:  newLimiter[countMinSketchSeries[N]](limit),
		values: make(map[attribute.Distinct]*countMinSketchSeries[N]),
	}
	v.seriesPool.New = func() any { return new(countMinSketchSeries[N]) }
	return v
}

func (d *countMinSketchValues[N]) newSeries(attr attribute.Set) *countMinSketchSeries[N] {
	series := d.seriesPool.Get().(*countMinSketchSeries[N])
	if series.sketch != nil {
		series.sketch.Reset() // reuse Count/Sum/Sum2 rows and L1/L2 arrays
	} else {
		sk, err := cms.NewCountMinSketch(d.rows, d.cols)
		if err != nil {
			otel.Handle(err)
			return nil
		}
		series.sketch = sk
	}
	series.attrs = attr
	series.seriesID = 0
	series.sampleCount = 0
	series.measuredSince = true
	series.idleCycles = 0
	return series
}

func (d *countMinSketchValues[N]) measure(
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
	key := fltrAttr.Encoded(attribute.DefaultEncoder())
	input := common.FromString(key)
	series.sketch.InsertWithHash(input.Hash)
	series.sampleCount++
}

type countMinSketchAgg[N int64 | float64] struct {
	*countMinSketchValues[N]
	start time.Time
}

func newCountMinSketchAgg[N int64 | float64](rows, cols, limit int) *countMinSketchAgg[N] {
	return &countMinSketchAgg[N]{
		countMinSketchValues: newCountMinSketchValues[N](rows, cols, limit),
		start:                now(),
	}
}

func (d *countMinSketchAgg[N]) measure(
	ctx context.Context,
	value N,
	fltrAttr attribute.Set,
	droppedAttr []attribute.KeyValue,
) {
	d.countMinSketchValues.measure(ctx, value, fltrAttr, droppedAttr)
}

func (d *countMinSketchAgg[N]) delta(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.CountMinSketch[N])
	data.Temporality = metricdata.DeltaTemporality

	d.valuesMu.Lock()
	defer d.valuesMu.Unlock()

	n := len(d.values)
	dPts := reset(data.DataPoints, n, n)

	var i int
	for _, series := range d.values {
		if series.sampleCount == 0 {
			continue
		}
		if d.exportDataPoint(series, t, &dPts[i]) {
			i++
		}
	}

	dPts = dPts[:i]

	for _, series := range d.values {
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.sampleCount = 0
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

func (d *countMinSketchAgg[N]) cumulative(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.CountMinSketch[N])
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

		if series.sampleCount == 0 {
			continue
		}
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
		series.sampleCount = 0
		series.measuredSince = false
		series.idleCycles = 0
		d.seriesPool.Put(series)
	}

	data.DataPoints = dPts
	*dest = data
	return len(dPts)
}

func (d *countMinSketchAgg[N]) exportDataPoint(
	series *countMinSketchSeries[N],
	t time.Time,
	dest *metricdata.CountMinSketchDataPoint[N],
) bool {
	sketchBytes, err := serializeCMSketch(series.sketch)
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
	dp.SampleCount = series.sampleCount
	dp.Rows = int32(series.sketch.Rows)
	dp.Cols = int32(series.sketch.Cols)
	dp.Encoding = metricdata.CountMinSketchEncodingGob
	dp.Sketch = sketchBytes
	return true
}

func serializeCMSketch(s *cms.CountMinSketch) ([]byte, error) {
	snap := countMinSketchSnapshot{
		Rows:  s.Rows,
		Cols:  s.Cols,
		Count: s.Count,
		Sum:   s.Sum,
		Sum2:  s.Sum2,
		L1:    s.L1,
		L2:    s.L2,
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(snap); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
