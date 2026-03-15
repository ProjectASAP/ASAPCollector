// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"sync"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const defaultDDSketchRelativeAccuracy = 0.01

type ddSketchSeries[N int64 | float64] struct {
	attrs    attribute.Set
	seriesID uint64
	res      FilteredExemplarReservoir[N]
	sketch   *ddsketch.DDSketch

	count uint64
	sum   N
	min   N
	max   N

	measuredSince bool
	idleCycles    uint8
}

// revive:disable-next-line:flag-parameter
func (s *ddSketchSeries[N]) updateStats(value N, trackMinMax bool) {
	s.count++
	if !trackMinMax {
		return
	}
	if s.count == 1 {
		s.min = value
		s.max = value
		return
	}
	if value < s.min {
		s.min = value
	}
	if value > s.max {
		s.max = value
	}
}

type ddSketchValues[N int64 | float64] struct {
	accuracy float64
	noMinMax bool
	noSum    bool

	newRes     func(attribute.Set) FilteredExemplarReservoir[N]
	limit      limiter[ddSketchSeries[N]]
	values     map[attribute.Distinct]*ddSketchSeries[N]
	valuesMu   sync.Mutex
	seriesPool sync.Pool
}

func newDDSketchValues[N int64 | float64](
	accuracy float64,
	noMinMax bool,
	noSum bool,
	limit int,
	r func(attribute.Set) FilteredExemplarReservoir[N],
) *ddSketchValues[N] {
	if accuracy <= 0 || accuracy >= 1 {
		accuracy = defaultDDSketchRelativeAccuracy
	}
	v := &ddSketchValues[N]{
		accuracy: accuracy,
		noMinMax: noMinMax,
		noSum:    noSum,
		newRes:   r,
		limit:    newLimiter[ddSketchSeries[N]](limit),
		values:   make(map[attribute.Distinct]*ddSketchSeries[N]),
	}
	v.seriesPool.New = func() any { return new(ddSketchSeries[N]) }
	return v
}

func (d *ddSketchValues[N]) newSeries(attr attribute.Set, value N) *ddSketchSeries[N] {
	series := d.seriesPool.Get().(*ddSketchSeries[N])
	if series.sketch != nil {
		series.sketch.Clear() // reuse internal bucket storage
	} else {
		sk, err := ddsketch.NewDefaultDDSketch(d.accuracy)
		if err != nil {
			otel.Handle(err)
			return nil
		}
		series.sketch = sk
	}
	series.attrs = attr
	series.seriesID = 0
	series.res = d.newRes(attr) // attr-dependent, always recreate
	series.count = 0
	series.sum = 0
	series.measuredSince = true
	series.idleCycles = 0
	if !d.noMinMax {
		series.min = value
		series.max = value
	} else {
		series.min = 0
		series.max = 0
	}
	return series
}

// Per time series, allocate a DDSketch and add the value to it.
func (d *ddSketchValues[N]) measure(
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

	if err := series.sketch.Add(float64(value)); err != nil {
		otel.Handle(err)
		return
	}
	series.updateStats(value, !d.noMinMax)
	if !d.noSum {
		series.sum += value
	}
	series.measuredSince = true
	series.res.Offer(ctx, value, droppedAttr)
}

type ddSketch[N int64 | float64] struct {
	*ddSketchValues[N]
	start time.Time
}

func newDDSketch[N int64 | float64](
	accuracy float64,
	noMinMax bool,
	noSum bool,
	limit int,
	r func(attribute.Set) FilteredExemplarReservoir[N],
) *ddSketch[N] {
	return &ddSketch[N]{
		ddSketchValues: newDDSketchValues[N](accuracy, noMinMax, noSum, limit, r),
		start:          now(),
	}
}

func (d *ddSketch[N]) measure(
	ctx context.Context,
	value N,
	fltrAttr attribute.Set,
	droppedAttr []attribute.KeyValue,
) {
	d.ddSketchValues.measure(ctx, value, fltrAttr, droppedAttr)
}

func (d *ddSketch[N]) delta(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.DDSketch[N])
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

	// Trim to the number of exported points (in case any were skipped).
	dPts = dPts[:i]

	for _, series := range d.values {
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.res = nil // release exemplar reservoir (attr-dependent)
		series.count = 0
		series.sum = 0
		series.min = 0
		series.max = 0
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

func (d *ddSketch[N]) cumulative(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.DDSketch[N])
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

		if series.count == 0 {
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
		series.res = nil
		series.count = 0
		series.sum = 0
		series.min = 0
		series.max = 0
		series.measuredSince = false
		series.idleCycles = 0
		d.seriesPool.Put(series)
	}

	data.DataPoints = dPts
	*dest = data
	return len(dPts)
}

func (d *ddSketch[N]) exportDataPoint(
	series *ddSketchSeries[N],
	t time.Time,
	dest *metricdata.DDSketchDataPoint[N],
) bool {
	bytes, err := serializeDDSketch(series.sketch)
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
	dp.Count = series.count
	if !d.noSum {
		dp.Sum = series.sum
	}
	if !d.noMinMax {
		dp.Min = metricdata.NewExtrema(series.min)
		dp.Max = metricdata.NewExtrema(series.max)
	}
	dp.Encoding = metricdata.DDSketchEncodingProto
	dp.Sketch = bytes
	collectExemplars(&dp.Exemplars, series.res.Collect)
	return true
}

func serializeDDSketch(sk *ddsketch.DDSketch) ([]byte, error) {
	if sk == nil {
		return nil, nil
	}
	return proto.Marshal(sk.ToProto())
}
