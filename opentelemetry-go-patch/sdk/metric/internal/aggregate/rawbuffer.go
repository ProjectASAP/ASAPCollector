// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// defaultRawBufferMaxEventsPerSeries caps per-attribute buffers when the
// caller doesn't specify a limit. Chosen as a soft cap that holds ~15 s
// of 1 kHz-per-series traffic before dropping; production deployments
// should tune this explicitly.
const defaultRawBufferMaxEventsPerSeries = 10000

type rawBufferSample[N int64 | float64] struct {
	ts    time.Time
	value N
}

type rawBufferSeries[N int64 | float64] struct {
	attrs attribute.Set
	// seriesID caches the collector-assigned series ID (see
	// otlpmetricgrpc/internal/series.Dictionary) across collect cycles so
	// repeat exports of an already-registered series can skip attrs and the
	// exporter's descriptorKey/attributesFingerprint work entirely. Persists
	// across collect() the same way ddSketchSeries.seriesID persists across
	// ddSketch.cumulative() — see exportDataPoint below.
	seriesID uint64
	buf      []rawBufferSample[N]
	// drops counts measurements rejected because buf reached the
	// per-series cap. Reset on collect alongside buf.
	drops uint64

	// measuredSince/idleCycles mirror the sketch aggregators' idle-eviction
	// pattern (see ddSketch.cumulative): a series must stay resident in the
	// map (not returned to the pool) across collect cycles for seriesID to
	// amortize, so untouched series are evicted after maxIdleCycles instead
	// of every cycle.
	measuredSince bool
	idleCycles    uint8
}

type rawBufferValues[N int64 | float64] struct {
	maxPerSeries int

	limit      limiter[rawBufferSeries[N]]
	values     map[attribute.Distinct]*rawBufferSeries[N]
	valuesMu   sync.Mutex
	seriesPool sync.Pool
}

func newRawBufferValues[N int64 | float64](maxPerSeries, limit int) *rawBufferValues[N] {
	if maxPerSeries <= 0 {
		maxPerSeries = defaultRawBufferMaxEventsPerSeries
	}
	v := &rawBufferValues[N]{
		maxPerSeries: maxPerSeries,
		limit:        newLimiter[rawBufferSeries[N]](limit),
		values:       make(map[attribute.Distinct]*rawBufferSeries[N]),
	}
	v.seriesPool.New = func() any { return new(rawBufferSeries[N]) }
	return v
}

func (d *rawBufferValues[N]) newSeries(attr attribute.Set) *rawBufferSeries[N] {
	series := d.seriesPool.Get().(*rawBufferSeries[N])
	series.attrs = attr
	series.seriesID = 0
	// Start with a modest capacity; grows up to maxPerSeries.
	if cap(series.buf) == 0 {
		series.buf = make([]rawBufferSample[N], 0, 64)
	} else {
		series.buf = series.buf[:0]
	}
	series.drops = 0
	series.measuredSince = true
	series.idleCycles = 0
	return series
}

// measure appends the observation to the per-attribute buffer. When
// the buffer has already reached maxPerSeries, the measurement is
// dropped and the per-series drop count is incremented.
func (d *rawBufferValues[N]) measure(
	_ context.Context,
	value N,
	fltrAttr attribute.Set,
	_ []attribute.KeyValue,
) {
	d.valuesMu.Lock()
	defer d.valuesMu.Unlock()

	series, ok := d.values[fltrAttr.Equivalent()]
	if !ok {
		fltrAttr = d.limit.Attributes(fltrAttr, d.values)
		series, ok = d.values[fltrAttr.Equivalent()]
		if !ok {
			series = d.newSeries(fltrAttr)
			d.values[fltrAttr.Equivalent()] = series
		}
	}
	series.measuredSince = true

	if len(series.buf) >= d.maxPerSeries {
		series.drops++
		return
	}
	series.buf = append(series.buf, rawBufferSample[N]{ts: now(), value: value})
}

type rawBuffer[N int64 | float64] struct {
	*rawBufferValues[N]
	start time.Time
}

func newRawBuffer[N int64 | float64](maxPerSeries, limit int) *rawBuffer[N] {
	return &rawBuffer[N]{
		rawBufferValues: newRawBufferValues[N](maxPerSeries, limit),
		start:           now(),
	}
}

// collect emits every buffered sample across every attribute set as
// its own Gauge DataPoint, then clears the buffers. Both delta and
// cumulative paths call this: raw-buffer has no cumulative meaning
// (re-emitting all history every tick is useless), so the buffered
// samples are always cleared after collect regardless of requested
// temporality. The returned Aggregation is typed as Gauge[N] because a raw
// observation has no aggregation semantics attached.
//
// Series identity (attrs/seriesID), unlike the sample buffer, DOES persist
// across collect cycles — mirroring ddSketch.cumulative's idle-eviction
// pattern — so that once the collector confirms a series' ID (via
// SeriesIDSink, populated by otlpmetricgrpc/internal/series's
// annotateNumberDataPoints), every later sample for that series can skip
// attrs and the exporter's descriptorKey/attributesFingerprint work: see
// exportDataPoint below. A series with no samples across maxIdleCycles
// consecutive collects is evicted, matching the sketch aggregators.
func (d *rawBuffer[N]) collect(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	d.valuesMu.Lock()
	defer d.valuesMu.Unlock()

	// Count total points across all series.
	total := 0
	for _, s := range d.values {
		total += len(s.buf)
	}

	data, _ := (*dest).(metricdata.Gauge[N])
	data.DataPoints = reset(data.DataPoints, 0, total)

	var toEvict []attribute.Distinct
	for key, series := range d.values {
		for _, sample := range series.buf {
			data.DataPoints = append(data.DataPoints, d.exportDataPoint(series, sample))
		}

		if series.measuredSince {
			series.measuredSince = false
			series.idleCycles = 0
		} else {
			series.idleCycles++
			if series.idleCycles >= maxIdleCycles {
				toEvict = append(toEvict, key)
			}
		}
		series.buf = series.buf[:0]
		series.drops = 0
	}

	for _, key := range toEvict {
		series := d.values[key]
		delete(d.values, key)
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.buf = series.buf[:0]
		series.drops = 0
		series.measuredSince = false
		series.idleCycles = 0
		d.seriesPool.Put(series)
	}

	d.start = t
	*dest = data
	return len(data.DataPoints)
}

// exportDataPoint mirrors ddSketch.exportDataPoint's cached-seriesID fast
// path: once the collector has confirmed this series (series.seriesID != 0,
// set via SeriesIDSink on a prior export), every DataPoint for it carries
// only the ID and never touches attrs again. Until then every DataPoint
// carries attrs and offers SeriesIDSink/AttrsClearer so the FIRST
// confirmation — whichever DataPoint the exporter processes first — writes
// the ID back into this series for every subsequent window.
func (d *rawBuffer[N]) exportDataPoint(series *rawBufferSeries[N], sample rawBufferSample[N]) metricdata.DataPoint[N] {
	dp := metricdata.DataPoint[N]{
		StartTime: d.start,
		Time:      sample.ts,
		Value:     sample.value,
	}
	if series.seriesID != 0 {
		dp.SeriesID = series.seriesID
	} else {
		dp.Attributes = series.attrs
		dp.SeriesIDSink = &series.seriesID
		dp.AttrsClearer = &series.attrs
	}
	return dp
}

// delta and cumulative both call collect — see the comment on collect
// for why cumulative semantics don't make sense for raw-buffer.
func (d *rawBuffer[N]) delta(dest *metricdata.Aggregation) int      { return d.collect(dest) }
func (d *rawBuffer[N]) cumulative(dest *metricdata.Aggregation) int { return d.collect(dest) }
