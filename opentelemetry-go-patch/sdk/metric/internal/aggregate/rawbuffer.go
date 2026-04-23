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
	buf   []rawBufferSample[N]
	// drops counts measurements rejected because buf reached the
	// per-series cap. Reset on collect alongside buf.
	drops uint64
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
	// Start with a modest capacity; grows up to maxPerSeries.
	if cap(series.buf) == 0 {
		series.buf = make([]rawBufferSample[N], 0, 64)
	} else {
		series.buf = series.buf[:0]
	}
	series.drops = 0
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
// (re-emitting all history every tick is useless), so we always
// reset after collect regardless of requested temporality. The
// returned Aggregation is typed as Gauge[N] because a raw
// observation has no aggregation semantics attached.
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

	for key, series := range d.values {
		for _, sample := range series.buf {
			data.DataPoints = append(data.DataPoints, metricdata.DataPoint[N]{
				Attributes: series.attrs,
				StartTime:  d.start,
				Time:       sample.ts,
				Value:      sample.value,
			})
		}
		// Return series to pool, clear map.
		series.attrs = attribute.Set{}
		series.buf = series.buf[:0]
		series.drops = 0
		d.seriesPool.Put(series)
		delete(d.values, key)
	}

	d.start = t
	*dest = data
	return len(data.DataPoints)
}

// delta and cumulative both call collect — see the comment on collect
// for why cumulative semantics don't make sense for raw-buffer.
func (d *rawBuffer[N]) delta(dest *metricdata.Aggregation) int      { return d.collect(dest) }
func (d *rawBuffer[N]) cumulative(dest *metricdata.Aggregation) int { return d.collect(dest) }
