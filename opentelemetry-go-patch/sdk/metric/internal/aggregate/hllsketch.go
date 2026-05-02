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

// maxIdleCycles is the number of consecutive cumulative export cycles during
// which a series receives no new measurements before it is evicted from the
// aggregator. Delta series are always evicted after every export.
const maxIdleCycles = 3

type hllSketchSeries struct {
	attrs    attribute.Set
	seriesID uint64

	sketch *hll.HyperLogLog
	count  uint64

	// measuredSince is set to true in measure() and cleared after each
	// cumulative export. It drives idle detection.
	measuredSince bool
	// idleCycles counts consecutive cumulative exports with no new measurements.
	idleCycles uint8
}

type hllSketchValues[N int64 | float64] struct {
	limit    limiter[hllSketchSeries]
	values   map[attribute.Distinct]*hllSketchSeries
	valuesMu sync.Mutex

	// seriesPool recycles hllSketchSeries structs to reduce GC pressure in
	// high-churn (delta) and idle-eviction (cumulative) scenarios.
	seriesPool sync.Pool

	// deltaTransmission enables sparse delta encoding for cumulative exports.
	deltaTransmission bool
	// snapshots holds a clone of the last-exported HLL per series, keyed by
	// attribute.Distinct. Used to compute sparse register deltas.
	snapshots   map[attribute.Distinct]*hll.HyperLogLog
	snapshotsMu sync.Mutex
}

func newHLLSketchValues[N int64 | float64](limit int, deltaTransmission bool) *hllSketchValues[N] {
	v := &hllSketchValues[N]{
		limit:             newLimiter[hllSketchSeries](limit),
		values:            make(map[attribute.Distinct]*hllSketchSeries),
		deltaTransmission: deltaTransmission,
		snapshots:         make(map[attribute.Distinct]*hll.HyperLogLog),
	}
	v.seriesPool.New = func() any { return new(hllSketchSeries) }
	return v
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
			series = d.seriesPool.Get().(*hllSketchSeries)
			if series.sketch != nil {
				series.sketch.Reset() // reuse register array
			} else {
				series.sketch = hll.NewHyperLogLog()
			}
			series.attrs = fltrAttr
			series.seriesID = 0
			series.count = 0
			series.measuredSince = true
			series.idleCycles = 0
			d.values[fltrAttr.Equivalent()] = series
		}
	}

	series.sketch.UpdateValue(float64(value))
	series.count++
	series.measuredSince = true
}

type hllSketch[N int64 | float64] struct {
	*hllSketchValues[N]
	start time.Time
}

func newHLLSketch[N int64 | float64](limit int, deltaTransmission bool) *hllSketch[N] {
	return &hllSketch[N]{
		hllSketchValues: newHLLSketchValues[N](limit, deltaTransmission),
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
		payload, enc, err := d.fullPayload(series.sketch)
		if err != nil {
			otel.Handle(err)
			continue
		}
		if d.exportDataPoint(series, t, payload, enc, &dPts[i]) {
			i++
		}
	}

	dPts = dPts[:i]

	// Return series structs to the pool before clearing the map.
	// The sketch is kept alive inside the struct so Reset() can reuse its
	// register array on the next measure() call.
	for _, series := range d.values {
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.count = 0
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

		payload, enc, err := d.payloadFor(key, series.sketch)
		if err != nil {
			otel.Handle(err)
			continue
		}
		if d.exportDataPoint(series, t, payload, enc, &dPts[i]) {
			i++
		}
	}
	dPts = dPts[:i]

	// Evict idle series and return their structs (with sketches) to the pool.
	for _, key := range toEvict {
		series := d.values[key]
		delete(d.values, key)
		// Also drop the snapshot for evicted series.
		d.snapshotsMu.Lock()
		delete(d.snapshots, key)
		d.snapshotsMu.Unlock()
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.count = 0
		series.measuredSince = false
		series.idleCycles = 0
		d.seriesPool.Put(series)
	}

	data.DataPoints = dPts
	*dest = data
	return len(dPts)
}

// payloadFor returns the sketch payload bytes and encoding for one cumulative
// export cycle. If deltaTransmission is enabled and a prior snapshot exists,
// it computes a sparse register delta; otherwise it returns the full binary
// payload and saves a new snapshot.
func (d *hllSketchValues[N]) payloadFor(key attribute.Distinct, sketch *hll.HyperLogLog) ([]byte, metricdata.HLLSketchEncoding, error) {
	if !d.deltaTransmission {
		return d.fullPayload(sketch)
	}

	d.snapshotsMu.Lock()
	snap, hasSnap := d.snapshots[key]
	d.snapshotsMu.Unlock()

	var payload []byte
	var enc metricdata.HLLSketchEncoding
	var err error

	if hasSnap && snap != nil {
		deltaMsg := hll.ComputeRegisterDelta(snap, sketch)
		payload, err = hll.SerializeRegisterDelta(deltaMsg)
		enc = metricdata.HLLSketchEncodingDelta
	} else {
		payload, err = sketch.SerializeProtoBytes()
		enc = metricdata.HLLSketchEncodingProto
	}
	if err != nil {
		return nil, "", err
	}

	// Save updated snapshot (clone current state).
	newSnap := cloneHLL(sketch)
	d.snapshotsMu.Lock()
	d.snapshots[key] = newSnap
	d.snapshotsMu.Unlock()

	return payload, enc, nil
}

// fullPayload returns a full proto serialization of sketch.
func (d *hllSketchValues[N]) fullPayload(sketch *hll.HyperLogLog) ([]byte, metricdata.HLLSketchEncoding, error) {
	b, err := sketch.SerializeProtoBytes()
	return b, metricdata.HLLSketchEncodingProto, err
}

func (d *hllSketch[N]) exportDataPoint(
	series *hllSketchSeries,
	t time.Time,
	payload []byte,
	encoding metricdata.HLLSketchEncoding,
	dest *metricdata.HLLSketchDataPoint,
) bool {
	cardinality := uint64(series.sketch.Estimate())

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
	dp.Cardinality = cardinality
	dp.Precision = hll.HLLPrecision
	dp.Encoding = encoding
	dp.Sketch = payload
	return true
}

// cloneHLL returns a deep copy of src suitable for use as a delta snapshot.
func cloneHLL(src *hll.HyperLogLog) *hll.HyperLogLog {
	data, err := src.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	clone, err := hll.DeserializeHyperLogLogFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return clone
}
