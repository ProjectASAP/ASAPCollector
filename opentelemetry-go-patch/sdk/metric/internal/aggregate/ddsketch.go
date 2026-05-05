// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"sync"
	"time"

	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
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

	// deltaTransmission enables sparse delta encoding for cumulative exports.
	deltaTransmission bool
	// deltaThreshold is the minimum absolute bucket count change to include.
	deltaThreshold uint64
	// snapshots holds a clone of the last-exported sketch per series, keyed
	// by attribute.Distinct. Used only when deltaTransmission=true so we can
	// invoke sketchlib-go's ComputeDelta(prev, curr, threshold) on the next
	// cumulative export.
	snapshots   map[attribute.Distinct]*ddsketch.DDSketch
	snapshotsMu sync.Mutex

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
	deltaTransmission bool,
	deltaThreshold uint64,
) *ddSketchValues[N] {
	if accuracy <= 0 || accuracy >= 1 {
		accuracy = defaultDDSketchRelativeAccuracy
	}
	if deltaTransmission && deltaThreshold == 0 {
		deltaThreshold = 1
	}
	v := &ddSketchValues[N]{
		accuracy:          accuracy,
		noMinMax:          noMinMax,
		noSum:             noSum,
		deltaTransmission: deltaTransmission,
		deltaThreshold:    deltaThreshold,
		snapshots:         make(map[attribute.Distinct]*ddsketch.DDSketch),
		newRes:            r,
		limit:             newLimiter[ddSketchSeries[N]](limit),
		values:            make(map[attribute.Distinct]*ddSketchSeries[N]),
	}
	v.seriesPool.New = func() any { return new(ddSketchSeries[N]) }
	return v
}

func (d *ddSketchValues[N]) newSeries(attr attribute.Set, value N) *ddSketchSeries[N] {
	series := d.seriesPool.Get().(*ddSketchSeries[N])
	// sketchlib-go's DDSketch has no in-place Reset(), so we always allocate a
	// fresh sketch for a new series. The pool still amortizes the
	// ddSketchSeries header allocation, which is the dominant cost.
	series.sketch = ddsketch.NewDDSketch(d.accuracy)
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

	// sketchlib-go's DDSketch silently drops non-positive / NaN / Inf values.
	// The DataDog sketch returned an explicit error for these; we mirror the
	// permissive sketchlib-go behavior since downstream consumers
	// (asap-precompute-{go,rs}, the agent decoder) all share the same
	// invariant set.
	series.sketch.Update(float64(value))
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
	deltaTransmission bool,
	deltaThreshold uint64,
) *ddSketch[N] {
	return &ddSketch[N]{
		ddSketchValues: newDDSketchValues[N](accuracy, noMinMax, noSum, limit, r, deltaTransmission, deltaThreshold),
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
		payload, encoding, err := serializeDDSketchFull(series.sketch)
		if err != nil {
			otel.Handle(err)
			continue
		}
		if d.exportDataPoint(series, encoding, payload, t, &dPts[i]) {
			i++
		}
	}

	// Trim to the number of exported points (in case any were skipped).
	dPts = dPts[:i]

	for _, series := range d.values {
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.res = nil // release exemplar reservoir (attr-dependent)
		series.sketch = nil
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

		payload, encoding, err := d.payloadFor(key, series.sketch)
		if err != nil {
			otel.Handle(err)
			continue
		}
		if d.exportDataPoint(series, encoding, payload, t, &dPts[i]) {
			i++
		}
	}
	dPts = dPts[:i]

	for _, key := range toEvict {
		series := d.values[key]
		delete(d.values, key)
		// Also evict the snapshot to avoid a memory leak.
		d.snapshotsMu.Lock()
		delete(d.snapshots, key)
		d.snapshotsMu.Unlock()
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.res = nil
		series.sketch = nil
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

// payloadFor returns the serialized payload and encoding for a cumulative
// export. If deltaTransmission is enabled and a prior snapshot exists, it
// invokes sketchlib-go's ComputeDelta to emit a sparse delta; otherwise it
// emits a full SketchEnvelope-wrapped portable payload.
//
// The snapshot map holds *ddsketch.DDSketch clones (not their serialized
// bytes) so ComputeDelta can iterate buckets directly without re-decoding —
// this matches the path taken by KLL/HLL/CountSketch/CountMinSketch siblings
// and asap-precompute-go's DDSketchWrapper.ComputeDeltaAgainst.
func (d *ddSketchValues[N]) payloadFor(key attribute.Distinct, sketch *ddsketch.DDSketch) ([]byte, metricdata.DDSketchEncoding, error) {
	if !d.deltaTransmission {
		return serializeDDSketchFull(sketch)
	}

	d.snapshotsMu.Lock()
	snap, hasSnap := d.snapshots[key]
	d.snapshotsMu.Unlock()

	if hasSnap && snap != nil {
		deltaPayload, deltaErr := ddsketch.ComputeDelta(snap, sketch, d.deltaThreshold)
		if deltaErr == nil {
			// Update snapshot to a clone of the current sketch; the receiver
			// will fold this delta into its prior cumulative state, so on
			// the next tick we want to delta against this same baseline.
			d.snapshotsMu.Lock()
			d.snapshots[key] = sketch.Clone()
			d.snapshotsMu.Unlock()
			return deltaPayload, metricdata.DDSketchEncodingProtoDelta, nil
		}
		// Fall through to full on error — keeps the emit path always
		// producing a valid payload, mirroring asap-precompute-go's
		// DDSketchWrapper.ComputeDeltaAgainst contract.
	}

	full, encoding, err := serializeDDSketchFull(sketch)
	if err != nil {
		return nil, encoding, err
	}
	d.snapshotsMu.Lock()
	d.snapshots[key] = sketch.Clone()
	d.snapshotsMu.Unlock()
	return full, encoding, nil
}

func (d *ddSketch[N]) exportDataPoint(
	series *ddSketchSeries[N],
	encoding metricdata.DDSketchEncoding,
	payload []byte,
	t time.Time,
	dest *metricdata.DDSketchDataPoint[N],
) bool {
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
	dp.Encoding = encoding
	dp.Sketch = payload
	collectExemplars(&dp.Exemplars, series.res.Collect)
	return true
}

// serializeDDSketchFull emits the canonical full-state portable wire format:
// a sketchlib-go SketchEnvelope wrapping a DDSketchState. The Producer and
// HashSpec fields are stripped to match what the asap-precompute-{go,rs}
// wrappers emit (see integration/parity/golden_test.go's per-sketch
// overlays); this preserves byte-parity with the cross-language fixtures.
func serializeDDSketchFull(sk *ddsketch.DDSketch) ([]byte, metricdata.DDSketchEncoding, error) {
	if sk == nil {
		return nil, metricdata.DDSketchEncodingProto, nil
	}
	env, err := sk.SerializePortable()
	if err != nil {
		return nil, metricdata.DDSketchEncodingProto, err
	}
	// Strip producer / hash_spec so the payload bytes are stable across
	// sketchlib-go version bumps and identical to the
	// asap-precompute-{go,rs} wrapper outputs (see
	// integration/parity/golden_test.go::TestGenerateGoldenFixtures).
	env.Producer = nil
	env.HashSpec = nil
	bytes, err := proto.Marshal(env)
	if err != nil {
		return nil, metricdata.DDSketchEncodingProto, err
	}
	return bytes, metricdata.DDSketchEncodingProto, nil
}
