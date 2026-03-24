// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"sync"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
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
	// snapshots holds the proto-serialized snapshot of the last exported sketch
	// per series, keyed by attribute.Distinct. Used only when deltaTransmission=true.
	snapshots   map[attribute.Distinct][]byte
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
		snapshots:         make(map[attribute.Distinct][]byte),
		newRes:            r,
		limit:             newLimiter[ddSketchSeries[N]](limit),
		values:            make(map[attribute.Distinct]*ddSketchSeries[N]),
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
		if d.exportDataPoint(series, metricdata.DDSketchEncodingProto, nil, t, &dPts[i]) {
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

// payloadFor returns the serialized payload and encoding for a cumulative export.
// If deltaTransmission is enabled and a prior snapshot exists, it returns a
// sparse delta; otherwise it returns the full proto payload.
func (d *ddSketchValues[N]) payloadFor(key attribute.Distinct, sketch *ddsketch.DDSketch) ([]byte, metricdata.DDSketchEncoding, error) {
	fullPayload, err := serializeDDSketch(sketch)
	if err != nil {
		return nil, metricdata.DDSketchEncodingProto, err
	}

	if !d.deltaTransmission {
		return fullPayload, metricdata.DDSketchEncodingProto, nil
	}

	d.snapshotsMu.Lock()
	snapPayload, hasSnap := d.snapshots[key]
	d.snapshotsMu.Unlock()

	var (
		payload  []byte
		encoding metricdata.DDSketchEncoding
	)
	if hasSnap && snapPayload != nil {
		deltaPayload, deltaErr := ddSketchDeltaPayload(snapPayload, sketch, d.deltaThreshold)
		if deltaErr != nil {
			// Fall back to full on error.
			payload = fullPayload
			encoding = metricdata.DDSketchEncodingProto
		} else {
			payload = deltaPayload
			encoding = metricdata.DDSketchEncodingProtoDelta
		}
	} else {
		payload = fullPayload
		encoding = metricdata.DDSketchEncodingProto
	}

	// Update snapshot with the current full payload.
	d.snapshotsMu.Lock()
	d.snapshots[key] = fullPayload
	d.snapshotsMu.Unlock()

	return payload, encoding, nil
}

func (d *ddSketch[N]) exportDataPoint(
	series *ddSketchSeries[N],
	encoding metricdata.DDSketchEncoding,
	payload []byte,
	t time.Time,
	dest *metricdata.DDSketchDataPoint[N],
) bool {
	// In delta() path payload is nil — serialize inline.
	if payload == nil {
		var err error
		payload, err = serializeDDSketch(series.sketch)
		if err != nil {
			otel.Handle(err)
			return false
		}
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
	dp.Encoding = encoding
	dp.Sketch = payload
	collectExemplars(&dp.Exemplars, series.res.Collect)
	return true
}

func serializeDDSketch(sk *ddsketch.DDSketch) ([]byte, error) {
	if sk == nil {
		return nil, nil
	}
	return proto.Marshal(sk.ToProto())
}

// ddSketchDeltaPayload computes a sparse delta between a proto-serialized
// snapshot and the current sketch. Only buckets whose count changed by at
// least threshold are included.
func ddSketchDeltaPayload(snapPayload []byte, current *ddsketch.DDSketch, threshold uint64) ([]byte, error) {
	var snap sketchpb.DDSketch
	if err := proto.Unmarshal(snapPayload, &snap); err != nil {
		return serializeDDSketch(current)
	}

	curr := current.ToProto()
	delta := &sketchpb.DDSketch{
		Mapping:   curr.Mapping,
		ZeroCount: curr.ZeroCount - snap.ZeroCount,
	}
	delta.PositiveValues = ddStoreDelta(snap.PositiveValues, curr.PositiveValues, float64(threshold))
	delta.NegativeValues = ddStoreDelta(snap.NegativeValues, curr.NegativeValues, float64(threshold))
	return proto.Marshal(delta)
}

// ddStoreDelta returns a sparse Store with only buckets where |Δcount| ≥ threshold.
func ddStoreDelta(snap, curr *sketchpb.Store, threshold float64) *sketchpb.Store {
	if curr == nil {
		return nil
	}
	snapCounts := ddStoreToMap(snap)
	currCounts := ddStoreToMap(curr)

	out := &sketchpb.Store{BinCounts: make(map[int32]float64)}
	for idx, cnt := range currCounts {
		d := cnt - snapCounts[idx]
		if d >= threshold || d <= -threshold {
			out.BinCounts[idx] = d
		}
	}
	if len(out.BinCounts) == 0 {
		return nil
	}
	return out
}

// ddStoreToMap converts a sketchpb.Store into a flat index→count map.
func ddStoreToMap(s *sketchpb.Store) map[int32]float64 {
	m := make(map[int32]float64)
	if s == nil {
		return m
	}
	for idx, cnt := range s.BinCounts {
		m[idx] += cnt
	}
	for i, cnt := range s.ContiguousBinCounts {
		idx := s.ContiguousBinIndexOffset + int32(i)
		m[idx] += cnt
	}
	return m
}
