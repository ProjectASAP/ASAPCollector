// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"sync"
	"time"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type countMinSketchSeries[N int64 | float64] struct {
	attrs       attribute.Set
	seriesID    uint64
	sketch      *cms.CountMinSketch
	sampleCount uint64

	// sampler is the per-series CONSISTENT row-admission sampler (non-nil only
	// when 0 < sampleP < 1): a stateless hash decision per (seed, occurrence,
	// row), so any other pipeline stage recomputing it agrees exactly — one
	// sampling owner regardless of location (design §3.1). Hosting it at the
	// SDK aggregator is the "admission at the SDK" location.
	sampler *common.ConsistentSampler

	measuredSince bool
	idleCycles    uint8
}

type countMinSketchValues[N int64 | float64] struct {
	rows int
	cols int

	// sampleP is the per-row admission rate. <=0 or >=1 disables sampling
	// (every insert touches all rows); 0 < sampleP < 1 installs a per-series
	// ConsistentSampler routing inserts through InsertWithHashSampledPerRow.
	sampleP float64
	// seedSalt decorrelates admission patterns across windows (mixed into every
	// series' sampler seed; refreshed to the window start in delta()).
	seedSalt uint64

	limit      limiter[countMinSketchSeries[N]]
	values     map[attribute.Distinct]*countMinSketchSeries[N]
	valuesMu   sync.Mutex
	seriesPool sync.Pool

	// deltaTransmission enables sparse delta encoding for cumulative exports.
	deltaTransmission bool
	// deltaThreshold is the minimum absolute cell change to include in a delta.
	deltaThreshold float64
	// snapshots holds a clone of the last-exported CMS per series, keyed by
	// attribute.Distinct. Used to compute sparse cell deltas.
	snapshots   map[attribute.Distinct]*cms.CountMinSketch
	snapshotsMu sync.Mutex
}

func newCountMinSketchValues[N int64 | float64](rows, cols, limit int, deltaTransmission bool, deltaThreshold float64, sampleP float64) *countMinSketchValues[N] {
	if rows <= 0 {
		rows = 4
	}
	if cols <= 0 {
		cols = 2048
	}
	if deltaTransmission && deltaThreshold <= 0 {
		deltaThreshold = 1.0
	}
	v := &countMinSketchValues[N]{
		rows:              rows,
		cols:              cols,
		sampleP:           sampleP,
		limit:             newLimiter[countMinSketchSeries[N]](limit),
		values:            make(map[attribute.Distinct]*countMinSketchSeries[N]),
		deltaTransmission: deltaTransmission,
		deltaThreshold:    deltaThreshold,
		snapshots:         make(map[attribute.Distinct]*cms.CountMinSketch),
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
	// Install (or reset, on pool reuse) the per-row consistent sampler. Seed =
	// FNV(attrs) ⊕ window salt (see countsketch.go).
	if d.sampleP > 0 && d.sampleP < 1 {
		seed := uint64(samplerSeedForAttrs(attr)) ^ d.seedSalt
		if series.sampler == nil {
			series.sampler = common.NewConsistentSampler(d.sampleP, seed)
		} else {
			series.sampler.Reset(d.sampleP, seed)
		}
	} else {
		series.sampler = nil
	}
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
	if series.sampler != nil {
		series.sketch.InsertWithHashSampledPerRow(input.Hash, series.sampler)
	} else {
		series.sketch.InsertWithHash(input.Hash)
	}
	// sampleCount tracks RAW observed items (not admitted rows); it is wire
	// metadata independent of sampling, so increment unconditionally.
	series.sampleCount++
}

type countMinSketchAgg[N int64 | float64] struct {
	*countMinSketchValues[N]
	start time.Time
}

func newCountMinSketchAgg[N int64 | float64](rows, cols, limit int, deltaTransmission bool, deltaThreshold float64, sampleP float64) *countMinSketchAgg[N] {
	a := &countMinSketchAgg[N]{
		countMinSketchValues: newCountMinSketchValues[N](rows, cols, limit, deltaTransmission, deltaThreshold, sampleP),
		start:                now(),
	}
	a.seedSalt = uint64(a.start.UnixNano())
	return a
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
		sketchBytes, enc, err := d.fullPayload(series.sketch)
		if err != nil {
			otel.Handle(err)
			continue
		}
		if d.exportDataPoint(series, t, sketchBytes, enc, &dPts[i]) {
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
	// New window → new admission salt (fresh decorrelated pattern next window).
	d.seedSalt = uint64(t.UnixNano())

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

		sketchBytes, enc, err := d.payloadFor(key, series.sketch)
		if err != nil {
			otel.Handle(err)
			continue
		}
		if d.exportDataPoint(series, t, sketchBytes, enc, &dPts[i]) {
			i++
		}
	}
	dPts = dPts[:i]

	for _, key := range toEvict {
		series := d.values[key]
		delete(d.values, key)
		d.snapshotsMu.Lock()
		delete(d.snapshots, key)
		d.snapshotsMu.Unlock()
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

// payloadFor returns the sketch payload bytes and encoding for one cumulative
// export cycle. If deltaTransmission is enabled and a prior snapshot exists,
// it computes a sparse cell delta; otherwise it returns the full gob payload
// and saves a new snapshot.
func (d *countMinSketchValues[N]) payloadFor(key attribute.Distinct, sketch *cms.CountMinSketch) ([]byte, metricdata.CountMinSketchEncoding, error) {
	if !d.deltaTransmission {
		return d.fullPayload(sketch)
	}

	d.snapshotsMu.Lock()
	snap, hasSnap := d.snapshots[key]
	d.snapshotsMu.Unlock()

	var payload []byte
	var enc metricdata.CountMinSketchEncoding
	var err error

	if hasSnap && snap != nil {
		var deltaMsg *cms.Delta
		deltaMsg, err = cms.ComputeDelta(snap, sketch, d.deltaThreshold)
		if err == nil {
			payload, err = cms.SerializeDelta(deltaMsg)
		}
		enc = metricdata.CountMinSketchEncodingDelta
		// Fall through to a full frame on any delta error (mirrors the
		// DDSketch payloadFor contract): always produce a valid payload and
		// refresh the snapshot, never wedge the series.
	}
	if payload == nil || err != nil {
		payload, err = serializeCMSketch(sketch)
		enc = metricdata.CountMinSketchEncodingProto
	}
	if err != nil {
		return nil, "", err
	}

	newSnap := cloneCMSketch(sketch)
	d.snapshotsMu.Lock()
	d.snapshots[key] = newSnap
	d.snapshotsMu.Unlock()

	return payload, enc, nil
}

// fullPayload returns a full proto serialization of sketch.
func (d *countMinSketchValues[N]) fullPayload(sketch *cms.CountMinSketch) ([]byte, metricdata.CountMinSketchEncoding, error) {
	b, err := serializeCMSketch(sketch)
	return b, metricdata.CountMinSketchEncodingProto, err
}

func (d *countMinSketchAgg[N]) exportDataPoint(
	series *countMinSketchSeries[N],
	t time.Time,
	sketchBytes []byte,
	encoding metricdata.CountMinSketchEncoding,
	dest *metricdata.CountMinSketchDataPoint[N],
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
	dp.SampleCount = series.sampleCount
	dp.Rows = int32(series.sketch.Rows)
	dp.Cols = int32(series.sketch.Cols)
	dp.Encoding = encoding
	dp.Sketch = sketchBytes
	return true
}

func serializeCMSketch(s *cms.CountMinSketch) ([]byte, error) {
	// Opt-1+Opt-2: FrequencyOnly + sint64 varint — omits Sum/Sum2 (valid for
	// unweighted telemetry streams) and uses packed zigzag varint encoding.
	// Reduces CMS payload ~10–15× vs legacy float64 full serialisation.
	return s.SerializeProtoBytesFO()
}

// cloneCMSketch returns a deep copy of src suitable for use as a delta snapshot.
func cloneCMSketch(src *cms.CountMinSketch) *cms.CountMinSketch {
	data, err := src.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	clone, err := cms.DeserializeCountMinSketchFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return clone
}
