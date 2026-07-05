// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/ProjectASAP/sketchlib-go/common"
	countsketch "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	defaultCountSketchRows = countsketch.DefaultRows
	defaultCountSketchCols = countsketch.DefaultCols
)

type countSketchSeries[N int64 | float64] struct {
	attrs     attribute.Set
	seriesID  uint64
	sketch    *countsketch.CountSketch
	dimension string
	epsilon   float64
	delta     float64

	// sampler is the per-series geometric row-admission sampler (NitroSketch
	// skip-sampling). Non-nil only when 0 < sampleP < 1; nil means every update
	// touches all d rows. Hosting it here — at the SDK aggregator, where the
	// measurement enters — is the "admission at the SDK" location of §3.1 of the
	// GOS design: the source decides which rows to admit and applies the 1/p
	// weight, so downstream collectors never re-sample.
	sampler *common.GeometricSampler

	measuredSince bool
	idleCycles    uint8
}

type countSketchValues[N int64 | float64] struct {
	rows      int
	cols      int
	epsilon   float64
	delta     float64
	dimension string

	// sampleP is the per-row geometric admission rate. Values <=0 or >=1 disable
	// sampling (every update touches all d rows); 0 < sampleP < 1 installs a
	// per-series GeometricSampler and routes updates through the per-row
	// drop-before-hash path with 1/p inverse-probability weighting.
	sampleP float64

	limit      limiter[countSketchSeries[N]]
	values     map[attribute.Distinct]*countSketchSeries[N]
	valuesMu   sync.Mutex
	seriesPool sync.Pool

	// deltaTransmission enables sparse delta encoding for cumulative exports.
	deltaTransmission bool
	// deltaThreshold is the minimum absolute cell change to include in a delta.
	deltaThreshold float64
	// snapshots holds a clone of the last-exported CS per series, keyed by
	// attribute.Distinct. Used to compute sparse cell deltas.
	snapshots   map[attribute.Distinct]*countsketch.CountSketch
	snapshotsMu sync.Mutex
}

func newCountSketchValues[N int64 | float64](rows, cols int, epsilon, delta float64, dimension string, limit int, deltaTransmission bool, deltaThreshold float64, sampleP float64) *countSketchValues[N] {
	if rows <= 0 {
		rows = defaultCountSketchRows
	}
	if cols <= 0 {
		cols = defaultCountSketchCols
	}
	if deltaTransmission && deltaThreshold <= 0 {
		deltaThreshold = 1.0
	}
	v := &countSketchValues[N]{
		rows:              rows,
		cols:              cols,
		epsilon:           epsilon,
		delta:             delta,
		dimension:         dimension,
		sampleP:           sampleP,
		limit:             newLimiter[countSketchSeries[N]](limit),
		values:            make(map[attribute.Distinct]*countSketchSeries[N]),
		deltaTransmission: deltaTransmission,
		deltaThreshold:    deltaThreshold,
		snapshots:         make(map[attribute.Distinct]*countsketch.CountSketch),
	}
	v.seriesPool.New = func() any { return new(countSketchSeries[N]) }
	return v
}

func (d *countSketchValues[N]) newSeries(attr attribute.Set) *countSketchSeries[N] {
	series := d.seriesPool.Get().(*countSketchSeries[N])
	if series.sketch != nil {
		series.sketch.Reset() // reuse Count rows and L2 backing arrays
	} else {
		sk, err := countsketch.NewCountSketch(d.rows, d.cols)
		if err != nil {
			otel.Handle(err)
			return nil
		}
		series.sketch = sk
	}
	series.attrs = attr
	series.seriesID = 0
	series.dimension = d.dimension
	series.epsilon = d.epsilon
	series.delta = d.delta
	series.measuredSince = true
	series.idleCycles = 0
	// Install (or reset, on pool reuse) the per-row geometric sampler. Seed it
	// deterministically from the series' attribute key so runs are reproducible
	// and distinct series decorrelate their admission streams.
	if d.sampleP > 0 && d.sampleP < 1 {
		seed := csSamplerSeed(attr)
		if series.sampler == nil {
			series.sampler = common.NewGeometricSampler(d.sampleP, seed)
		} else {
			series.sampler.Reset(d.sampleP, seed)
		}
	} else {
		series.sampler = nil
	}
	return series
}

// csSamplerSeed derives a stable per-series RNG seed from the series' encoded
// attribute set (FNV-1a-64), so the geometric sampler is reproducible across
// process restarts and independent across series.
func csSamplerSeed(attr attribute.Set) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(attr.Encoded(attribute.DefaultEncoder())))
	return int64(h.Sum64())
}

func (d *countSketchValues[N]) measure(
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
	// Use attribute set as the tracked key
	key := fltrAttr.Encoded(attribute.DefaultEncoder())
	if series.sampler != nil {
		// Per-row geometric admission (drop-before-hash, 1/p weight). A fully
		// unadmitted update touches no row — the source-side realization of the
		// §3.1 sampling split.
		series.sketch.UpdateStringSampledPerRow(key, float64(value), series.sampler)
	} else {
		series.sketch.UpdateString(key, float64(value))
	}
}

type countSketchAgg[N int64 | float64] struct {
	*countSketchValues[N]
	start time.Time
}

func newCountSketchAgg[N int64 | float64](rows, cols int, epsilon, delta float64, dimension string, limit int, deltaTransmission bool, deltaThreshold float64, sampleP float64) *countSketchAgg[N] {
	return &countSketchAgg[N]{
		countSketchValues: newCountSketchValues[N](rows, cols, epsilon, delta, dimension, limit, deltaTransmission, deltaThreshold, sampleP),
		start:             now(),
	}
}

func (d *countSketchAgg[N]) measure(
	ctx context.Context,
	value N,
	fltrAttr attribute.Set,
	droppedAttr []attribute.KeyValue,
) {
	d.countSketchValues.measure(ctx, value, fltrAttr, droppedAttr)
}

func (d *countSketchAgg[N]) delta(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.CountSketch[N])
	data.Temporality = metricdata.DeltaTemporality

	d.valuesMu.Lock()
	defer d.valuesMu.Unlock()

	n := len(d.values)
	dPts := reset(data.DataPoints, n, n)

	var i int
	for _, series := range d.values {
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

	for _, series := range d.values {
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.dimension = ""
		series.epsilon = 0
		series.delta = 0
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

func (d *countSketchAgg[N]) cumulative(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	t := now()

	data, _ := (*dest).(metricdata.CountSketch[N])
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

	for _, key := range toEvict {
		series := d.values[key]
		delete(d.values, key)
		d.snapshotsMu.Lock()
		delete(d.snapshots, key)
		d.snapshotsMu.Unlock()
		series.attrs = attribute.Set{}
		series.seriesID = 0
		series.dimension = ""
		series.epsilon = 0
		series.delta = 0
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
// it computes a sparse cell delta; otherwise it returns the full proto payload
// and saves a new snapshot.
func (d *countSketchValues[N]) payloadFor(key attribute.Distinct, sketch *countsketch.CountSketch) ([]byte, metricdata.CountSketchEncoding, error) {
	if !d.deltaTransmission {
		return d.fullPayload(sketch)
	}

	d.snapshotsMu.Lock()
	snap, hasSnap := d.snapshots[key]
	d.snapshotsMu.Unlock()

	var payload []byte
	var enc metricdata.CountSketchEncoding
	var err error

	if hasSnap && snap != nil {
		var deltaMsg *countsketch.Delta
		deltaMsg, err = countsketch.ComputeDelta(snap, sketch, d.deltaThreshold)
		if err == nil {
			payload, err = countsketch.SerializeDelta(deltaMsg)
		}
		enc = metricdata.CountSketchEncodingDelta
	} else {
		payload, err = sketch.SerializeProtoBytes()
		enc = metricdata.CountSketchEncodingProto
	}
	if err != nil {
		return nil, "", err
	}

	newSnap := cloneCSSketch(sketch)
	d.snapshotsMu.Lock()
	d.snapshots[key] = newSnap
	d.snapshotsMu.Unlock()

	return payload, enc, nil
}

// fullPayload returns a full proto serialization of sketch.
func (d *countSketchValues[N]) fullPayload(sketch *countsketch.CountSketch) ([]byte, metricdata.CountSketchEncoding, error) {
	b, err := sketch.SerializeProtoBytes()
	return b, metricdata.CountSketchEncodingProto, err
}

func (d *countSketchAgg[N]) exportDataPoint(
	series *countSketchSeries[N],
	t time.Time,
	payload []byte,
	encoding metricdata.CountSketchEncoding,
	dest *metricdata.CountSketchDataPoint[N],
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
	dp.Dimension = series.dimension
	dp.Epsilon = series.epsilon
	dp.Delta = series.delta
	dp.Encoding = encoding
	dp.Sketch = payload
	return true
}

// cloneCSSketch returns a deep copy of src suitable for use as a delta snapshot.
func cloneCSSketch(src *countsketch.CountSketch) *countsketch.CountSketch {
	data, err := src.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	clone, err := countsketch.DeserializeCountSketchFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return clone
}
