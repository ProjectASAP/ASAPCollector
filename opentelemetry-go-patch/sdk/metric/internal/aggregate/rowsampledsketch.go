// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/sketchlib-go/common"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// rowSampledTarget is the shared state for ONE precompute.AggregationIdentity
// — the physical collector-side sketch instance every series routed here
// folds into. All such series share ONE GeometricSampler + ONE live grant,
// since together they form one "update stream" in the NitroSketch sense
// (see precompute.AggregationRouter's doc). rows is this target's row
// fan-out d.
type rowSampledTarget[N int64 | float64] struct {
	mu      sync.Mutex
	rows    int
	sampler *common.GeometricSampler
	grant   *liveSampleGrant
	// seed is derived from the complete physical target identity and producer.
	seed int64
	// sampleEpoch prevents a probability change from replaying the same random
	// stream prefix. It advances only when the applied probability changes.
	sampleEpoch uint64
	// pending buffers admitted occurrences since the last delta() drain.
	pending []metricdata.RowSampledSketchDataPoint[N]
}

type rowSampledSketchValues[N int64 | float64] struct {
	router           precompute.AggregationRouter
	coordinatorURL   string
	edgeID           string
	windowMs         uint64
	bootstrapSampleP float64

	targetsMu sync.RWMutex
	targets   map[precompute.AggregationIdentity]*rowSampledTarget[N]
	deltaMu   sync.Mutex
}

func newRowSampledSketchValues[N int64 | float64](
	router precompute.AggregationRouter, coordinatorURL, edgeID string, windowMs uint64, bootstrapSampleP float64,
) *rowSampledSketchValues[N] {
	// AggregationRowSampledSketch documents its Go zero value as exact. Normalize
	// here as well as at the public builder boundary so direct internal callers
	// cannot accidentally turn an omitted value into near-total data loss.
	if bootstrapSampleP == 0 {
		bootstrapSampleP = 1
	}
	return &rowSampledSketchValues[N]{
		router:           router,
		coordinatorURL:   coordinatorURL,
		edgeID:           edgeID,
		windowMs:         windowMs,
		bootstrapSampleP: bootstrapSampleP,
		targets:          make(map[precompute.AggregationIdentity]*rowSampledTarget[N]),
	}
}

func rowSampleSeed(edgeID string, id precompute.AggregationIdentity) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(edgeID))
	var agg [8]byte
	binary.LittleEndian.PutUint64(agg[:], id.AggID)
	_, _ = h.Write(agg[:])
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(id.Filter))
	return int64(h.Sum64())
}

func rowSampleEpochSeed(base int64, epoch uint64) int64 {
	x := uint64(base) + epoch*0x9e3779b97f4a7c15
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return int64(x)
}

// targetFor returns the shared rowSampledTarget for id, creating it (and
// its GeometricSampler + liveSampleGrant) on first use. The map lock is held
// only for lookup/construction; unrelated targets sample concurrently.
func (d *rowSampledSketchValues[N]) targetFor(id precompute.AggregationIdentity, rows int) *rowSampledTarget[N] {
	d.targetsMu.RLock()
	if t, ok := d.targets[id]; ok {
		d.targetsMu.RUnlock()
		return t
	}
	d.targetsMu.RUnlock()

	d.targetsMu.Lock()
	defer d.targetsMu.Unlock()
	if t, ok := d.targets[id]; ok {
		return t
	}
	// Seed from the complete physical identity and producer: reproducible
	// across process restarts and independent across target sketches. Unlike
	// ConsistentSampler's per-window salt (needed for hash-reproducibility
	// across independent evaluation sites), a single GeometricSampler
	// instance run continuously needs no per-window reseed for
	// correctness — only a genuine p CHANGE requires Reset (see measure()).
	seed := rowSampleSeed(d.edgeID, id)
	t := &rowSampledTarget[N]{
		rows:    rows,
		sampler: common.NewGeometricSampler(d.bootstrapSampleP, rowSampleEpochSeed(seed, 0)),
		grant:   newLiveSampleGrant(d.coordinatorURL, d.edgeID, id.AggID, d.windowMs, d.bootstrapSampleP),
		seed:    seed,
	}
	d.targets[id] = t
	return t
}

// measure decides row admission for ONE raw occurrence and, if at least one
// row admits, buffers it as an individually-exportable data point — the
// occurrence's raw value is NEVER pre-merged with any other occurrence's
// (unlike a per-interval running sum), because the target sketch's eventual
// COLUMN for this occurrence's key is the collector's job and depends on
// the key, which a merged sum would have destroyed. An occurrence that
// admits NO row is discarded here — it is never buffered, never
// serialized, never sent.
func (d *rowSampledSketchValues[N]) measure(
	ctx context.Context, value N, fltrAttr attribute.Set, droppedAttr []attribute.KeyValue,
) {
	id, rows, ok := d.router(fltrAttr)
	if !ok || rows > 64 {
		return
	}
	// Non-matrix materializations (Sum and DDSketch) advertise rows=0 in
	// AggregationPolicy. They are the d=1 whole-item case: one admission bit
	// decides whether the raw occurrence crosses the SDK/Collector boundary.
	if rows <= 1 {
		rows = 1
	}

	t := d.targetFor(id, rows)
	t.mu.Lock()
	defer t.mu.Unlock()

	// Report this occurrence toward the target's rate signal (BEFORE the
	// admission decision — rate tracks arriving traffic, independent of
	// sampling outcome), then read the currently-granted p. currentP() only
	// changes value at a window roll; when it does, the sampler's skip-gap
	// (computed under the OLD p) is stale and must be redrawn.
	t.grant.reportOccurrence()
	p := t.grant.currentP()
	if p != t.sampler.P() {
		t.sampleEpoch++
		t.sampler.Reset(p, rowSampleEpochSeed(t.seed, t.sampleEpoch))
	}

	// Consume this occurrence's complete row block with NitroSketch's direct
	// geometric cursor jump. A gap spanning the block costs one comparison and
	// subtraction rather than d per-row Admit calls. The returned mask is
	// seed-for-seed identical to scanning the flattened (occurrence,row) stream.
	admittedRows := t.sampler.AdmitRows(rows)
	if admittedRows == 0 {
		return // R(x)=∅ — discarded, never buffered, never exported
	}
	t.pending = append(t.pending, metricdata.RowSampledSketchDataPoint[N]{
		Attributes:   fltrAttr,
		Time:         now(),
		Value:        value, // RAW, unscaled — consumer rescales ×1/SampleP
		AdmittedRows: admittedRows,
		Rows:         int32(rows),
		SampleP:      p,
	})
}

type rowSampledSketchAgg[N int64 | float64] struct {
	*rowSampledSketchValues[N]
	start time.Time
}

func newRowSampledSketchAgg[N int64 | float64](
	router precompute.AggregationRouter, coordinatorURL, edgeID string, windowMs uint64, bootstrapSampleP float64,
) *rowSampledSketchAgg[N] {
	return &rowSampledSketchAgg[N]{
		rowSampledSketchValues: newRowSampledSketchValues[N](router, coordinatorURL, edgeID, windowMs, bootstrapSampleP),
		start:                  now(),
	}
}

func (d *rowSampledSketchAgg[N]) measure(
	ctx context.Context, value N, fltrAttr attribute.Set, droppedAttr []attribute.KeyValue,
) {
	d.rowSampledSketchValues.measure(ctx, value, fltrAttr, droppedAttr)
}

// delta drains every target's buffered admitted occurrences into one flat
// slice. Unlike every other aggregation in this package, the point COUNT
// here is NOT one-per-series — it is however many raw occurrences were
// admitted across every target since the last drain (zero to many).
func (d *rowSampledSketchAgg[N]) delta(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	d.deltaMu.Lock()
	defer d.deltaMu.Unlock()
	t := now()

	data, _ := (*dest).(metricdata.RowSampledSketch[N])
	data.Temporality = metricdata.DeltaTemporality

	d.targetsMu.RLock()
	targets := make([]*rowSampledTarget[N], 0, len(d.targets))
	for _, target := range d.targets {
		targets = append(targets, target)
	}
	d.targetsMu.RUnlock()
	var pts []metricdata.RowSampledSketchDataPoint[N]
	for _, target := range targets {
		target.mu.Lock()
		for i := range target.pending {
			target.pending[i].StartTime = d.start
		}
		pts = append(pts, target.pending...)
		target.pending = nil
		target.mu.Unlock()
	}
	d.start = t

	data.DataPoints = pts
	*dest = data
	return len(pts)
}

// cumulative has no sensible semantics for admitted-per-occurrence raw
// samples — there is no "value since a fixed start" to re-emit, unlike a
// running sum or a sketch's accumulated state. Alias to delta so a
// misconfigured cumulative temporality still drains correctly each
// interval rather than accumulating an unbounded buffer.
func (d *rowSampledSketchAgg[N]) cumulative(
	dest *metricdata.Aggregation, //nolint:gocritic // pointer required by interface
) int {
	return d.delta(dest)
}
