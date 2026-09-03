// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"context"
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
	rows    int
	sampler *common.GeometricSampler
	grant   *liveSampleGrant
	// seed is the sampler's base seed (derived from AggID) — reused across
	// Reset calls so a repeated p doesn't need re-deriving a new seed.
	seed int64
	// pending buffers admitted occurrences since the last delta() drain.
	pending []metricdata.RowSampledSketchDataPoint[N]
}

type rowSampledSketchValues[N int64 | float64] struct {
	router           precompute.AggregationRouter
	coordinatorURL   string
	edgeID           string
	windowMs         uint64
	bootstrapSampleP float64

	mu      sync.Mutex
	targets map[precompute.AggregationIdentity]*rowSampledTarget[N]
}

func newRowSampledSketchValues[N int64 | float64](
	router precompute.AggregationRouter, coordinatorURL, edgeID string, windowMs uint64, bootstrapSampleP float64,
) *rowSampledSketchValues[N] {
	return &rowSampledSketchValues[N]{
		router:           router,
		coordinatorURL:   coordinatorURL,
		edgeID:           edgeID,
		windowMs:         windowMs,
		bootstrapSampleP: bootstrapSampleP,
		targets:          make(map[precompute.AggregationIdentity]*rowSampledTarget[N]),
	}
}

// targetFor returns the shared rowSampledTarget for id, creating it (and
// its GeometricSampler + liveSampleGrant) on first use. Caller holds d.mu.
func (d *rowSampledSketchValues[N]) targetFor(id precompute.AggregationIdentity, rows int) *rowSampledTarget[N] {
	if t, ok := d.targets[id]; ok {
		return t
	}
	// Seed from AggID: reproducible across process restarts for the same
	// policy, independent across distinct target sketches. Unlike
	// ConsistentSampler's per-window salt (needed for hash-reproducibility
	// across independent evaluation sites), a single GeometricSampler
	// instance run continuously needs no per-window reseed for
	// correctness — only a genuine p CHANGE requires Reset (see measure()).
	seed := int64(id.AggID)
	t := &rowSampledTarget[N]{
		rows:    rows,
		sampler: common.NewGeometricSampler(d.bootstrapSampleP, seed),
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

	d.mu.Lock()
	defer d.mu.Unlock()

	t := d.targetFor(id, rows)

	// Report this occurrence toward the target's rate signal (BEFORE the
	// admission decision — rate tracks arriving traffic, independent of
	// sampling outcome), then read the currently-granted p. currentP() only
	// changes value at a window roll; when it does, the sampler's skip-gap
	// (computed under the OLD p) is stale and must be redrawn.
	t.grant.reportOccurrence()
	p := t.grant.currentP()
	if p != t.sampler.P() {
		t.sampler.Reset(p, t.seed)
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
	t := now()

	data, _ := (*dest).(metricdata.RowSampledSketch[N])
	data.Temporality = metricdata.DeltaTemporality

	d.mu.Lock()
	defer d.mu.Unlock()

	var pts []metricdata.RowSampledSketchDataPoint[N]
	for _, target := range d.targets {
		for i := range target.pending {
			target.pending[i].StartTime = d.start
		}
		pts = append(pts, target.pending...)
		target.pending = nil
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
