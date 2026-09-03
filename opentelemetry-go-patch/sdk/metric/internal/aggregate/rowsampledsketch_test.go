// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"context"
	"testing"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/sketchlib-go/common"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// constRouter routes every series to the SAME AggregationIdentity/rows —
// the common single-target-sketch case.
func constRouter(id precompute.AggregationIdentity, rows int) precompute.AggregationRouter {
	return func(attribute.Set) (precompute.AggregationIdentity, int, bool) {
		return id, rows, true
	}
}

// unsampled (bootstrapSampleP=1.0) must admit every row of every
// occurrence — byte-equivalent to unsampled passthrough, matching every
// other family's p>=1 convention.
func TestRowSampledSketch_UnsampledAdmitsAllRows(t *testing.T) {
	id := precompute.AggregationIdentity{AggID: 1, Filter: ""}
	agg := newRowSampledSketchAgg[int64](constRouter(id, 4), "", "edge-1", 60, 1.0)

	for i := 0; i < 50; i++ {
		agg.measure(context.Background(), 1, attribute.NewSet(attribute.String("k", "x")), nil)
	}

	var dest metricdata.Aggregation
	n := agg.delta(&dest)
	if n != 50 {
		t.Fatalf("got %d admitted occurrences, want 50 (p=1.0 must admit every occurrence)", n)
	}
	data := dest.(metricdata.RowSampledSketch[int64])
	for _, dp := range data.DataPoints {
		if dp.AdmittedRows != 0b1111 {
			t.Fatalf("AdmittedRows = %04b, want 1111 (p=1.0 must admit every row)", dp.AdmittedRows)
		}
		if dp.SampleP != 1.0 {
			t.Fatalf("SampleP = %v, want 1.0", dp.SampleP)
		}
	}
}

// R(x)=∅ occurrences (no row admits) must be discarded entirely — never
// appear in the drained output.
func TestRowSampledSketch_ZeroSampleP_DropsEverything(t *testing.T) {
	id := precompute.AggregationIdentity{AggID: 2, Filter: ""}
	agg := newRowSampledSketchAgg[int64](constRouter(id, 4), "", "edge-1", 60, 0)

	for i := 0; i < 20; i++ {
		agg.measure(context.Background(), 1, attribute.NewSet(attribute.String("k", "x")), nil)
	}

	var dest metricdata.Aggregation
	n := agg.delta(&dest)
	if n != 0 {
		t.Fatalf("got %d admitted occurrences, want 0 (p<=0 must admit nothing)", n)
	}
}

// Sum and DDSketch policies use Rows=0 because they are not row-replicated
// matrices. The SDK normalizes them to the d=1 whole-item admission protocol.
func TestRowSampledSketch_NonMatrixPolicyUsesOneAdmissionRow(t *testing.T) {
	for _, family := range []string{"Sum", "DDSketch"} {
		t.Run(family, func(t *testing.T) {
			id := precompute.AggregationIdentity{AggID: 20, Filter: family}
			agg := newRowSampledSketchAgg[float64](constRouter(id, 0), "", "edge-1", 60, 1)
			agg.measure(context.Background(), 4, attribute.NewSet(attribute.String("k", "x")), nil)

			var dest metricdata.Aggregation
			if n := agg.delta(&dest); n != 1 {
				t.Fatalf("admitted occurrences = %d, want 1", n)
			}
			dp := dest.(metricdata.RowSampledSketch[float64]).DataPoints[0]
			if dp.Rows != 1 || dp.AdmittedRows != 1 {
				t.Fatalf("Rows/AdmittedRows = %d/%#x, want 1/0x1", dp.Rows, dp.AdmittedRows)
			}
		})
	}
}

// The core correctness property the row-sum-vector design got wrong:
// DIFFERENT keys sharing ONE target AggregationIdentity must each survive
// with their OWN individual key intact — never merged/summed together,
// since the collector needs each occurrence's key to pick its own column.
func TestRowSampledSketch_DifferentKeysNeverMerged(t *testing.T) {
	id := precompute.AggregationIdentity{AggID: 3, Filter: ""}
	agg := newRowSampledSketchAgg[int64](constRouter(id, 2), "", "edge-1", 60, 1.0)

	keys := []string{"AAPL", "GOOG", "AAPL", "MSFT", "GOOG", "AAPL"}
	for _, k := range keys {
		agg.measure(context.Background(), 1, attribute.NewSet(attribute.String("symbol", k)), nil)
	}

	var dest metricdata.Aggregation
	n := agg.delta(&dest)
	if n != len(keys) {
		t.Fatalf("got %d admitted occurrences, want %d — every occurrence must survive individually (p=1.0), not merged", n, len(keys))
	}
	data := dest.(metricdata.RowSampledSketch[int64])

	gotBySymbol := map[string]int{}
	for _, dp := range data.DataPoints {
		v, ok := dp.Attributes.Value(attribute.Key("symbol"))
		if !ok {
			t.Fatalf("data point missing symbol attribute: %+v", dp)
		}
		gotBySymbol[v.Emit()]++
	}
	want := map[string]int{"AAPL": 3, "GOOG": 2, "MSFT": 1}
	for sym, wantCount := range want {
		if gotBySymbol[sym] != wantCount {
			t.Fatalf("symbol %q: got %d individual points, want %d (each occurrence must stay separate, not summed into one point)", sym, gotBySymbol[sym], wantCount)
		}
	}
}

// A router returning ok=false must be a pure no-op — the occurrence is
// neither buffered nor does it touch any target's sampler/grant state.
func TestRowSampledSketch_UnroutedSeriesIsNoop(t *testing.T) {
	never := func(attribute.Set) (precompute.AggregationIdentity, int, bool) {
		return precompute.AggregationIdentity{}, 0, false
	}
	agg := newRowSampledSketchAgg[int64](never, "", "edge-1", 60, 1.0)
	agg.measure(context.Background(), 1, attribute.NewSet(attribute.String("k", "x")), nil)

	var dest metricdata.Aggregation
	n := agg.delta(&dest)
	if n != 0 {
		t.Fatalf("got %d admitted occurrences from an unrouted series, want 0", n)
	}
	if len(agg.targets) != 0 {
		t.Fatalf("targets = %d, want 0 (an unrouted series must never create a target)", len(agg.targets))
	}
}

// Distinct AggregationIdentity values (different target sketches) must get
// independent samplers/buffers — an occurrence routed to identity A must
// never influence identity B's admitted set.
func TestRowSampledSketch_DistinctIdentitiesAreIndependent(t *testing.T) {
	idA := precompute.AggregationIdentity{AggID: 10, Filter: "zone=us"}
	idB := precompute.AggregationIdentity{AggID: 10, Filter: "zone=eu"}
	router := func(attrs attribute.Set) (precompute.AggregationIdentity, int, bool) {
		v, _ := attrs.Value(attribute.Key("zone"))
		if v.Emit() == "us" {
			return idA, 3, true
		}
		return idB, 3, true
	}
	agg := newRowSampledSketchAgg[int64](router, "", "edge-1", 60, 1.0)

	agg.measure(context.Background(), 1, attribute.NewSet(attribute.String("zone", "us")), nil)
	agg.measure(context.Background(), 1, attribute.NewSet(attribute.String("zone", "eu")), nil)
	agg.measure(context.Background(), 1, attribute.NewSet(attribute.String("zone", "eu")), nil)

	if len(agg.targets) != 2 {
		t.Fatalf("targets = %d, want 2 (distinct AggregationIdentity values must get independent targets)", len(agg.targets))
	}

	var dest metricdata.Aggregation
	n := agg.delta(&dest)
	if n != 3 {
		t.Fatalf("got %d admitted occurrences, want 3", n)
	}
}

// The SDK owns the direct geometric cursor. It must export exactly the
// non-empty masks produced by AdmitRows and must never turn a skipped source
// occurrence into an OTLP datapoint for the Collector processor.
func TestRowSampledSketch_DirectJumpDropsBeforeExport(t *testing.T) {
	const (
		rows = 5
		p    = 0.01
		n    = 100_000
	)
	id := precompute.AggregationIdentity{AggID: 9182, Filter: ""}
	agg := newRowSampledSketchAgg[int64](constRouter(id, rows), "", "edge-1", 60, p)
	expectedSampler := common.NewGeometricSampler(p, int64(id.AggID))

	var wantMasks []uint64
	attrs := attribute.NewSet(attribute.String("symbol", "AAPL"))
	for i := 0; i < n; i++ {
		if mask := expectedSampler.AdmitRows(rows); mask != 0 {
			wantMasks = append(wantMasks, mask)
		}
		agg.measure(context.Background(), 1, attrs, nil)
	}

	var dest metricdata.Aggregation
	agg.delta(&dest)
	got := dest.(metricdata.RowSampledSketch[int64]).DataPoints
	if len(got) != len(wantMasks) {
		t.Fatalf("exported datapoints=%d, want %d non-empty source masks", len(got), len(wantMasks))
	}
	if len(got) >= n/10 {
		t.Fatalf("exported datapoints=%d: source sampling did not reduce the %d raw inputs", len(got), n)
	}
	for i, dp := range got {
		if dp.AdmittedRows == 0 {
			t.Fatalf("exported datapoint %d has an empty admission mask", i)
		}
		if dp.AdmittedRows != wantMasks[i] {
			t.Fatalf("exported mask %d=%#x, want direct-jump mask %#x", i, dp.AdmittedRows, wantMasks[i])
		}
	}
}
