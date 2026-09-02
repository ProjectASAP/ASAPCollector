// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	"google.golang.org/protobuf/proto"
)

// TestDDSketchWrapper_ObserveSnapshotApplyDelta drives a minimal
// observe -> snapshot -> ApplyDelta cycle and verifies the wrapped
// DDSketch reproduces the observed quantile after a round-trip. This
// is a smoke test only: the parity harness in integration/parity
// covers byte-equality with the legacy emit path.
func TestDDSketchWrapper_ObserveSnapshotApplyDelta(t *testing.T) {
	t.Parallel()
	w := NewDDSketchWrapper(0.01)
	for _, v := range []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		w.Update(v)
	}
	snap, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) == 0 {
		t.Fatal("snapshot empty")
	}

	// ApplyDelta into a fresh wrapper and check quantile parity.
	other := NewDDSketchWrapper(0.01)
	if err := other.ApplyDelta(snap); err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	want := w.Quantile(0.5)
	got := other.Quantile(0.5)
	// DDSketch reports a relative-accuracy bound; allow alpha=0.01 margin.
	if rel := abs(got-want) / want; rel > 0.011 {
		t.Fatalf("Quantile mismatch: want %f, got %f (rel %f)", want, got, rel)
	}
}

// TestDDSketchWrapper_ComputeDeltaShape exercises the
// ComputeDeltaAgainst path: prev empty -> isFull=true; non-empty prev
// with per-bucket changes -> a non-empty delta payload with
// isFull=false. The actual delta-apply correctness lives in
// sketchlib-go's own tests; this test just confirms the wrapper
// dispatches into the right sketchlib API.
//
// NOTE: the DDSketchDelta wire format no longer carries the
// DataPoint-level count/sum/min/max scalar deltas
// (ProjectASAP/sketchlib-go#61 + ProjectASAP/asap_sketchlib#57), so a
// delta now marshals to non-empty bytes only when at least one bucket
// passes the threshold. Use threshold=1 so the new buckets from the
// second batch are emitted; a previously-used huge threshold (1<<30)
// would now drop every bucket and produce a legitimately empty payload.
func TestDDSketchWrapper_ComputeDeltaShape(t *testing.T) {
	t.Parallel()
	w := NewDDSketchWrapper(0.01)
	for i := 1; i <= 50; i++ {
		w.Update(float64(i))
	}
	// First call: no prev -> full snapshot.
	full, isFull, err := w.ComputeDeltaAgainst(nil, 1)
	if err != nil || !isFull || len(full) == 0 {
		t.Fatalf("first call: full=%v err=%v len=%d", isFull, err, len(full))
	}
	prev := full
	for i := 51; i <= 100; i++ {
		w.Update(float64(i))
	}
	delta, isFull, err := w.ComputeDeltaAgainst(prev, 1)
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	if isFull {
		t.Fatal("expected per-bucket delta, got isFull=true")
	}
	if len(delta) == 0 {
		t.Fatal("empty delta")
	}
}

// TestDDSketchObserver verifies the SketchObserver dispatches a
// KindFloat observation into the underlying wrapper and rejects
// unsupported kinds.
func TestDDSketchObserver(t *testing.T) {
	t.Parallel()
	w := NewDDSketchWrapper(0.01)
	if err := (DDSketchObserver{}).Observe(w, precompute.ObservationValue{Kind: precompute.KindFloat, Float: 42}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if got := w.Quantile(0.5); abs(got-42) > 1 {
		t.Fatalf("post-observe quantile: %f", got)
	}
	if err := (DDSketchObserver{}).Observe(w, precompute.ObservationValue{Kind: precompute.KindHash}); err == nil {
		t.Fatal("expected error for unsupported kind")
	}
}

func TestDDSketchObserver_RowSampledAppliesAdmissionOnce(t *testing.T) {
	w := NewDDSketchWrapper(0.01).WithSampleP(0.01)
	v := precompute.FloatValue(42)
	v.RowSampled = true
	v.AdmittedRows = 1
	v.SampleP = 0.25
	if err := (DDSketchObserver{}).Observe(w, v); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if got := w.Quantile(0.5); abs(got-42) > 1 {
		t.Fatalf("externally admitted value was sampled a second time: quantile=%v", got)
	}
	if got := w.SampleP(); got != 0.25 {
		t.Fatalf("SampleP = %v, want 0.25", got)
	}
	snapshot, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var envelope envpb.SketchEnvelope
	if err := proto.Unmarshal(snapshot, &envelope); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if envelope.SampleP != 0.25 {
		t.Fatalf("wire sample_p = %v, want 0.25", envelope.SampleP)
	}
	v.SampleP = 0.5
	if err := (DDSketchObserver{}).Observe(w, v); err == nil {
		t.Fatal("expected a mid-window sample_p change to fail")
	}

	v.AdmittedRows = 3
	v.SampleP = 0.25
	if err := (DDSketchObserver{}).Observe(w, v); err == nil {
		t.Fatal("expected invalid one-row admission mask to fail")
	}
}

// TestDDSketch_PerWindowEmptyBase_DeltaAgainstEmptyRoundTrips drives the
// precompute.SnapshotCache delta path (per-window empty-base model,
// delta-baseline-contract.md): window 1 emits a full frame; window 2 — a fresh
// per-window sketch — emits a DELTA computed against the empty base the cache
// stored at window-1 close. Applying that delta to an EMPTY base reconstructs
// the window's state within the alpha relative-accuracy bound.
//
// Window 2 uses SPARSE values (buckets far apart) so the sparse indexed delta
// is strictly smaller than the contiguous positional full frame and the
// min(full, delta) clamp keeps it a delta. (Dense data clamps to full — see
// TestDDSketch_PerWindowEmptyBase_NeverExceedsFull.)
func TestDDSketch_PerWindowEmptyBase_DeltaAgainstEmptyRoundTrips(t *testing.T) {
	t.Parallel()
	const alpha = 0.01
	c := precompute.NewSnapshotCache()
	sparse := []float64{1, 1000, 1_000_000}

	w1 := NewDDSketchWrapper(alpha)
	for _, v := range sparse {
		w1.Update(v)
	}
	_, isFull, err := c.ComputeDelta("series", w1, 1)
	if err != nil {
		t.Fatalf("w1: %v", err)
	}
	if !isFull {
		t.Fatal("window 1 must emit a full frame")
	}

	w2 := NewDDSketchWrapper(alpha)
	for _, v := range sparse {
		w2.Update(v)
	}
	payload, isFull, err := c.ComputeDelta("series", w2, 1)
	if err != nil {
		t.Fatalf("w2: %v", err)
	}
	if isFull {
		t.Fatal("window 2 (sparse) must emit a delta, not a full frame")
	}
	if len(payload) == 0 {
		t.Fatal("window 2 delta payload empty")
	}

	recon := NewDDSketchWrapper(alpha)
	if err := recon.ApplyDelta(payload); err != nil {
		t.Fatalf("apply delta: %v", err)
	}
	for _, q := range []float64{0.25, 0.5, 0.9} {
		want := w2.Quantile(q)
		got := recon.Quantile(q)
		if rel := abs(got-want) / want; rel > 0.011 {
			t.Fatalf("q=%v: want %f got %f rel %f", q, want, got, rel)
		}
	}
}

// TestDDSketch_PerWindowEmptyBase_NoCrossWindowSubtraction verifies two
// consecutive windows each emit their OWN state — window 2 is never diffed
// against window 1. Window 1 inserts each value 5×, window 2 inserts the SAME
// values 1×. A cross-window diff would yield negative bucket counts that
// reconstruct to garbage; the per-window empty-base path always diffs against a
// fresh empty base, so the emit reconstructs window 2 exactly. Sparse values
// keep it on the delta path.
func TestDDSketch_PerWindowEmptyBase_NoCrossWindowSubtraction(t *testing.T) {
	t.Parallel()
	const alpha = 0.01
	c := precompute.NewSnapshotCache()
	sparse := []float64{2, 5000, 2_000_000}

	w1 := NewDDSketchWrapper(alpha)
	for _, v := range sparse {
		for k := 0; k < 5; k++ {
			w1.Update(v)
		}
	}
	if _, isFull, err := c.ComputeDelta("s", w1, 1); err != nil || !isFull {
		t.Fatalf("w1: full=%v err=%v", isFull, err)
	}

	w2 := NewDDSketchWrapper(alpha)
	for _, v := range sparse {
		w2.Update(v)
	}
	payload, isFull, err := c.ComputeDelta("s", w2, 1)
	if err != nil {
		t.Fatalf("w2: %v", err)
	}
	if isFull {
		t.Fatal("window 2 (sparse) must emit a delta")
	}
	if len(payload) == 0 {
		t.Fatal("window 2 delta empty — cross-window subtraction leaked")
	}

	recon := NewDDSketchWrapper(alpha)
	if err := recon.ApplyDelta(payload); err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, q := range []float64{0.25, 0.5, 0.9} {
		want := w2.Quantile(q)
		got := recon.Quantile(q)
		if rel := abs(got-want) / want; rel > 0.011 {
			t.Fatalf("q=%v: want %f got %f rel %f", q, want, got, rel)
		}
	}
}

// TestDDSketch_PerWindowEmptyBase_NeverExceedsFull verifies the min(full,delta)
// bandwidth invariant: for dense contiguous data the emitted per-window frame
// is never larger than the equivalent full frame. The DDSketch full state is a
// positional count array (no per-bucket index), so a sparse indexed delta can
// be larger; the clamp emits the full frame in that case. Either way the
// emitted payload reconstructs the window on a fresh base.
func TestDDSketch_PerWindowEmptyBase_NeverExceedsFull(t *testing.T) {
	t.Parallel()
	const alpha = 0.01
	c := precompute.NewSnapshotCache()

	w1 := NewDDSketchWrapper(alpha)
	for i := 1; i <= 200; i++ {
		w1.Update(float64(i))
	}
	if _, isFull, err := c.ComputeDelta("d", w1, 1); err != nil || !isFull {
		t.Fatalf("w1: full=%v err=%v", isFull, err)
	}

	w2 := NewDDSketchWrapper(alpha)
	for i := 1; i <= 200; i++ {
		w2.Update(float64(i))
	}
	full, err := w2.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	payload, _, err := c.ComputeDelta("d", w2, 1)
	if err != nil {
		t.Fatalf("w2: %v", err)
	}
	if len(payload) > len(full) {
		t.Fatalf("invariant violated: payload %d > full %d bytes", len(payload), len(full))
	}
	recon := NewDDSketchWrapper(alpha)
	if err := recon.ApplyDelta(payload); err != nil {
		t.Fatalf("apply: %v", err)
	}
	want := w2.Quantile(0.5)
	got := recon.Quantile(0.5)
	if rel := abs(got-want) / want; rel > 0.011 {
		t.Fatalf("median: want %f got %f rel %f", want, got, rel)
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
