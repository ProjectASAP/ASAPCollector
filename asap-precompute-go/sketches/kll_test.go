// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	"google.golang.org/protobuf/proto"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// decodeKLLState unmarshals a KLLWrapper.Snapshot payload back into its
// portable KLLState so tests can assert the on-the-wire fields.
func decodeKLLState(t *testing.T, payload []byte) *envpb.SketchEnvelope {
	t.Helper()
	var env envpb.SketchEnvelope
	if err := proto.Unmarshal(payload, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.GetKll() == nil {
		t.Fatalf("envelope carries no KLLState")
	}
	return &env
}

// TestKLLWrapper_SnapshotCarriesConfiguredKRawF64 asserts the BUG-1 fix: a
// populated KLL serializes the configured k (never 0) AND uses the RAW-F64
// items[] wire form (residuals empty) even for exact-integer samples — the
// only form the ASAPQuery backend's KLL decoder reconstructs correctly. The
// value-offset fixed-point form (items empty, residuals set) is what produced
// the `KllState.k must be >= 8 (got 0)` backend errors for integer-valued
// metrics like http_requests_total_latency_ms.
func TestKLLWrapper_SnapshotCarriesConfiguredKRawF64(t *testing.T) {
	t.Parallel()
	w := NewKLLWrapper(200, nil)
	// Exact-integer samples — the latency-metric value model that always trips
	// the fixed-point encoder under SerializePortable.
	for i := 0; i < 100; i++ {
		w.Update(float64(i%500 + 1))
	}
	payload, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if payload == nil {
		t.Fatal("populated KLL emitted nil payload")
	}
	st := decodeKLLState(t, payload).GetKll()
	if st.GetK() != 200 {
		t.Fatalf("serialized K=%d, want 200", st.GetK())
	}
	if len(st.GetResiduals()) != 0 {
		t.Fatalf("snapshot used fixed-point residuals (%d) — backend cannot decode; want raw items[]", len(st.GetResiduals()))
	}
	if len(st.GetItems()) == 0 {
		t.Fatal("snapshot carried no raw items[]")
	}
}

// TestKLLWrapper_EmptyWindowEmitsNothing asserts an empty (never-updated) KLL
// window produces a nil payload, so no degenerate k=0 frame is ever emitted.
func TestKLLWrapper_EmptyWindowEmitsNothing(t *testing.T) {
	t.Parallel()
	w := NewKLLWrapper(200, nil)
	payload, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if payload != nil {
		t.Fatalf("empty KLL window emitted a %d-byte payload, want nil", len(payload))
	}
}

// TestKLLWrapper_DeterministicSeed verifies that two KLL wrappers
// constructed with the same (k, seed) and fed the same input produce
// byte-identical Snapshots — the parity-harness invariant the OTel
// shim relies on.
func TestKLLWrapper_DeterministicSeed(t *testing.T) {
	t.Parallel()
	seed := int64(42)
	a := NewKLLWrapper(200, &seed)
	b := NewKLLWrapper(200, &seed)
	for _, v := range []float64{1, 5, 9, 4, 8, 2, 7, 6, 3, 10} {
		a.Update(v)
		b.Update(v)
	}
	sa, err := a.Snapshot()
	if err != nil {
		t.Fatalf("a.Snapshot: %v", err)
	}
	sb, err := b.Snapshot()
	if err != nil {
		t.Fatalf("b.Snapshot: %v", err)
	}
	if string(sa) != string(sb) {
		t.Fatalf("seeded snapshots differ (%d vs %d bytes)", len(sa), len(sb))
	}
}

// TestKLLWrapper_ComputeDeltaAlwaysFull confirms the KLL wrapper
// honors its design contract: KLL is not additively mergeable, so
// ComputeDeltaAgainst always returns isFull=true regardless of input.
func TestKLLWrapper_ComputeDeltaAlwaysFull(t *testing.T) {
	t.Parallel()
	seed := int64(1)
	w := NewKLLWrapper(200, &seed)
	for i := 0; i < 100; i++ {
		w.Update(float64(i))
	}
	_, isFull, err := w.ComputeDeltaAgainst([]byte("anything"), 1024)
	if err != nil {
		t.Fatalf("delta: %v", err)
	}
	if !isFull {
		t.Fatal("KLL ComputeDeltaAgainst must always report isFull=true")
	}
}

// TestKLLObserver verifies the KLL SketchObserver hot path.
func TestKLLObserver(t *testing.T) {
	t.Parallel()
	seed := int64(7)
	w := NewKLLWrapper(200, &seed)
	if err := (KLLObserver{}).Observe(w, precompute.ObservationValue{Kind: precompute.KindFloat, Float: 99.0}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if w.Count() != 1 {
		t.Fatalf("count after one observe: %d", w.Count())
	}
}

// TestKLLWrapper_ApplyDeltaFromSnapshot confirms a Snapshot can be
// re-loaded into an empty wrapper via ApplyDelta — the inbound
// envelope merge path the runtime uses.
func TestKLLWrapper_ApplyDeltaFromSnapshot(t *testing.T) {
	t.Parallel()
	seed := int64(13)
	src := NewKLLWrapper(200, &seed)
	for i := 0; i < 200; i++ {
		src.Update(float64(i))
	}
	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("snap: %v", err)
	}
	dst := NewKLLWrapper(200, &seed)
	if err := dst.ApplyDelta(snap); err != nil {
		t.Fatalf("ApplyDelta: %v", err)
	}
	if dst.Count() == 0 {
		t.Fatal("dst empty after ApplyDelta")
	}
}
