// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"
	"time"

	"github.com/ProjectASAP/sketchlib-go/common"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// TestHLLWrapper_FullStateApplyDelta is the regression test for the P0
// bug where HLLWrapper.ApplyDelta tried the sparse RegisterDelta decode
// FIRST. A full-state HyperLogLogState envelope decodes into the delta
// message as an EMPTY delta (proto3 tolerates the unknown fields), so a
// full-state envelope used to silently merge to cardinality 0. The fix
// attempts the full-state decode first.
func TestHLLWrapper_FullStateApplyDelta(t *testing.T) {
	t.Parallel()

	src := NewHLLWrapper()
	const n = 1000
	for i := 0; i < n; i++ {
		src.UpdateValue(float64(i))
	}
	if est := src.Estimate(); est < 900 || est > 1100 {
		t.Fatalf("source estimate %d not within ~10%% of %d", est, n)
	}

	// Full-state proto bytes (PROTO_FULL envelope payload).
	full, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(full) == 0 {
		t.Fatal("empty full-state snapshot")
	}

	// Apply the full state into a FRESH wrapper (the merge-from-empty
	// path the runtime's mergeFullEnvelope uses).
	dst := NewHLLWrapper()
	if err := dst.ApplyDelta(full); err != nil {
		t.Fatalf("ApplyDelta(full): %v", err)
	}
	if est := dst.Estimate(); est < 900 || est > 1100 {
		t.Fatalf("BUG: full-state merged to estimate %d, want ~%d (the pre-fix bug produced 0)", est, n)
	}
}

// TestCountSketchWrapper_FullStateApplyDelta is the regression test for
// the P0 bug where CountSketchWrapper.ApplyDelta had no full-state
// branch at all: a full-state envelope would fail / decode to an empty
// delta and silently merge to an estimate of 0. The fix adds a
// full-state proto decode first.
func TestCountSketchWrapper_FullStateApplyDelta(t *testing.T) {
	t.Parallel()

	src, err := NewCountSketchWrapper(5, 1024)
	if err != nil {
		t.Fatalf("NewCountSketchWrapper: %v", err)
	}
	const key = "hot-key"
	const occurrences = 100
	for i := 0; i < occurrences; i++ {
		src.UpdateString(key, 1)
	}
	if est := src.EstimateCount([]byte(key)); est < 90 || est > 110 {
		t.Fatalf("source estimate %v not within ~10%% of %d", est, occurrences)
	}

	full, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(full) == 0 {
		t.Fatal("empty full-state snapshot")
	}

	dst, err := NewCountSketchWrapper(5, 1024)
	if err != nil {
		t.Fatalf("NewCountSketchWrapper(dst): %v", err)
	}
	if err := dst.ApplyDelta(full); err != nil {
		t.Fatalf("ApplyDelta(full): %v", err)
	}
	if est := dst.EstimateCount([]byte(key)); est < 90 || est > 110 {
		t.Fatalf("BUG: full-state merged to estimate %v, want ~%d (the pre-fix bug produced 0)", est, occurrences)
	}
}

// TestHLLWrapper_FullStateViaObserveEnvelope drives the full-state merge
// through the REAL runtime ObserveEnvelope -> mergeFullEnvelope ->
// ApplyDelta path with the real HLL wrapper, then Drain()s and confirms
// the emitted envelope round-trips back to ~1000 cardinality.
func TestHLLWrapper_FullStateViaObserveEnvelope(t *testing.T) {
	t.Parallel()

	src := NewHLLWrapper()
	const n = 1000
	for i := 0; i < n; i++ {
		src.UpdateValue(float64(i))
	}
	full, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	cfg := &precompute.PrecomputeConfig{
		AggID:      1,
		SketchType: precompute.SketchTypeHLLSketch,
		Mode:       precompute.Tumbling,
		Window:     precompute.WindowSpec{Size: 60 * time.Second},
	}
	p := precompute.New(cfg,
		func() precompute.Sketch { return NewHLLWrapper() },
		HLLObserver{},
	)

	env := &precompute.SketchEnvelope{
		SketchType:    precompute.SketchTypeHLLSketch,
		AggID:         1,
		Encoding:      precompute.EncodingProtoFull,
		Payload:       full,
		WindowStartMs: 1000,
		WindowEndMs:   2000,
	}
	if err := p.ObserveEnvelope(env); err != nil {
		t.Fatalf("ObserveEnvelope: %v", err)
	}

	out := p.Drain()
	if len(out) != 1 {
		t.Fatalf("Drain returned %d envelopes, want 1", len(out))
	}
	// The emitted payload must itself decode back to ~1000 distinct.
	check := NewHLLWrapper()
	if err := check.ApplyDelta(out[0].Payload); err != nil {
		t.Fatalf("ApplyDelta(emitted): %v", err)
	}
	if est := check.Estimate(); est < 900 || est > 1100 {
		t.Fatalf("BUG: round-trip through ObserveEnvelope gave estimate %d, want ~%d", est, n)
	}

	// Telemetry wiring (P2 fix 9): InputEnvelopes + LastEmittedEnvelopes.
	stats := p.Stats().Snapshot()
	if stats.InputEnvelopes != 1 {
		t.Fatalf("InputEnvelopes = %d, want 1", stats.InputEnvelopes)
	}
	if stats.LastEmittedEnvelopes != 1 {
		t.Fatalf("LastEmittedEnvelopes = %d, want 1", stats.LastEmittedEnvelopes)
	}
}

// TestCountSketchWrapper_FullStateViaObserveEnvelope drives the
// CountSketch full-state merge through the runtime ObserveEnvelope path.
func TestCountSketchWrapper_FullStateViaObserveEnvelope(t *testing.T) {
	t.Parallel()

	const rows, cols = 5, 1024
	src, err := NewCountSketchWrapper(rows, cols)
	if err != nil {
		t.Fatalf("NewCountSketchWrapper: %v", err)
	}
	const key = "hot-key"
	const occurrences = 100
	for i := 0; i < occurrences; i++ {
		src.UpdateString(key, 1)
	}
	full, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	cfg := &precompute.PrecomputeConfig{
		AggID:      1,
		SketchType: precompute.SketchTypeCountSketch,
		Mode:       precompute.Tumbling,
		Window:     precompute.WindowSpec{Size: 60 * time.Second},
	}
	p := precompute.New(cfg,
		func() precompute.Sketch {
			w, _ := NewCountSketchWrapper(rows, cols)
			return w
		},
		CountSketchObserver{DefaultKey: key},
	)

	env := &precompute.SketchEnvelope{
		SketchType:    precompute.SketchTypeCountSketch,
		AggID:         1,
		Encoding:      precompute.EncodingProtoFull,
		Payload:       full,
		WindowStartMs: 1000,
		WindowEndMs:   2000,
	}
	if err := p.ObserveEnvelope(env); err != nil {
		t.Fatalf("ObserveEnvelope: %v", err)
	}

	out := p.Drain()
	if len(out) != 1 {
		t.Fatalf("Drain returned %d envelopes, want 1", len(out))
	}
	check, err := NewCountSketchWrapper(rows, cols)
	if err != nil {
		t.Fatalf("NewCountSketchWrapper(check): %v", err)
	}
	if err := check.ApplyDelta(out[0].Payload); err != nil {
		t.Fatalf("ApplyDelta(emitted): %v", err)
	}
	if est := check.EstimateCount([]byte(key)); est < 90 || est > 110 {
		t.Fatalf("BUG: round-trip through ObserveEnvelope gave estimate %v, want ~%d", est, occurrences)
	}
}

// TestNewCMSWrapper_DimensionNormalization covers fixes 4 & 5: non-pow2
// cols is rounded up (no panic on query) and rows is clamped so the
// per-row hash slicing never overflows 64 bits.
func TestNewCMSWrapper_DimensionNormalization(t *testing.T) {
	t.Parallel()

	// Non-power-of-two cols: must not panic on insert+query and must
	// round up to a power of two (1000 -> 1024).
	w := NewCMSWrapper(4, 1000, false)
	if got := w.cols; got != 1024 {
		t.Fatalf("cols not rounded up: got %d, want 1024", got)
	}
	const key = "abc"
	// Insert via the same hash EstimateCount queries with (common.FromBytes)
	// so the count agrees regardless of column folding.
	keyHash := common.FromBytes([]byte(key)).Hash
	for i := 0; i < 50; i++ {
		w.InsertHash(keyHash)
	}
	if est := w.EstimateCount([]byte(key)); est < 45 || est > 80 {
		t.Fatalf("CMS estimate after 50 inserts looks wrong: %v", est)
	}

	// Excessive rows: 1024 cols => 10 bits/row; 10 rows*10 = 100 > 64,
	// so rows must be clamped to floor(64/10) = 6.
	clamped := NewCMSWrapper(10, 1024, false)
	if clamped.rows != 6 {
		t.Fatalf("rows not clamped for hash budget: got %d, want 6", clamped.rows)
	}
}

// TestNewCountSketchWrapper_RowHashBudget covers fix 5 for CountSketch:
// dimensions whose per-row hash slices overflow 64 bits are rejected.
func TestNewCountSketchWrapper_RowHashBudget(t *testing.T) {
	t.Parallel()

	// 1024 cols => 10 bits/row; 7 rows*10 = 70 > 64 must be rejected.
	if _, err := NewCountSketchWrapper(7, 1024); err == nil {
		t.Fatal("expected error for rows*bits exceeding 64-bit budget, got nil")
	}
	// A fitting configuration still succeeds.
	if _, err := NewCountSketchWrapper(6, 1024); err != nil {
		t.Fatalf("unexpected error for fitting dims: %v", err)
	}
}
