// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"testing"

	"github.com/ProjectASAP/sketchlib-go/common"
)

// ApplyAdmittedOccurrence must plumb straight through to the sketchlib
// primitive: admitting all rows at p=1 must equal the plain insert path
// (UpdateString / InsertHash), and a zero bitmask must be a true no-op.
func TestCountSketchWrapper_ApplyAdmittedOccurrence(t *testing.T) {
	t.Parallel()
	w, err := NewCountSketchWrapper(5, 1024)
	if err != nil {
		t.Fatalf("NewCountSketchWrapper: %v", err)
	}
	allRows := uint64(0b11111)
	for i := 0; i < 500; i++ {
		w.ApplyAdmittedOccurrence("hot", 1.0, allRows, 1.0)
	}
	if got := w.EstimateCount([]byte("hot")); got < 490 || got > 510 {
		t.Fatalf("estimate = %v, want ~500 (all rows admitted at p=1)", got)
	}

	w2, _ := NewCountSketchWrapper(5, 1024)
	w2.ApplyAdmittedOccurrence("x", 3.0, 0, 0.3)
	if got := w2.EstimateCount([]byte("x")); got != 0 {
		t.Fatalf("zero admittedRows must be a no-op, got estimate %v", got)
	}
}

func TestCMSWrapper_ApplyAdmittedOccurrence(t *testing.T) {
	t.Parallel()
	w := NewCMSWrapper(5, 1024, false)
	hash := common.FromBytes([]byte("hot")).Hash
	allRows := uint64(0b11111)
	for i := 0; i < 500; i++ {
		w.ApplyAdmittedOccurrence(hash, 1.0, allRows, 1.0)
	}
	if got := w.EstimateCount([]byte("hot")); got < 490 || got > 510 {
		t.Fatalf("estimate = %v, want ~500 (all rows admitted at p=1)", got)
	}

	w2 := NewCMSWrapper(5, 1024, false)
	w2.ApplyAdmittedOccurrence(common.FromBytes([]byte("x")).Hash, 3.0, 0, 0.3)
	if got := w2.EstimateCount([]byte("x")); got != 0 {
		t.Fatalf("zero admittedRows must be a no-op, got estimate %v", got)
	}
}

// A fractional p correctly rescales — admitting all rows at p=0.5 must
// estimate roughly double the raw occurrence count (the 1/p correction).
func TestCountSketchWrapper_ApplyAdmittedOccurrence_RescalesByP(t *testing.T) {
	t.Parallel()
	w, err := NewCountSketchWrapper(5, 2048)
	if err != nil {
		t.Fatalf("NewCountSketchWrapper: %v", err)
	}
	allRows := uint64(0b11111)
	const n, p = 1000, 0.5
	for i := 0; i < n; i++ {
		w.ApplyAdmittedOccurrence("hot", 1.0, allRows, p)
	}
	want := float64(n) / p
	got := w.EstimateCount([]byte("hot"))
	rel := (got - want) / want
	if rel < -0.1 || rel > 0.1 {
		t.Fatalf("estimate = %v, want ~%v (n/p, rel_err %v exceeds 10%%)", got, want, rel)
	}
}
