// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package sketches

import (
	"bytes"
	"math"
	"runtime"
	"testing"
)

// hllStdErrTolerance returns a generous absolute tolerance for an HLL estimate
// at the fixed precision (1.04/sqrt(m) relative error, scaled by the true
// count, with a 4x margin so the assertion is not flaky).
func hllStdErrTolerance(trueCount float64) float64 {
	m := float64(uint64(1) << 14) // hll.HLLPrecision = 14
	relErr := 1.04 / math.Sqrt(m)
	return 4 * relErr * trueCount
}

// TestNewHLLWrapperSparse_Construct verifies the sparse constructor builds a
// usable, empty wrapper.
func TestNewHLLWrapperSparse_Construct(t *testing.T) {
	t.Parallel()
	w := NewHLLWrapperSparse()
	if w == nil {
		t.Fatal("NewHLLWrapperSparse returned nil")
	}
	if got := w.EstimateCardinality(); got > 1.0 {
		t.Fatalf("fresh sparse wrapper estimate = %v, want ~0", got)
	}
	// SampleP defaults to disabled (1.0), same as the dense constructor.
	if got := w.SampleP(); got != 1.0 {
		t.Fatalf("SampleP = %v, want 1.0", got)
	}
}

// TestHLLWrapperSparse_EstimateMatchesDense feeds the same distinct values into
// a sparse and a dense wrapper and asserts the cardinality estimates agree. The
// sparse base is API-compatible, so the estimates are identical for identical
// inputs (across the sparse->dense promotion threshold).
func TestHLLWrapperSparse_EstimateMatchesDense(t *testing.T) {
	t.Parallel()
	const n = 8000 // above the sparse->dense promotion threshold to exercise both regimes
	dense := NewHLLWrapper()
	sparse := NewHLLWrapperSparse()
	for i := 0; i < n; i++ {
		v := float64(i)
		dense.UpdateValue(v)
		sparse.UpdateValue(v)
	}

	de := dense.EstimateCardinality()
	se := sparse.EstimateCardinality()
	if de != se {
		t.Errorf("sparse estimate %v != dense estimate %v (must be identical for identical inputs)", se, de)
	}
	if tol := hllStdErrTolerance(n); math.Abs(se-float64(n)) > tol {
		t.Errorf("sparse estimate %v not within %v of true %d", se, tol, n)
	}
}

// TestHLLWrapperSparse_LowCardinalityEstimate checks accuracy while still in the
// sparse regime (few distinct values, below the promotion threshold).
func TestHLLWrapperSparse_LowCardinalityEstimate(t *testing.T) {
	t.Parallel()
	const n = 200
	sparse := NewHLLWrapperSparse()
	for i := 0; i < n; i++ {
		sparse.UpdateValue(float64(i))
	}
	se := sparse.EstimateCardinality()
	if tol := hllStdErrTolerance(n); math.Abs(se-float64(n)) > tol {
		t.Errorf("low-cardinality sparse estimate %v not within %v of true %d", se, tol, n)
	}
}

// TestHLLWrapperSparse_SnapshotByteIdentical asserts that for the same inputs
// the sparse wrapper's Snapshot bytes are byte-identical to the dense wrapper's,
// as guaranteed by the API-compatible serialization design.
func TestHLLWrapperSparse_SnapshotByteIdentical(t *testing.T) {
	t.Parallel()
	for _, n := range []int{0, 50, 2000, 8000} {
		dense := NewHLLWrapper()
		sparse := NewHLLWrapperSparse()
		for i := 0; i < n; i++ {
			dense.UpdateValue(float64(i))
			sparse.UpdateValue(float64(i))
		}
		db, err := dense.Snapshot()
		if err != nil {
			t.Fatalf("n=%d: dense Snapshot: %v", n, err)
		}
		sb, err := sparse.Snapshot()
		if err != nil {
			t.Fatalf("n=%d: sparse Snapshot: %v", n, err)
		}
		if !bytes.Equal(db, sb) {
			t.Errorf("n=%d: sparse snapshot (%d bytes) not byte-identical to dense (%d bytes)", n, len(sb), len(db))
		}
	}
}

// TestHLLWrapperSparse_MergeInteropWithDense verifies a dense snapshot merges
// into a sparse wrapper (via ApplyDelta) and yields the union cardinality, and
// that a sparse wrapper Merges into a dense one (same-type Merge path).
func TestHLLWrapperSparse_MergeInteropWithDense(t *testing.T) {
	t.Parallel()
	dense := NewHLLWrapper()
	sparse := NewHLLWrapperSparse()
	for i := 0; i < 1000; i++ {
		dense.UpdateValue(float64(i))
	}
	for i := 500; i < 1500; i++ {
		sparse.UpdateValue(float64(i))
	}

	// ApplyDelta a dense full snapshot into the sparse wrapper; union is [0,1500).
	denseSnap, err := dense.Snapshot()
	if err != nil {
		t.Fatalf("dense Snapshot: %v", err)
	}
	if err := sparse.ApplyDelta(denseSnap); err != nil {
		t.Fatalf("sparse ApplyDelta(dense snapshot): %v", err)
	}
	if got, tol := sparse.EstimateCardinality(), hllStdErrTolerance(1500); math.Abs(got-1500) > tol {
		t.Errorf("merged sparse estimate %v not within %v of 1500", got, tol)
	}

	// Merge a sparse wrapper into a dense one.
	dense2 := NewHLLWrapper()
	for i := 0; i < 1000; i++ {
		dense2.UpdateValue(float64(i))
	}
	sparse2 := NewHLLWrapperSparse()
	for i := 500; i < 1500; i++ {
		sparse2.UpdateValue(float64(i))
	}
	if err := dense2.Merge(sparse2); err != nil {
		t.Fatalf("dense2.Merge(sparse2): %v", err)
	}
	if got, tol := dense2.EstimateCardinality(), hllStdErrTolerance(1500); math.Abs(got-1500) > tol {
		t.Errorf("dense+sparse merged estimate %v not within %v of 1500", got, tol)
	}
}

// TestHLLWrapperSparse_Reset returns the sketch to empty and keeps it usable.
func TestHLLWrapperSparse_Reset(t *testing.T) {
	t.Parallel()
	w := NewHLLWrapperSparse()
	for i := 0; i < 1000; i++ {
		w.UpdateValue(float64(i))
	}
	if w.EstimateCardinality() < 100 {
		t.Fatalf("pre-reset estimate too low: %v", w.EstimateCardinality())
	}
	w.Reset()
	if got := w.EstimateCardinality(); got > 1.0 {
		t.Fatalf("post-reset estimate = %v, want ~0", got)
	}
	// Still usable after Reset.
	w.UpdateValue(42)
	if got := w.EstimateCardinality(); got <= 0 {
		t.Fatalf("post-reset-and-add estimate = %v, want > 0", got)
	}
}

// TestHLLWrapperSparse_LowCardinalityMemory is a coarse heap check: a large
// population of sparse wrappers each holding a handful of distinct values should
// use far less heap than the same number of dense wrappers, which each eagerly
// allocate the full 1<<14 register array (~16KB/series). Generous threshold to
// stay non-flaky.
func TestHLLWrapperSparse_LowCardinalityMemory(t *testing.T) {
	const series = 2000
	const distinctPerSeries = 4

	measure := func(sparse bool) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)

		holders := make([]*HLLWrapper, series)
		for i := 0; i < series; i++ {
			var w *HLLWrapper
			if sparse {
				w = NewHLLWrapperSparse()
			} else {
				w = NewHLLWrapper()
			}
			for j := 0; j < distinctPerSeries; j++ {
				w.UpdateValue(float64(i*100000 + j))
			}
			holders[i] = w
		}

		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(holders)
		if after.HeapInuse < before.HeapInuse {
			return 0
		}
		return after.HeapInuse - before.HeapInuse
	}

	denseHeap := measure(false)
	sparseHeap := measure(true)
	t.Logf("dense heap=%d B (%d B/series), sparse heap=%d B (%d B/series)",
		denseHeap, denseHeap/series, sparseHeap, sparseHeap/series)

	// Dense must be near the 1<<14 bytes/series floor.
	if want := uint64(series) * uint64(1<<14) / 2; denseHeap < want {
		t.Errorf("dense heap %d below expected ~16KB/series floor (%d)", denseHeap, want)
	}
	// Sparse at this cardinality must be dramatically smaller — assert >=4x.
	if sparseHeap >= denseHeap/4 {
		t.Errorf("sparse heap %d not far below dense %d (want < dense/4)", sparseHeap, denseHeap)
	}
}
