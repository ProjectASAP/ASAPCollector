// Tests taken from Precompute Engine https://github.com/approx-telemetry/PrecomputeEngine/blob/main/aggregators/KLL/kll_test.go
package kll

import (
	"math"
	"sort"
	"testing"
)

// Helper: build a sorted float slice [start..end] inclusive.
func seq(start, end int) []float64 {
	n := end - start + 1
	out := make([]float64, 0, n)
	for i := start; i <= end; i++ {
		out = append(out, float64(i))
	}
	return out
}

// Insert values into a sketch.
func insertAll(s *Sketch, vals []float64) {
	for _, v := range vals {
		s.Update(v)
	}
}

// With sufficiently large k and small n, the sketch will not compact,
// so results should be exact for Rank/Quantile/Count and the CDF mapping.
func TestKLL_NoCompaction_Exactness(t *testing.T) {
	k := 256
	vals := seq(1, 20) // n=20 << capacity(0) for most reasonable computeHeight
	s := New(k)
	insertAll(s, vals)

	// Count is exact when no compaction occurs.
	if got := s.Count(); got != len(vals) {
		t.Fatalf("Count() mismatch: got=%d want=%d", got, len(vals))
	}

	// Rank and Quantile should align exactly on midpoints.
	if got := s.Rank(10.5); got != 10 {
		t.Fatalf("Rank(10.5)=%d want=10", got)
	}
	if got := s.Quantile(10.5); math.Abs(got-0.5) > 1e-9 {
		t.Fatalf("Quantile(10.5)=%.6f want=0.5", got)
	}

	// CDF should map 0.5 back to ~10 exactly (discrete).
	cdf := s.CDF()
	if got := cdf.Query(0.5); got != 10 {
		t.Fatalf("CDF.Query(0.5)=%.6f want=10", got)
	}

	// Endpoints
	if q0 := cdf.Query(0.0); q0 != 1 {
		t.Fatalf("CDF.Query(0.0)=%.6f want=1 (min value)", q0)
	}
	if q1 := cdf.Query(1.0); q1 != 20 {
		t.Fatalf("CDF.Query(1.0)=%.6f want=20 (max value)", q1)
	}
}

func TestKLL_CDF_Monotonicity_And_Inverses(t *testing.T) {
	k := 512
	vals := append(seq(1, 50), seq(100, 110)...) // gaps, still no compaction
	s := New(k)
	insertAll(s, vals)
	cdf := s.CDF()

	// Monotonicity by value and cumulative probability.
	for i := 1; i < len(cdf); i++ {
		if !(cdf[i].V >= cdf[i-1].V) {
			t.Fatalf("CDF not non-decreasing by value at i=%d: %v !>= %v", i, cdf[i].V, cdf[i-1].V)
		}
		if !(cdf[i].Q >= cdf[i-1].Q) {
			t.Fatalf("CDF not non-decreasing by Q at i=%d: %v !>= %v", i, cdf[i].Q, cdf[i-1].Q)
		}
	}

	// Inverse checks (stepwise-safe): for x in the support,
	// let q = CDF.Quantile(x) be the left-closed CDF; then
	// Query(q+ε) must be >= x for any tiny ε>0.
	points := []float64{
		vals[len(vals)/4],
		vals[len(vals)/2],
		vals[3*len(vals)/4],
	}
	const eps = 1e-12
	for _, x := range points {
		q := cdf.Quantile(x)
		vRight := cdf.Query(math.Min(q+eps, 1.0))
		if vRight < x {
			t.Fatalf("Inverse sandwich fails at x=%v: Query(Quantile(x)+eps)=%v < x", x, vRight)
		}
		// Optional additional check: x should not be greater than the next step’s inverse either.
		// (i.e., x is bracketed by the step to the right)
		if q > 0 {
			vLeft := cdf.Query(math.Max(q-eps, 0.0))
			// vLeft can be <= x (often strictly < x). This should always hold:
			if vLeft > vRight {
				t.Fatalf("Query left/right ordering broken at x=%v: %v > %v", x, vLeft, vRight)
			}
		}
	}
}

// Merge two sketches built on disjoint ranges (no compaction) and verify exactness.
func TestKLL_Merge_Disjoint_NoCompaction(t *testing.T) {
	k := 256
	s1 := New(k)
	s2 := New(k)
	insertAll(s1, seq(1, 50))
	insertAll(s2, seq(51, 100))

	// Before merge sanity
	if s1.Count() != 50 || s2.Count() != 50 {
		t.Fatalf("pre-merge counts wrong: s1=%d s2=%d", s1.Count(), s2.Count())
	}

	// Merge s2 into s1
	s1.Merge(s2)

	// Total count should be 100
	if s1.Count() != 100 {
		t.Fatalf("post-merge Count()=%d want=100", s1.Count())
	}

	// Rank/Quantile exact on uniform 1..100
	if s1.Rank(50) != 50 {
		t.Fatalf("Rank(50)=%d want=50", s1.Rank(50))
	}
	if math.Abs(s1.Quantile(50)-0.5) > 1e-9 {
		t.Fatalf("Quantile(50)=%.6f want=0.5", s1.Quantile(50))
	}

	// CDF Query: with uniform integers, Q(V)=V/100
	cdf := s1.CDF()
	if q95 := cdf.Query(0.95); q95 != 95 {
		t.Fatalf("CDF.Query(0.95)=%.6f want=95", q95)
	}
	if q05 := cdf.Query(0.05); q05 != 5 {
		t.Fatalf("CDF.Query(0.05)=%.6f want=5", q05)
	}
}

// Linear interpolation behavior at midpoints.
// We create a small sketch with evenly spaced values so LI is easy to verify.
func TestKLL_LinearInterpolation(t *testing.T) {
	k := 128
	vals := []float64{0, 10, 20, 30}
	s := New(k)
	insertAll(s, vals)
	cdf := s.CDF()

	// With four items, cumulative Q are 0.25, 0.5, 0.75, 1.0
	// QuantileLI at x=5 should be halfway between 0.25 and 0.5 => 0.375
	if got := cdf.QuantileLI(5); math.Abs(got-0.375) > 1e-9 {
		t.Fatalf("QuantileLI(5)=%.6f want=0.375", got)
	}

	// QueryLI at p=0.6 should be between 10 and 20, closer to 10:
	// Expected 14 via linear interpolation.
	if got := cdf.QueryLI(0.6); math.Abs(got-14.0) > 1e-9 {
		t.Fatalf("QueryLI(0.6)=%.6f want=14", got)
	}

	// Endpoints behavior
	if got := cdf.QuantileLI(-1); got != 0 {
		t.Fatalf("QuantileLI(-1)=%.6f want=0", got)
	}
	if got := cdf.QueryLI(1.0); got != 30 {
		t.Fatalf("QueryLI(1.0)=%.6f want=30", got)
	}
}

// Memory sanity: just ensure GetMemoryBytes reports a positive footprint.
func TestKLL_MemoryBytes_Positive(t *testing.T) {
	s := New(64)
	insertAll(s, seq(1, 10))
	if mb := s.GetMemoryBytes(); !(mb > 0) {
		t.Fatalf("GetMemoryBytes()=%.2f want>0", mb)
	}
}

// Quantile vs Rank/Count consistency on a random-ish permutation (no compaction).
func TestKLL_Quantile_Rank_Consistency(t *testing.T) {
	k := 256
	vals := []float64{7, 2, 9, 1, 6, 3, 8, 4, 10, 5} // permutation 1..10
	s := New(k)
	insertAll(s, vals)

	// For x in [1..10], Quantile(x) should equal Rank(x)/10 exactly (no compaction).
	for x := 1.0; x <= 10.0; x++ {
		r := s.Rank(x)
		q := s.Quantile(x)
		expect := float64(r) / float64(s.Count())
		if math.Abs(q-expect) > 1e-12 {
			t.Fatalf("Quantile(%v)=%.6f, Rank/Count=%.6f", x, q, expect)
		}
	}

	// CDF order matches sorted input.
	cdf := s.CDF()
	sorted := append([]float64(nil), vals...)
	sort.Float64s(sorted)
	for i := range sorted {
		if cdf[i].V != sorted[i] {
			t.Fatalf("CDF sorted mismatch at %d: got=%v want=%v", i, cdf[i].V, sorted[i])
		}
	}
}