package sketches

import (
	"fmt"
	"math"
	"testing"

	"github.com/ProjectASAP/sketchlib-go/common"
	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	"google.golang.org/protobuf/proto"
)

const (
	ciMetric  = "http.server.duration"
	ciBaseMs  = uint64(1_700_000_000_000)
	ciSampleP = 0.3
)

// filterWouldKeep recomputes the wire filter's whole-datapoint decision from
// shared inputs only: keep iff any of the d rows admits at (seed, occMs).
func filterWouldKeep(seed, occMs uint64, rows int, p float64) bool {
	for r := 0; r < rows; r++ {
		if common.ConsistentAdmit(seed, occMs, r, p) {
			return true
		}
	}
	return false
}

// TestCSWrapper_ConsistentAgreesWithFilter: with per-item identities threaded,
// the CS wrapper touches the sketch iff the wire filter's decision keeps the
// datapoint — the two stages are views of one decision (design §3.1.1).
func TestCSWrapper_ConsistentAgreesWithFilter(t *testing.T) {
	w, err := NewCountSketchWrapper(5, 512)
	if err != nil {
		t.Fatalf("NewCountSketchWrapper: %v", err)
	}
	w.SetSampleP(ciSampleP)
	seed := common.SeedForMetric(ciMetric)

	snapshot := func() []float64 {
		flat := make([]float64, 0, w.cs.Rows*w.cs.Cols)
		for r := 0; r < w.cs.Rows; r++ {
			flat = append(flat, w.cs.Count[r]...)
		}
		return flat
	}
	differs := func(a, b []float64) bool {
		for i := range a {
			if a[i] != b[i] {
				return true
			}
		}
		return false
	}

	kept, dropped := 0, 0
	for i := 0; i < 2000; i++ {
		occ := ciBaseMs + uint64(i)
		before := snapshot()
		w.SetSampleIdentity(ciMetric, occ)
		w.UpdateString(fmt.Sprintf("k%d", i%50), 1.0)
		changed := differs(before, snapshot())

		expect := filterWouldKeep(seed, occ, w.cs.Rows, ciSampleP)
		if changed != expect {
			t.Fatalf("item %d (occ=%d): sketch changed=%v but filter keep=%v — stages disagree",
				i, occ, changed, expect)
		}
		if expect {
			kept++
		} else {
			dropped++
		}
	}
	if dropped == 0 || kept == 0 {
		t.Fatalf("degenerate run: kept=%d dropped=%d", kept, dropped)
	}
	t.Logf("CS wrapper ≡ filter on all 2000 items (kept=%d dropped=%d)", kept, dropped)
}

// TestCSWrapper_ConsistentUnbiased: identity-threaded consistent sampling
// keeps the frequency estimate unbiased (1/p in place).
func TestCSWrapper_ConsistentUnbiased(t *testing.T) {
	const n = 20000
	w, err := NewCountSketchWrapper(5, 4096) // 5 rows × 12 bits ≤ 64-bit row-hash budget
	if err != nil {
		t.Fatalf("NewCountSketchWrapper: %v", err)
	}
	w.SetSampleP(0.5)
	for i := 0; i < n; i++ {
		w.SetSampleIdentity(ciMetric, ciBaseMs+uint64(i))
		w.UpdateString("hot", 1.0)
		w.SetSampleIdentity(ciMetric, ciBaseMs+uint64(i))
		w.UpdateString(fmt.Sprintf("bg%d", i%500), 1.0)
	}
	est := float64(w.cs.EstimateStringCount("hot"))
	if rel := math.Abs(est-n) / n; rel > 0.15 {
		t.Fatalf("estimate %.0f vs %d (rel %.3f) — consistent identity sampling should stay unbiased", est, n, rel)
	}
}

// TestCMSWrapper_ConsistentExactEnvelope: the first identity switches CMS from
// the internal whole-item sampler (envelope-p) to per-row in-place weighting —
// so the serialized envelope must advertise sample_p == 0 (exact) and the
// estimate must be unbiased WITHOUT any downstream rescale.
func TestCMSWrapper_ConsistentExactEnvelope(t *testing.T) {
	const n = 20000
	w := NewCMSWrapper(4, 4096, false)
	w.SetSampleP(0.5)

	hot := common.FromString("hot").Hash
	for i := 0; i < n; i++ {
		w.SetSampleIdentity(ciMetric, ciBaseMs+uint64(i))
		w.InsertHash(hot)
		w.SetSampleIdentity(ciMetric, ciBaseMs+uint64(i))
		w.InsertHash(common.FromString(fmt.Sprintf("bg%d", i%500)).Hash)
	}

	// Unbiased with NO rescale (weights in place).
	est := w.sk.FastEstimateWithHash(hot)
	if rel := math.Abs(est-n) / n; rel > 0.15 {
		t.Fatalf("estimate %.0f vs %d (rel %.3f) — in-place weighting should be exact-wire unbiased", est, n, rel)
	}

	// Envelope must be exact (no sample_p): downstream must not double-correct.
	raw, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	var env envpb.SketchEnvelope
	if err := proto.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.GetSampleP() != 0 {
		t.Fatalf("consistent CMS envelope must be exact, got sample_p=%v", env.GetSampleP())
	}
}

// TestDDSketchWrapper_ConsistentD1: DDSketch is the d=1 whole-item case —
// admissions equal the filter's row-0 decisions exactly, the internal sampler
// is off, and the envelope still stamps p for the consumer's ×1/p rescale.
func TestDDSketchWrapper_ConsistentD1(t *testing.T) {
	const n = 5000
	w := NewDDSketchWrapper(0.01)
	w.SetSampleP(ciSampleP)
	seed := common.SeedForMetric(ciMetric)

	expectAdmits := uint64(0)
	for i := 0; i < n; i++ {
		occ := ciBaseMs + uint64(i)
		w.SetSampleIdentity(ciMetric, occ)
		w.Update(1.0 + float64(i%10))
		if common.ConsistentAdmit(seed, occ, 0, ciSampleP) {
			expectAdmits++
		}
	}
	if got := w.sk.Count(); got != expectAdmits {
		t.Fatalf("admitted=%d != filter row-0 decisions=%d — stages disagree", got, expectAdmits)
	}

	// Envelope must advertise p (raw counts + consumer rescale convention).
	env, err := w.sk.SerializePortable()
	if err != nil {
		t.Fatalf("SerializePortable: %v", err)
	}
	if math.Abs(env.GetSampleP()-ciSampleP) > 1e-12 {
		t.Fatalf("DDSketch envelope sample_p=%v, want %v", env.GetSampleP(), ciSampleP)
	}
	t.Logf("DDSketch d=1: %d/%d admitted, envelope p=%.2f", expectAdmits, n, env.GetSampleP())
}

// TestWrapper_ZeroTimestampCounterFallback: a zero timestamp (the filter's
// fail-open passthrough) still samples at rate p via the wrapper's occurrence
// counter — exactly once end-to-end, never left unsampled at weight 1.
func TestWrapper_ZeroTimestampCounterFallback(t *testing.T) {
	const n = 20000
	w, err := NewCountSketchWrapper(5, 4096) // 5 rows × 12 bits ≤ 64-bit row-hash budget
	if err != nil {
		t.Fatalf("NewCountSketchWrapper: %v", err)
	}
	w.SetSampleP(0.5)
	for i := 0; i < n; i++ {
		w.SetSampleIdentity(ciMetric, 0) // no wire identity
		w.UpdateString("hot", 1.0)
	}
	est := float64(w.cs.EstimateStringCount("hot"))
	if rel := math.Abs(est-n) / n; rel > 0.15 {
		t.Fatalf("estimate %.0f vs %d (rel %.3f) — counter fallback should stay unbiased", est, n, rel)
	}
}
