package harness

import (
	"errors"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
	cs "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"
	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// The harness implements precompute.Sketch wrappers around the
// sketchlib-go primitives so a Precompute instance can be assembled
// from outside the collector binary. These wrappers are TEST-ONLY:
// they exist solely to drive the runtime path of the parity harness.
// When step 2.5–2.9 lands the legacy processors as shims, the shims
// will provide their own production wrappers — the harness's wrappers
// are not part of the import graph then.
//
// The byte-format invariant we rely on: legacy processors and these
// wrappers both call sketchlib-go's `SerializePortable` (or
// `serializeXxx` thin proto-marshaler that wraps it). For the same
// sketch state, the resulting bytes ARE identical — that is what
// makes the byte-for-byte comparison in this harness meaningful.

// ===== DDSketch wrapper =====

type ddSketchWrapper struct {
	sk    *ddsketch.DDSketch
	alpha float64
}

func newDDSketchWrapper(alpha float64) *ddSketchWrapper {
	return &ddSketchWrapper{sk: ddsketch.NewDDSketch(alpha), alpha: alpha}
}

func (w *ddSketchWrapper) update(v float64) { w.sk.Update(v) }

func (w *ddSketchWrapper) Snapshot() ([]byte, error) {
	env, err := w.sk.SerializePortable()
	if err != nil {
		return nil, fmt.Errorf("ddsketch.SerializePortable: %w", err)
	}
	return proto.Marshal(env)
}

func (w *ddSketchWrapper) ComputeDeltaAgainst(prev []byte, threshold uint64) ([]byte, bool, error) {
	// Mirrors computeDDSketchDelta in the legacy processor. If we
	// can't decode prev (e.g. first-frame), fall back to full state.
	if len(prev) == 0 {
		full, err := w.Snapshot()
		return full, true, err
	}
	prevSk, err := decodeDDSketchPortable(prev)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	delta, err := ddsketch.ComputeDelta(prevSk, w.sk, threshold)
	if err != nil {
		full, fErr := w.Snapshot()
		return full, true, fErr
	}
	return delta, false, nil
}

func (w *ddSketchWrapper) ApplyDelta(_ []byte) error {
	// The harness's runtime path never receives sketch envelopes
	// inbound (BuildInput emits scalar Gauge / Sum points only),
	// so this code path is unused. Keep it as an explicit error
	// rather than silently no-op so any future change that hits
	// this path surfaces loudly.
	return errors.New("harness ddSketch wrapper: ApplyDelta unused in parity harness")
}

func (w *ddSketchWrapper) Merge(other precompute.Sketch) error {
	o, ok := other.(*ddSketchWrapper)
	if !ok {
		return fmt.Errorf("harness ddSketch wrapper: Merge with %T", other)
	}
	return w.sk.Merge(o.sk)
}

func (w *ddSketchWrapper) Reset() {
	w.sk = ddsketch.NewDDSketch(w.alpha)
}

func decodeDDSketchPortable(b []byte) (*ddsketch.DDSketch, error) {
	// SerializePortable wraps DDSketchState in a SketchEnvelope.
	var env envpb.SketchEnvelope
	if err := proto.Unmarshal(b, &env); err != nil {
		return nil, err
	}
	st := env.GetDdsketch()
	if st == nil {
		return nil, errors.New("envelope did not carry DDSketchState")
	}
	return ddsketch.NewFromState(st)
}

// ===== KLL wrapper =====

type kllWrapper struct {
	sk   *kll.KLLSketch
	k    int
	seed int64
}

// newKLLWrapper builds a deterministic KLL via NewKLLSketchWithSeed so two
// instances fed identical input produce byte-identical SerializePortable
// output. The legacy KLL processor's sketchlib-go default constructor seeds
// from time.Now() — see the parallel sketchlib-go fix that adds the seedable
// API. Both paths use the same fixed seed (HarnessKLLSeed = 42), so byte
// parity holds end-to-end.
func newKLLWrapper(k int, seed int64) *kllWrapper {
	sk, _ := kll.NewKLLSketchWithSeed(k, seed)
	return &kllWrapper{sk: sk, k: k, seed: seed}
}

func (w *kllWrapper) update(v float64) { w.sk.Update(v) }

func (w *kllWrapper) Snapshot() ([]byte, error) {
	env, err := w.sk.SerializePortable()
	if err != nil {
		return nil, fmt.Errorf("kll.SerializePortable: %w", err)
	}
	return proto.Marshal(env)
}

// KLL doesn't support delta transmission today — the legacy KLL
// processor explicitly rejects it. We return the full snapshot.
func (w *kllWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
}

func (w *kllWrapper) ApplyDelta(_ []byte) error {
	return errors.New("harness KLL wrapper: ApplyDelta unsupported (no KLL delta format)")
}

func (w *kllWrapper) Merge(other precompute.Sketch) error {
	o, ok := other.(*kllWrapper)
	if !ok {
		return fmt.Errorf("harness KLL wrapper: Merge with %T", other)
	}
	return w.sk.Merge(o.sk)
}

func (w *kllWrapper) Reset() {
	// Re-seed deterministically — sketchlib-go's seeded sketch already
	// re-seeds itself in Clear(); rebuilding from scratch with the same
	// seed keeps the contract explicit at the wrapper layer too.
	w.sk, _ = kll.NewKLLSketchWithSeed(w.k, w.seed)
}

// ===== HLL wrapper =====

type hllWrapper struct {
	sk *hll.HyperLogLog
}

func newHLLWrapper() *hllWrapper {
	return &hllWrapper{sk: hll.NewHyperLogLog()}
}

// updateValue mirrors the legacy hllprocessor batch path that calls
// bs.sketch.UpdateValue(dp.DoubleValue()) on every Gauge data point.
// Both code paths see the same float bytes hashed identically.
func (w *hllWrapper) updateValue(v float64) { w.sk.UpdateValue(v) }

func (w *hllWrapper) Snapshot() ([]byte, error) {
	return w.sk.SerializeProtoBytes()
}

func (w *hllWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	// HLL register-deltas are supported in sketchlib-go but the
	// harness disables delta transmission so both Path A and Path B
	// emit only PROTO_FULL envelopes. Keeps the comparison crisp.
	full, err := w.Snapshot()
	return full, true, err
}

func (w *hllWrapper) ApplyDelta(_ []byte) error {
	return errors.New("harness HLL wrapper: ApplyDelta unused in parity harness")
}

func (w *hllWrapper) Merge(other precompute.Sketch) error {
	o, ok := other.(*hllWrapper)
	if !ok {
		return fmt.Errorf("harness HLL wrapper: Merge with %T", other)
	}
	return w.sk.Merge(o.sk)
}

func (w *hllWrapper) Reset() {
	w.sk = hll.NewHyperLogLog()
}

// ===== CountSketch wrapper =====

type countSketchWrapper struct {
	sk   *cs.CountSketch
	rows int
	cols int
}

func newCountSketchWrapper(rows, cols int) *countSketchWrapper {
	sk, _ := cs.NewCountSketch(rows, cols)
	return &countSketchWrapper{sk: sk, rows: rows, cols: cols}
}

// updateString mirrors the legacy CountSketch processor's
// UpdateString(itemKey, value) call where itemKey == metricName and
// value == 1.0. Both pipelines must invoke the same path on the same
// inputs so the matrix cell counts agree byte-for-byte.
func (w *countSketchWrapper) updateString(key string, value float64) {
	w.sk.UpdateString(key, value)
}

func (w *countSketchWrapper) Snapshot() ([]byte, error) {
	return w.sk.SerializeProtoBytes()
}

func (w *countSketchWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
}

func (w *countSketchWrapper) ApplyDelta(_ []byte) error {
	return errors.New("harness CountSketch wrapper: ApplyDelta unused")
}

func (w *countSketchWrapper) Merge(other precompute.Sketch) error {
	o, ok := other.(*countSketchWrapper)
	if !ok {
		return fmt.Errorf("harness CountSketch wrapper: Merge with %T", other)
	}
	return w.sk.Merge(o.sk)
}

func (w *countSketchWrapper) Reset() {
	w.sk, _ = cs.NewCountSketch(w.rows, w.cols)
}

// CountSketchDims derives (rows, cols) from epsilon/delta the same
// way the legacy countsketchprocessor does (rows = ceil(ln(1/delta)),
// cols = nextPowerOfTwo(ceil(1/eps^2))). Exposed so the harness can
// build sketchlib instances with identical dimensions on both Path A
// and Path B.
func CountSketchDims(epsilon, delta float64) (rows, cols int) {
	rows = int(math.Ceil(math.Log(1.0 / delta)))
	if rows < 1 {
		rows = 1
	}
	cols = int(math.Ceil(1.0 / (epsilon * epsilon)))
	if cols < 2 {
		cols = 2
	}
	p := 1
	for p < cols {
		p <<= 1
	}
	cols = p
	return rows, cols
}

// ===== CountMinSketch wrapper =====

type cmsWrapper struct {
	sk   *cms.CountMinSketch
	rows int
	cols int
}

func newCMSWrapper(rows, cols int) *cmsWrapper {
	sk, _ := cms.NewCountMinSketch(rows, cols)
	return &cmsWrapper{sk: sk, rows: rows, cols: cols}
}

// insertHash mirrors the legacy CMS processor's
// ws.cms.InsertWithHash(common.FromString(flowKey).Hash) call. The
// harness builds the same flowKey on both sides so the hash agrees.
func (w *cmsWrapper) insertHash(h uint64) { w.sk.InsertWithHash(h) }

func (w *cmsWrapper) Snapshot() ([]byte, error) {
	// CMS's portable serializer has two flavors — full and
	// frequency-only (FO). The legacy processor uses the FO path
	// (SerializeProtoBytesFO) when the encoding is the default
	// "proto"; we mirror that exactly so the wire bytes align.
	return w.sk.SerializeProtoBytesFO()
}

func (w *cmsWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
}

func (w *cmsWrapper) ApplyDelta(_ []byte) error {
	return errors.New("harness CMS wrapper: ApplyDelta unused")
}

func (w *cmsWrapper) Merge(other precompute.Sketch) error {
	o, ok := other.(*cmsWrapper)
	if !ok {
		return fmt.Errorf("harness CMS wrapper: Merge with %T", other)
	}
	return w.sk.Merge(o.sk)
}

func (w *cmsWrapper) Reset() {
	w.sk, _ = cms.NewCountMinSketch(w.rows, w.cols)
}

