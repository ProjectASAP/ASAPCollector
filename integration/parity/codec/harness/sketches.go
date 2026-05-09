package harness

import (
	"errors"
	"fmt"
	"math"

	"google.golang.org/protobuf/proto"

	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
	cs "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"
	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// HarnessKLLSeed is the fixed RNG seed used by the cross-host runtime
// driver's KLL sketch wrapper. KLL's compaction is randomized; pinning
// a deterministic seed makes the resulting wire bytes reproducible
// across runs (and across the OTel and Telegraf paths in this harness).
// Mirrors integration/parity/'s seed convention.
const HarnessKLLSeed int64 = 42

// The wrappers below are test-only implementations of the runtime's
// precompute.Sketch interface. They delegate to sketchlib-go's
// SerializePortable / SerializeProtoBytes calls — the same calls the
// production processors make — so an identical sketch state always
// yields identical bytes regardless of which adapter (OTel or
// Telegraf) drove the observations.
//
// Cross-host parity is byte-equivalence at the SketchEnvelope.Payload
// level. Both paths in this harness construct fresh wrapper instances
// from the SAME factory closures, so wrapper-internal randomness is
// the only place divergence could leak in. Where the underlying
// sketch supports a seeded constructor (KLL), the harness uses it;
// the others are deterministic by construction once the input
// observation stream is fixed.

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

func (w *ddSketchWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
}

func (w *ddSketchWrapper) ApplyDelta(_ []byte) error {
	return errors.New("cross-host harness ddSketch wrapper: ApplyDelta unused")
}

func (w *ddSketchWrapper) Merge(other precompute.Sketch) error {
	o, ok := other.(*ddSketchWrapper)
	if !ok {
		return fmt.Errorf("cross-host harness ddSketch wrapper: Merge with %T", other)
	}
	return w.sk.Merge(o.sk)
}

func (w *ddSketchWrapper) Reset() {
	w.sk = ddsketch.NewDDSketch(w.alpha)
}

// ===== KLL wrapper =====

type kllWrapper struct {
	sk   *kll.KLLSketch
	k    int
	seed int64
}

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

func (w *kllWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
}

func (w *kllWrapper) ApplyDelta(_ []byte) error {
	return errors.New("cross-host harness KLL wrapper: ApplyDelta unsupported")
}

func (w *kllWrapper) Merge(other precompute.Sketch) error {
	o, ok := other.(*kllWrapper)
	if !ok {
		return fmt.Errorf("cross-host harness KLL wrapper: Merge with %T", other)
	}
	return w.sk.Merge(o.sk)
}

func (w *kllWrapper) Reset() {
	w.sk, _ = kll.NewKLLSketchWithSeed(w.k, w.seed)
}

// ===== HLL wrapper =====

type hllWrapper struct {
	sk *hll.HyperLogLog
}

func newHLLWrapper() *hllWrapper {
	return &hllWrapper{sk: hll.NewHyperLogLog()}
}

func (w *hllWrapper) updateValue(v float64) { w.sk.UpdateValue(v) }

func (w *hllWrapper) Snapshot() ([]byte, error) {
	return w.sk.SerializeProtoBytes()
}

func (w *hllWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
}

func (w *hllWrapper) ApplyDelta(_ []byte) error {
	return errors.New("cross-host harness HLL wrapper: ApplyDelta unused")
}

func (w *hllWrapper) Merge(other precompute.Sketch) error {
	o, ok := other.(*hllWrapper)
	if !ok {
		return fmt.Errorf("cross-host harness HLL wrapper: Merge with %T", other)
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
	return errors.New("cross-host harness CountSketch wrapper: ApplyDelta unused")
}

func (w *countSketchWrapper) Merge(other precompute.Sketch) error {
	o, ok := other.(*countSketchWrapper)
	if !ok {
		return fmt.Errorf("cross-host harness CountSketch wrapper: Merge with %T", other)
	}
	return w.sk.Merge(o.sk)
}

func (w *countSketchWrapper) Reset() {
	w.sk, _ = cs.NewCountSketch(w.rows, w.cols)
}

// CountSketchDims derives (rows, cols) from epsilon/delta the same
// way the legacy countsketchprocessor does. Mirrored from
// integration/parity/harness/sketches.go so the cross-host harness
// can build sketchlib instances with identical dimensions on both
// the OTel and Telegraf driving paths.
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

func (w *cmsWrapper) insertHash(h uint64) { w.sk.InsertWithHash(h) }

func (w *cmsWrapper) Snapshot() ([]byte, error) {
	// Mirrors legacy countminsketchprocessor's default proto encoding
	// (frequency-only state). Both paths in this harness call the same
	// helper, so the wire bytes agree by construction.
	return w.sk.SerializeProtoBytesFO()
}

func (w *cmsWrapper) ComputeDeltaAgainst(_ []byte, _ uint64) ([]byte, bool, error) {
	full, err := w.Snapshot()
	return full, true, err
}

func (w *cmsWrapper) ApplyDelta(_ []byte) error {
	return errors.New("cross-host harness CMS wrapper: ApplyDelta unused")
}

func (w *cmsWrapper) Merge(other precompute.Sketch) error {
	o, ok := other.(*cmsWrapper)
	if !ok {
		return fmt.Errorf("cross-host harness CMS wrapper: Merge with %T", other)
	}
	return w.sk.Merge(o.sk)
}

func (w *cmsWrapper) Reset() {
	w.sk, _ = cms.NewCountMinSketch(w.rows, w.cols)
}
