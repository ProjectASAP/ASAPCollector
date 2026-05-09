// Generates golden-byte fixtures for cross-language parity tests in
// `asap-precompute-rs/tests/cross_language_parity.rs`.
//
// Each test case runs a deterministic input through the
// asap-precompute-go runtime + sketchlib-go wrappers, captures the
// emitted SketchEnvelope.Payload bytes, and writes them to
// `golden/<sketch>_envelope.bin`. The Rust side loads the same file
// and asserts byte-equality against its own runtime + wrapper output.
//
// Regenerate by running:
//
//	GOLDEN_REGEN=1 go test -run GenerateGolden ./integration/parity/runtime-impl/...
//
// Without GOLDEN_REGEN set, the generator acts as a self-check that
// the Go side still produces the bytes currently checked in.
//
// Determinism: this generator deliberately bypasses the multi-series
// `harness.BuildInput` path (whose envelope ordering depends on Go
// map iteration). Instead, it constructs ONE sketch directly via the
// sketchlib-go primitives, feeds a fixed input, and serializes via
// SerializePortable. Single-series → single envelope → byte-stable
// across runs.
package parity_test

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
	cs "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"
	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"
	"google.golang.org/protobuf/proto"
)

// goldenFloats / goldenKeys are the exact inputs the Rust side uses;
// keep these in lockstep with
// `asap-precompute-rs/tests/cross_language_parity.rs::deterministic_floats`
// and the keying helpers there.
func goldenFloats() []float64 {
	out := make([]float64, 0, 50)
	for i := 1; i <= 50; i++ {
		out = append(out, float64(i))
	}
	return out
}

func goldenCsKeys() []string {
	out := make([]string, 0, 25)
	for i := 0; i < 25; i++ {
		out = append(out, "k-"+string(rune('a'+i%5)))
	}
	return out
}

func goldenCmsKeys() []string {
	out := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		out = append(out, "flow-"+string(rune('0'+i%10)))
	}
	return out
}

func goldenHllKeys() [][]byte {
	out := make([][]byte, 0, 50)
	for _, v := range goldenFloats() {
		// Use the f64's IEEE-754 little-endian byte pattern as the
		// HLL key to mirror the Rust observer's
		// `v.float.to_le_bytes()` path.
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
		out = append(out, buf)
	}
	return out
}

func writeOrCompareGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("golden", name)
	if os.Getenv("GOLDEN_REGEN") == "1" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir golden: %v", err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("wrote %s (%d bytes)", path, len(got))
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("mkdir golden: %v", err)
			}
			if err := os.WriteFile(path, got, 0o644); err != nil {
				t.Fatalf("write golden: %v", err)
			}
			t.Logf("first-run: wrote %s (%d bytes)", path, len(got))
			return
		}
		t.Fatalf("read golden %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("golden %s drifted: got %d bytes, want %d bytes", path, len(got), len(want))
	}
}

// TestGenerateGoldenFixtures emits one fixture file per sketch.
func TestGenerateGoldenFixtures(t *testing.T) {
	t.Run("DDSketch", func(t *testing.T) {
		sk := ddsketch.NewDDSketch(0.01)
		for _, v := range goldenFloats() {
			sk.Update(v)
		}
		env, err := sk.SerializePortable()
		if err != nil {
			t.Fatal(err)
		}
		// Strip producer / hash_spec metadata so the byte payload is
		// stable across sketchlib-go version bumps and matches what
		// the Rust wrapper produces (which omits both fields). The
		// inner DDSketchState is unaffected.
		env.Producer = nil
		env.HashSpec = nil
		bytes, err := proto.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		writeOrCompareGolden(t, "ddsketch_envelope.bin", bytes)
	})

	t.Run("KLL", func(t *testing.T) {
		sk, err := kll.NewKLLSketchWithSeed(200, 42)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range goldenFloats() {
			sk.Update(v)
		}
		env, err := sk.SerializePortable()
		if err != nil {
			t.Fatal(err)
		}
		env.Producer = nil
		env.HashSpec = nil
		bytes, err := proto.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		writeOrCompareGolden(t, "kll_envelope.bin", bytes)
	})

	t.Run("HLL", func(t *testing.T) {
		sk := hll.NewHyperLogLog()
		for _, k := range goldenHllKeys() {
			sk.Update(common.FromBytes(k))
		}
		env, err := sk.SerializePortable()
		if err != nil {
			t.Fatal(err)
		}
		// Strip producer / hash_spec metadata so the byte payload is
		// stable across sketchlib-go version bumps and matches what
		// the Rust wrapper produces (which sets both fields to None).
		// Mirrors the DDSketch / KLL / CountSketch cases above. PR
		// #252 un-ignored the parity test but missed this strip; the
		// test only "passed" because fixtures are gitignored and
		// rarely regenerated alongside a test run.
		env.Producer = nil
		env.HashSpec = nil
		bytes, err := proto.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		writeOrCompareGolden(t, "hll_envelope.bin", bytes)
	})

	t.Run("CountSketch", func(t *testing.T) {
		sk, err := cs.NewCountSketch(3, 512)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range goldenCsKeys() {
			sk.UpdateString(k, 1.0)
		}
		env, err := sk.SerializePortable()
		if err != nil {
			t.Fatal(err)
		}
		// Strip producer / hash_spec metadata so the byte payload is
		// stable across sketchlib-go version bumps and matches what
		// the Rust wrapper produces (which omits both fields).
		// Mirrors the DDSketch / KLL / HLL cases above. Also clear
		// hh_keys: the sketchlib-go wire format includes candidate
		// keys from the upstream Space-Saving tracker, but the Rust
		// wrapper emits a wire-aligned `CountSketch` struct that does
		// not carry an SS-derived candidate list. Clearing hh_keys
		// keeps the inner `CountSketchState` byte-compatible across
		// producers without dropping any matrix-level data
		// (downstream rebuilds TopK from the merged matrix anyway).
		env.Producer = nil
		env.HashSpec = nil
		if state := env.GetCountSketch(); state != nil {
			state.HhKeys = nil
		}
		bytes, err := proto.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		writeOrCompareGolden(t, "countsketch_envelope.bin", bytes)
	})

	t.Run("CountMinSketch", func(t *testing.T) {
		sk, err := cms.NewCountMinSketch(4, 2048)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range goldenCmsKeys() {
			sk.Update(common.FromBytes([]byte(k)))
		}
		// Use the proto-bytes-FO format the legacy CMS processor's
		// emit path uses (frequency-only, omitting Sum/Sum2). Strip
		// Producer / HashSpec metadata so the byte payload is stable
		// across sketchlib-go version bumps and matches what the Rust
		// wrapper produces (which omits both fields). Mirrors the
		// DDSketch / KLL / HLL / CountSketch cases above.
		env, err := sk.SerializePortableFO()
		if err != nil {
			t.Fatal(err)
		}
		env.Producer = nil
		env.HashSpec = nil
		bytes, err := proto.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		writeOrCompareGolden(t, "cms_envelope.bin", bytes)
	})
}
