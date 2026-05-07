// Package crosshostparity reproduces the cross-language byte-parity
// gate (#243) canonical envelope bytes inline. These are the exact
// bytes each ASAP-flavored agent — asap-otel (OTel-Go), asap-otap
// (OTAP-Rust), asap-telegraf (Telegraf-Go) — MUST emit when fed the
// canonical golden_input/inputs.json fixture.
//
// Why duplicate the integration/parity/golden_test.go logic here:
//
//  1. Self-containedness — the cross_host_parity test should run
//     without first regenerating fixtures in the parity/ sibling.
//  2. Active verification — re-running sketchlib-go's
//     SerializePortable* path each test execution catches a
//     sketchlib-go API drift the moment the test runs (rather than
//     pinning to a stale checked-in fixture).
//  3. Independence from the legacy OTel processor build chain — the
//     existing parity/ package transitively imports the OTel
//     collector pdata module, which requires the
//     opentelemetry-collector submodule to be initialized. That's a
//     hard prereq for parity/golden_test.go; cross_host_parity/
//     deliberately depends ONLY on sketchlib-go + protobuf so it is
//     runnable in lighter environments (CI, engineer laptops).
//
// Inputs are bit-identical to integration/parity/golden_test.go's
// goldenFloats / goldenCsKeys / goldenCmsKeys / goldenHllKeys and
// asap-precompute-rs/tests/cross_language_parity.rs::deterministic_*.
// Any divergence here is the test's job to surface — fail loud, not
// drift silently.
package crosshostparity

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
	cs "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"
	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"
	"google.golang.org/protobuf/proto"
)

// goldenFloats returns the canonical floating-point input the
// cross-language gate uses for DDSketch and KLL. Mirrors
// integration/parity/golden_test.go::goldenFloats and
// asap-precompute-rs/tests/cross_language_parity.rs::deterministic_floats.
func goldenFloats() []float64 {
	out := make([]float64, 0, 50)
	for i := 1; i <= 50; i++ {
		out = append(out, float64(i))
	}
	return out
}

// goldenCsKeys returns the canonical CountSketch input keys.
// Mirrors integration/parity/golden_test.go::goldenCsKeys.
func goldenCsKeys() []string {
	out := make([]string, 0, 25)
	for i := 0; i < 25; i++ {
		out = append(out, "k-"+string(rune('a'+i%5)))
	}
	return out
}

// goldenCmsKeys returns the canonical CountMinSketch input keys.
// Mirrors integration/parity/golden_test.go::goldenCmsKeys.
func goldenCmsKeys() []string {
	out := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		out = append(out, "flow-"+string(rune('0'+i%10)))
	}
	return out
}

// goldenHllKeys returns the canonical HLL input keys: each value v
// is keyed by its IEEE-754 little-endian f64 byte pattern, mirroring
// the Rust observer's `v.float.to_le_bytes()` path. Mirrors
// integration/parity/golden_test.go::goldenHllKeys.
func goldenHllKeys() [][]byte {
	out := make([][]byte, 0, 50)
	for _, v := range goldenFloats() {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
		out = append(out, buf)
	}
	return out
}

// CanonicalEnvelopeBytes returns the canonical SketchEnvelope.Payload
// bytes for the given sketch family, produced by running the golden
// input through sketchlib-go's SerializePortable* helpers + the
// Producer/HashSpec strip applied by the cross-language gate.
//
// The bytes are deterministic across runs (fixed seeds, fixed
// hash-spec stripping) — calling this twice yields byte-equal
// results. Calling it on the Rust side via asap_sketchlib's mirrored
// types yields the same bytes too (the closed #243 gate).
//
// sketch must be one of: "DDSketch", "KLL", "HLL", "CountSketch",
// "CountMinSketch". Unknown values return an error rather than panic
// so the caller (the test) can surface a missing-coverage failure
// loudly.
func CanonicalEnvelopeBytes(sketch string) ([]byte, error) {
	switch sketch {
	case "DDSketch":
		sk := ddsketch.NewDDSketch(0.01)
		for _, v := range goldenFloats() {
			sk.Update(v)
		}
		env, err := sk.SerializePortable()
		if err != nil {
			return nil, fmt.Errorf("ddsketch.SerializePortable: %w", err)
		}
		// Strip Producer / HashSpec — same strip integration/parity/
		// golden_test.go applies, matches the Rust wrapper which
		// omits both fields.
		env.Producer = nil
		env.HashSpec = nil
		return proto.Marshal(env)

	case "KLL":
		sk, err := kll.NewKLLSketchWithSeed(200, 42)
		if err != nil {
			return nil, fmt.Errorf("kll.NewKLLSketchWithSeed: %w", err)
		}
		for _, v := range goldenFloats() {
			sk.Update(v)
		}
		env, err := sk.SerializePortable()
		if err != nil {
			return nil, fmt.Errorf("kll.SerializePortable: %w", err)
		}
		env.Producer = nil
		env.HashSpec = nil
		return proto.Marshal(env)

	case "HLL":
		sk := hll.NewHyperLogLog()
		for _, k := range goldenHllKeys() {
			sk.Update(common.FromBytes(k))
		}
		env, err := sk.SerializePortable()
		if err != nil {
			return nil, fmt.Errorf("hll.SerializePortable: %w", err)
		}
		env.Producer = nil
		env.HashSpec = nil
		return proto.Marshal(env)

	case "CountSketch":
		sk, err := cs.NewCountSketch(3, 512)
		if err != nil {
			return nil, fmt.Errorf("cs.NewCountSketch: %w", err)
		}
		for _, k := range goldenCsKeys() {
			sk.UpdateString(k, 1.0)
		}
		env, err := sk.SerializePortable()
		if err != nil {
			return nil, fmt.Errorf("cs.SerializePortable: %w", err)
		}
		// Mirrors integration/parity/golden_test.go: strip Producer +
		// HashSpec, plus clear hh_keys (the Rust wrapper emits a
		// wire-aligned CountSketch struct that does not carry a
		// Space-Saving-derived candidate list; clearing hh_keys keeps
		// the inner CountSketchState byte-compatible across producers
		// without dropping any matrix-level data — downstream rebuilds
		// TopK from the merged matrix anyway).
		env.Producer = nil
		env.HashSpec = nil
		if state := env.GetCountSketch(); state != nil {
			state.HhKeys = nil
		}
		return proto.Marshal(env)

	case "CountMinSketch":
		sk, err := cms.NewCountMinSketch(4, 2048)
		if err != nil {
			return nil, fmt.Errorf("cms.NewCountMinSketch: %w", err)
		}
		for _, k := range goldenCmsKeys() {
			sk.Update(common.FromBytes([]byte(k)))
		}
		// SerializePortableFO is the frequency-only format the legacy
		// CMS processor's emit path uses, matched by the Rust wrapper.
		env, err := sk.SerializePortableFO()
		if err != nil {
			return nil, fmt.Errorf("cms.SerializePortableFO: %w", err)
		}
		env.Producer = nil
		env.HashSpec = nil
		return proto.Marshal(env)

	default:
		return nil, fmt.Errorf("unknown sketch family %q (want DDSketch|"+
			"KLL|HLL|CountSketch|CountMinSketch)", sketch)
	}
}
