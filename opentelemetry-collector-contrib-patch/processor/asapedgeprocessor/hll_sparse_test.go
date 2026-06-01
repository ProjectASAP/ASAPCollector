// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"

	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.uber.org/zap"
)

// TestNewSketchAggregator_HLLSparseDefaultOff: hll_sparse unset => dense base,
// SketchParams["sparse"] absent/zero, factory builds a usable HLL sketch.
func TestNewSketchAggregator_HLLSparseDefaultOff(t *testing.T) {
	fam := MetricFamily{Metric: "m", Family: FamilyHLL}
	sa, ok := newSketchAggregator("m", &fam, sketchOpts{window: time.Hour}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator(HLL) returned ok=false")
	}
	if v := sa.pcfg.SketchParams["sparse"]; v != 0 {
		t.Fatalf("default HLL must be dense: SketchParams[sparse]=%v, want 0/absent", v)
	}
	sk := sa.factory()
	if sk == nil {
		t.Fatal("HLL factory returned nil sketch")
	}
	if _, isHLL := sk.(*sketches.HLLWrapper); !isHLL {
		t.Fatalf("HLL factory built %T, want *sketches.HLLWrapper", sk)
	}
}

// TestNewSketchAggregator_HLLSparseEnabled: hll_sparse=true => SketchParams
// ["sparse"]=1 and the factory builds a usable (sparse-backed) HLL sketch.
func TestNewSketchAggregator_HLLSparseEnabled(t *testing.T) {
	fam := MetricFamily{Metric: "m", Family: FamilyHLL, HLLSparse: true}
	sa, ok := newSketchAggregator("m", &fam, sketchOpts{window: time.Hour}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator(HLL, sparse) returned ok=false")
	}
	if v := sa.pcfg.SketchParams["sparse"]; v != 1 {
		t.Fatalf("hll_sparse=true must set SketchParams[sparse]=1, got %v", v)
	}
	sk := sa.factory()
	if sk == nil {
		t.Fatal("sparse HLL factory returned nil sketch")
	}
	if _, isHLL := sk.(*sketches.HLLWrapper); !isHLL {
		t.Fatalf("sparse HLL factory built %T, want *sketches.HLLWrapper", sk)
	}
}

// TestConfigValidate_HLLSparseRoundTripAndFamilyGuard exercises the config
// surface: hll_sparse round-trips on an HLL family and is rejected on a
// non-HLL family (mirrors the emit_heap family guard).
func TestConfigValidate_HLLSparseRoundTripAndFamilyGuard(t *testing.T) {
	// Round-trip: hll_sparse=true on family=hll validates and is preserved.
	okCfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "uniques", Family: FamilyHLL, HLLSparse: true}},
	}
	if err := okCfg.Validate(); err != nil {
		t.Fatalf("validate(hll_sparse on family=hll) unexpected error: %v", err)
	}
	if !okCfg.Metrics[0].HLLSparse {
		t.Fatal("hll_sparse=true was not preserved through Validate")
	}

	// Default false on an HLL family with the flag unset.
	defCfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "uniques", Family: FamilyHLL}},
	}
	if err := defCfg.Validate(); err != nil {
		t.Fatalf("validate(default HLL) unexpected error: %v", err)
	}
	if defCfg.Metrics[0].HLLSparse {
		t.Fatal("hll_sparse must default to false")
	}

	// Family guard: hll_sparse on a non-HLL family is rejected.
	badCfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01, HLLSparse: true}},
	}
	if err := badCfg.Validate(); err == nil {
		t.Fatal("validate(hll_sparse on family=ddsketch) expected an error, got nil")
	}
}
