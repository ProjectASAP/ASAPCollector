// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"
)

// TestConfigValidate_GosHLL covers the config-validation rules added for HLL's
// GOS register-change adapter: gos_delta_epsilon (reinterpreted as τ) is
// accepted on family=hll with any positive value (NOT bounded to (0,1) like
// CountSketch's ε), but rejected when combined with hll_sparse (the sparse
// base has no per-register GOS path).
func TestConfigValidate_GosHLL(t *testing.T) {
	// Accepted: dense HLL with τ >= 1 (a doublings count, no (0,1) upper bound).
	for _, tau := range []float64{0.5, 1, 2, 8} {
		cfg := &Config{
			ShardCount:     1,
			WindowDuration: time.Hour,
			Metrics:        []MetricFamily{{Metric: "uniques", Family: FamilyHLL, GosDeltaEpsilon: tau}},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("validate(dense HLL, gos τ=%v) unexpected error: %v", tau, err)
		}
	}

	// Rejected: HLL + gos_delta_epsilon + hll_sparse (no sparse GOS path).
	sparseCfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "uniques", Family: FamilyHLL, GosDeltaEpsilon: 1, HLLSparse: true}},
	}
	if err := sparseCfg.Validate(); err == nil {
		t.Fatal("validate(HLL gos + hll_sparse) expected an error, got nil")
	}

	// Still rejected on an unrelated family (e.g. DDSketch).
	ddCfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01, GosDeltaEpsilon: 1}},
	}
	if err := ddCfg.Validate(); err == nil {
		t.Fatal("validate(gos_delta_epsilon on family=ddsketch) expected an error, got nil")
	}

	// CountSketch's ε still keeps its (0,1) upper-bound (τ's relaxation must
	// NOT leak into the CountSketch branch).
	csCfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "events", Family: FamilyCountSketch, GosDeltaEpsilon: 2}},
	}
	if err := csCfg.Validate(); err == nil {
		t.Fatal("validate(countsketch gos ε=2) expected an error (ε must be in (0,1)), got nil")
	}
}
