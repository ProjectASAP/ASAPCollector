// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"strings"
	"testing"
	"time"
)

// TestGosDeltaEpsilon_FamilyValidation locks in that config_validate accepts
// gos_delta_epsilon on the GOS-converted families available at this point in
// the stack (ddsketch, countminsketch, sum, kll, dense hll, and non-heap
// countsketch) and rejects it everywhere else. Uses a minimal, otherwise-valid
// Config per case so only the gos gate is under test.
func TestGosDeltaEpsilon_FamilyValidation(t *testing.T) {
	mk := func(fam FamilyKind, emitHeap bool, eps float64) *Config {
		m := MetricFamily{
			Metric:          "m",
			Family:          fam,
			GosDeltaEpsilon: eps,
			GosSites:        1,
		}
		switch fam {
		case FamilyDDSketch:
			m.RelativeAccuracy = 0.01
		case FamilyCountSketch:
			m.EmitHeap = emitHeap
			if emitHeap {
				m.ItemLabel = "ep"
			}
		}
		return &Config{
			ShardCount:     1,
			WindowDuration: time.Hour,
			Metrics:        []MetricFamily{m},
			Cold:           ColdConfig{Enabled: false},
		}
	}

	cases := []struct {
		name      string
		cfg       *Config
		wantError bool
	}{
		{"ddsketch accepted", mk(FamilyDDSketch, false, 0.5), false},
		{"countsketch non-heap accepted", mk(FamilyCountSketch, false, 0.5), false},
		{"countsketch emit_heap rejected", mk(FamilyCountSketch, true, 0.5), true},
		{"ddsketch epsilon>=1 rejected", mk(FamilyDDSketch, false, 1.0), true},
		{"sum accepted", mk(FamilySum, false, 0.5), false},
		{"kll accepted", mk(FamilyKLL, false, 0.5), false},
		{"hll dense accepted", mk(FamilyHLL, false, 0.5), false},
		{"countmin accepted", mk(FamilyCountMinSketch, false, 0.5), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantError && err == nil {
				t.Fatal("expected a validation error, got nil")
			}
			if !tc.wantError && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if tc.wantError && err != nil && !strings.Contains(err.Error(), "gos_delta_epsilon") {
				// Guard that the REJECTION is the gos gate's, not some unrelated
				// field — the epsilon>=1 and wrong-family messages both mention it.
				t.Fatalf("expected a gos_delta_epsilon validation error, got %v", err)
			}
		})
	}
}
