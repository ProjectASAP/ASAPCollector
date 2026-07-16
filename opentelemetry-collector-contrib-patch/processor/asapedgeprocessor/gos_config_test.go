// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"

	"go.opentelemetry.io/collector/confmap"
)

// TestGosKnobsDecode is the regression guard for the last hop of the GOS control
// loop: the control plane (emit/agent.rs) emits per-metric `gos_delta_epsilon`
// and `gos_sites` for a `family: countsketch` entry. Before the MetricFamily
// struct defined these fields the collector's STRICT mapstructure decode
// rejected them ("metrics[N] has invalid keys: gos_delta_epsilon"), so
// applyGosMode could never be reached in production and the GOS delta-gating
// path was test-only. A confmap (the real config-apply path) carrying the GOS
// knobs must now decode and land on the fields.
func TestGosKnobsDecode(t *testing.T) {
	raw := map[string]any{
		"shard_count":     1,
		"window_duration": "1h",
		"metrics": []any{
			map[string]any{
				"metric":            "freq",
				"family":            "countsketch",
				"gos_delta_epsilon": 0.1,
				"gos_sites":         4,
			},
			// GOS unset entirely — the fixed DeltaThreshold path (epsilon 0).
			map[string]any{"metric": "lat", "family": "ddsketch"},
		},
		"cold": map[string]any{"enabled": false},
	}
	cfg := createDefaultConfig().(*Config)
	if err := confmap.NewFromStringMap(raw).Unmarshal(cfg); err != nil {
		t.Fatalf("strict decode of config with gos_* knobs must not fail: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	by := map[string]MetricFamily{}
	for i := range cfg.Metrics {
		by[cfg.Metrics[i].Metric] = cfg.Metrics[i]
	}

	if f := by["freq"]; f.GosDeltaEpsilon != 0.1 || f.GosSites != 4 {
		t.Fatalf("freq: got eps=%v sites=%d; want 0.1/4",
			f.GosDeltaEpsilon, f.GosSites)
	}
	if f := by["lat"]; f.GosDeltaEpsilon != 0 {
		t.Fatalf("lat: GOS must be unset (eps=0); got eps=%v", f.GosDeltaEpsilon)
	}
}
