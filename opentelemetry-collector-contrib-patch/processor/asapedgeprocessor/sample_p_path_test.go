// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/confmap"
	"go.uber.org/zap"
)

// samplePProbe is implemented by the sampling-aware sketch wrappers
// (HLL / CountMinSketch). SampleP() reports the configured probability
// (1.0 when sampling is disabled). Used here to assert the fused
// sketch-build wired sample_p through to sketchlib-go.
type samplePProbe interface {
	SampleP() float64
}

// TestFusedConfigSamplePDecodes is the regression for the crash: the control
// plane emits a per-metric `sample_p` into the fused asap_edge config and the
// collector's STRICT mapstructure decode rejected the unknown key
// ("metrics[N] has invalid keys: sample_p"), crash-looping the agent. The
// per-metric struct now defines the field, so a confmap (the real config-apply
// path) carrying sample_p must decode without error.
func TestFusedConfigSamplePDecodes(t *testing.T) {
	raw := map[string]any{
		"shard_count":     2,
		"window_duration": "1h",
		"drop_original":   true,
		"metrics": []any{
			map[string]any{"metric": "card", "family": "hll", "sample_p": 0.5},
			map[string]any{"metric": "freq", "family": "countminsketch", "sample_p": 0.25},
			map[string]any{"metric": "lat", "family": "ddsketch"}, // sample_p unset
		},
		"cold": map[string]any{"enabled": false},
	}
	cfg := createDefaultConfig().(*Config)
	if err := confmap.NewFromStringMap(raw).Unmarshal(cfg); err != nil {
		t.Fatalf("strict decode of fused config with sample_p must not fail: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	got := map[string]float64{}
	for i := range cfg.Metrics {
		got[cfg.Metrics[i].Metric] = cfg.Metrics[i].SampleP
	}
	if got["card"] != 0.5 {
		t.Fatalf("hll metric sample_p: want 0.5, got %v", got["card"])
	}
	if got["freq"] != 0.25 {
		t.Fatalf("cms metric sample_p: want 0.25, got %v", got["freq"])
	}
	// Unset normalises to 1.0 (sampling disabled — byte-identical default).
	if got["lat"] != 1.0 {
		t.Fatalf("unset sample_p must normalise to 1.0, got %v", got["lat"])
	}
}

// TestFusedConfigSamplePRejectsOutOfRange asserts Validate rejects an
// out-of-range sample_p rather than silently clamping, so a control-plane typo
// surfaces at agent boot.
func TestFusedConfigSamplePRejectsOutOfRange(t *testing.T) {
	for _, p := range []float64{-0.1, 1.5} {
		cfg := &Config{
			ShardCount:     1,
			WindowDuration: time.Hour,
			Metrics:        []MetricFamily{{Metric: "m", Family: FamilyHLL, SampleP: p}},
			Cold:           ColdConfig{Enabled: false},
		}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("sample_p=%v must be rejected", p)
		}
	}
}

// TestFusedSketchBuildAppliesSampleP asserts the fused warm sketch-build
// (newSketchAggregator) wires sample_p into the families whose geometric skip
// actually avoids work — DDSketch and CountMinSketch — so a config with
// sample_p=0.5 builds a sketch whose SampleP()==0.5, and an unset sample_p
// builds an unsampled sketch (SampleP()==1.0 — byte-identical to today).
// HLL is deliberately FORCED unsampled: its hash is needed for both the
// admission threshold and the register index, so sampling buys ~0 CPU while
// degrading cardinality accuracy. An HLL metric therefore builds SampleP()==1.0
// even when the config requests sampling.
func TestFusedSketchBuildAppliesSampleP(t *testing.T) {
	cases := []struct {
		name    string
		family  FamilyKind
		sampleP float64
		wantP   float64
		isProbe bool // family exposes SampleP() (sampling-aware)
	}{
		{"hll_forced_unsampled_despite_config", FamilyHLL, 0.5, 1.0, true},
		{"hll_unset", FamilyHLL, 0, 1.0, true},
		{"cms_sampled", FamilyCountMinSketch, 0.5, 0.5, true},
		{"cms_unset", FamilyCountMinSketch, 0, 1.0, true},
		{"ddsketch_sampled", FamilyDDSketch, 0.5, 0.5, true},
		{"ddsketch_unset", FamilyDDSketch, 0, 1.0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fam := &MetricFamily{Metric: "m", Family: tc.family, SampleP: tc.sampleP, RelativeAccuracy: 0.01}
			// Validate normalises 0 => 1.0, matching the live config-apply path.
			cfg := &Config{ShardCount: 1, WindowDuration: time.Hour, Metrics: []MetricFamily{*fam}, Cold: ColdConfig{Enabled: false}}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			sa, ok := newSketchAggregator("m", &cfg.Metrics[0], sketchOpts{window: time.Hour}, zap.NewNop())
			if !ok {
				t.Fatalf("family %s: newSketchAggregator returned not-ok", tc.family)
			}
			sk := sa.factory() // the per-window sketch the processor builds
			probe, isProbe := sk.(samplePProbe)
			if isProbe != tc.isProbe {
				t.Fatalf("family %s: SampleP-probe support = %v, want %v", tc.family, isProbe, tc.isProbe)
			}
			if isProbe {
				if got := probe.SampleP(); got != tc.wantP {
					t.Fatalf("family %s: built sketch SampleP()=%v, want %v", tc.family, got, tc.wantP)
				}
			}
		})
	}
}
