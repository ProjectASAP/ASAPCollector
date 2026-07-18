// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"go.uber.org/zap"
)

func hasEncoding(envs []*precompute.SketchEnvelope, want precompute.Encoding) bool {
	for _, e := range envs {
		if e.Encoding == want {
			return true
		}
	}
	return false
}

func encodings(envs []*precompute.SketchEnvelope) []precompute.Encoding {
	out := make([]precompute.Encoding, 0, len(envs))
	for _, e := range envs {
		out = append(out, e.Encoding)
	}
	return out
}

// TestDeltaTransmissionEmitsDeltaEncoding covers P1 #5: with delta enabled, the
// first window emits PROTO_FULL (no prior snapshot) and the SECOND window emits
// PROTO_DELTA.
func TestDeltaTransmissionEmitsDeltaEncoding(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sa, ok := newSketchAggregator("lat", &cfg.Metrics[0],
		sketchOpts{window: time.Hour, delta: true}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator returned ok=false")
	}
	if !sa.pcfg.DeltaTransmission {
		t.Fatal("DeltaTransmission not set on PrecomputeConfig")
	}

	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	am := map[string]string{"zone": "z0"}

	// Window 1 -> PROTO_FULL (first snapshot for the series).
	for i := 0; i < 10; i++ {
		sa.observe(am, float64(i), base+uint64(i), false, 0, 0)
	}
	envs1 := sa.pc.Drain()
	if !hasEncoding(envs1, precompute.EncodingProtoFull) {
		t.Fatalf("window 1: expected a PROTO_FULL envelope, got %v", encodings(envs1))
	}

	// Window 2 -> PROTO_DELTA (against the cached window-1 snapshot).
	for i := 0; i < 10; i++ {
		sa.observe(am, float64(i), base+1000+uint64(i), false, 0, 0)
	}
	envs2 := sa.pc.Drain()
	if !hasEncoding(envs2, precompute.EncodingProtoDelta) {
		t.Fatalf("window 2: expected a PROTO_DELTA envelope, got %v", encodings(envs2))
	}
}

// TestDeltaDefaultOff confirms the conservative default: without enabling delta
// (top-level default false, unset per-metric), the family ships PROTO_FULL
// every window.
func TestDeltaDefaultOff(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Metrics[0].effectiveDelta(cfg.DeltaTransmission) {
		t.Fatal("delta should default OFF when neither top-level nor per-metric set")
	}
}
