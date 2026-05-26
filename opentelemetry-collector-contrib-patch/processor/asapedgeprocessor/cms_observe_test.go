// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.uber.org/zap"
)

// TestCountMinSketchRecordsFrequency guards the fused CountMinSketch path.
// CMSObserver only accepts KindBytes (it hashes the encoded attribute key to
// count series cardinality, not the numeric value). The fused observe used to
// hand it precompute.FloatValue for every family, so CMSObserver.Observe
// rejected every sample and the (then-swallowed) error left the sketch empty —
// an envelope was still emitted, so the existing presence-only test stayed
// green while nothing was recorded.
//
// This asserts both halves of the fix: ObserveKeyed no longer errors for CMS,
// and the emitted envelope, once reconstructed, actually estimates the inserted
// key's frequency.
func TestCountMinSketchRecordsFrequency(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "m", Family: FamilyCountMinSketch}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sa, ok := newSketchAggregator("m", &cfg.Metrics[0], time.Hour, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator(CountMinSketch) returned ok=false")
	}

	const n = 40
	am := map[string]string{"zone": "z0"}
	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	for i := 0; i < n; i++ {
		sa.observe(am, float64(i), base+uint64(i))
	}

	// Core invariant: the value kind matches the observer. Pre-fix this is
	// non-nil ("CMSObserver: expected KindBytes, got KindFloat").
	if sa.lastObserveErr != nil {
		t.Fatalf("CMS observe errored — value-kind regressed: %v", sa.lastObserveErr)
	}

	// Semantic check: reconstruct the emitted CMS and confirm it estimates the
	// inserted attribute key's frequency (~n), not zero.
	envs := sa.pc.Drain()
	rows, cols := csmDims(&cfg.Metrics[0])
	rebuilt := sketches.NewCMSWrapper(rows, cols, false)
	gotEnvelope := false
	for _, env := range envs {
		if env.SketchType != precompute.SketchTypeCountMinSketch || len(env.Payload) == 0 {
			continue
		}
		if err := rebuilt.ApplyDelta(env.Payload); err != nil {
			t.Fatalf("ApplyDelta(payload): %v", err)
		}
		gotEnvelope = true
	}
	if !gotEnvelope {
		t.Fatal("no CountMinSketch envelope with a non-empty payload was emitted")
	}

	key := []byte(precompute.AttributesKey([]precompute.KeyValue{{Key: "zone", Value: "z0"}}, nil))
	// CMS never underestimates; with a single distinct key there is nothing to
	// collide with, so the estimate should be the exact insert count.
	if got := rebuilt.EstimateCount(key); got < float64(n) {
		t.Fatalf("EstimateCount(zone=z0)=%v, want >= %d (frequency not recorded)", got, n)
	}
}
