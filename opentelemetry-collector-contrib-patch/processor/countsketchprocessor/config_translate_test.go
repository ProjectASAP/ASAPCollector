// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countsketchprocessor

import (
	"math/bits"
	"testing"

	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

// TestConfigDimensionsRowHashBudget guards the CountSketch crash fix.
//
// sketchlib bit-slices the single 64-bit per-item hash as row*log2(cols),
// so NewCountSketchWrapper rejects (returns an error) any config with
// rows*ceil(log2(cols)) > 64. The processor's SketchFactory is a
// `func() Sketch` that cannot propagate that error — before the fix it
// did `w, _ := NewCountSketchWrapper(...)` and handed the runtime a nil
// wrapper that SIGSEGV'd on the first observe.
//
// configDimensions must now clamp rows so every (epsilon, delta) in the
// valid (0,1) range yields constructible dimensions. The mvp single-sketch
// demo config (epsilon=0.01, delta=0.01) is the exact case that crashed:
// it derives rows=5, cols=16384 (5*14=70 > 64).
func TestConfigDimensionsRowHashBudget(t *testing.T) {
	cases := []struct{ eps, delta float64 }{
		{0.01, 0.01}, // the demo config that crashed the agent
		{0.001, 0.001},
		{0.005, 0.02},
		{0.02, 0.01},
		{0.05, 0.05},
		{0.1, 0.5},
	}
	for _, tc := range cases {
		cfg := &Config{Epsilon: tc.eps, Delta: tc.delta}
		rows, cols := configDimensions(cfg)

		bitsPerRow := bits.TrailingZeros(uint(cols))
		if got := rows * bitsPerRow; got > maxRowHashBits {
			t.Fatalf("eps=%v delta=%v: rows*log2(cols)=%d*%d=%d exceeds %d-bit budget",
				tc.eps, tc.delta, rows, bitsPerRow, got, maxRowHashBits)
		}

		// The clamped dims must be accepted by the wrapper (no error, non-nil):
		// this is what the SketchFactory relies on to never produce a nil sketch.
		w, err := sketches.NewCountSketchWrapper(rows, cols)
		if err != nil {
			t.Fatalf("eps=%v delta=%v: NewCountSketchWrapper(%d,%d) rejected clamped dims: %v",
				tc.eps, tc.delta, rows, cols, err)
		}
		if w == nil {
			t.Fatalf("eps=%v delta=%v: nil wrapper for clamped dims (%d,%d)", tc.eps, tc.delta, rows, cols)
		}
	}
}

// TestConfigDimensionsClampPreservesWidth pins the demo case: epsilon=0.01
// keeps the full epsilon-driven width (cols=16384) and only rows is clamped
// down from the delta-derived 5 to the in-budget 4.
func TestConfigDimensionsClampPreservesWidth(t *testing.T) {
	rows, cols := configDimensions(&Config{Epsilon: 0.01, Delta: 0.01})
	if cols != 16384 {
		t.Fatalf("epsilon=0.01: want cols=16384 (width preserved), got %d", cols)
	}
	if rows != 4 {
		t.Fatalf("delta=0.01,cols=16384: want rows clamped to 4 (4*14=56<=64), got %d", rows)
	}
}

// TestResolveOutputMetricName guards the topk dispatch fix: a configured
// metric_name is used as the emitted sketch's metric name (so it registers
// under the name the backend streaming-config + PromQL queries key by);
// when unset it falls back to the legacy fixed name for back-compat.
func TestResolveOutputMetricName(t *testing.T) {
	if got := resolveOutputMetricName(&Config{MetricName: "top_endpoint_qps"}); got != "top_endpoint_qps" {
		t.Fatalf("configured metric_name: want top_endpoint_qps, got %q", got)
	}
	if got := resolveOutputMetricName(&Config{}); got != outputMetricName {
		t.Fatalf("unset metric_name: want legacy %q, got %q", outputMetricName, got)
	}
	// The emitted PrecomputeConfig must carry the resolved name.
	if got := toPrecomputeConfig(&Config{MetricName: "top_endpoint_qps"}).MetricName; got != "top_endpoint_qps" {
		t.Fatalf("toPrecomputeConfig MetricName: want top_endpoint_qps, got %q", got)
	}
}
