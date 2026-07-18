// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestValidateCountSketchDims rejects dimensions that overflow the 64-bit
// row-hash budget (P0-1a) and accepts the defaults.
func TestValidateCountSketchDims(t *testing.T) {
	cases := []struct {
		name      string
		rows      int
		cols      int
		wantError bool
	}{
		{"defaults (5x2048)", 0, 0, false},    // csmDims -> 5 rows * 11 bits = 55 <= 64
		{"5 x 2048 explicit", 5, 2048, false}, // 5 * 11 = 55
		{"6 x 2048 overflows", 6, 2048, true}, // 6 * 11 = 66 > 64
		{"8 x 256 fits", 8, 256, false},       // 8 * 8 = 64
		{"9 x 256 overflows", 9, 256, true},   // 9 * 8 = 72 > 64
		{"non-pow2 cols rejected", 5, 1000, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				ShardCount:     1,
				WindowDuration: time.Hour,
				Metrics: []MetricFamily{
					{Metric: "events", Family: FamilyCountSketch, Rows: tc.rows, Cols: tc.cols},
				},
				Cold: ColdConfig{Enabled: false},
			}
			err := cfg.Validate()
			if tc.wantError && err == nil {
				t.Fatalf("rows=%d cols=%d: want validation error, got nil", tc.rows, tc.cols)
			}
			if !tc.wantError && err != nil {
				t.Fatalf("rows=%d cols=%d: want no error, got %v", tc.rows, tc.cols, err)
			}
		})
	}
}

// TestNewSketchAggregatorSkipsBadCountSketchDims verifies P0-1(b): when the
// CountSketch wrapper constructor would reject the dimensions, the factory no
// longer discards the error and wires a nil-backed wrapper; instead
// newSketchAggregator returns (nil, false) so the family is skipped.
func TestNewSketchAggregatorSkipsBadCountSketchDims(t *testing.T) {
	// 6 rows * 11 bits (2048) = 66 > 64: the wrapper constructor errors.
	fam := &MetricFamily{Metric: "events", Family: FamilyCountSketch, Rows: 6, Cols: 2048}
	sa, ok := newSketchAggregator("events", fam, sketchOpts{window: time.Hour}, zap.NewNop())
	if ok || sa != nil {
		t.Fatalf("want (nil,false) for over-budget CountSketch dims, got (%v,%v)", sa, ok)
	}

	// emit_heap variant with the same bad dims is also skipped.
	famHeap := &MetricFamily{Metric: "events", Family: FamilyCountSketch, Rows: 6, Cols: 2048, EmitHeap: true, HeapSize: 100}
	saH, okH := newSketchAggregator("events", famHeap, sketchOpts{window: time.Hour}, zap.NewNop())
	if okH || saH != nil {
		t.Fatalf("want (nil,false) for over-budget heap CountSketch dims, got (%v,%v)", saH, okH)
	}
}
