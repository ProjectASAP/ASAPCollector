// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"

	"github.com/ProjectASAP/sketchlib-go/wire/asapmsgpack"
	"go.uber.org/zap"
)

// adversarialFeed is a stream where the value-rank and the count-rank
// deliberately DISAGREE, so a heap that ranks by occurrence count and a heap
// that ranks by summed value pick different winners:
//
//	endpoint  events  value/event  count-sum  value-sum
//	/big      10      50           10         500   <- value rank #1, count rank #3
//	/mid      30      5            30         150   <- value rank #2, count rank #2
//	/small    100     1            100        100   <- value rank #3, count rank #1
//
//	value ranking : /big  > /mid  > /small
//	count ranking : /small> /mid  > /big
var adversarialFeed = []struct {
	ep    string
	n     int
	value float64
}{
	{"/big", 10, 50},
	{"/mid", 30, 5},
	{"/small", 100, 1},
}

// feedAdversarial drives the aggregator with adversarialFeed and returns the
// decoded wire heap (highest first), via the SAME msgpack decode the ASAPQuery
// backend uses (asapmsgpack.UnmarshalCountSketchWithHeap).
func feedAdversarial(t *testing.T, sa *sketchAggregator) []asapmsgpack.HeapItem {
	t.Helper()
	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	tick := uint64(0)
	for _, f := range adversarialFeed {
		for i := 0; i < f.n; i++ {
			sa.observe(map[string]string{"endpoint": f.ep}, f.value, base+tick, false, 0, 0)
			tick++
		}
	}
	if sa.lastObserveErr != nil {
		t.Fatalf("observe errored: %v", sa.lastObserveErr)
	}
	envs := sa.pc.Drain()
	for _, env := range envs {
		if len(env.Payload) == 0 {
			continue
		}
		_, _, _, heap, _, err := asapmsgpack.UnmarshalCountSketchWithHeap(env.Payload)
		if err != nil {
			continue
		}
		if len(heap) > 0 {
			return heap
		}
	}
	t.Fatal("no non-empty heap-bearing CountSketch envelope was emitted")
	return nil
}

func newAdversarialAggregator(t *testing.T, weightMode string) *sketchAggregator {
	t.Helper()
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics: []MetricFamily{{
			Metric:     "top_endpoint_bytes",
			Family:     FamilyCountSketch,
			EmitHeap:   true,
			ItemLabel:  "endpoint",
			WeightMode: weightMode,
			// Wide matrix → low signed-CountSketch estimate variance so the
			// median-of-rows estimate preserves the (well separated) ranking.
			Rows: 4,
			Cols: 8192,
		}},
		Cold: ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	sa, ok := newSketchAggregator("top_endpoint_bytes", &cfg.Metrics[0], sketchOpts{window: time.Hour}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator returned ok=false")
	}
	return sa
}

// TestWeightMode_DefaultValueWeightedTopK asserts the agent-built heap ranks by
// SUMMED VALUE by default (weight_mode unset), so the top-k-by-value answer has
// recall 1.0 on a stream where value-rank != count-rank. This is the Mode 1
// (modified-OTLP) gap PR #372 left open on the raw-input path.
func TestWeightMode_DefaultValueWeightedTopK(t *testing.T) {
	sa := newAdversarialAggregator(t, "") // default
	if sa.weightMode != topkWeightValue {
		t.Fatalf("default weightMode = %v, want topkWeightValue", sa.weightMode)
	}
	heap := feedAdversarial(t, sa)
	if len(heap) != 3 {
		t.Fatalf("heap must rank all 3 endpoints, got %d: %v", len(heap), heap)
	}
	// Value ranking: /big (500) > /mid (150) > /small (100).
	wantTop := []string{"/big", "/mid", "/small"}
	for i, w := range wantTop {
		if heap[i].Key != w {
			t.Fatalf("value-weighted heap[%d].Key = %q, want %q (full heap %v)", i, heap[i].Key, w, heap)
		}
	}
	// recall@k of the true top-k-by-value set, for k = 1..3.
	trueByValue := map[int][]string{
		1: {"/big"},
		2: {"/big", "/mid"},
		3: {"/big", "/mid", "/small"},
	}
	for k := 1; k <= 3; k++ {
		got := map[string]bool{}
		for i := 0; i < k && i < len(heap); i++ {
			got[heap[i].Key] = true
		}
		hits := 0
		for _, key := range trueByValue[k] {
			if got[key] {
				hits++
			}
		}
		if recall := float64(hits) / float64(k); recall != 1.0 {
			t.Fatalf("value-weighted recall@%d = %.2f, want 1.0 (heap %v)", k, recall, heap)
		}
	}
	// Descending by value.
	for i := 1; i < len(heap); i++ {
		if heap[i-1].Value < heap[i].Value {
			t.Fatalf("heap not descending by value: %v", heap)
		}
	}
	// /small (highest count, lowest value) must NOT be ranked first — the gate
	// that distinguishes value-weighting from the old +1 frequency build.
	if heap[0].Key == "/small" {
		t.Fatalf("value-weighted heap ranked the highest-COUNT item first: %v", heap)
	}
}

// TestWeightMode_CountStillRanksByFrequency asserts the explicit opt-in
// (weight_mode: count) still ranks by occurrence frequency — the textbook
// heavy-hitter top-k — on the same adversarial stream.
func TestWeightMode_CountStillRanksByFrequency(t *testing.T) {
	sa := newAdversarialAggregator(t, "count")
	if sa.weightMode != topkWeightCount {
		t.Fatalf("weightMode = %v, want topkWeightCount", sa.weightMode)
	}
	heap := feedAdversarial(t, sa)
	if len(heap) != 3 {
		t.Fatalf("heap must rank all 3 endpoints, got %d: %v", len(heap), heap)
	}
	// Count ranking: /small (100) > /mid (30) > /big (10).
	wantTop := []string{"/small", "/mid", "/big"}
	for i, w := range wantTop {
		if heap[i].Key != w {
			t.Fatalf("count-weighted heap[%d].Key = %q, want %q (full heap %v)", i, heap[i].Key, w, heap)
		}
	}
	for i := 1; i < len(heap); i++ {
		if heap[i-1].Value < heap[i].Value {
			t.Fatalf("heap not descending by count: %v", heap)
		}
	}
}

// TestWeightMode_AliasesAndValidation pins the accepted aliases (value/sum vs
// count/frequency/freq), the unknown-value rejection, and the rejection of
// weight_mode on a non-emit_heap family.
func TestWeightMode_AliasesAndValidation(t *testing.T) {
	valueAliases := []string{"", "value", "sum", "VALUE", " Value "}
	for _, a := range valueAliases {
		sa := newAdversarialAggregator(t, a)
		if sa.weightMode != topkWeightValue {
			t.Fatalf("alias %q → weightMode %v, want value", a, sa.weightMode)
		}
	}
	countAliases := []string{"count", "frequency", "freq", "COUNT", " Freq "}
	for _, a := range countAliases {
		sa := newAdversarialAggregator(t, a)
		if sa.weightMode != topkWeightCount {
			t.Fatalf("alias %q → weightMode %v, want count", a, sa.weightMode)
		}
	}
	// Unknown weight_mode is rejected at validation.
	bad := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics: []MetricFamily{{
			Metric: "m", Family: FamilyCountSketch, EmitHeap: true,
			ItemLabel: "endpoint", WeightMode: "bogus",
		}},
		Cold: ColdConfig{Enabled: false},
	}
	if err := bad.Validate(); err == nil {
		t.Fatal("Validate must reject an unknown weight_mode")
	}
	// weight_mode on a non-emit_heap family is rejected.
	misplaced := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics: []MetricFamily{{
			Metric: "m", Family: FamilyCountMinSketch, WeightMode: "value",
		}},
		Cold: ColdConfig{Enabled: false},
	}
	if err := misplaced.Validate(); err == nil {
		t.Fatal("Validate must reject weight_mode on a non-emit_heap family")
	}
}
