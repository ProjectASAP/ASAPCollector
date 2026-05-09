// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hllmergeprocessor

import (
	"context"
	"testing"

	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// buildHLL creates a HyperLogLog sketch and inserts a deterministic set of
// "hash" values. These are not real hashes — InsertWithHash treats them as
// pre-hashed 64-bit values, which is sufficient for testing register state
// round-trips.
func buildHLL(_ *testing.T, hashes []uint64) *hll.HyperLogLog {
	h := hll.New()
	for _, x := range hashes {
		h.InsertWithHash(x)
	}
	return h
}

func protoBytes(t *testing.T, h *hll.HyperLogLog) []byte {
	t.Helper()
	b, err := h.SerializeProtoBytes()
	if err != nil {
		t.Fatalf("SerializeProtoBytes: %v", err)
	}
	return b
}

func newHLLMetricsBatch(metricName, attrKey, attrVal string, payload []byte, encoding pmetric.HLLSketchEncoding) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	hm := m.SetEmptyHLLSketch()
	dp := hm.DataPoints().AppendEmpty()
	dp.Attributes().PutStr(attrKey, attrVal)
	dp.SetSketch(payload)
	dp.SetEncoding(encoding)
	return md
}

// fixedHashes returns a deterministic set of pseudo-hashes that exercise
// many distinct registers (high-bit variation drives the register index).
func fixedHashes(n int) []uint64 {
	out := make([]uint64, n)
	for i := 0; i < n; i++ {
		// Simple xorshift-style spread.
		v := uint64(i+1) * 0x9E3779B97F4A7C15
		v ^= v >> 33
		out[i] = v
	}
	return out
}

func TestHLLMergeProcessor_ProtoIngest(t *testing.T) {
	cfg := &Config{}
	p := newProcessor(cfg, zap.NewNop(), nil)

	src := buildHLL(t, fixedHashes(200))
	expEst := src.Estimate()

	md := newHLLMetricsBatch("hll_sketch", "service", "checkout",
		protoBytes(t, src), pmetric.HLLSketchEncodingProto)
	out, err := p.processMetrics(context.Background(), md)
	if err != nil {
		t.Fatalf("processMetrics: %v", err)
	}
	if got := out.MetricCount(); got != 1 {
		t.Fatalf("MetricCount mismatch: got %d, want 1", got)
	}

	acc, ok := p.GetAccumulator("service=checkout")
	if !ok {
		t.Fatal("accumulator missing")
	}
	if got := acc.Estimate(); got != expEst {
		t.Fatalf("accumulator Estimate = %d, want %d", got, expEst)
	}
}

func TestHLLMergeProcessor_DeltaMerge(t *testing.T) {
	cfg := &Config{}
	p := newProcessor(cfg, zap.NewNop(), nil)

	// Step 1: ingest a snapshot.
	snap := buildHLL(t, fixedHashes(100))
	if _, err := p.processMetrics(context.Background(),
		newHLLMetricsBatch("hll_sketch", "service", "checkout",
			protoBytes(t, snap), pmetric.HLLSketchEncodingProto)); err != nil {
		t.Fatal(err)
	}

	// Step 2: build a "current" sketch with extra inserts.
	current := buildHLL(t, fixedHashes(300))
	delta := hll.ComputeRegisterDelta(snap, current)
	deltaBytes, err := hll.SerializeRegisterDelta(delta)
	if err != nil {
		t.Fatalf("SerializeRegisterDelta: %v", err)
	}
	expEst := current.Estimate()

	if _, err := p.processMetrics(context.Background(),
		newHLLMetricsBatch("hll_sketch", "service", "checkout",
			deltaBytes, pmetric.HLLSketchEncodingDelta)); err != nil {
		t.Fatal(err)
	}

	acc, ok := p.GetAccumulator("service=checkout")
	if !ok {
		t.Fatal("accumulator missing")
	}
	if got := acc.Estimate(); got != expEst {
		t.Fatalf("after delta merge Estimate = %d, want %d", got, expEst)
	}
}

func TestHLLMergeProcessor_DeltaWithoutSnapshotDropped(t *testing.T) {
	cfg := &Config{}
	p := newProcessor(cfg, zap.NewNop(), nil)

	snap := buildHLL(t, fixedHashes(50))
	current := buildHLL(t, fixedHashes(100))
	delta := hll.ComputeRegisterDelta(snap, current)
	deltaBytes, err := hll.SerializeRegisterDelta(delta)
	if err != nil {
		t.Fatalf("SerializeRegisterDelta: %v", err)
	}

	if _, err := p.processMetrics(context.Background(),
		newHLLMetricsBatch("hll_sketch", "service", "checkout",
			deltaBytes, pmetric.HLLSketchEncodingDelta)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.GetAccumulator("service=checkout"); ok {
		t.Fatal("accumulator should not exist after orphan delta")
	}
}

func TestHLLMergeProcessor_MetricNameFilter(t *testing.T) {
	cfg := &Config{MetricName: "request_uniques_hll_cardinality"}
	p := newProcessor(cfg, zap.NewNop(), nil)

	payload := protoBytes(t, buildHLL(t, fixedHashes(50)))
	if _, err := p.processMetrics(context.Background(),
		newHLLMetricsBatch("hll_sketch", "service", "checkout", payload, pmetric.HLLSketchEncodingProto)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.GetAccumulator("service=checkout"); ok {
		t.Fatal("accumulator should not exist for unmatched metric name")
	}
	if _, err := p.processMetrics(context.Background(),
		newHLLMetricsBatch("request_uniques_hll_cardinality", "service", "checkout", payload, pmetric.HLLSketchEncodingProto)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.GetAccumulator("service=checkout"); !ok {
		t.Fatal("accumulator should exist for matched metric name")
	}
}
