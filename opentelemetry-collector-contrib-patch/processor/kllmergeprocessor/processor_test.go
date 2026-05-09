// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kllmergeprocessor

import (
	"context"
	"testing"

	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// buildKLLPayload constructs a real KLL sketch with a fixed sequence of
// inserts and returns the proto-serialized payload along with a few
// sample queries the test can verify against the merged accumulator.
func buildKLLPayload(t *testing.T, values []float64) []byte {
	t.Helper()
	s := kll.New()
	for _, v := range values {
		s.Update(v)
	}
	payload, err := s.SerializeProtoBytes()
	if err != nil {
		t.Fatalf("SerializeProtoBytes: %v", err)
	}
	return payload
}

func newKLLMetricsBatch(metricName, attrKey, attrVal string, payload []byte, encoding pmetric.KLLSketchEncoding) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	kllMetric := m.SetEmptyKLLSketch()
	dp := kllMetric.DataPoints().AppendEmpty()
	dp.Attributes().PutStr(attrKey, attrVal)
	dp.SetSketch(payload)
	dp.SetEncoding(encoding)
	return md
}

func TestKLLMergeProcessor_ProtoIngest(t *testing.T) {
	cfg := &Config{}
	p := newProcessor(cfg, zap.NewNop(), nil)

	payload := buildKLLPayload(t, []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	md := newKLLMetricsBatch("kll_sketch", "service", "checkout", payload, pmetric.KLLSketchEncodingProto)

	out, err := p.processMetrics(context.Background(), md)
	if err != nil {
		t.Fatalf("processMetrics: %v", err)
	}
	// Pass-through: same number of metrics, same name.
	if got := out.MetricCount(); got != 1 {
		t.Fatalf("MetricCount mismatch: got %d, want 1", got)
	}

	// Accumulator should be populated under the synthesized key.
	acc, ok := p.GetAccumulator("service=checkout")
	if !ok {
		t.Fatal("accumulator missing for key 'service=checkout'")
	}
	if got := acc.Count(); got != 10 {
		t.Fatalf("accumulator Count = %d, want 10", got)
	}
}

func TestKLLMergeProcessor_SecondBatchReplaces(t *testing.T) {
	cfg := &Config{MetricName: "kll_sketch"}
	p := newProcessor(cfg, zap.NewNop(), nil)

	first := buildKLLPayload(t, []float64{1, 2, 3, 4, 5})
	if _, err := p.processMetrics(context.Background(),
		newKLLMetricsBatch("kll_sketch", "service", "checkout", first, pmetric.KLLSketchEncodingProto)); err != nil {
		t.Fatal(err)
	}

	// Send a second batch that contains a strictly larger snapshot;
	// because Proto encoding is a full snapshot, the accumulator
	// should be *replaced* rather than merged.
	second := buildKLLPayload(t, []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120})
	if _, err := p.processMetrics(context.Background(),
		newKLLMetricsBatch("kll_sketch", "service", "checkout", second, pmetric.KLLSketchEncodingProto)); err != nil {
		t.Fatal(err)
	}

	acc, ok := p.GetAccumulator("service=checkout")
	if !ok {
		t.Fatal("accumulator missing")
	}
	if got := acc.Count(); got != 12 {
		t.Fatalf("after second batch Count = %d, want 12 (replaced)", got)
	}
}

func TestKLLMergeProcessor_MetricNameFilter(t *testing.T) {
	cfg := &Config{MetricName: "request_latency_kll"}
	p := newProcessor(cfg, zap.NewNop(), nil)

	payload := buildKLLPayload(t, []float64{1, 2, 3})
	// Wrong metric name → ignored.
	if _, err := p.processMetrics(context.Background(),
		newKLLMetricsBatch("kll_sketch", "service", "checkout", payload, pmetric.KLLSketchEncodingProto)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.GetAccumulator("service=checkout"); ok {
		t.Fatal("accumulator should not exist for unmatched metric name")
	}

	// Correct metric name → ingested.
	if _, err := p.processMetrics(context.Background(),
		newKLLMetricsBatch("request_latency_kll", "service", "checkout", payload, pmetric.KLLSketchEncodingProto)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.GetAccumulator("service=checkout"); !ok {
		t.Fatal("accumulator should exist for matched metric name")
	}
}

func TestKLLMergeProcessor_UnsupportedEncodingDropped(t *testing.T) {
	cfg := &Config{}
	p := newProcessor(cfg, zap.NewNop(), nil)

	payload := buildKLLPayload(t, []float64{1, 2, 3})
	// Msgpack encoding is reserved-but-unsupported for KLL.
	if _, err := p.processMetrics(context.Background(),
		newKLLMetricsBatch("kll_sketch", "service", "checkout", payload, pmetric.KLLSketchEncodingMsgpack)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.GetAccumulator("service=checkout"); ok {
		t.Fatal("accumulator should not exist after unsupported encoding")
	}
}
