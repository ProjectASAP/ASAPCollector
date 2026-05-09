// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchmergeprocessor

import (
	"context"
	"testing"

	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const testAlpha = 0.01

func buildDDSketch(t *testing.T, values []float64) *ddsketch.DDSketch {
	t.Helper()
	s := ddsketch.New(testAlpha)
	for _, v := range values {
		s.Update(v)
	}
	return s
}

func protoStateBytes(t *testing.T, s *ddsketch.DDSketch) []byte {
	t.Helper()
	b, err := s.SerializeStateProtoBytes()
	if err != nil {
		t.Fatalf("SerializeStateProtoBytes: %v", err)
	}
	return b
}

func newDDSketchMetricsBatch(metricName, attrKey, attrVal string, payload []byte, encoding pmetric.DDSketchEncoding) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(metricName)
	dm := m.SetEmptyDDSketch()
	dp := dm.DataPoints().AppendEmpty()
	dp.Attributes().PutStr(attrKey, attrVal)
	dp.SetSketch(payload)
	dp.SetEncoding(encoding)
	return md
}

func TestDDSketchMergeProcessor_ProtoIngest(t *testing.T) {
	cfg := &Config{}
	p := newProcessor(cfg, zap.NewNop(), nil)

	src := buildDDSketch(t, []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	payload := protoStateBytes(t, src)

	md := newDDSketchMetricsBatch("ddsketch", "service", "checkout", payload, pmetric.DDSketchEncodingProto)
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
	if got := acc.Count(); got != 10 {
		t.Fatalf("accumulator Count = %d, want 10", got)
	}
}

func TestDDSketchMergeProcessor_DeltaMerge(t *testing.T) {
	cfg := &Config{}
	p := newProcessor(cfg, zap.NewNop(), nil)

	// Step 1: ingest a snapshot.
	snap := buildDDSketch(t, []float64{1, 2, 3, 4, 5})
	if _, err := p.processMetrics(context.Background(),
		newDDSketchMetricsBatch("ddsketch", "service", "checkout",
			protoStateBytes(t, snap), pmetric.DDSketchEncodingProto)); err != nil {
		t.Fatal(err)
	}

	// Step 2: build a "current" sketch with extra inserts and compute the delta against snap.
	current := buildDDSketch(t, []float64{1, 2, 3, 4, 5, 6, 7, 8})
	deltaBytes, err := ddsketch.ComputeDelta(snap, current, 1)
	if err != nil {
		t.Fatalf("ComputeDelta: %v", err)
	}

	if _, err := p.processMetrics(context.Background(),
		newDDSketchMetricsBatch("ddsketch", "service", "checkout",
			deltaBytes, pmetric.DDSketchEncodingProtoDelta)); err != nil {
		t.Fatal(err)
	}

	acc, ok := p.GetAccumulator("service=checkout")
	if !ok {
		t.Fatal("accumulator missing")
	}
	if got := acc.Count(); got != 8 {
		t.Fatalf("after delta merge Count = %d, want 8", got)
	}
}

func TestDDSketchMergeProcessor_DeltaWithoutSnapshotDropped(t *testing.T) {
	cfg := &Config{}
	p := newProcessor(cfg, zap.NewNop(), nil)

	snap := buildDDSketch(t, []float64{1, 2, 3})
	current := buildDDSketch(t, []float64{1, 2, 3, 4, 5})
	deltaBytes, err := ddsketch.ComputeDelta(snap, current, 1)
	if err != nil {
		t.Fatalf("ComputeDelta: %v", err)
	}

	// No prior snapshot ingested → delta should be dropped.
	if _, err := p.processMetrics(context.Background(),
		newDDSketchMetricsBatch("ddsketch", "service", "checkout",
			deltaBytes, pmetric.DDSketchEncodingProtoDelta)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.GetAccumulator("service=checkout"); ok {
		t.Fatal("accumulator should not exist after orphan delta")
	}
}

func TestDDSketchMergeProcessor_MetricNameFilter(t *testing.T) {
	cfg := &Config{MetricName: "request_latency_ddsketch"}
	p := newProcessor(cfg, zap.NewNop(), nil)

	payload := protoStateBytes(t, buildDDSketch(t, []float64{1, 2, 3}))
	if _, err := p.processMetrics(context.Background(),
		newDDSketchMetricsBatch("ddsketch", "service", "checkout", payload, pmetric.DDSketchEncodingProto)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.GetAccumulator("service=checkout"); ok {
		t.Fatal("accumulator should not exist for unmatched metric name")
	}
	if _, err := p.processMetrics(context.Background(),
		newDDSketchMetricsBatch("request_latency_ddsketch", "service", "checkout", payload, pmetric.DDSketchEncodingProto)); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.GetAccumulator("service=checkout"); !ok {
		t.Fatal("accumulator should exist for matched metric name")
	}
}
