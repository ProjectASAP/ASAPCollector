// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"encoding/binary"
	"math"
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// TestSketchAggregatorObserve_RowSampled_RescalesByP drives observe() with
// rowSampled=true directly against a CMS aggregator and checks the emitted
// envelope's estimate reflects the SDK's 1/SampleP correction — proving
// ApplyAdmittedOccurrence, not InsertHash, was used.
func TestSketchAggregatorObserve_RowSampled_RescalesByP(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "m", Family: FamilyCountMinSketch}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sa, ok := newSketchAggregator("m", &cfg.Metrics[0], sketchOpts{window: time.Hour}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator(CountMinSketch) returned ok=false")
	}

	const n, p = 1000, 0.5
	am := map[string]string{"zone": "z0"}
	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	allRows := uint64(0b11111) // csmDims defaults to 5 rows
	for i := 0; i < n; i++ {
		sa.observe(am, 1, base+uint64(i), true, allRows, p)
	}
	if sa.lastObserveErr != nil {
		t.Fatalf("row-sampled observe errored: %v", sa.lastObserveErr)
	}

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
	want := float64(n) / p
	got := rebuilt.EstimateCount(key)
	if rel := (got - want) / want; rel < -0.1 || rel > 0.1 {
		t.Fatalf("EstimateCount = %v, want ~%v (n/p rescale)", got, want)
	}
}

// TestSketchAggregatorObserve_RowSampled_UnsupportedFamilyDrops verifies a
// row-sampled observation against an unsupported scalar family is dropped
// rather than silently
// misapplied — this can only happen if the SDK's AggregationRouter and this
// collector's AggID disagreed about which family a PolicyFingerprint
// targets, and that must fail loud (a counted drop), not corrupt state.
func TestSketchAggregatorObserve_RowSampled_UnsupportedFamilyDrops(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyKLL}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sa, ok := newSketchAggregator("lat", &cfg.Metrics[0], sketchOpts{window: time.Hour}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator(KLL) returned ok=false")
	}

	before := sa.droppedSamples.Load()
	sa.observe(map[string]string{"zone": "z0"}, 1, uint64(time.Now().UnixMilli()), true, 0b1, 0.5)
	if got := sa.droppedSamples.Load(); got != before+1 {
		t.Fatalf("droppedSamples = %d, want %d (row-sampled obs against obsKindFloat must drop)", got, before+1)
	}
}

func TestSketchAggregatorObserve_RowSampledSumAndDDSketch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		family FamilyKind
		value  float64
	}{
		{name: "sum", family: FamilySum, value: 4},
		{name: "ddsketch", family: FamilyDDSketch, value: 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fam := MetricFamily{Metric: "m", Family: tc.family, RelativeAccuracy: 0.01}
			cfg := &Config{ShardCount: 1, WindowDuration: time.Hour, Metrics: []MetricFamily{fam}, Cold: ColdConfig{Enabled: false}}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			sa, ok := newSketchAggregator("m", &cfg.Metrics[0], sketchOpts{window: time.Hour}, zap.NewNop())
			if !ok {
				t.Fatalf("newSketchAggregator(%s) returned ok=false", tc.family)
			}
			sa.observe(map[string]string{"zone": "z0"}, tc.value, uint64(time.Now().UnixMilli()), true, 1, 0.25)
			if sa.lastObserveErr != nil {
				t.Fatalf("row-sampled observe: %v", sa.lastObserveErr)
			}
			envs := sa.pc.Drain()
			if got := len(envs); got == 0 {
				t.Fatal("row-sampled observation emitted no aggregate")
			}
			switch tc.family {
			case FamilySum:
				if len(envs[0].Payload) != 16 {
					t.Fatalf("Sum payload length = %d, want 16", len(envs[0].Payload))
				}
				got := math.Float64frombits(binary.LittleEndian.Uint64(envs[0].Payload[:8]))
				if got != tc.value/0.25 {
					t.Fatalf("sampled Sum = %v, want %v", got, tc.value/0.25)
				}
			case FamilyDDSketch:
				rebuilt := sketches.NewDDSketchWrapper(0.01)
				if err := rebuilt.ApplyDelta(envs[0].Payload); err != nil {
					t.Fatalf("ApplyDelta: %v", err)
				}
				if got := rebuilt.Quantile(0.5); math.Abs(got-tc.value) > 1 {
					t.Fatalf("sampled DDSketch quantile = %v, want near %v", got, tc.value)
				}
			}

			before := sa.droppedSamples.Load()
			sa.observeRowSampled(map[string]string{"zone": "z0"}, tc.value, uint64(time.Now().UnixMilli()), 1, 2, 0.25)
			if got := sa.droppedSamples.Load(); got != before+1 {
				t.Fatalf("invalid row count drops = %d, want %d", got, before+1)
			}
		})
	}
}

// TestConsumeMetrics_RowSampled_StripsReservedAttrsAndRescales drives the
// real OTLP ingest path (ConsumeMetrics): a Gauge data point carrying the 3
// reserved row-sampled attrs must be (a) routed through
// ApplyAdmittedOccurrence with the correct 1/SampleP rescale and (b) have the
// reserved keys stripped before they ever reach series identity — they must
// not appear in any emitted output label.
func TestConsumeMetrics_RowSampled_StripsReservedAttrsAndRescales(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "conns", Family: FamilyCountMinSketch}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	sink := &capMetrics{}
	p, err := newProcessor(cfg, set, sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	const n, sampleP = 200, 0.4
	allRows := uint64(0b11111)
	base := time.Unix(1700000000, 0)
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("conns")
	g := m.SetEmptyGauge()
	for i := 0; i < n; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("zone", "z0")
		dp.Attributes().PutInt(rowSampledAdmittedRowsKey, int64(allRows))
		dp.Attributes().PutInt(rowSampledRowsKey, 5)
		dp.Attributes().PutDouble(rowSampledSamplePKey, sampleP)
		dp.SetDoubleValue(1)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Millisecond)))
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	sa := p.shards[0].sketchAggs["conns"]
	if sa == nil {
		t.Fatal("no sketch aggregator for \"conns\"")
	}
	if sa.lastObserveErr != nil {
		t.Fatalf("row-sampled ingest errored: %v", sa.lastObserveErr)
	}

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

	// The series key must be built from {zone=z0} ONLY — the 3 reserved keys
	// must never reach AttributesKey/series identity.
	key := []byte(precompute.AttributesKey([]precompute.KeyValue{{Key: "zone", Value: "z0"}}, nil))
	want := float64(n) / sampleP
	got := rebuilt.EstimateCount(key)
	if rel := (got - want) / want; rel < -0.1 || rel > 0.1 {
		t.Fatalf("EstimateCount = %v, want ~%v (n/sampleP rescale)", got, want)
	}

	// A key built with any reserved attribute folded in must estimate ~0 —
	// proof the reserved attrs never leaked into the series-identifying set.
	poisoned := []byte(precompute.AttributesKey([]precompute.KeyValue{
		{Key: "zone", Value: "z0"},
		{Key: rowSampledAdmittedRowsKey, Value: "31"},
	}, nil))
	if got := rebuilt.EstimateCount(poisoned); got > float64(n)/sampleP*0.1 {
		t.Fatalf("EstimateCount(poisoned key with reserved attr) = %v, want ~0 (reserved attrs leaked into series identity)", got)
	}
}
