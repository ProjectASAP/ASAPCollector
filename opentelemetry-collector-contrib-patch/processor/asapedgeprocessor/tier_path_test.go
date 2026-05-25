package asapedgeprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// appendGauge adds a single-attribute gauge metric with n samples to md.
func appendGauge(md pmetric.Metrics, name string, n int) {
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	g := m.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	for i := 0; i < n; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("zone", "z0")
		dp.SetDoubleValue(float64(i % 7))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Millisecond)))
	}
}

// drainColdMetrics force-drains every shard's cold encoder and returns the set
// of metric names that produced XOR-chunk fragments.
func drainColdMetrics(t *testing.T, p *asapEdgeProcessor) map[string]struct{} {
	t.Helper()
	got := map[string]struct{}{}
	for _, sh := range p.shards {
		if sh.cold == nil {
			continue
		}
		frags, derr := sh.cold.Drain(true)
		if derr != nil {
			t.Fatalf("cold drain: %v", derr)
		}
		for _, f := range frags {
			got[f.MetricName] = struct{}{}
		}
	}
	return got
}

// TestTierWarmSkipsColdButBuildsSketch proves a tier=warm metric's samples are
// NOT added to the cold gorilla encoder while its warm sketch IS built, and a
// tier=cold metric is cold-archived but no warm sketch is built. A tier=both
// (default) metric does both, unchanged.
func TestTierWarmSkipsColdButBuildsSketch(t *testing.T) {
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics: []MetricFamily{
			{Metric: "warm_only", Family: FamilyHLL, Tier: TierWarm},
			{Metric: "cold_only", Family: FamilyHLL, Tier: TierCold},
			{Metric: "both_default", Family: FamilyHLL}, // tier omitted => both
		},
		Cold: ColdConfig{Enabled: true}, // no ShipEndpoint => drain-only, no network
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	// The warm aggregator should exist ONLY for warm-eligible tiers.
	for _, sh := range p.shards {
		if _, ok := sh.sketchAggs["warm_only"]; !ok {
			t.Fatalf("warm_only: expected a warm sketch aggregator (tier=warm)")
		}
		if _, ok := sh.sketchAggs["both_default"]; !ok {
			t.Fatalf("both_default: expected a warm sketch aggregator (tier omitted => both)")
		}
		if _, ok := sh.sketchAggs["cold_only"]; ok {
			t.Fatalf("cold_only: did NOT expect a warm sketch aggregator (tier=cold)")
		}
	}

	md := pmetric.NewMetrics()
	appendGauge(md, "warm_only", 30)
	appendGauge(md, "cold_only", 30)
	appendGauge(md, "both_default", 30)
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	// Cold encoder must hold cold_only + both_default, but NOT warm_only.
	cold := drainColdMetrics(t, p)
	if _, ok := cold["warm_only"]; ok {
		t.Fatalf("warm_only was cold-archived; tier=warm must skip the cold encoder")
	}
	if _, ok := cold["cold_only"]; !ok {
		t.Fatalf("cold_only was NOT cold-archived; tier=cold must feed the cold encoder")
	}
	if _, ok := cold["both_default"]; !ok {
		t.Fatalf("both_default was NOT cold-archived; tier=both must feed the cold encoder")
	}
}

// TestTierWarmSketchFlushEmits confirms a tier=warm metric still emits its warm
// sketch envelope on flush (the warm path is fully live for warm/both tiers),
// while the cold tier stays disabled here so only the warm output is forwarded.
func TestTierWarmSketchFlushEmits(t *testing.T) {
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics: []MetricFamily{
			{Metric: "warm_only", Family: FamilyDDSketch, RelativeAccuracy: 0.01, Tier: TierWarm},
		},
		Cold: ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatal(err)
	}

	md := pmetric.NewMetrics()
	appendGauge(md, "warm_only", 30)
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	p.flushAll(context.Background())
	if len(cap.got) != 1 {
		t.Fatalf("expected 1 flushed warm batch, got %d", len(cap.got))
	}
	if cap.got[0].DataPointCount() == 0 {
		t.Fatalf("expected warm sketch envelope data points, got 0")
	}
}

// TestTierConfigValidation rejects an unknown tier and accepts the documented
// set (including the empty default).
func TestTierConfigValidation(t *testing.T) {
	for _, tr := range []Tier{"", TierWarm, TierBoth, TierCold} {
		cfg := &Config{Metrics: []MetricFamily{{Metric: "m", Family: FamilyHLL, Tier: tr}}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("tier %q should be valid: %v", tr, err)
		}
	}
	cfg := &Config{Metrics: []MetricFamily{{Metric: "m", Family: FamilyHLL, Tier: "lukewarm"}}}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("tier %q should be rejected", "lukewarm")
	}
}
