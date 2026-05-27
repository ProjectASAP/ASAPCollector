package asapedgeprocessor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"github.com/ProjectASAP/sketchlib-go/common"
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// runItemLabelMetric drives one configured metric through the processor with a
// gauge bearing {zone, <itemLabel>} per data point, then returns the captured
// output metrics.
func runItemLabelMetric(t *testing.T, fam *MetricFamily, dps func(g pmetric.Gauge, base time.Time)) pmetric.Metrics {
	t.Helper()
	cap := &capMetrics{}
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Metrics:        []MetricFamily{*fam},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, cap)
	if err != nil {
		t.Fatalf("newProcessor: %v", err)
	}
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(fam.Metric)
	g := m.SetEmptyGauge()
	dps(g, time.Unix(1700000000, 0))
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	p.flushAll(context.Background())
	if len(cap.got) != 1 {
		t.Fatalf("expected 1 flushed batch, got %d", len(cap.got))
	}
	return cap.got[0]
}

// forEachMetric walks every Metric in md.
func forEachMetric(md pmetric.Metrics, fn func(pmetric.Metric)) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				fn(ms.At(k))
			}
		}
	}
}

// TestHLLItemLabelProjectsAndCounts asserts the BUG-2 HLL behavior:
//   - item_label (user_id) is PROJECTED OUT of the emitted series labels.
//   - there is ONE HLL per group (zone), not one per user_id value.
//   - that HLL counts DISTINCT user_ids in the group.
func TestHLLItemLabelProjectsAndCounts(t *testing.T) {
	fam := &MetricFamily{
		Metric:            "unique_users_per_min",
		Family:            FamilyHLL,
		Tier:              TierWarm,
		ItemLabel:         "user_id",
		DeltaTransmission: boolPtr(false), // full frames, simplest to decode
	}
	// zone z0: 200 distinct users; zone z1: 50 distinct users.
	out := runItemLabelMetric(t, fam, func(g pmetric.Gauge, base time.Time) {
		emit := func(zone string, users int) {
			for rep := 0; rep < 3; rep++ {
				for u := 0; u < users; u++ {
					dp := g.DataPoints().AppendEmpty()
					dp.Attributes().PutStr("zone", zone)
					dp.Attributes().PutStr("user_id", fmt.Sprintf("u%05d", u))
					dp.SetDoubleValue(1)
					dp.SetTimestamp(pcommon.NewTimestampFromTime(base))
				}
			}
		}
		emit("z0", 200)
		emit("z1", 50)
	})

	groups := map[string]int{} // zone -> estimated cardinality
	forEachMetric(out, func(m pmetric.Metric) {
		if m.Type() != pmetric.MetricTypeHLLSketch {
			return
		}
		dps := m.HLLSketch().DataPoints()
		for d := 0; d < dps.Len(); d++ {
			dp := dps.At(d)
			// item_label MUST NOT survive in the output labels.
			if _, ok := dp.Attributes().Get("user_id"); ok {
				t.Fatalf("user_id leaked into emitted HLL labels: %v", dp.Attributes().AsRaw())
			}
			zoneVal := "<none>"
			if zv, ok := dp.Attributes().Get("zone"); ok {
				zoneVal = zv.AsString()
			}
			w := sketches.NewHLLWrapper()
			if err := w.ApplyDelta(dp.Sketch()); err != nil {
				t.Fatalf("decode HLL frame: %v", err)
			}
			groups[zoneVal] = int(w.Estimate())
		}
	})

	if len(groups) != 2 {
		t.Fatalf("expected one HLL per zone (2 groups), got %d: %v", len(groups), groups)
	}
	if e := groups["z0"]; e < 180 || e > 220 {
		t.Fatalf("z0 cardinality %d, want ~200", e)
	}
	if e := groups["z1"]; e < 45 || e > 55 {
		t.Fatalf("z1 cardinality %d, want ~50", e)
	}
}

// TestCMSItemLabelProjectsAndCounts asserts the BUG-2 CMS behavior:
//   - item_label (endpoint) is PROJECTED OUT of the emitted series labels.
//   - there is ONE CMS per group (zone).
//   - the CMS estimates per-endpoint frequency within the group.
func TestCMSItemLabelProjectsAndCounts(t *testing.T) {
	fam := &MetricFamily{
		Metric:            "endpoint_request_freq",
		Family:            FamilyCountMinSketch,
		Tier:              TierWarm,
		Rows:              5,
		Cols:              2048,
		ItemLabel:         "endpoint",
		DeltaTransmission: boolPtr(false),
	}
	// zone z0: /api/ep000 hit 100×, /api/ep001 hit 10×.
	out := runItemLabelMetric(t, fam, func(g pmetric.Gauge, base time.Time) {
		emit := func(zone, ep string, n int) {
			for i := 0; i < n; i++ {
				dp := g.DataPoints().AppendEmpty()
				dp.Attributes().PutStr("zone", zone)
				dp.Attributes().PutStr("endpoint", ep)
				dp.SetDoubleValue(1)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(base))
			}
		}
		emit("z0", "/api/ep000", 100)
		emit("z0", "/api/ep001", 10)
	})

	frames := 0
	forEachMetric(out, func(m pmetric.Metric) {
		if m.Type() != pmetric.MetricTypeCountMinSketch {
			return
		}
		dps := m.CountMinSketch().DataPoints()
		for d := 0; d < dps.Len(); d++ {
			dp := dps.At(d)
			if _, ok := dp.Attributes().Get("endpoint"); ok {
				t.Fatalf("endpoint leaked into emitted CMS labels: %v", dp.Attributes().AsRaw())
			}
			frames++
			sk, err := cms.DeserializeCountMinSketchFromProtoBytes(dp.Sketch())
			if err != nil {
				t.Fatalf("decode CMS frame: %v", err)
			}
			est0 := sk.FastEstimateWithHash(common.FromBytes([]byte("/api/ep000")).Hash)
			est1 := sk.FastEstimateWithHash(common.FromBytes([]byte("/api/ep001")).Hash)
			if est0 < 100 || est0 > 130 {
				t.Fatalf("ep000 freq estimate %.0f, want ~100", est0)
			}
			if est1 < 10 || est1 > 40 {
				t.Fatalf("ep001 freq estimate %.0f, want ~10", est1)
			}
		}
	})
	if frames != 1 {
		t.Fatalf("expected exactly one CMS frame (one per zone group), got %d", frames)
	}
}

func boolPtr(b bool) *bool { return &b }
