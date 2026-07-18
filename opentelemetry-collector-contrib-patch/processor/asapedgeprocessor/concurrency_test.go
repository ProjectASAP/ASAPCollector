// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestConcurrentConsumeMetricsRace covers #11: ConsumeMetrics is hammered from
// many goroutines (the Collector fans out Export calls) to prove the shard hot
// path is race-free under `go test -race`.
func TestConcurrentConsumeMetricsRace(t *testing.T) {
	cfg := &Config{
		ShardCount:     8,
		WindowDuration: 10 * time.Millisecond,
		DropOriginal:   true,
		Metrics: []MetricFamily{
			{Metric: "http_requests_total", Family: FamilySum, AggregateBy: []string{"zone"}},
			{Metric: "http_latency_ms", Family: FamilyDDSketch, RelativeAccuracy: 0.01},
			{Metric: "flows", Family: FamilyCountMinSketch},
			{Metric: "events", Family: FamilyCountSketch},
		},
		Cold: ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background(), componenttestHost{}); err != nil {
		t.Fatal(err)
	}

	const goroutines = 16
	const itersPer = 40
	var wg sync.WaitGroup
	wg.Add(goroutines)
	base := time.Unix(1700000000, 0)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for it := 0; it < itersPer; it++ {
				md := pmetric.NewMetrics()
				sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
				// Sum
				sumM := sm.Metrics().AppendEmpty()
				sumM.SetName("http_requests_total")
				s := sumM.SetEmptySum()
				s.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
				sdp := s.DataPoints().AppendEmpty()
				sdp.Attributes().PutStr("zone", "z"+string(rune('0'+g%4)))
				sdp.SetDoubleValue(1)
				sdp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(it) * time.Millisecond)))
				// DDSketch gauge
				gM := sm.Metrics().AppendEmpty()
				gM.SetName("http_latency_ms")
				gg := gM.SetEmptyGauge()
				gdp := gg.DataPoints().AppendEmpty()
				gdp.Attributes().PutStr("ep", "e"+string(rune('0'+(g+it)%6)))
				gdp.SetDoubleValue(float64(it))
				gdp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(it) * time.Millisecond)))
				// CMS gauge
				cM := sm.Metrics().AppendEmpty()
				cM.SetName("flows")
				cg := cM.SetEmptyGauge()
				cdp := cg.DataPoints().AppendEmpty()
				cdp.Attributes().PutStr("src", "s"+string(rune('0'+g)))
				cdp.SetDoubleValue(1)
				cdp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(it) * time.Millisecond)))
				// CountSketch gauge
				csM := sm.Metrics().AppendEmpty()
				csM.SetName("events")
				csg := csM.SetEmptyGauge()
				csdp := csg.DataPoints().AppendEmpty()
				csdp.Attributes().PutStr("kind", "k"+string(rune('0'+it%3)))
				csdp.SetDoubleValue(1)
				csdp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(it) * time.Millisecond)))

				if err := p.ConsumeMetrics(context.Background(), md); err != nil {
					t.Errorf("ConsumeMetrics: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(shCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
