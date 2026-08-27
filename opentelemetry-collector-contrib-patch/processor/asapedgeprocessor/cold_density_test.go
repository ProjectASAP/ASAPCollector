// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/ProjectASAP/asap-gorilla-go/coldpart"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// TestColdPathPreservesDensePerSeriesSamples is the cold-density regression
// guard. It feeds a SINGLE window's worth of a dense, per-event stream (the
// shape the raw-buffer producer delivers: one datapoint per generated event,
// distinct timestamps, NOT a single per-window roll-up) and asserts the cold
// archive carries every sample for every series — i.e. ~freqHz×window
// samples/series, never the ~1/series collapse seen when the producer
// pre-aggregates with the OTel-default cumulative-Sum aggregation.
//
// At freqHz=100 over a 6 s window this is 600 samples/series; the test asserts
// the sealed coldpart.Part carries exactly that for each series, demonstrating
// that a 60 s part at 100 Hz would likewise carry ~6000 samples/series. The
// processor→encoder→accumulator chain is verified end-to-end via the real
// flush path (no helper shortcuts).
func TestColdPathPreservesDensePerSeriesSamples(t *testing.T) {
	const (
		freqHz    = 100               // events/series/sec the producer generates
		windowMs  = 6000              // 6 s window
		stepMs    = 1000 / freqHz     // 10 ms between dense events
		perSeries = windowMs / stepMs // 600 dense samples/series
		numSeries = 3
	)

	pc := newPartCollector(t)

	cfg := &Config{
		ShardCount:     1, // single shard => flushAll is the per-tick unit
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Cold: ColdConfig{
			Enabled:          true,
			Format:           ColdFormatIntchunk,
			ColdPartEndpoint: pc.srv.URL,
			ExternalLabels:   map[string]string{"agent": "edge-dense"},
			// A 1 ms block window makes the single flushAll's accumulated span
			// (6 s) cross the boundary and seal the part immediately, so the
			// assertion runs against a real sealed-on-flush part.
			BlockDuration: time.Millisecond,
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	// Build one OTLP batch carrying the full dense window: perSeries datapoints
	// per series, each its own distinct-timestamp datapoint (the raw-buffer
	// producer shape), interleaved across series exactly as the SDK emits them.
	base := time.Unix(1700000000, 0)
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	g := m.SetEmptyGauge()
	for i := 0; i < perSeries; i++ {
		ts := base.Add(time.Duration(i*stepMs) * time.Millisecond)
		for s := 0; s < numSeries; s++ {
			dp := g.DataPoints().AppendEmpty()
			dp.Attributes().PutStr("pod", podName(s))
			dp.SetDoubleValue(float64(i))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		}
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	p.flushAll(context.Background())

	pc.waitForParts(1, 3*time.Second)
	if got := pc.count(); got != 1 {
		t.Fatalf("parts emitted = %d, want exactly 1 (one dense block)", got)
	}

	pc.mu.Lock()
	body := pc.bods[0]
	pc.mu.Unlock()
	part, err := coldpart.OpenPart(body)
	if err != nil {
		t.Fatalf("OpenPart: %v", err)
	}
	if part.NumSeries() != numSeries {
		t.Fatalf("NumSeries = %d, want %d", part.NumSeries(), numSeries)
	}

	got, err := part.Series(nil, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if len(got) != numSeries {
		t.Fatalf("Series returned %d, want %d", len(got), numSeries)
	}
	for _, sd := range got {
		// The core assertion: each series carries the FULL dense run, not the
		// ~1/series collapse. A bug that fed the cold encoder a per-window
		// roll-up (or a producer pre-aggregating to cumulative Sum) would make
		// this 1.
		if len(sd.Samples) != perSeries {
			t.Fatalf("series %s: %d samples/series, want %d (dense 100 Hz × 6 s) — cold density collapsed",
				sd.Labels.String(), len(sd.Samples), perSeries)
		}
		// Timestamps must be the distinct 10 ms-spaced run we fed (strictly
		// increasing), confirming no exact-T dedup collapsed distinct samples.
		for i := 1; i < len(sd.Samples); i++ {
			if sd.Samples[i].T <= sd.Samples[i-1].T {
				t.Fatalf("series %s: non-increasing T at %d (%d <= %d) — dedup collapsed distinct samples",
					sd.Labels.String(), i, sd.Samples[i].T, sd.Samples[i-1].T)
			}
		}
	}
}

func podName(i int) string {
	return "pod-" + string(rune('a'+i))
}
