// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestShutdownDrainsColdPartEvenWithExpiredCtx covers P0 #3: when the Shutdown
// context is already expired, the durable cold-part drain must STILL run (under
// a fresh bounded best-effort deadline) instead of being skipped — otherwise
// the partial intchunk block buffered in the accumulators is lost. The previous
// code returned early on ctx.Done() and dropped it.
func TestShutdownDrainsColdPartEvenWithExpiredCtx(t *testing.T) {
	pc := newPartCollector(t)
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		DropOriginal:   true,
		Cold: ColdConfig{
			Enabled:          true,
			Format:           ColdFormatIntchunk,
			ColdPartEndpoint: pc.srv.URL,
			ExternalLabels:   map[string]string{"agent": "edge-exp"},
			BlockDuration:    60 * time.Second,
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	// Feed one in-block flush so the accumulator holds a partial (unsealed) block.
	base := time.Unix(1700000000, 0)
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("cpu_seconds_total")
	g := m.SetEmptyGauge()
	dp := g.DataPoints().AppendEmpty()
	dp.Attributes().PutStr("core", "0")
	dp.SetDoubleValue(42)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(base))
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	p.flushAll(context.Background())

	// Already-expired context.
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	// The flush loop was never started, so flushLoopDone is immediately true and
	// the accumulator drain runs under the fresh grace deadline.
	_ = p.Shutdown(expired) // returns ctx.Err(); the drain still happens

	pc.waitForParts(1, 2*time.Second)
	if got := pc.count(); got != 1 {
		t.Fatalf("parts after Shutdown(expired ctx) = %d, want 1 (partial block must still ship)", got)
	}
}
