// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

// Gorilla EDGE processor measurement (FULL processor path). Mirrors
// ddsketchprocessor/processor_bench_test.go (deterministic synthetic pmetric
// input, b.ReportAllocs, ns/op/B/op/allocs/op).
//
// This drives the FUSED asapedgeprocessor through ConsumeMetrics in the
// gorilla cold-fragment path: one metric configured Tier=cold (cold-archive
// ONLY — no warm sketch contamination) with Cold.Enabled and an EMPTY
// ShipEndpoint (drain-only: fragments are XOR-encoded + chunk-managed + queued,
// but NOT POSTed/gzipped — so the bench measures the edge's decode + shard
// route + XOR chunk-management cost minus the network). This captures the
// per-OTLP-batch cost the codec-core bench (in asap-gorilla-go) does not:
// pmetric decode, attribute extraction, shard hashing, and AddSample dispatch.
//
// Note the processor's newColdEncoder hardcodes the default 120 samples/chunk
// (it does not expose SamplesPerChunk), so chunk-size (flush-size) variation is
// measured in the codec-core bench, not here.

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

const (
	edgeProcBenchSeed     int64 = 0x5A9C011EC709072
	edgeProcSamplesPerSer       = 120 // one default chunk's worth per series per batch
	edgeProcBaseUnixSec   int64 = 1700000000
)

// buildEdgeProcMetrics builds a deterministic pmetric.Metrics with `series`
// distinct (host) Gauge series, `perSeries` time-ascending data points each,
// starting at base time `iter*perSeries` seconds — so every benchmark
// iteration's batch carries STRICTLY NEWER timestamps than the previous one.
// This is essential: the encoder drops a sample whose ts <= the series' last
// appended ts, so re-feeding an identical batch each iteration would measure
// the cheap drop path (most samples dropped as duplicates) instead of the XOR
// encode path. Advancing the base per iteration keeps every iteration a real
// encode of `series*perSeries` fresh samples. Gauge matches the counter-sample
// shape the cold gorilla tier XOR-encodes.
func buildEdgeProcMetrics(series, perSeries, iter int) pmetric.Metrics {
	rng := rand.New(rand.NewSource(edgeProcBenchSeed + int64(iter)))
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("node_cpu_seconds_total")
	g := m.SetEmptyGauge()
	base := time.Unix(edgeProcBaseUnixSec+int64(iter*perSeries), 0).UTC()
	vals := make([]float64, series)
	hosts := make([]string, series)
	for s := 0; s < series; s++ {
		vals[s] = 1000 + rng.Float64()*1000
		hosts[s] = fmt.Sprintf("host-%05d", s)
	}
	// Time-ascending across series so the encoder watermark advances + chunks flush.
	for i := 0; i < perSeries; i++ {
		ts := pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Second))
		for s := 0; s < series; s++ {
			vals[s] += rng.Float64()*2 - 0.5
			dp := g.DataPoints().AppendEmpty()
			dp.Attributes().PutStr("host", hosts[s])
			dp.Attributes().PutStr("job", "node")
			dp.SetTimestamp(ts)
			dp.SetDoubleValue(vals[s])
		}
	}
	return md
}

func newEdgeProcForBench(b *testing.B) *asapEdgeProcessor {
	b.Helper()
	cfg := &Config{
		ShardCount:     12,
		WindowDuration: time.Hour, // never auto-rotates inside the bench
		DropOriginal:   true,
		// No Metrics entry => the metric is UNCONFIGURED, which the ingest path
		// cold-archives only (no warm sketch path) — the cleanest gorilla-only
		// route (see ingest.go consumeMetric: coldArchive := !coldSkip).
		// Enabled + empty ShipEndpoint => drain-only: XOR-encode + chunk-manage,
		// no POST/gzip. Measures the pure edge encode + chunk-management cost.
		Cold: ColdConfig{Enabled: true, ReorderGrace: 2 * time.Second},
	}
	if err := cfg.Validate(); err != nil {
		b.Fatalf("validate: %v", err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, &capMetrics{})
	if err != nil {
		b.Fatalf("newProcessor: %v", err)
	}
	return p
}

func benchmarkEdgeProcConsume(b *testing.B, series int) {
	ctx := context.Background()
	// Pre-build a pool of fixtures with advancing, NON-overlapping time windows
	// so cycling them never feeds the encoder a stale (would-be-dropped) batch
	// within one processor lifetime. Built before the timer so fixture
	// construction is excluded from ns/op + B/op. When b.N exceeds the pool, we
	// recreate the processor (fresh encoder state) under StopTimer and continue
	// from the pool head — keeping every TIMED ConsumeMetrics a real encode.
	const pool = 32
	fixtures := make([]pmetric.Metrics, pool)
	for i := range fixtures {
		fixtures[i] = buildEdgeProcMetrics(series, edgeProcSamplesPerSer, i)
	}
	p := newEdgeProcForBench(b)
	b.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i > 0 && i%pool == 0 {
			// Pool wrap: the next fixture's timestamps are OLDER than the encoder's
			// current watermark, so reset to a fresh processor (untimed) to keep the
			// upcoming TIMED encode real rather than a dropped-duplicate path.
			b.StopTimer()
			_ = p.Shutdown(context.Background())
			p = newEdgeProcForBench(b)
			b.StartTimer()
		}
		if err := p.ConsumeMetrics(ctx, fixtures[i%pool]); err != nil {
			b.Fatalf("ConsumeMetrics: %v", err)
		}
	}
}

// BenchmarkEdgeProcConsume_* times one ConsumeMetrics call through the fused
// processor's gorilla cold path (decode + shard route + XOR encode + chunk
// manage) at 100/1k/10k series, each carrying 120 points per call.
func BenchmarkEdgeProcConsume_100Series(b *testing.B) { benchmarkEdgeProcConsume(b, 100) }
func BenchmarkEdgeProcConsume_1kSeries(b *testing.B)  { benchmarkEdgeProcConsume(b, 1000) }
func BenchmarkEdgeProcConsume_10kSeries(b *testing.B) { benchmarkEdgeProcConsume(b, 10000) }

// TestEdgeOTLPInputSizeBaseline reports the OTLP-protobuf-encoded size of the
// exact synthetic batch the edge consumes, as the "vs OTLP" baseline for the
// codec-core bytes/sample measurement (in asap-gorilla-go's
// TestGorillaEdgeBytesPerSample). The edge's job is to shrink this OTLP
// input down to the shipped ASAPFRG1 frame; this gives the numerator's
// before-figure (OTLP bytes/sample) so the edge's compression vs OTLP — not
// just vs an idealized raw 16 B/sample — is visible.
func TestEdgeOTLPInputSizeBaseline(t *testing.T) {
	const series = 1000
	const perSeries = 120
	md := buildEdgeProcMetrics(series, perSeries, 0)
	total := series * perSeries

	marshaler := &pmetric.ProtoMarshaler{}
	otlpBytes := marshaler.MetricsSize(md)
	t.Logf("OTLP-INPUT: %d samples (%d series x %d), OTLP-proto = %d B => %.3f bytes/sample (raw ref = 16 B/sample)",
		total, series, perSeries, otlpBytes, float64(otlpBytes)/float64(total))
}
