// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// TestRawBufferEmitsExpectedCount wires the same View + Aggregation
// construction that main.go uses, but replaces the OTLP gRPC exporter
// with a ManualReader so we can count DataPoints deterministically.
//
// Contract under test: AggregationRawBuffer with FREQ_HZ events/sec
// per series × CARDINALITY series × 2 instruments (counter + gauge)
// should emit exactly that many DataPoints at each ManualReader
// Collect() call, bounded only by MaxEventsPerSeries.
//
// If this count is way below expected, the bug is in the Aggregator
// or View wiring — not in gzip / docker / network.
func TestRawBufferEmitsExpectedCount(t *testing.T) {
	const (
		cardinality  = 50
		samplesEach  = 3 // per series per instrument
		maxBufferCap = 100
	)

	agg := sdkmetric.AggregationRawBuffer{MaxEventsPerSeries: maxBufferCap}
	stream := sdkmetric.Stream{Aggregation: agg}
	view := sdkmetric.NewView(sdkmetric.Instrument{Name: "*"}, stream)

	reader := sdkmetric.NewManualReader()
	res := resource.Default()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(view),
	)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	meter := provider.Meter("diag")
	counter, err := meter.Float64Counter("http_requests_total")
	if err != nil {
		t.Fatal(err)
	}
	gauge, err := meter.Float64Gauge("http_requests_total_latency_ms")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < cardinality; i++ {
		attrs := metric.WithAttributes(
			attribute.String("zone", fmt.Sprintf("z%d", i%4)),
			attribute.String("pod", fmt.Sprintf("pod-%d", i)),
		)
		for s := 0; s < samplesEach; s++ {
			counter.Add(ctx, 1, attrs)
			gauge.Record(ctx, float64(s*10), attrs)
		}
	}

	var got metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &got); err != nil {
		t.Fatal(err)
	}

	var total int
	for _, sm := range got.ScopeMetrics {
		for _, m := range sm.Metrics {
			g, ok := m.Data.(metricdata.Gauge[float64])
			if !ok {
				t.Logf("metric %s data type %T (not Gauge[float64])", m.Name, m.Data)
				continue
			}
			t.Logf("metric=%s datapoints=%d", m.Name, len(g.DataPoints))
			total += len(g.DataPoints)
		}
	}

	want := cardinality * samplesEach * 2 // 2 instruments
	if total != want {
		t.Errorf("raw-buffer total data points = %d, want %d", total, want)
	}
}

// TestRawBufferBytesEstimate gives a rough back-of-envelope figure
// for how big the OTLP payload should be for the §6.2c encoding
// baseline. Not a correctness test — just a sanity floor for the
// sweep's producer_bytes_out_per_s.
func TestRawBufferBytesEstimate(t *testing.T) {
	const (
		cardinality = 50
		freqHz      = 5.0
		windowS     = 5.0
		// OTLP NumberDataPoint ≈ 1 (tag/wire) + 2×(label KV ≈ 30B)
		// + 8 (timestamp) + 8 (value) + framing ≈ 80B.
		bytesPerDataPoint = 80
		instruments       = 2
	)

	eventsPerTick := int(freqHz * windowS * float64(cardinality) * instruments)
	bytesPerTick := eventsPerTick * bytesPerDataPoint
	bytesPerS := float64(bytesPerTick) / windowS

	// This is a LOG statement, not an assertion — just leaves a
	// marker in test output for comparison against sweep data.
	t.Logf("expected raw-buffer emission:")
	t.Logf("  events/tick = freq × window × card × instruments = %d", eventsPerTick)
	t.Logf("  bytes/tick ≈ events × %d = %d B", bytesPerDataPoint, bytesPerTick)
	t.Logf("  bytes/s (uncompressed) ≈ %.0f", bytesPerS)
	_ = time.Second // keep time import alive for future refinements
}
