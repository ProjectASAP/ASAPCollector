// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// collectCounterDataPoints wires the exact View + Aggregation main.go builds for
// EXPORTER_SDK_AGG=aggName, fires `events` Add(1) calls per series across
// `cardinality` series of one Counter, and returns the total DataPoints the
// periodic reader would emit on one Collect. This is the agent's per-window
// input: it is what the asap_edge cold encoder archives.
func collectCounterDataPoints(t *testing.T, aggName string, maxBuf, cardinality, events int) int {
	t.Helper()

	agg := parseAgg(aggName, maxBuf)
	stream := sdkmetric.Stream{Aggregation: agg}
	view := sdkmetric.NewView(sdkmetric.Instrument{Name: "*"}, stream)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource.Default()),
		sdkmetric.WithView(view),
	)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	counter, err := provider.Meter("density").Float64Counter("http_requests_total")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for s := 0; s < cardinality; s++ {
		attrs := metric.WithAttributes(attribute.String("pod", fmt.Sprintf("pod-%d", s)))
		for e := 0; e < events; e++ {
			counter.Add(ctx, 1, attrs)
		}
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[float64]: // raw-buffer emits one Gauge DP per event
				total += len(d.DataPoints)
			case metricdata.Sum[float64]: // default/sum collapse to one DP/series
				total += len(d.DataPoints)
			}
		}
	}
	return total
}

// TestColdDensityAggContrast is the root-cause guard for the cold-archive
// density bug: the multinode workload (topology.env) must drive the producer
// with EXPORTER_SDK_AGG=raw-buffer, NOT the OTel default. With `default`
// (cumulative Sum) all freqHz events/series in a window collapse to ONE
// datapoint/series, so the agent — and therefore the cold gorilla archive — sees
// ~1 sample/series. raw-buffer keeps every event as its own datapoint, feeding
// the cold tier its dense per-series raw stream.
func TestColdDensityAggContrast(t *testing.T) {
	const (
		cardinality = 5
		// One window at 100 Hz: 100 events/series. Use a comfortably larger
		// per-series buffer cap (the topology default is 4096) so raw-buffer
		// drops nothing.
		eventsPerWindow = 100
		bufCap          = 4096
	)

	// default: cumulative-Sum roll-up => exactly one datapoint per series.
	gotDefault := collectCounterDataPoints(t, "default", bufCap, cardinality, eventsPerWindow)
	if gotDefault != cardinality {
		t.Fatalf("default agg datapoints = %d, want %d (one roll-up/series)", gotDefault, cardinality)
	}

	// raw-buffer: every generated event survives as its own datapoint.
	wantDense := cardinality * eventsPerWindow
	gotRaw := collectCounterDataPoints(t, "raw-buffer", bufCap, cardinality, eventsPerWindow)
	if gotRaw != wantDense {
		t.Fatalf("raw-buffer datapoints = %d, want %d (dense %d Hz × window)", gotRaw, wantDense, eventsPerWindow)
	}

	// The whole point: raw-buffer is ~eventsPerWindow× denser than default. A
	// 60 s window at 100 Hz therefore carries ~6000 samples/series under
	// raw-buffer vs ~1 under default.
	if gotRaw <= gotDefault {
		t.Fatalf("raw-buffer (%d) must be far denser than default (%d)", gotRaw, gotDefault)
	}

	// Guard the cap: a too-small buffer (the old MAX_BUFFER_PER_SERIES=10) drops
	// most of a 100 Hz window — re-starving the cold archive. Document that the
	// cap must exceed freqHz × window.
	gotCapped := collectCounterDataPoints(t, "raw-buffer", 10, cardinality, eventsPerWindow)
	if gotCapped >= wantDense {
		t.Fatalf("raw-buffer with cap=10 kept %d datapoints; expected drops below %d", gotCapped, wantDense)
	}
	if perSeries := gotCapped / cardinality; perSeries != 10 {
		t.Fatalf("raw-buffer cap=10 kept %d/series, want 10 (cap enforced)", perSeries)
	}
}
