// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// observeLatencyMeterName scopes the per-Observe latency histogram to
// the ddsketch shim. Distinct from selfmonitor's meter so the upstream
// vendored selfmonitor stays untouched (we don't own that module).
const observeLatencyMeterName = "github.com/ProjectASAP/asap-precompute-go/processor/ddsketch"

// enableSelfMonitoring wires the OTel-side runtime self-monitor.
// activeSeriesCount sums per-Precompute ActiveSeries counters across
// every per-metric Precompute the shim is currently tracking.
//
// Side effect: also constructs the per-Observe latency histogram
// (`asap_processor_observe_seconds`) and stores it on the processor.
// New per-metric Precompute instances pick this up via
// recordObserveLatency in config_translate.go. Closes Phase 2.11B
// gap #3 — deployed shim now publishes the same per-observation gate
// metric Phase 2.11A measured under `testing.B`.
func (p *ddsketchProcessor) enableSelfMonitoring(settings component.TelemetrySettings, processorID string) {
	monitor, err := selfmonitor.New(settings, processorID, Type.String(), p.activeSeriesCount)
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("ddsketchprocessor: failed to initialize self-monitoring", zap.Error(err))
		}
		return
	}
	p.monitor = monitor

	if settings.MeterProvider == nil {
		return
	}
	meter := settings.MeterProvider.Meter(observeLatencyMeterName)
	hist, err := meter.Float64Histogram(
		"asap_processor_observe_seconds",
		metric.WithDescription("Per-observation latency inside Precompute.Observe (matchers + window admit + sketch update)."),
		metric.WithUnit("s"),
		// Bucket boundaries chosen to span the post-shim Phase 2.11A
		// micro envelope (~80–500 ns/op for DDSketch+sketches, plus
		// upper buckets for tail latency and accidental millisecond
		// outliers). Histogram view config can override these via the
		// MeterProvider; the default works for Phase 2.11B.
		metric.WithExplicitBucketBoundaries(
			50e-9, 100e-9, 250e-9, 500e-9,
			1e-6, 2.5e-6, 5e-6, 10e-6,
			25e-6, 50e-6, 100e-6,
			500e-6, 1e-3, 10e-3,
		),
	)
	if err != nil {
		if p.logger != nil {
			p.logger.Warn("ddsketchprocessor: failed to create observe-latency histogram", zap.Error(err))
		}
		return
	}
	p.observeLatency = hist
	p.observeLatencyAttrs = metric.WithAttributeSet(attribute.NewSet(
		attribute.String("processor.id", processorID),
		attribute.String("processor.type", Type.String()),
	))
}

// recordObserveLatency is the LatencyObserver wired into each
// per-metric Precompute instance. Pre-bound attribute set keeps the
// hot-path allocation-free.
func (p *ddsketchProcessor) recordObserveLatency(d time.Duration) {
	if p == nil || p.observeLatency == nil {
		return
	}
	p.observeLatency.Record(context.Background(), d.Seconds(), p.observeLatencyAttrs)
}

func (p *ddsketchProcessor) shutdownMonitor() {
	if p.monitor != nil {
		p.monitor.Shutdown()
	}
}

func (p *ddsketchProcessor) recordInput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordInput(ctx, md)
	}
}

func (p *ddsketchProcessor) recordOutput(ctx context.Context, md pmetric.Metrics) {
	if p.monitor != nil {
		p.monitor.RecordOutput(ctx, md)
	}
}

func (p *ddsketchProcessor) activeSeriesCount() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	var total int64
	for _, pc := range p.precomputes {
		if s := pc.Stats(); s != nil {
			total += int64(s.ActiveSeries.Load())
		}
	}
	return total
}
