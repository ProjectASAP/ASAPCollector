// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package ddsketchprocessor implements the DDSketch metrics processor
// as a thin shim that delegates the windowing, snapshot caching, and
// delta encoding to the host-neutral asap-precompute-go
// runtime (ADR-0002, Phase 2 step 2.5). The shim itself only owns
// OTel-side lifecycle, config translation, and quantile materialization
// when TransmitSketch=false; in-place md merging and metadata
// propagation helpers live in shim_helpers.go.
package ddsketchprocessor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// ddsketchProcessor is the Layer-4 OTel shim. State is keyed per
// input-metric-name because the legacy processor emits one synthesized
// output metric per input metric name; the runtime models one
// Precompute per (sketch type, agg_id), so we run one Precompute per
// distinct input metric name.
type ddsketchProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	monitor      *selfmonitor.Monitor

	// observeLatency is the per-Observe-call Prom histogram surfaced on
	// the deployed shim's /metrics. Nil when self-monitoring is
	// disabled or when the meter rejected the histogram registration.
	// See monitor.go::enableSelfMonitoring for construction and
	// config_translate.go::getOrCreate for wiring.
	observeLatency      metric.Float64Histogram
	observeLatencyAttrs metric.RecordOption

	mu          sync.Mutex
	precomputes map[string]precompute.Precompute

	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *ddsketchProcessor {
	return &ddsketchProcessor{
		cfg: cfg, logger: logger, nextConsumer: next,
		precomputes: make(map[string]precompute.Precompute),
		stopCh:      make(chan struct{}), doneCh: make(chan struct{}),
	}
}

func (p *ddsketchProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// Start kicks off the window-mode tick goroutine. Batch mode flushes
// inline inside ConsumeMetrics, so Start is a no-op there.
func (p *ddsketchProcessor) Start(ctx context.Context, _ component.Host) error {
	if p.cfg.Mode != ModeWindow {
		return nil
	}
	ticker := time.NewTicker(p.cfg.WindowDuration)
	go func() {
		p.windowStarted.Store(true)
		defer func() { ticker.Stop(); close(p.doneCh) }()
		for {
			select {
			case <-ctx.Done():
				_ = p.FlushWindow(context.Background())
				return
			case <-p.stopCh:
				_ = p.FlushWindow(context.Background())
				return
			case <-ticker.C:
				_ = p.FlushWindow(context.Background())
			}
		}
	}()
	return nil
}

func (p *ddsketchProcessor) Shutdown(ctx context.Context) error {
	defer p.shutdownMonitor()
	if p.cfg.Mode != ModeWindow || !p.windowStarted.Load() {
		return nil
	}
	close(p.stopCh)
	select {
	case <-p.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ConsumeMetrics is the Layer-4 entry point.
//
// Batch mode:
//   - DropOriginal=true (default since the ① bandwidth FAIL fix):
//     decodes → observes → ticks → encodes → forwards sketch-only.
//     The raw md is dropped from the outbound stream — it is already
//     preserved upstream by gorillas3processor's archive write.
//   - DropOriginal=false: legacy "originals + sketch" shape; merges
//     synthesized sketch metrics into md and forwards the union.
//
// Window mode:
//   - DropOriginal=true: observes only on each call; the tick
//     goroutine drives flushes via FlushWindow. Forwards an empty
//     pmetric so the raw md does not reach the next consumer.
//   - DropOriginal=false: input md is forwarded unchanged so chained
//     processors see the raw inputs (PR #211).
func (p *ddsketchProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	p.recordInput(ctx, md)
	switch p.cfg.Mode {
	case ModeBatch:
		out, err := p.ProcessBatch(ctx, md)
		if err != nil {
			return err
		}
		p.recordOutput(ctx, out)
		return p.nextConsumer.ConsumeMetrics(ctx, out)
	case ModeWindow:
		if err := p.ProcessMetrics(ctx, md); err != nil {
			return err
		}
		if p.cfg.DropOriginal {
			// Window mode: FlushWindow tick goroutine forwards sketch
			// output on its own cadence; the live ConsumeMetrics call
			// must not also forward raw md or the wire carries
			// raw + sketch.
			return nil
		}
		return p.nextConsumer.ConsumeMetrics(ctx, md)
	default:
		return nil
	}
}

// ProcessBatch is the synchronous decode → observe → tick → encode
// path used by batch-mode ConsumeMetrics and exposed as a public test
// hook (ADR-0002 §"Test API contract").
//
//   - DropOriginal=true (default): returns ONLY the synthesized sketch
//     output (sketch envelopes when TransmitSketch=true, gauge-quantile
//     metrics otherwise). The raw md is not included in the return so
//     the outbound pmetric stream carries sketch-only.
//   - DropOriginal=false: legacy shape — returns md with sketch /
//     quantile metrics appended (originals + sketch summaries).
func (p *ddsketchProcessor) ProcessBatch(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.ResourceMetrics().Len() == 0 {
		return md, nil
	}
	batch := make(map[string]precompute.Precompute)
	if err := p.observeInto(md, batch); err != nil {
		return md, err
	}
	sketch := p.flushToMetrics(batch)
	if p.cfg.DropOriginal {
		return sketch, nil
	}
	mergeAppend(md, sketch)
	return md, nil
}

// ProcessMetrics observes md into the shim's persistent per-metric
// Precompute map without flushing. Window-mode ConsumeMetrics calls
// it; tests use it directly per ADR-0002 §"Test API contract".
func (p *ddsketchProcessor) ProcessMetrics(_ context.Context, md pmetric.Metrics) error {
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.precomputes == nil {
		p.precomputes = make(map[string]precompute.Precompute)
	}
	return p.observeInto(md, p.precomputes)
}

// FlushWindow forces a tick across every active per-metric Precompute,
// builds the synthesized output, and forwards it via nextConsumer.
// No-op when no Precomputes have data (matches TestEmptyInput).
//
// Concurrency: swaps the per-metric Precompute map under the shim's
// mutex so the flush loop owns the snapshot exclusively. Concurrent
// ProcessMetrics calls land in the freshly-installed empty map and
// don't race with the in-flight flush.
func (p *ddsketchProcessor) FlushWindow(ctx context.Context) error {
	p.mu.Lock()
	live := p.precomputes
	p.precomputes = make(map[string]precompute.Precompute)
	p.mu.Unlock()
	if len(live) == 0 {
		return nil
	}
	out := p.flushToMetrics(live)
	if out.ResourceMetrics().Len() == 0 {
		return nil
	}
	p.recordOutput(ctx, out)
	return p.nextConsumer.ConsumeMetrics(ctx, out)
}

