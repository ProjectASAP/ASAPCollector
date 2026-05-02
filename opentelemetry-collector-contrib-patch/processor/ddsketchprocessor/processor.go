// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package ddsketchprocessor implements the DDSketch metrics processor
// as a thin shim that delegates the windowing, snapshot caching, and
// delta encoding state machine to the host-neutral asap-precompute-go
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

// ConsumeMetrics is the Layer-4 entry point. Batch mode synchronously
// decodes → observes → ticks → encodes → merges into md → forwards.
// Window mode observes-only on each call (state lives across calls);
// the tick goroutine drives flushes. Input md is forwarded unchanged
// in window mode so chained processors see the raw inputs (PR #211).
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
		return p.nextConsumer.ConsumeMetrics(ctx, md)
	default:
		return nil
	}
}

// ProcessBatch is the synchronous decode → observe → tick → encode →
// append-into-md path used by batch-mode ConsumeMetrics and exposed
// as a public test hook (ADR-0002 §"Test API contract"). Returns md
// with sketch/quantile metrics appended.
func (p *ddsketchProcessor) ProcessBatch(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.ResourceMetrics().Len() == 0 {
		return md, nil
	}
	batch := make(map[string]precompute.Precompute)
	if err := p.observeInto(md, batch); err != nil {
		return md, err
	}
	mergeAppend(md, p.flushToMetrics(batch))
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

