// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package countsketchprocessor is a thin OTel-host shim around the
// asap-precompute-go runtime. The runtime (windowing, snapshot
// caching, delta encoding) lives in asap-precompute-go; this file owns
// only the OTel binding (decode pmetric → host-neutral Observation,
// encode SketchEnvelope → pmetric, ConsumeMetrics / Start / Shutdown).
//
// The shim is the Layer-4 OTel-side state holder. CountSketch's
// GlobalAggregation=true config means all observations collapse into
// a single shared series — so a single Precompute instance suffices
// (unlike DDSketch / KLL / HLL / CMS which key by metric-name).
//
// Phase 2 step 2.8 — see ADR-0002 and docs/phase-2-execution-plan.md.
package countsketchprocessor

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.uber.org/zap"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	otelpre "github.com/ProjectASAP/asap-precompute-go/otel"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

// countSketchProcessor is the Layer-4 OTel shim over precompute.Precompute.
//
// Lifecycle:
//   - Start spawns a ticker goroutine that drives FlushWindow every
//     WindowDuration in window mode; in batch mode the goroutine is
//     not started.
//   - ConsumeMetrics decodes input → Precompute.Observe each →
//     forwards the input unchanged to next (window mode) or returns
//     the synthesized output via processMetrics (batch mode).
//   - Shutdown cancels the ticker and runs one final flush so
//     in-flight window state is not lost.
type countSketchProcessor struct {
	logger  *zap.Logger
	next    consumer.Metrics
	config  *Config
	monitor *selfmonitor.Monitor

	mu      sync.Mutex
	pc      precompute.Precompute
	adapter *otelpre.Adapter

	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool
}

// newProcessor wires up the runtime + OTel adapter for one Config.
// Does NOT start any goroutine — that happens in Start. Tests that
// instantiate Config literals without calling Validate may leave
// Mode empty; default to Batch here to match the legacy newProcessor.
func newProcessor(logger *zap.Logger, cfg *Config, next consumer.Metrics) *countSketchProcessor {
	if cfg.Mode == "" {
		cfg.Mode = ModeBatch
	}
	rows, cols := configDimensions(cfg)
	// configDimensions clamps rows to the 64-bit row-hash budget, so the
	// dims below are always constructible. Surface the clamp so an operator
	// who set an over-budget (epsilon, delta) learns their delta isn't met.
	if wantRows := int(math.Ceil(math.Log(1 / cfg.Delta))); wantRows > rows && logger != nil {
		logger.Warn("countsketch: rows clamped to fit the 64-bit row-hash budget; "+
			"effective failure probability exceeds the configured delta",
			zap.Int("requested_rows", wantRows), zap.Int("rows", rows), zap.Int("cols", cols))
	}
	pcfg := toPrecomputeConfig(cfg)
	factory := precompute.SketchFactory(func() precompute.Sketch {
		var (
			w   *sketches.CountSketchWrapper
			err error
		)
		if cfg.EmitHeap {
			// Heap-bearing variant: Snapshot() emits the msgpack
			// `{sketch, topk_heap, heap_size}` payload the backend reads
			// via CountMinSketchWithHeap::from_msgpack, promoting the sid
			// to CountSketchWithHeap / FrequencyTopk.
			w, err = sketches.NewCountSketchWithHeapWrapper(rows, cols, cfg.HeapSize)
		} else {
			w, err = sketches.NewCountSketchWrapper(rows, cols)
		}
		if err != nil {
			// Unreachable given the clamp above; kept as a hard guard so a
			// future dims regression fails loudly instead of returning a nil
			// sketch that SIGSEGVs on the first observe (the original bug).
			if logger != nil {
				logger.Error("countsketch: sketch construction failed; using minimal fallback dims",
					zap.Int("rows", rows), zap.Int("cols", cols), zap.Error(err))
			}
			w, _ = sketches.NewCountSketchWrapper(1, 2)
		}
		return w
	})
	pp := precompute.New(pcfg, factory, sketches.CountSketchObserver{DefaultKey: outputMetricName})
	adapter := otelpre.New(&otelpre.AdapterConfig{
		ScopeName: "otelcol/countsketch",
	}, nil)
	return &countSketchProcessor{
		logger:  logger,
		next:    next,
		config:  cfg,
		pc:      pp,
		adapter: adapter,
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
}

// Capabilities implements processor.Metrics. The shim does not mutate
// input md in place: ProcessBatch returns a fresh pmetric.Metrics
// (ADR-0002 §"Test API contract").
func (p *countSketchProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Start launches the window-mode ticker (no-op in batch mode).
func (p *countSketchProcessor) Start(ctx context.Context, _ component.Host) error {
	if p.config.Mode == ModeBatch {
		return nil
	}
	if p.config.WindowDuration <= 0 {
		return nil
	}
	p.windowStarted.Store(true)
	go p.runWindowLoop(ctx)
	return nil
}

// Shutdown stops the ticker, drains one final window, and tears down
// the self-monitor.
func (p *countSketchProcessor) Shutdown(ctx context.Context) error {
	defer p.shutdownMonitor()

	if p.config.Mode != ModeWindow || !p.windowStarted.Load() {
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

// ProcessMetrics is the public test API ADR-0002 promotes from the
// legacy `processMetrics` private method. Decodes input → observes
// each → optionally Ticks (batch mode) → returns the synthesized
// output. Does NOT touch nextConsumer.
func (p *countSketchProcessor) ProcessMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	return p.processMetrics(ctx, md)
}

// ProcessBatch is an alias for ProcessMetrics so tests written
// against the standardized DDSketch-style name keep compiling.
// ADR-0002 lists both spellings as equivalent test hooks.
func (p *countSketchProcessor) ProcessBatch(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	return p.processMetrics(ctx, md)
}

// FlushWindow forces a Tick on the runtime and forwards the
// synthesized envelopes via nextConsumer.ConsumeMetrics. No-op when
// no closed windows have data (matches TestEmptyInput shape).
func (p *countSketchProcessor) FlushWindow(ctx context.Context) error {
	p.mu.Lock()
	out := p.flushToMetrics()
	p.mu.Unlock()
	if out.ResourceMetrics().Len() == 0 {
		return nil
	}
	p.recordOutput(ctx, out)
	return p.next.ConsumeMetrics(ctx, out)
}

// processMetrics is the shared decode/observe core for ConsumeMetrics,
// ProcessMetrics, and ProcessBatch. In batch mode it Ticks inline so
// the caller gets the synthesized output back. In window mode it
// returns either the input md (DropOriginal=false) or an empty
// pmetric (DropOriginal=true) so chained processors can keep working
// on raw samples.
func (p *countSketchProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	p.recordInput(ctx, md)

	p.mu.Lock()
	if err := p.observeInto(md); err != nil {
		p.mu.Unlock()
		return md, err
	}

	switch p.config.Mode {
	case ModeBatch:
		out := p.flushToMetrics()
		p.mu.Unlock()
		if !p.config.DropOriginal {
			if out.ResourceMetrics().Len() == 0 {
				p.recordOutput(ctx, md)
				return md, nil
			}
			merged := pmetric.NewMetrics()
			md.ResourceMetrics().MoveAndAppendTo(merged.ResourceMetrics())
			out.ResourceMetrics().MoveAndAppendTo(merged.ResourceMetrics())
			p.recordOutput(ctx, merged)
			return merged, nil
		}
		p.recordOutput(ctx, out)
		return out, nil
	case ModeWindow:
		p.mu.Unlock()
		if p.config.DropOriginal {
			return pmetric.NewMetrics(), nil
		}
		p.recordOutput(ctx, md)
		return md, nil
	default:
		p.mu.Unlock()
		return pmetric.NewMetrics(), nil
	}
}

// runWindowLoop is the ticker goroutine for window mode. Drives a
// FlushWindow on every WindowDuration; on shutdown, runs one final
// flush so in-flight window state isn't lost.
func (p *countSketchProcessor) runWindowLoop(ctx context.Context) {
	t := time.NewTicker(p.config.WindowDuration)
	defer func() {
		t.Stop()
		_ = p.FlushWindow(context.Background())
		close(p.doneCh)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-t.C:
			if err := p.FlushWindow(context.Background()); err != nil && p.logger != nil {
				p.logger.Error("countsketchprocessor: emit failed", zap.Error(err))
			}
		}
	}
}
