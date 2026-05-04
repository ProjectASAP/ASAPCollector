// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package countminsketchprocessor is the OTel CountMinSketch processor
// shim. As of Phase 2 step 2.9 the windowing / series-keying / snapshot
// state machine lives in github.com/ProjectASAP/asap-precompute-go;
// this file is a thin adapter that decodes pmetric.Metrics into
// precompute Observations, drives a Precompute per input metric, and
// re-encodes the emitted SketchEnvelopes back into the legacy pmetric
// output shape. Encode helpers live in shim_helpers.go; sketch
// wrappers come from the canonical asap-precompute-go/sketches
// package; selfmonitor wiring in monitor.go.
package countminsketchprocessor

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
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

// cmsProcessor is the Phase-2 thin shim. One *Precompute is lazily
// instantiated per input metric name (the legacy `seriesKey` is
// `metricName + "::" + encodeKey(dpAttrs)`, partitioning state by
// metric; the runtime's series key does not include metric name, so
// the shim partitions explicitly via per-name Precomputes).
type cmsProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	monitor      *selfmonitor.Monitor

	mu       sync.Mutex
	pcByName map[string]precompute.Precompute

	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool
}

// windowedCountMinSketchProcessor is preserved as a type alias so
// callers / tests that reference the legacy name continue to compile
// without churn. Production code uses cmsProcessor directly.
type windowedCountMinSketchProcessor = cmsProcessor

func newProcessor(cfg *Config, next consumer.Metrics, logger *zap.Logger) *cmsProcessor {
	return &cmsProcessor{
		cfg: cfg, logger: logger, nextConsumer: next,
		pcByName: make(map[string]precompute.Precompute),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Capabilities advertises MutatesData=true: the batch path appends
// synthesized output onto the input md before forwarding (legacy
// behavior preserved via the (pmetric.Metrics, error) return shape).
func (p *cmsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// Start kicks off the window-mode tick goroutine. Batch mode flushes
// inline inside ConsumeMetrics, so Start is a no-op there.
func (p *cmsProcessor) Start(ctx context.Context, _ component.Host) error {
	if p.logger != nil {
		p.logger.Info("Starting Count-Min Sketch processor",
			zap.String("mode", string(p.cfg.Mode)),
			zap.Duration("window_duration", p.cfg.WindowDuration),
		)
	}
	if p.cfg.Mode != ModeWindow {
		return nil
	}
	if p.cfg.WindowDuration <= 0 {
		return nil
	}
	t := time.NewTicker(p.cfg.WindowDuration)
	p.windowStarted.Store(true)
	go func() {
		defer func() { t.Stop(); close(p.doneCh) }()
		for {
			select {
			case <-ctx.Done():
				_ = p.FlushWindow(context.Background())
				return
			case <-p.stopCh:
				_ = p.FlushWindow(context.Background())
				return
			case <-t.C:
				_ = p.FlushWindow(context.Background())
			}
		}
	}()
	return nil
}

func (p *cmsProcessor) Shutdown(ctx context.Context) error {
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

// ConsumeMetrics is the OTel pipeline entry. The signature is the
// processorhelper.ProcessMetricsFunc (`(ctx, md) (pmetric.Metrics,
// error)`) — the helper forwards the returned md to the next
// consumer.
//
// Batch mode synthesizes one output containing originals + sketch
// metrics (DropOriginal=false) or sketch-only (DropOriginal=true).
//
// Window mode observes md into the persistent per-metric Precompute
// map and forwards md unchanged (DropOriginal=false) or empty
// (DropOriginal=true). The tick goroutine drives flushes via
// FlushWindow → nextConsumer.ConsumeMetrics directly.
func (p *cmsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	p.recordInput(ctx, md)
	switch p.cfg.Mode {
	case ModeBatch:
		out, err := p.ProcessBatch(ctx, md)
		if err != nil {
			return md, err
		}
		p.recordOutput(ctx, out)
		return out, nil
	case ModeWindow:
		if err := p.ProcessMetrics(ctx, md); err != nil {
			return md, err
		}
		if p.cfg.DropOriginal {
			return pmetric.NewMetrics(), nil
		}
		p.recordOutput(ctx, md)
		return md, nil
	default:
		if p.logger != nil {
			p.logger.Error("countminsketchprocessor: unknown mode, dropping metrics", zap.Any("mode", p.cfg.Mode))
		}
		return pmetric.NewMetrics(), nil
	}
}

// ProcessBatch / ProcessMetrics / FlushWindow are the public test
// hooks pinned by ADR-0002 §"Test API contract". ProcessBatch
// synchronously decodes → observes → ticks → encodes; the result is
// the legacy "input + sketch" or "sketch-only" md depending on
// DropOriginal. ProcessMetrics observes md into the persistent
// per-metric Precompute map without flushing. FlushWindow forces a
// tick across every active Precompute and forwards via nextConsumer.
func (p *cmsProcessor) ProcessBatch(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.ResourceMetrics().Len() == 0 {
		return md, nil
	}
	// Observe-then-tick: each batch is its own window in batch mode
	// because the per-metric Precompute is rebuilt per call (cleared
	// after Tick drains it). The pcByName map persists across calls
	// for window mode but the tick path drops every drained name.
	if err := p.observeAll(md); err != nil {
		return md, err
	}
	// Each batch is its own window in batch mode (PrecomputeConfig.
	// Mode=Batch makes Tick always drain). Forcing a far-future
	// timestamp ensures the active window rotates regardless of
	// wall-clock. We keep pcByName entries across calls so the
	// runtime's snapshot cache survives — that's what
	// DeltaTransmission needs to compute window-N deltas against
	// the window-(N-1) snapshot.
	const forceTickMs uint64 = 1<<62 - 1
	out := p.tickAndEncode(forceTickMs)
	if p.cfg.DropOriginal {
		return out, nil
	}
	if out.ResourceMetrics().Len() == 0 {
		return md, nil
	}
	// Expansion mode: graft sketch RMs onto md so the legacy
	// "originals + appended sketch summaries" shape holds.
	out.ResourceMetrics().MoveAndAppendTo(md.ResourceMetrics())
	return md, nil
}

func (p *cmsProcessor) ProcessMetrics(_ context.Context, md pmetric.Metrics) error {
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	return p.observeAll(md)
}

func (p *cmsProcessor) FlushWindow(ctx context.Context) error {
	out := p.tickAndEncode(uint64(time.Now().UnixMilli()))
	if out.ResourceMetrics().Len() == 0 {
		return nil
	}
	p.recordOutput(ctx, out)
	return p.nextConsumer.ConsumeMetrics(ctx, out)
}

// precomputeForLocked returns (creating if necessary) the Precompute
// for the given input metric name. Caller MUST hold p.mu.
func (p *cmsProcessor) precomputeForLocked(name string) precompute.Precompute {
	if pp, ok := p.pcByName[name]; ok {
		return pp
	}
	useMsgpack := p.cfg.Encoding == EncodingMsgpack && !p.cfg.DeltaTransmission
	pp := precompute.New(
		p.cfg.toPrecomputeConfig(name),
		func() precompute.Sketch {
			return sketches.NewCMSWrapper(p.cfg.Rows, p.cfg.Columns, useMsgpack)
		},
		sketches.CMSObserver{},
	)
	p.pcByName[name] = pp
	return pp
}
