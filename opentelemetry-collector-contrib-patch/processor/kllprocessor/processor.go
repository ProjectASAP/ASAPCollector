// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package kllprocessor is the OTel KLL processor shim. As of Phase 2
// step 2.6 the windowing / series-keying / snapshot state machine
// lives in github.com/ProjectASAP/asap-precompute-go; this file is a
// thin adapter that decodes pmetric.Metrics into precompute
// Observations, drives a Precompute per input metric, and re-encodes
// the emitted SketchEnvelopes back into the legacy pmetric output
// shape. Encode helpers live in encode.go; sketch wrappers come from
// the canonical asap-precompute-go/sketches package.
package kllprocessor

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
	otelpre "github.com/ProjectASAP/asap-precompute-go/otel"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
)

// kllProcessor is the Phase-2 thin shim. One *Precompute is lazily
// instantiated per input metric name (legacy `metricWindow` keying
// preserved that boundary; the runtime's series key does not include
// metric name, so the shim partitions explicitly).
type kllProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	monitor      *selfmonitor.Monitor

	mu            sync.Mutex
	pcByName      map[string]precompute.Precompute
	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *kllProcessor {
	return &kllProcessor{
		cfg: cfg, logger: logger, nextConsumer: next,
		pcByName: make(map[string]precompute.Precompute),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Capabilities advertises MutatesData=true: the batch path appends
// synthesized RMs onto input md before forwarding (legacy behavior).
func (p *kllProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *kllProcessor) Start(ctx context.Context, _ component.Host) error {
	if p.cfg.Mode != ModeWindow {
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

func (p *kllProcessor) Shutdown(ctx context.Context) error {
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

// ConsumeMetrics is the OTel pipeline entry. Batch grafts synthesized
// output onto input md before forwarding; window observes only and
// forwards input unchanged (PR #211 — chained sketch processors share
// a single pipeline).
func (p *kllProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	p.recordInput(ctx, md)
	if p.cfg.Mode == ModeBatch {
		out, err := p.ProcessBatch(ctx, md)
		if err != nil {
			return err
		}
		appendMetrics(md, out)
		p.recordOutput(ctx, md)
		return p.nextConsumer.ConsumeMetrics(ctx, md)
	}
	if err := p.observeAll(md); err != nil {
		return err
	}
	return p.nextConsumer.ConsumeMetrics(ctx, md)
}

// ProcessBatch / ProcessMetrics / FlushWindow are the public
// test-friendly hooks pinned by ADR-0002 §"Test API contract".
// ProcessBatch and ProcessMetrics return synthesized output without
// touching nextConsumer; FlushWindow forces a tick and forwards via
// nextConsumer (no-op when no closed window has data).
func (p *kllProcessor) ProcessBatch(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if err := p.observeAll(md); err != nil {
		return pmetric.NewMetrics(), err
	}
	return p.tickAndEncode(uint64(time.Now().UnixMilli())), nil
}

func (p *kllProcessor) ProcessMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	return p.ProcessBatch(ctx, md)
}

func (p *kllProcessor) FlushWindow(ctx context.Context) error {
	out := p.tickAndEncode(uint64(time.Now().UnixMilli()))
	if out.ResourceMetrics().Len() == 0 {
		return nil
	}
	p.recordOutput(ctx, out)
	return p.nextConsumer.ConsumeMetrics(ctx, out)
}

// observeAll routes each metric in md through its per-name Precompute,
// lazily constructing instances on first sight.
func (p *kllProcessor) observeAll(md pmetric.Metrics) error {
	obs, err := otelpre.Decode(md, &otelpre.AdapterConfig{ReadAsInt: p.cfg.ReadAsInt})
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range obs {
		if err := p.precomputeForLocked(obs[i].Metric).Observe(&obs[i]); err != nil && p.logger != nil {
			p.logger.Debug("kllprocessor: observe", zap.Error(err))
		}
	}
	return nil
}

// tickAndEncode rotates every per-metric window and synthesizes one
// pmetric.Metrics under a single "otelcol/kllprocessor" scope.
func (p *kllProcessor) tickAndEncode(nowMs uint64) pmetric.Metrics {
	out := pmetric.NewMetrics()
	p.mu.Lock()
	pcs := make(map[string]precompute.Precompute, len(p.pcByName))
	for n, pp := range p.pcByName {
		pcs[n] = pp
	}
	p.mu.Unlock()
	var sm pmetric.ScopeMetrics
	var smInit bool
	for name, pp := range pcs {
		envs := pp.Tick(nowMs)
		if len(envs) == 0 {
			continue
		}
		if !smInit {
			sm = out.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
			sm.Scope().SetName("otelcol/kllprocessor")
			smInit = true
		}
		p.encodeEnvelopes(sm.Metrics(), name, envs)
	}
	return out
}

// precomputeForLocked returns (creating if necessary) the Precompute
// for the given metric name. Caller MUST hold p.mu.
func (p *kllProcessor) precomputeForLocked(name string) precompute.Precompute {
	if pp, ok := p.pcByName[name]; ok {
		return pp
	}
	pp := precompute.New(p.cfg.toPrecomputeConfig(name), func() precompute.Sketch {
		return sketches.NewKLLWrapper(p.cfg.K, p.cfg.Seed)
	}, sketches.KLLObserver{})
	p.pcByName[name] = pp
	return pp
}

