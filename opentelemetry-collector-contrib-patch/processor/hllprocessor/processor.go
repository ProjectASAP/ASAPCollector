// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package hllprocessor is the OTel HLL processor shim. As of Phase 2
// step 2.7 the windowing / series-keying / snapshot runtime
// lives in github.com/ProjectASAP/asap-precompute-go; this file is a
// thin adapter that decodes pmetric.Metrics into precompute
// Observations, drives a Precompute per input metric, and re-encodes
// the emitted SketchEnvelopes back into the legacy pmetric output
// shape. Encode helpers live in encode.go; sketch wrappers come from
// the canonical asap-precompute-go/sketches package.
package hllprocessor

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

// hllProcessor is the Phase-2 thin shim. One *Precompute is lazily
// instantiated per input metric name (legacy `metricWindow` keying
// preserved that boundary; the runtime's series key does not include
// metric name, so the shim partitions explicitly).
type hllProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	monitor      *selfmonitor.Monitor

	mu            sync.Mutex
	pcByName      map[string]precompute.Precompute
	stopCh        chan struct{}
	doneCh        chan struct{}
	windowStarted atomic.Bool

	// cardSnapshots tracks the last reconstructed full HLL bytes per
	// (input metric, series-attrs) so the shim can stamp a non-zero
	// `dp.Cardinality()` on delta-encoded outbound dps. The runtime
	// keeps its own cache for delta computation but does not expose
	// it; rather than reach into the runtime's internals we reproduce
	// the small bit needed for the typed Cardinality field on emit.
	// Only used when TransmitSketch=true.
	cardMu        sync.Mutex
	cardSnapshots map[string][]byte
}

func newProcessor(cfg *Config, logger *zap.Logger, next consumer.Metrics) *hllProcessor {
	return &hllProcessor{
		cfg: cfg, logger: logger, nextConsumer: next,
		pcByName:      make(map[string]precompute.Precompute),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		cardSnapshots: make(map[string][]byte),
	}
}

// Capabilities advertises MutatesData=true: the batch path appends
// synthesized RMs onto input md before forwarding (legacy behavior).
func (p *hllProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *hllProcessor) Start(ctx context.Context, _ component.Host) error {
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

func (p *hllProcessor) Shutdown(ctx context.Context) error {
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

// ConsumeMetrics is the OTel pipeline entry.
//
// Batch mode:
//   - DropOriginal=true (default since the ① bandwidth FAIL fix):
//     forwards ONLY the synthesized sketch output. The raw md is
//     dropped from the outbound stream — it is already preserved
//     upstream by gorillas3processor's archive write.
//   - DropOriginal=false: grafts synthesized output onto md and
//     forwards the union (legacy "originals + sketch summaries"
//     shape).
//
// Window mode:
//   - DropOriginal=true: observes md into per-name Precomputes and
//     forwards an empty pmetric.Metrics so the raw md does not
//     reach the next consumer. The ticker goroutine emits sketch
//     output via FlushWindow on its own cadence (PR #211 still
//     applies — chained sketch processors share a single pipeline).
//   - DropOriginal=false: observes md and forwards md unchanged.
func (p *hllProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	p.recordInput(ctx, md)
	if p.cfg.Mode == ModeBatch {
		out, err := p.ProcessBatch(ctx, md)
		if err != nil {
			return err
		}
		if p.cfg.DropOriginal {
			if out.ResourceMetrics().Len() == 0 {
				return nil
			}
			p.recordOutput(ctx, out)
			return p.nextConsumer.ConsumeMetrics(ctx, out)
		}
		appendMetrics(md, out)
		p.recordOutput(ctx, md)
		return p.nextConsumer.ConsumeMetrics(ctx, md)
	}
	if err := p.observeAll(md); err != nil {
		return err
	}
	if p.cfg.DropOriginal {
		// Window mode: the FlushWindow tick goroutine forwards sketch
		// output on its own cadence; the live ConsumeMetrics call must
		// not also forward the raw md or the wire carries raw + sketch.
		return nil
	}
	return p.nextConsumer.ConsumeMetrics(ctx, md)
}

// ProcessBatch / ProcessMetrics / FlushWindow are the public
// test-friendly hooks pinned by ADR-0002 §"Test API contract".
// ProcessBatch and ProcessMetrics return synthesized output without
// touching nextConsumer; FlushWindow forces a tick and forwards via
// nextConsumer (no-op when no closed window has data).
//
// All three paths route through Precompute.Drain rather than Tick:
// the legacy flushWindow rotated regardless of wall-clock, and
// Drain is the runtime's dedicated unconditional-rotation entry
// point introduced precisely so callers don't need pseudo-timestamp
// workarounds (see asap-precompute-go/precompute.go::Drain).
func (p *hllProcessor) ProcessBatch(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if err := p.observeAll(md); err != nil {
		return pmetric.NewMetrics(), err
	}
	return p.drainAndEncode(), nil
}

func (p *hllProcessor) ProcessMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	return p.ProcessBatch(ctx, md)
}

func (p *hllProcessor) FlushWindow(ctx context.Context) error {
	out := p.drainAndEncode()
	if out.ResourceMetrics().Len() == 0 {
		return nil
	}
	p.recordOutput(ctx, out)
	return p.nextConsumer.ConsumeMetrics(ctx, out)
}

// observeAll routes each metric in md through its per-name Precompute,
// lazily constructing instances on first sight.
func (p *hllProcessor) observeAll(md pmetric.Metrics) error {
	obs, err := otelpre.Decode(md, &otelpre.AdapterConfig{})
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range obs {
		if !p.matchesLegacyMatchers(obs[i].Labels) {
			continue
		}
		if err := p.precomputeForLocked(obs[i].Metric).Observe(&obs[i]); err != nil && p.logger != nil {
			p.logger.Debug("hllprocessor: observe", zap.Error(err))
		}
	}
	return nil
}

// drainAndEncode rotates every per-metric window unconditionally and
// synthesizes one pmetric.Metrics under a single "otelcol/hllprocessor"
// scope. Drain is the right primitive here because the legacy
// flushWindow rotated regardless of wall-clock, the ticker goroutine
// fires once per WindowDuration so every fire wants to drain, and the
// shutdown path needs to flush mid-window state that Tick(time.Now())
// would silently drop.
func (p *hllProcessor) drainAndEncode() pmetric.Metrics {
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
		envs := pp.Drain()
		if len(envs) == 0 {
			continue
		}
		if !smInit {
			sm = out.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
			sm.Scope().SetName("otelcol/hllprocessor")
			smInit = true
		}
		p.encodeEnvelopes(sm.Metrics(), name, envs)
	}
	return out
}

// precomputeForLocked returns (creating if necessary) the Precompute
// for the given metric name. Caller MUST hold p.mu.
func (p *hllProcessor) precomputeForLocked(name string) precompute.Precompute {
	if pp, ok := p.pcByName[name]; ok {
		return pp
	}
	sampleP := p.cfg.SampleP
	pp := precompute.New(p.cfg.toPrecomputeConfig(name), func() precompute.Sketch {
		// WithSampleP(1.0) is an exact no-op, so the default path is
		// byte-identical to the pre-sampling build.
		return sketches.NewHLLWrapper().WithSampleP(sampleP)
	}, sketches.HLLObserver{})
	p.pcByName[name] = pp
	return pp
}

// matchesLegacyMatchers replicates the legacy hllProcessor's
// matchesMatchers semantics on a host-neutral []KeyValue. Filtering
// happens BEFORE Observe so a fully-filtered batch never spawns a
// per-metric Precompute.
func (p *hllProcessor) matchesLegacyMatchers(labels []precompute.KeyValue) bool {
	if len(p.cfg.LabelMatchers) == 0 {
		return true
	}
	for _, m := range p.cfg.LabelMatchers {
		hit := false
		for i := range labels {
			if labels[i].Key == m.Key {
				if labels[i].Value != m.Value {
					return false
				}
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	return true
}
