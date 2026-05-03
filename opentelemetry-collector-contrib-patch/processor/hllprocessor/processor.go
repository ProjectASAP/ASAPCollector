// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package hllprocessor is the OTel HLL processor shim. As of Phase 2
// step 2.7 the windowing / series-keying / snapshot state machine
// lives in github.com/ProjectASAP/asap-precompute-go; this file is a
// thin adapter that decodes pmetric.Metrics into precompute
// Observations, drives a Precompute per input metric, and re-encodes
// the emitted SketchEnvelopes back into the legacy pmetric output
// shape. Encode helpers live in encode.go; sketch wrappers in
// sketch_wrapper.go.
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

	// flushSeq monotonically advances on every FlushWindow / batch
	// ProcessBatch call so the runtime's per-Precompute window keeps
	// rotating. Without it, repeated Tick(nowMs) calls with the same
	// timestamp would land back in the same already-advanced bucket
	// and the window would refuse to rotate (nowMs < activeEndMs).
	flushSeq atomic.Uint64
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

// ConsumeMetrics is the OTel pipeline entry. Batch grafts synthesized
// output onto input md before forwarding; window observes only and
// forwards input unchanged (PR #211 — chained sketch processors share
// a single pipeline).
func (p *hllProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
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
func (p *hllProcessor) ProcessBatch(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if err := p.observeAll(md); err != nil {
		return pmetric.NewMetrics(), err
	}
	return p.tickAndEncode(p.nextFlushTick()), nil
}

func (p *hllProcessor) ProcessMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	return p.ProcessBatch(ctx, md)
}

func (p *hllProcessor) FlushWindow(ctx context.Context) error {
	out := p.tickAndEncode(p.nextFlushTick())
	if out.ResourceMetrics().Len() == 0 {
		return nil
	}
	p.recordOutput(ctx, out)
	return p.nextConsumer.ConsumeMetrics(ctx, out)
}

// nextFlushTick returns a monotonically-increasing pseudo-timestamp
// that's always far enough in the future to force a window rotation
// on the next Precompute.Tick. The runtime rotates when
// `nowMs >= activeEndMs`; after each rotation the active window
// snaps to a bucket containing the supplied timestamp, so a fixed
// constant would refuse to rotate the second time. Bumping by a
// large stride per call (well above any test's window duration)
// keeps each rotation crisp without leaking real-time semantics
// into the runtime — the legacy flushWindow rotated regardless of
// wall-clock.
func (p *hllProcessor) nextFlushTick() uint64 {
	const stride uint64 = 1 << 32 // ~50 days in ms; far exceeds any window
	const base uint64 = 1 << 50   // start in the deep future to avoid races with real-time observe timestamps
	return base + p.flushSeq.Add(1)*stride
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

// tickAndEncode rotates every per-metric window and synthesizes one
// pmetric.Metrics under a single "otelcol/hllprocessor" scope.
func (p *hllProcessor) tickAndEncode(nowMs uint64) pmetric.Metrics {
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
	pp := precompute.New(p.cfg.toPrecomputeConfig(name), func() precompute.Sketch {
		return newHLLSketchWrapper()
	}, hllSketchObserver{})
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
