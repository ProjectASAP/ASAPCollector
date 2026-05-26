// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"hash/maphash"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type asapEdgeProcessor struct {
	cfg       *Config
	logger    *zap.Logger
	next      consumer.Metrics
	telemetry component.TelemetrySettings
	hashSeed  maphash.Seed

	shards        []*shard
	sumMetrics    map[string]*MetricFamily
	sketchMetrics map[string]*MetricFamily
	// configured is the set of every metric name listed in cfg.Metrics
	// (regardless of tier). Used by DropOriginal so a tier=cold metric's raw
	// is dropped too (its cold archive carries the data downstream).
	configured map[string]struct{}
	// coldSkip is the set of configured metric names whose tier excludes the
	// cold gorilla archive (tier=warm). Series for these are NOT added to the
	// cold fragment encoder; everything else (unconfigured or tier∈{both,cold})
	// is archived as before.
	coldSkip map[string]struct{}

	coldEnabled   bool
	coldExtLabels map[string]string
	coldSource    string
	shipper       *fragmentShipper
	shipWorker    *shipWorker
	// coldFormat selects the cold archive wire format. The default
	// (ColdFormatFragment) ships gorilla-XOR fragments via shipWorker (today's
	// behavior, unchanged). ColdFormatIntchunk routes the SAME drained
	// fragments through coldPartShip instead.
	coldFormat   ColdFormat
	coldPartShip *coldPartShipper
	// coldBlockMs is the intchunk cold-part accumulation window (cfg.Cold.
	// BlockDuration, default = WindowDuration) in absolute ms. A per-shard
	// accumulator buffers each flush's drained fragments and only seals + POSTs
	// ONE coldpart.Part once its buffered sample span reaches coldBlockMs, so a
	// part amortizes the per-part index + symbol-table overhead over ~a block's
	// worth of samples/series instead of one flush's ~1-2. Intchunk-only.
	coldBlockMs int64
	// coldAccum holds one accumulator per shard (intchunk format only; nil
	// otherwise). Each is touched only from the single flush goroutine
	// (flushShardWarmCold / flushAll / Shutdown), so it needs no lock.
	coldAccum []*coldPartAccumulator

	windowStartMs atomic.Uint64
	maxObservedMs atomic.Uint64

	stopCh       chan struct{}
	doneCh       chan struct{}
	flushStarted bool
}

func newProcessor(cfg *Config, set processor.Settings, next consumer.Metrics) (*asapEdgeProcessor, error) {
	p := &asapEdgeProcessor{
		cfg:           cfg,
		logger:        set.Logger,
		next:          next,
		telemetry:     set.TelemetrySettings,
		hashSeed:      maphash.MakeSeed(),
		shards:        make([]*shard, cfg.ShardCount),
		sumMetrics:    make(map[string]*MetricFamily),
		sketchMetrics: make(map[string]*MetricFamily),
		configured:    make(map[string]struct{}),
		coldSkip:      make(map[string]struct{}),
		coldEnabled:   cfg.Cold.Enabled,
		coldExtLabels: cfg.Cold.ExternalLabels,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	for i := range cfg.Metrics {
		m := &cfg.Metrics[i]
		p.configured[m.Metric] = struct{}{}
		// Build the warm aggregator only when the tier includes warm
		// (warm|both). A tier=cold metric is cold-archived only.
		if m.warmEligible() {
			if m.Family == FamilySum {
				p.sumMetrics[m.Metric] = m
			} else {
				p.sketchMetrics[m.Metric] = m
			}
		}
		// A tier=warm metric is excluded from the cold gorilla archive.
		if !m.coldEligible() {
			p.coldSkip[m.Metric] = struct{}{}
		}
	}
	if p.coldEnabled {
		p.coldSource = coldSourceFromLabels(p.coldExtLabels)
		p.shipper = newFragmentShipper(cfg.Cold.ShipEndpoint)
		p.shipWorker = newShipWorker(p.shipper, cfg.Cold, p.logger)
		p.coldFormat = cfg.Cold.Format.normalized()
		if p.coldFormat == ColdFormatIntchunk {
			p.coldPartShip = newColdPartShipper(cfg.Cold.ColdPartEndpoint, p.coldExtLabels)
			p.coldBlockMs = cfg.Cold.BlockDuration.Milliseconds()
			p.coldAccum = make([]*coldPartAccumulator, cfg.ShardCount)
			for i := range p.coldAccum {
				p.coldAccum[i] = newColdPartAccumulator(p.coldExtLabels)
			}
		}
	}
	for i := range p.shards {
		sh := &shard{
			sumAggs:    make(map[string]*sumAggregator, len(p.sumMetrics)),
			sketchAggs: make(map[string]*sketchAggregator, len(p.sketchMetrics)),
		}
		for name, fam := range p.sumMetrics {
			sh.sumAggs[name] = newSumAggregator(fam.AggregateBy)
		}
		for name, fam := range p.sketchMetrics {
			if sa, ok := newSketchAggregator(name, fam, cfg.WindowDuration, p.logger); ok {
				sh.sketchAggs[name] = sa
			}
		}
		if p.coldEnabled {
			sh.cold = p.newColdEncoder()
		}
		p.shards[i] = sh
	}
	return p, nil
}

func (p *asapEdgeProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (p *asapEdgeProcessor) Start(_ context.Context, _ component.Host) error {
	p.windowStartMs.Store(uint64(time.Now().UnixMilli()))
	// Start the async ship worker (drains the spool + re-ships failed batches)
	// before the flush loop so the first flush's batch has a worker to receive
	// it. No-op when the cold tier is disabled.
	p.shipWorker.start()
	if p.cfg.WindowDuration > 0 {
		p.flushStarted = true
		go p.flushLoop()
	}
	return nil
}

func (p *asapEdgeProcessor) Shutdown(ctx context.Context) error {
	// Stop the flush loop first (its final flushAll enqueues the last batch),
	// then drain the worker's in-flight queue + one spool pass under the
	// Shutdown deadline — never context.Background() for the final ship.
	if p.flushStarted {
		close(p.stopCh)
		select {
		case <-p.doneCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Intchunk cold-part path: the final flushAll above buffered each shard's
	// last drain but only seals a part at the block boundary, so any partial
	// (span < BlockDuration) block is still in the accumulators. Force-seal +
	// POST them now so no cold samples are lost on Shutdown.
	p.flushColdPartAccumulators(ctx)
	p.shipWorker.shutdown(ctx)
	return nil
}

// forward sends a flushed metrics batch downstream (no-op if empty).
func (p *asapEdgeProcessor) forward(ctx context.Context, out pmetric.Metrics) {
	if out.ResourceMetrics().Len() == 0 {
		return
	}
	if err := p.next.ConsumeMetrics(ctx, out); err != nil {
		p.logger.Warn("asap_edge: forward flushed metrics failed", zap.Error(err))
	}
}
