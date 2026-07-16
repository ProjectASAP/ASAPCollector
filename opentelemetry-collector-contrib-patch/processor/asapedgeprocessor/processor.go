// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"hash/maphash"
	"os"
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

	// unsupportedTypeCount counts data points belonging to a metric type the
	// warm/cold aggregation path does not handle yet (Histogram, Summary,
	// ExponentialHistogram). Such metrics are NEVER removed from the
	// passthrough stream regardless of drop_original/tier config — they are
	// always forwarded unchanged so the type is never silently lost. The first
	// occurrence is logged once (loggedUnsupported) to avoid log spam.
	unsupportedTypeCount atomic.Uint64
	loggedUnsupported    atomic.Bool
	// sketchDropCount counts samples dropped by a sketch aggregator's
	// ObserveKeyed (see warm_sketch.go) so latched log-once drops stay
	// observable.
	sketchDropCount atomic.Uint64
	// sketchEncodeDropCount counts flush envelopes dropped because a sketch
	// aggregator's oteladapter.Encode failed (P0-2). Without it an Encode
	// failure dropped the window's envelopes silently; this keeps the loss
	// observable across all aggregators.
	sketchEncodeDropCount atomic.Uint64
	stopCh                chan struct{}
	doneCh                chan struct{}
	flushStarted          bool

	// wakeCh requests an out-of-cycle sub-window flush (see wakeSubWindow in
	// flush.go). Buffered 1 and drained non-blocking: a pending wake already
	// covers any crossing that arrives before the flush loop gets to it, so
	// callers never block on send. Always present, independent of whether
	// SubWindowInterval/subC is configured — a family with insert-time GOS
	// detection wakes the loop regardless of the legacy sub-window ticker.
	wakeCh chan struct{}

	// ctrlChan is the optional control-plane poll channel (nil when the
	// ControlChannel config block is unset). When set, Start() spawns a poll
	// loop that applies received config updates to the live Precompute
	// instances via UpdateConfig, and Shutdown stops it.
	ctrlChan      controlChannel
	ctrlStopCh    chan struct{}
	ctrlDoneCh    chan struct{}
	ctrlStarted   bool
	ctrlLastApply atomic.Uint64
}

func newProcessor(cfg *Config, set processor.Settings, next consumer.Metrics) (*asapEdgeProcessor, error) {
	p := &asapEdgeProcessor{
		cfg:           cfg,
		logger:        set.Logger,
		next:          next,
		telemetry:     set.TelemetrySettings,
		hashSeed:      maphash.MakeSeed(),
		shards:        make([]*shard, cfg.ShardCount),
		sketchMetrics: make(map[string]*MetricFamily),
		configured:    make(map[string]struct{}),
		coldSkip:      make(map[string]struct{}),
		coldEnabled:   cfg.Cold.Enabled,
		coldExtLabels: cfg.Cold.ExternalLabels,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
		wakeCh:        make(chan struct{}, 1),
	}
	for i := range cfg.Metrics {
		m := &cfg.Metrics[i]
		p.configured[m.Metric] = struct{}{}
		// Build the warm aggregator only when the tier includes warm
		// (warm|both). A tier=cold metric is cold-archived only.
		if m.warmEligible() {
			// Sum routes through the same sketchMetrics/precompute path as the
			// sketch families now (FamilySum builds a SumWrapper aggregator and
			// emits a first-class SumAgg envelope); there is no separate sum path.
			p.sketchMetrics[m.Metric] = m
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
	// Default the edge identity to the OS hostname so continuous monitoring
	// activates without explicit config; a blank hostname leaves it disabled.
	if cfg.EdgeID == "" {
		if hn, err := os.Hostname(); err == nil {
			cfg.EdgeID = hn
		}
	}
	for i := range p.shards {
		sh := &shard{
			sketchAggs: make(map[string]*sketchAggregator, len(p.sketchMetrics)),
		}
		for name, fam := range p.sketchMetrics {
			opts := sketchOpts{
				window:         cfg.WindowDuration,
				maxSeries:      uint64(fam.MaxSeries),
				delta:          fam.effectiveDelta(cfg.DeltaTransmission),
				deltaThreshold: fam.DeltaThreshold,
				// P1-1: the warm window uses its OWN late-data grace
				// (default = WindowDuration), decoupled from the cold tier's
				// ~2s reorder grace, so processing-delayed-but-in-window
				// samples are not dropped as late.
				allowedLateness:   cfg.WarmAllowedLateness,
				edgeID:            cfg.EdgeID,
				subWindowInterval: cfg.SubWindowInterval,
				subWindowEpsilon:  cfg.SubWindowEpsilon,
				gosDeltaEpsilon:   fam.GosDeltaEpsilon,
				gosSites:          fam.GosSites,
				gosAnisotropic:    fam.GosAnisotropic,
			}
			if sa, ok := newSketchAggregator(name, fam, opts, p.logger); ok {
				sa.procDropCount = &p.sketchDropCount
				sa.procEncodeDropCount = &p.sketchEncodeDropCount
				sh.sketchAggs[name] = sa
			}
		}
		if p.coldEnabled {
			sh.cold = p.newColdEncoder()
		}
		p.shards[i] = sh
	}
	// Optional control-plane poll channel (nil when control_channel is unset).
	ch, err := newControlChannel(cfg.ControlChannel, p.logger)
	if err != nil {
		return nil, err
	}
	p.ctrlChan = ch
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
	//
	// INVARIANT (cold-ship / control-channel DECOUPLING): cold-fragment shipping
	// is gated SOLELY on cfg.Cold.Enabled — it is started here unconditionally and
	// is NOT coupled to the control channel. The control channel
	// (startControlPlane below) only carries coordinated-sampling config updates
	// (precompute.PrecomputeConfigSet); it must NEVER gate whether cold fragments
	// are shipped. So a static-config edge (cold.enabled: true,
	// control_channel.enabled: false) still ships its cold tier to the merger.
	// Pinned by TestColdShipsWithControlChannelDisabled; do not re-couple these.
	p.shipWorker.start()
	if p.cfg.WindowDuration > 0 {
		p.flushStarted = true
		go p.flushLoop()
	}
	// Control-plane config-poll loop (no-op when control_channel is unset).
	p.startControlPlane()
	return nil
}

func (p *asapEdgeProcessor) Shutdown(ctx context.Context) error {
	// Stop the control-plane poll loop first so no config swap races the drain.
	p.stopControlPlane()
	// Stop any CDM monitor transports (background gRPC stream goroutines).
	for i := range p.shards {
		for _, sa := range p.shards[i].sketchAggs {
			sa.closeMonitor()
		}
	}
	// Stop the flush loop (its final flushAll enqueues the last batch). The
	// previous code returned early on ctx.Done() while waiting on doneCh, which
	// SKIPPED the cold-part accumulator force-seal AND the ship-worker drain,
	// dropping buffered intchunk blocks + queued/spooled fragment batches.
	//
	// We now ALWAYS run the durable drain. The cold-part accumulators are owned
	// by the single flush goroutine and are not lock-protected, so we must NOT
	// touch them until that goroutine has exited (flushLoopDone). To bound
	// Shutdown when the original ctx is already (or nearly) expired, we wait for
	// the loop's doneCh under the original ctx AND a fresh best-effort grace
	// deadline. The ship-worker drain has its own locking and runs regardless.
	flushLoopDone := !p.flushStarted // nothing to wait on if the loop never ran
	if p.flushStarted {
		close(p.stopCh)
		graceCtx, cancel := context.WithTimeout(context.Background(), shutdownDrainGrace)
		select {
		case <-p.doneCh:
			flushLoopDone = true
		case <-ctx.Done():
			// Original deadline hit; give the loop a final bounded grace to
			// finish its last flushAll so the accumulators go quiescent.
			select {
			case <-p.doneCh:
				flushLoopDone = true
			case <-graceCtx.Done():
			}
		}
		cancel()
	}

	// Build the drain context: prefer the caller's ctx, but if it has expired
	// derive a fresh bounded best-effort one so the final ship still gets a
	// chance to deliver instead of being a guaranteed no-op.
	drainCtx := ctx
	var cancel context.CancelFunc
	if ctx.Err() != nil {
		drainCtx, cancel = context.WithTimeout(context.Background(), shutdownDrainGrace)
		defer cancel()
	}
	// Intchunk cold-part path: the final flushAll buffered each shard's last
	// drain but only seals a part at the block boundary, so any partial
	// (span < BlockDuration) block is still in the accumulators. Force-seal +
	// POST them now so no cold samples are lost on Shutdown. Only safe once the
	// flush goroutine has exited (else we'd race its accumulator writes); if it
	// is somehow still running we skip this rather than race — the ship-worker
	// drain below still runs.
	if flushLoopDone {
		p.flushColdPartAccumulators(drainCtx)
	} else {
		p.logger.Warn("asap_edge: flush loop did not stop within drain grace; skipping cold-part accumulator force-seal to avoid a data race")
	}
	p.shipWorker.shutdown(drainCtx)
	return ctx.Err()
}

// shutdownDrainGrace bounds the best-effort durable drain that runs after the
// flush-loop wait expires on Shutdown, so a wedged downstream can't hang the
// process indefinitely while still giving the last buffered block a chance to
// ship.
const shutdownDrainGrace = 5 * time.Second

// forward sends a flushed metrics batch downstream (no-op if empty).
func (p *asapEdgeProcessor) forward(ctx context.Context, out pmetric.Metrics) {
	if out.ResourceMetrics().Len() == 0 {
		return
	}
	if err := p.next.ConsumeMetrics(ctx, out); err != nil {
		p.logger.Warn("asap_edge: forward flushed metrics failed", zap.Error(err))
	}
}
