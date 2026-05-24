// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// shard is one independent ingestion partition. Concurrent OTLP Export
// goroutines whose series hash to different shards proceed in parallel —
// each shard serializes only its own aggregators behind mu.
type shard struct {
	mu sync.Mutex
	// cold is the per-shard Gorilla XOR-chunk fragment encoder, fed samples
	// via AddSample. It produces compact XOR-chunk fragments (Drain) that are
	// shipped to the backend merger — the edge no longer builds TSDB blocks.
	// nil when the cold tier is disabled.
	cold *gorilla.StreamingFragmentEncoder
	// sumAggs holds one sumAggregator per Sum-family metric (cross-shard
	// merged at flush).
	sumAggs map[string]*sumAggregator
	// sketchAggs holds one precompute-backed aggregator per sketch-family
	// metric. Series live in a single shard, so these flush independently
	// per shard (no cross-shard merge, unlike sum).
	sketchAggs map[string]*sketchAggregator
}

type asapEdgeProcessor struct {
	cfg       *Config
	logger    *zap.Logger
	next      consumer.Metrics
	telemetry component.TelemetrySettings
	hashSeed  maphash.Seed

	shards        []*shard
	sumMetrics    map[string]*MetricFamily
	sketchMetrics map[string]*MetricFamily

	coldEnabled   bool
	coldExtLabels map[string]string
	coldSource    string
	shipper       *fragmentShipper
	shipWorker    *shipWorker

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
		coldEnabled:   cfg.Cold.Enabled,
		coldExtLabels: cfg.Cold.ExternalLabels,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	for i := range cfg.Metrics {
		m := &cfg.Metrics[i]
		if m.Family == FamilySum {
			p.sumMetrics[m.Metric] = m
		} else {
			p.sketchMetrics[m.Metric] = m
		}
	}
	if p.coldEnabled {
		p.coldSource = coldSourceFromLabels(p.coldExtLabels)
		p.shipper = newFragmentShipper(cfg.Cold.ShipEndpoint)
		p.shipWorker = newShipWorker(p.shipper, cfg.Cold, p.logger)
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
			if sa, ok := newSketchAggregator(name, fam, cfg.WindowDuration); ok {
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

// newColdEncoder makes a fresh per-window Gorilla XOR-chunk fragment encoder.
// Unlike the old TSDB block builder it needs no temp dir — it only buffers a
// bounded out-of-order window per series and emits XOR-chunk fragments on
// Drain.
func (p *asapEdgeProcessor) newColdEncoder() *gorilla.StreamingFragmentEncoder {
	return gorilla.NewStreamingFragmentEncoder(gorilla.StreamingFragmentOptions{
		ReorderGrace: p.cfg.Cold.ReorderGrace,
		Source:       p.coldSource,
	})
}

// coldSourceFromLabels derives the fragment Source (the agent/source id) from
// the configured external labels — checking common keys. Empty is fine.
func coldSourceFromLabels(ext map[string]string) string {
	for _, k := range []string{"agent", "source", "agent_id", "instance"} {
		if v, ok := ext[k]; ok && v != "" {
			return v
		}
	}
	return ""
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
	p.shipWorker.shutdown(ctx)
	return nil
}

// flushLoop drives the warm/cold flush cadence.
//
// Staggered (the default, ShardCount > 1): tick every WindowDuration/ShardCount
// and on tick k flush ONLY shard k%ShardCount's cold fragments + sketches. Each
// shard still flushes once per WindowDuration, but the N shards are phase-shifted
// by WindowDuration/ShardCount, so their state builds + releases interleave
// instead of bursting together — the aggregate mem sawtooth and CPU spike split
// into N smaller, offset ones. The cross-shard sum is merged + emitted once per
// full WindowDuration cycle (sum state is tiny, so it's not the mem driver, and
// a unified flush keeps the backend's per-group delta total unchanged).
//
// Single-flush fallback (ShardCount <= 1 or staggering disabled): tick every
// WindowDuration and flush all shards together (the original behavior).
func (p *asapEdgeProcessor) flushLoop() {
	defer close(p.doneCh)
	if !p.staggered() {
		t := time.NewTicker(p.cfg.WindowDuration)
		defer t.Stop()
		for {
			select {
			case <-p.stopCh:
				p.flushAll(context.Background())
				return
			case <-t.C:
				p.flushAll(context.Background())
			}
		}
	}

	n := len(p.shards)
	t := time.NewTicker(p.cfg.WindowDuration / time.Duration(n))
	defer t.Stop()
	tick := 0
	for {
		select {
		case <-p.stopCh:
			// Final drain: flush every shard (cold + sketch) AND the unified
			// sum so no un-flushed shard is lost on Shutdown.
			p.flushAll(context.Background())
			return
		case <-t.C:
			shardIdx := tick % n
			p.flushShardWarmCold(context.Background(), shardIdx)
			// Once per full window cycle (after the last shard in a round),
			// merge + emit the cross-shard sum so its output cadence and totals
			// stay window-aligned and unchanged.
			if shardIdx == n-1 {
				p.flushSum(context.Background())
			}
			tick++
		}
	}
}

// staggered reports whether the round-robin per-shard flush is active. It is
// disabled (single-flush fallback) for a single shard or a non-positive window.
func (p *asapEdgeProcessor) staggered() bool {
	return len(p.shards) > 1 && p.cfg.WindowDuration > 0
}

// attrMapPool reuses the decoded attribute map across samples (the shared
// decode: pcommon.Map -> map[string]string, used for the gorilla key +
// cold AddSample + sum, then returned).
var attrMapPool = sync.Pool{New: func() any { return make(map[string]string, 16) }}

func getAttrMap(src pcommon.Map) map[string]string {
	m := attrMapPool.Get().(map[string]string)
	for k := range m {
		delete(m, k)
	}
	src.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}

func putAttrMap(m map[string]string) { attrMapPool.Put(m) }

func (p *asapEdgeProcessor) shardForKey(key string) int {
	if len(p.shards) <= 1 {
		return 0
	}
	var h maphash.Hash
	h.SetSeed(p.hashSeed)
	_, _ = h.WriteString(key)
	return int(h.Sum64() % uint64(len(p.shards)))
}

// ConsumeMetrics is the single decode pass: each data point's attributes are
// decoded once, the gorilla series key is built once (for shard selection), and
// the sample is dispatched to its shard's cold fragment encoder + (if
// configured) the metric's warm aggregator. DropOriginal controls raw
// passthrough; warm/cold output is emitted on the flush tick.
func (p *asapEdgeProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				p.consumeMetric(ms.At(k))
			}
		}
	}
	if !p.cfg.DropOriginal {
		return p.next.ConsumeMetrics(ctx, md) // forward everything raw
	}
	// DropOriginal: forward only UNMATCHED (passthrough) metrics — e.g.
	// freshness probes and any metric with no configured family. The raw of
	// AGGREGATED metrics is dropped here; their sum/sketch output is emitted
	// on the flush tick instead. (Cold already archived all metrics above.)
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				_, isSum := p.sumMetrics[m.Name()]
				_, isSketch := p.sketchMetrics[m.Name()]
				return isSum || isSketch
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	return p.next.ConsumeMetrics(ctx, md)
}

func (p *asapEdgeProcessor) consumeMetric(m pmetric.Metric) {
	name := m.Name()
	sumAgg := p.sumMetrics[name] // nil if not a Sum-family metric

	var dps pmetric.NumberDataPointSlice
	switch m.Type() {
	case pmetric.MetricTypeSum:
		dps = m.Sum().DataPoints()
	case pmetric.MetricTypeGauge:
		dps = m.Gauge().DataPoints()
	default:
		return // non-number metrics not handled yet (cold-only TODO)
	}

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		am := getAttrMap(dp.Attributes()) // shared decode (once)
		key := gorilla.SeriesKey(name, am, p.coldExtLabels)
		val := numberValue(dp)
		ts := dp.Timestamp().AsTime()
		p.observeMax(uint64(ts.UnixMilli()))

		tsMs := uint64(ts.UnixMilli())
		sh := p.shards[p.shardForKey(key)]
		sh.mu.Lock()
		if sh.cold != nil {
			// The fragment encoder rekeys internally by (metric, attrs); the
			// shared SeriesKey above is kept for shard selection only.
			_ = sh.cold.AddSample(gorilla.TSDBSample{
				MetricName: name,
				Attributes: am,
				Timestamp:  ts,
				Value:      val,
			})
		}
		if sumAgg != nil {
			sh.sumAggs[name].observe(am, val)
		} else if sa := sh.sketchAggs[name]; sa != nil {
			sa.observe(am, val, tsMs)
		}
		sh.mu.Unlock()
		putAttrMap(am)
	}
}

func (p *asapEdgeProcessor) observeMax(tsMs uint64) {
	for {
		cur := p.maxObservedMs.Load()
		if tsMs <= cur || p.maxObservedMs.CompareAndSwap(cur, tsMs) {
			return
		}
	}
}

func numberValue(dp pmetric.NumberDataPoint) float64 {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(dp.IntValue())
	}
	return dp.DoubleValue()
}

// flushAll drains the window for EVERY shard at once: per shard, swap+drain the
// cold fragment encoder and ship the collected XOR-chunk fragments; flush each
// shard's sketch aggregators; collect+merge sum partials across shards and emit
// one delta Sum metric per Sum metric. Forwards the result downstream.
//
// This is the single-flush path: the fallback cadence (ShardCount <= 1 /
// WindowDuration <= 0) and the final drain on Shutdown both call it so no shard
// is left un-flushed.
func (p *asapEdgeProcessor) flushAll(ctx context.Context) {
	// Cold: drain every shard's encoder and ship as one binary batch.
	if p.coldEnabled {
		var frags []gorilla.Fragment
		for _, sh := range p.shards {
			frags = append(frags, p.drainShardCold(sh)...)
		}
		p.shipFragments(frags)
	}

	out := pmetric.NewMetrics()
	// Warm sum: merge partials across all shards, emit one delta Sum per metric.
	p.appendSumMetrics(out)
	// Warm sketches: per-shard flush (each series lives in one shard).
	for _, sh := range p.shards {
		sh.mu.Lock()
		for _, sa := range sh.sketchAggs {
			sa.flush(out)
		}
		sh.mu.Unlock()
	}
	p.forward(ctx, out)
}

// flushShardWarmCold drains ONE shard's cold fragments and flushes that shard's
// sketch aggregators, then forwards the sketch envelopes. The cross-shard sum is
// NOT touched here — it is merged + emitted on the window-aligned cadence by
// flushSum so its delta totals stay unchanged. This is the staggered per-tick
// unit of work: only shard idx's state is built up and released, so the N shards'
// sawtooths phase-shift instead of releasing in lockstep.
func (p *asapEdgeProcessor) flushShardWarmCold(ctx context.Context, idx int) {
	if idx < 0 || idx >= len(p.shards) {
		return
	}
	sh := p.shards[idx]

	if p.coldEnabled {
		p.shipFragments(p.drainShardCold(sh))
	}

	out := pmetric.NewMetrics()
	sh.mu.Lock()
	for _, sa := range sh.sketchAggs {
		sa.flush(out)
	}
	sh.mu.Unlock()
	p.forward(ctx, out)
}

// flushSum merges the sum partials across ALL shards and emits one delta Sum
// metric per Sum metric, then resets every shard's partials. Run once per
// WindowDuration so the backend's per-group delta total per window is identical
// to the original single-flush behavior.
func (p *asapEdgeProcessor) flushSum(ctx context.Context) {
	if len(p.sumMetrics) == 0 {
		return
	}
	out := pmetric.NewMetrics()
	p.appendSumMetrics(out)
	p.forward(ctx, out)
}

// drainShardCold swaps in a fresh cold encoder under the shard lock, then drains
// the old one OUTSIDE the lock so ingestion continues during the (heavier)
// drain. Returns the drained fragments (nil on a disabled/empty shard or a drain
// error, which is logged).
func (p *asapEdgeProcessor) drainShardCold(sh *shard) []gorilla.Fragment {
	sh.mu.Lock()
	old := sh.cold
	if old != nil {
		sh.cold = p.newColdEncoder()
	}
	sh.mu.Unlock()
	if old == nil {
		return nil
	}
	drained, derr := old.Drain(true)
	if derr != nil {
		p.logger.Warn("asap_edge: cold drain failed", zap.Error(derr))
		return nil
	}
	return drained
}

// shipFragments hands a drained fragment batch to the async ship worker
// (non-blocking): flushes never wait on the network, and a ship failure is
// spooled + retried. encode runs here (cheap, off the worker) so an encode error
// is logged in the flush path.
func (p *asapEdgeProcessor) shipFragments(frags []gorilla.Fragment) {
	if len(frags) == 0 {
		return
	}
	if serr := p.shipWorker.shipBatch(frags); serr != nil {
		p.logger.Warn("asap_edge: encode cold fragments failed", zap.Error(serr))
	}
}

// appendSumMetrics merges each Sum metric's partials across all shards (resetting
// each shard) and appends one delta Sum metric per name to out. Sum is
// associative, so this is byte/semantically identical regardless of how many
// shard-ticks elapsed since the last sum flush.
func (p *asapEdgeProcessor) appendSumMetrics(out pmetric.Metrics) {
	startMs := p.windowStartMs.Load()
	endMs := p.maxObservedMs.Load()
	if endMs < startMs {
		endMs = uint64(time.Now().UnixMilli())
	}
	for name := range p.sumMetrics {
		merged := make(map[string]*sumGroup)
		for _, sh := range p.shards {
			sh.mu.Lock()
			mergeSumGroups(merged, sh.sumAggs[name].groups)
			sh.sumAggs[name].reset()
			sh.mu.Unlock()
		}
		emitSumMetric(out, name, merged, startMs, endMs)
	}
	p.windowStartMs.Store(endMs)
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
