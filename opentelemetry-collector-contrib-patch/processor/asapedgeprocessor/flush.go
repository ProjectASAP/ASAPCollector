// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

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

// flushAll drains the window for EVERY shard at once: per shard, swap+drain the
// cold fragment encoder and ship the collected XOR-chunk fragments; flush each
// shard's sketch aggregators; collect+merge sum partials across shards and emit
// one delta Sum metric per Sum metric. Forwards the result downstream.
//
// This is the single-flush path: the fallback cadence (ShardCount <= 1 /
// WindowDuration <= 0) and the final drain on Shutdown both call it so no shard
// is left un-flushed.
func (p *asapEdgeProcessor) flushAll(ctx context.Context) {
	// Cold: drain every shard's encoder and ship.
	if p.coldEnabled {
		if p.coldFormat == ColdFormatIntchunk {
			// Intchunk: drain + buffer each shard into its own accumulator,
			// keeping per-shard block bounds, and seal a part only when the shard's
			// buffered span reaches BlockDuration (per-shard, like the staggered
			// path). flushAll runs for the single-flush fallback and final
			// Shutdown drain; the partial (sub-block) tail left buffered here is
			// force-sealed by flushColdPartAccumulators on Shutdown.
			for i, sh := range p.shards {
				p.shipFragmentsForShard(i, p.drainShardCold(sh))
			}
		} else {
			// Fragment: drain every shard's encoder and ship as one binary batch.
			var frags []gorilla.Fragment
			for _, sh := range p.shards {
				frags = append(frags, p.drainShardCold(sh)...)
			}
			p.shipFragments(frags)
		}
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
		p.shipFragmentsForShard(idx, p.drainShardCold(sh))
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
