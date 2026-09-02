// Copyright ProjectASAP Authors
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
	// Optional threshold-driven sub-window check ticker: fires the per-series
	// divergence-gated EmitSubWindow every SubWindowInterval, IN ADDITION to the
	// window-boundary flush. Disabled (nil channel, never fires) unless enabled.
	var subC <-chan time.Time
	if p.subWindowEnabled() {
		subT := time.NewTicker(p.cfg.SubWindowInterval)
		defer subT.Stop()
		subC = subT.C
	}
	if !p.staggered() {
		t := time.NewTicker(p.cfg.WindowDuration)
		defer t.Stop()
		for {
			select {
			case <-p.stopCh:
				p.flushAll(context.Background())
				return
			case <-subC:
				p.flushSubWindow(context.Background())
			case <-p.wakeCh:
				p.flushSubWindow(context.Background())
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
			// Final drain: flush every shard (cold + sketch) so no un-flushed
			// shard is lost on Shutdown.
			p.flushAll(context.Background())
			return
		case <-subC:
			// Sub-window incremental emit across all shards (not staggered:
			// each tick must cover every active series; threshold-gated emits
			// are small).
			p.flushSubWindow(context.Background())
		case <-p.wakeCh:
			// Out-of-cycle wake (e.g. an insert-time GOS threshold crossing).
			// Same handler as subC: emits whatever's currently divergent/dirty
			// across every shard, just triggered early instead of by the timer.
			p.flushSubWindow(context.Background())
		case <-t.C:
			shardIdx := tick % n
			p.flushShardWarmCold(context.Background(), shardIdx)
			tick++
		}
	}
}

// wakeSubWindow requests an out-of-cycle sub-window flush — e.g. an
// insert-time GOS threshold crossing that shouldn't wait for the next
// SubWindowInterval tick (or, for a GOS-only family with no sub-window
// ticker configured at all, that would otherwise have no flush path short of
// window close). Non-blocking: if a wake is already pending, this is a
// no-op — the pending flushSubWindow call will pick up every series'
// current dirty state, including whatever just crossed threshold.
func (p *asapEdgeProcessor) wakeSubWindow() {
	select {
	case p.wakeCh <- struct{}{}:
	default:
	}
}

// subWindowEnabled reports whether the threshold-driven sub-window producer is
// active: a positive interval shorter than the window (validated at config).
func (p *asapEdgeProcessor) subWindowEnabled() bool {
	return p.cfg.SubWindowInterval > 0 && p.cfg.SubWindowInterval < p.cfg.WindowDuration
}

// flushSubWindow fires a divergence-gated sub-window delta emit for every
// shard's sketch aggregators and forwards the result, WITHOUT rotating windows.
//
// No p.subWindowEnabled() guard here: this now runs from two triggers (the
// legacy subC ticker, gated at the call site by subWindowEnabled(), and the
// wakeCh out-of-cycle signal, which is NOT gated by it — a GOS-driven family
// must be able to wake a flush even with SubWindowInterval unset). Per-series
// gating happens inside sa.emitSubWindow / s.subWindowEnabled(), which accepts
// either a positive SubWindowInterval or active GOS insert-time detection.
func (p *asapEdgeProcessor) flushSubWindow(ctx context.Context) {
	nowMs := uint64(time.Now().UnixMilli())
	out := pmetric.NewMetrics()
	for _, sh := range p.shards {
		sh.mu.Lock()
		for _, sa := range sh.sketchAggs {
			sa.emitSubWindow(out, nowMs)
		}
		sh.mu.Unlock()
	}
	p.forward(ctx, out)
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
	// Warm: per-shard flush of every aggregator (sketches + Sum, which now
	// flushes a SumAgg envelope through the same path; each series lives in one
	// shard and the backend sums the per-window SumAgg deltas for the same sid).
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
// sketch aggregators (including Sum, which now flushes a SumAgg envelope through
// the same path), then forwards the envelopes. This is the staggered per-tick
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
