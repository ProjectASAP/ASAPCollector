// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"go.uber.org/zap"
)

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

// shipFragments hands a drained fragment batch to the default (fragment) cold
// archive ship path: the async ship worker (non-blocking) — flushes never wait
// on the network, and a ship failure is spooled + retried. It is the
// fragment-format path only; the intchunk cold-part path routes per shard
// through shipFragmentsForShard so each shard's part accumulates across a block.
func (p *asapEdgeProcessor) shipFragments(frags []gorilla.Fragment) {
	if len(frags) == 0 {
		return
	}
	if serr := p.shipWorker.shipBatch(frags); serr != nil {
		p.logger.Warn("asap_edge: encode cold fragments failed", zap.Error(serr))
	}
}

// shipFragmentsForShard routes one shard's drained fragments to the cold archive
// ship path. The default (fragment) format ignores the shard (one shared async
// worker). The intchunk format buffers the fragments into shard idx's
// accumulator and seals + POSTs a coldpart.Part only when the buffered sample
// span reaches BlockDuration — amortizing the per-part index + symbol-table
// overhead over ~a block's worth of samples instead of one flush's ~1-2.
func (p *asapEdgeProcessor) shipFragmentsForShard(idx int, frags []gorilla.Fragment) {
	if p.coldFormat != ColdFormatIntchunk {
		p.shipFragments(frags)
		return
	}
	p.accumulateColdPart(idx, frags)
}

// accumulateColdPart appends shard idx's drained fragments to its accumulator
// and, once the buffered span reaches BlockDuration, seals one coldpart.Part and
// POSTs it. A no-op shipper still drains+buffers (kept bounded by the block
// seal) but never POSTs. Called only from the single flush goroutine, so the
// per-shard accumulator needs no lock.
func (p *asapEdgeProcessor) accumulateColdPart(idx int, frags []gorilla.Fragment) {
	if idx < 0 || idx >= len(p.coldAccum) {
		return
	}
	acc := p.coldAccum[idx]
	if err := acc.add(frags); err != nil {
		p.logger.Warn("asap_edge: buffer cold part failed", zap.Error(err))
		return
	}
	// Seal once the accumulated sample span reaches the block window. spanMs is
	// max-min sample T, so a part covers ~BlockDuration regardless of how many
	// sub-window flushes contributed.
	if p.coldBlockMs > 0 && acc.spanMs() >= p.coldBlockMs {
		p.sealAndShipColdPart(idx)
	}
}

// sealColdPartBody seals shard idx's accumulator (always resetting it so the
// next block starts fresh) and encodes the buffered series into one serialized
// coldpart.Part. It is the single seal->encode helper shared by the async
// per-block ship (sealAndShipColdPart) and the synchronous Shutdown drain
// (flushColdPartAccumulators) so the two paths can never drift. Returns
// (nil, nil) — no body to ship — for an empty buffer, a no-op (endpointless)
// shipper, an empty series set, or an empty encoding. An encode error is logged
// and surfaced so the caller can skip the POST.
func (p *asapEdgeProcessor) sealColdPartBody(idx int) ([]byte, error) {
	acc := p.coldAccum[idx]
	if acc.empty() {
		return nil, nil
	}
	series, blockStart, blockEnd := acc.seal()
	if len(series) == 0 || p.coldPartShip.noop() {
		return nil, nil
	}
	body, err := encodePart(blockStart, blockEnd, series)
	if err != nil {
		p.logger.Warn("asap_edge: build cold part failed", zap.Error(err))
		return nil, err
	}
	if len(body) == 0 {
		return nil, nil
	}
	return body, nil
}

// sealAndShipColdPart seals shard idx's accumulator into one coldpart.Part and
// POSTs it (async, bounded context, mirroring the fragment path's async ship).
// An empty buffer or a no-op shipper is a clean no-op; the buffer is always
// reset (seal resets it) so the next block starts fresh.
func (p *asapEdgeProcessor) sealAndShipColdPart(idx int) {
	body, err := p.sealColdPartBody(idx)
	if err != nil || len(body) == 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if serr := p.coldPartShip.shipEncoded(ctx, body); serr != nil {
			p.logger.Warn("asap_edge: ship cold part failed", zap.Error(serr))
		}
	}()
}

// flushColdPartAccumulators force-seals + POSTs every shard's buffered cold-part
// accumulator (intchunk only), draining any partial (sub-BlockDuration) block on
// Shutdown. It POSTs synchronously under the Shutdown deadline ctx (not a
// detached goroutine) so the last block is delivered before the process exits.
func (p *asapEdgeProcessor) flushColdPartAccumulators(ctx context.Context) {
	if p.coldFormat != ColdFormatIntchunk {
		return
	}
	for idx := range p.coldAccum {
		body, err := p.sealColdPartBody(idx)
		if err != nil || len(body) == 0 {
			continue
		}
		if serr := p.coldPartShip.shipEncoded(ctx, body); serr != nil {
			p.logger.Warn("asap_edge: ship cold part failed", zap.Error(serr))
		}
	}
}
