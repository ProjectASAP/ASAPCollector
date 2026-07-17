// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate // import "go.opentelemetry.io/otel/sdk/metric/internal/aggregate"

import (
	"sync"
	"time"

	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient"
)

// liveSampleGrant maintains a live, coordinator-granted sample_p for ONE
// precompute.AggregationIdentity. Mirrors otel-app/sample_controller.go's
// sampleController — the SAME monitor.Engine + grpcclient.Client wiring the
// edge collector's continuous monitor uses to reach the coordinator — but
// scoped to a single AggID (sample_controller reports for a list of
// monitors and takes the max grant; a row-sampled AggregationIdentity has
// exactly one) and embedded directly in the SDK process instead of a
// separate app-level binary. Dials the coordinator DIRECTLY, bypassing the
// edge collector.
//
// Registration happens implicitly through Observe — there is no separate
// register call (mirrors sample_controller.go, which never calls anything
// on monitor.Engine but NewEngine / SetReporter / Observe / EpochReset /
// GrantedSampleP).
//
// This type lives in the SDK module (not asap-precompute-go's core module)
// because it imports monitor/grpcclient — a deliberately SEPARATE nested Go
// module so the gRPC dependency tree never contaminates the core runtime's
// module graph (see monitor/grpcclient's module doc). Any consumer that
// wants live grants, not just a static bootstrap p, accepts that
// dependency directly — mirroring how otel-app does it.
type liveSampleGrant struct {
	mu sync.Mutex

	aggID      uint64
	bootstrapP float64
	windowMs   uint64

	engine *monitor.Engine
	client *grpcclient.Client

	windowStartMs uint64
	windowValue   float64 // running Σ this window, mirrors sample_controller's windowValue for the "sum" functional
	p             float64 // currently-applied p; bootstrapP until the first grant
}

// newLiveSampleGrant dials coordinatorURL for aggID and reports under
// edgeID. windowMs is the CDM epoch length — should match the collector's
// warm-tier window (the coordinator's slack-countdown protocol is a
// per-epoch round). bootstrapP is used until the first grant arrives (and
// permanently if coordinatorURL is empty — the "no coordinator configured"
// fallback, mirroring sample_controller.go).
func newLiveSampleGrant(coordinatorURL, edgeID string, aggID uint64, windowMs uint64, bootstrapP float64) *liveSampleGrant {
	g := &liveSampleGrant{aggID: aggID, bootstrapP: bootstrapP, windowMs: windowMs, p: bootstrapP}
	if coordinatorURL == "" {
		return g
	}
	g.engine = monitor.NewEngine(edgeID, windowMs, nil)
	g.client = grpcclient.New(coordinatorURL, g.engine)
	g.engine.SetReporter(g.client)
	g.windowStartMs = alignedWindowStartMs(windowMs)
	return g
}

// alignedWindowStartMs floors now to the window boundary, matching
// sample_controller.alignedNow().
func alignedWindowStartMs(windowMs uint64) uint64 {
	now := uint64(time.Now().UnixMilli())
	if windowMs == 0 {
		return now
	}
	return now - (now % windowMs)
}

// reportOccurrence feeds one raw occurrence into this AggregationIdentity's
// rate accounting — call once per measure() invocation (BEFORE the
// row-admission decision: rate tracks arriving traffic, independent of
// sampling outcome), mirroring sample_controller.observe()'s per-event
// engine.Observe call so the coordinator sees the live-climbing rate
// within the epoch, not just a value reported at window close.
func (g *liveSampleGrant) reportOccurrence() {
	if g == nil || g.engine == nil {
		return
	}
	g.mu.Lock()
	g.windowValue++
	v, ws := g.windowValue, g.windowStartMs
	g.mu.Unlock()
	g.engine.Observe(g.aggID, nil, v, ws)
}

// currentP rolls the window if the wall clock crossed a boundary — flushing
// the closing window's final value, resetting the epoch, and re-reading the
// grant for the new window — then returns the p to apply now. Mirrors
// sample_controller.currentP(); the one-window latency (this window's p is
// based on the PRIOR window's reported rate) is inherent to the protocol,
// not a bug.
func (g *liveSampleGrant) currentP() float64 {
	if g == nil || g.engine == nil {
		return g.bootstrapP
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	now := alignedWindowStartMs(g.windowMs)
	if now != g.windowStartMs {
		// Flush the closing window's final value under its OWN windowStartMs
		// before rolling, so the coordinator has the complete window.
		g.engine.Observe(g.aggID, nil, g.windowValue, g.windowStartMs)
		g.engine.EpochReset(now)
		g.windowStartMs = now
		g.windowValue = 0
		if pm := g.engine.GrantedSampleP(g.aggID); pm > 0 {
			g.p = pm
		} else {
			g.p = g.bootstrapP
		}
	}
	return g.p
}
