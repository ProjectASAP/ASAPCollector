// MVP v6 freshness probes — three synthetic counters whose
// cumulative value equals the Unix epoch ms at the moment of the
// most recent emission.
//
// See deploy/configs/mvp-v6-freshness-probes.yaml and
// docs/spec-mvp-v6-controller-driven-multi-stage-demo.md §⑥
// Freshness for the protocol.
//
// Mechanic:
//
//   - Each probe is a Float64Counter that ticks at FRESHNESS_PROBE_HZ
//     (default 1 Hz).
//   - At each tick we Add(now_ms - last_emit_ms). The SDK's cumulative
//     temporality means the wire-side cumulative value equals
//     `last_emit_ms`. The first tick adds the absolute timestamp, so
//     the very first observed cumulative is also `now_ms` exactly.
//   - The replay client polls `last_over_time(<probe>[10s])`; the
//     value field IS the timestamp of the last emission.
//
// Three probes — one per serving path:
//
//	http_freshness_probe_raw      → Prometheus B0
//	http_freshness_probe_warm     → sketch warm tier (agent processor)
//	http_freshness_probe_archive  → Gorilla-archive (gorillas3processor)
//
// Routing is determined entirely by metric name + agent / gateway
// pipeline configs (see configs/sketchcol-agent-*-tier.yaml). The
// fake-exporter is path-agnostic; it simply emits the three counters.
//
// Env knobs:
//
//	EXPORTER_FRESHNESS_PROBES        on | off (default on)
//	EXPORTER_FRESHNESS_PROBE_HZ      tick rate, Hz (default 1)
package main

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// freshnessProbeNames is the canonical list of probe metric names.
// Phase E config emitters route on these names exactly.
var freshnessProbeNames = []string{
	"http_freshness_probe_raw",
	"http_freshness_probe_warm",
	"http_freshness_probe_archive",
}

// startFreshnessProbes wires the three timestamp-encoded counters and
// fires a goroutine per probe that ticks at hz Hz. Returns a no-op
// closure when probes are disabled so callers can `defer` it
// unconditionally.
//
// The probes share the meter passed in (so they ride the same
// PeriodicReader / View / projection / aggregation as the rest of the
// fake-exporter). Phase C overlay routing handles per-metric pipeline
// selection at the agent.
func startFreshnessProbes(ctx context.Context, meter metric.Meter) (stop func()) {
	enabled := envBool("EXPORTER_FRESHNESS_PROBES", true)
	// Tolerate the explicit "off" string the spec calls out.
	if v := strings.ToLower(strings.TrimSpace(envOr("EXPORTER_FRESHNESS_PROBES", ""))); v == "off" {
		enabled = false
	}
	if !enabled {
		log.Printf("freshness probes disabled (EXPORTER_FRESHNESS_PROBES=off)")
		return func() {}
	}

	hz := envFloat("EXPORTER_FRESHNESS_PROBE_HZ", 1.0)
	if hz <= 0 {
		log.Printf("warning: EXPORTER_FRESHNESS_PROBE_HZ=%v invalid, using 1.0", hz)
		hz = 1.0
	}
	period := time.Duration(float64(time.Second) / hz)

	probeCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup

	for _, name := range freshnessProbeNames {
		counter, err := meter.Float64Counter(
			name,
			metric.WithDescription(
				"MVP v6 freshness probe — cumulative value equals "+
					"unix_ts_ms of the most recent emission. "+
					"Polled via last_over_time(...[10s]) by the replay client."),
			metric.WithUnit("ms"),
		)
		if err != nil {
			// Non-fatal — the rest of the exporter can still run. The
			// freshness measurement just won't have data for this path.
			log.Printf("freshness probe %s init failed: %v (skipping)", name, err)
			continue
		}
		wg.Add(1)
		go runFreshnessProbe(probeCtx, &wg, counter, name, period)
	}

	log.Printf(
		"freshness probes started: %v at %.2f Hz (period=%s)",
		freshnessProbeNames, hz, period,
	)

	return func() {
		cancel()
		wg.Wait()
	}
}

// runFreshnessProbe ticks at `period` and Add()s the delta-ms since
// the previous emission. The cumulative counter value (as seen by
// Prometheus / the warm tier / the archive tier) thus equals
// `now_ms` at the moment of the most recent tick.
//
// The first tick adds the absolute timestamp (last==0), making the
// wire-side cumulative exactly `now_ms` straight away. This matters
// because some counter aggregators reset on first observation; we
// want every observed value to be a usable timestamp.
func runFreshnessProbe(
	ctx context.Context,
	wg *sync.WaitGroup,
	counter metric.Float64Counter,
	name string,
	period time.Duration,
) {
	defer wg.Done()

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	var lastMs int64 // 0 → first tick adds the absolute timestamp
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nowMs := time.Now().UnixMilli()
			delta := nowMs - lastMs
			if delta < 0 {
				// Clock jumped backwards — skip this tick rather than
				// emit a non-monotonic delta the SDK would reject.
				continue
			}
			counter.Add(ctx, float64(delta))
			lastMs = nowMs
			_ = name // reserved for future per-probe debug logging
		}
	}
}
