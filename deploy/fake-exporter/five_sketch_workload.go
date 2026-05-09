// Five-sketch MVP workload — emits the four new metrics that exercise
// the KLL / HLL / CountSketch / CountMinSketch families end-to-end
// (DDSketch + Sum are already covered by `runSynthetic`'s
// `<metric>` counter and `<metric>_latency_ms` gauge).
//
// Shared contract (issue #46 — "5-sketch MVP workload"):
//
//	┌──────────────────────────┬────────────────────┬──────────────────────────────────────────┐
//	│ metric                   │ family             │ replay-side query class                  │
//	├──────────────────────────┼────────────────────┼──────────────────────────────────────────┤
//	│ http_requests_total      │ raw passthrough    │ sum by (zone) (rate(... [5m]))            │
//	│ http_latency_ms          │ DDSketch           │ quantile_over_time(0.99, ... [1m])        │
//	│ request_size_bytes       │ KLL                │ quantile_over_time(0.99, ... [1m])        │
//	│ unique_users_per_min     │ HLL                │ count(unique_users_per_min)               │
//	│ top_endpoint_qps         │ CountSketch        │ topk(5, top_endpoint_qps)                 │
//	│ endpoint_request_freq    │ CountMinSketch     │ rate(endpoint_request_freq[5m])           │
//	└──────────────────────────┴────────────────────┴──────────────────────────────────────────┘
//
// The first two columns already exist in `runSynthetic`. This file
// adds the four new metrics and is gated by EXPORTER_FIVE_SKETCH.
//
// ## Distribution shapes (chosen so the matched sketch family is
//    actually stressed):
//
//   - `request_size_bytes` (KLL): per-event Gauge. Body sizes drawn
//     log-normal in [100B, 10 KB] — heavy right-tail, so the
//     rank-error metric KLL targets is the natural scoring axis.
//
//   - `unique_users_per_min` (HLL): per-event Counter labelled with a
//     synthetic `user_id` drawn from a rotating pool of size
//     EXPORTER_FIVE_SKETCH_USER_POOL (default 100, range
//     50-2000). Cardinality of the active user set in any 1-minute
//     window is the property HLL estimates. The default was lowered
//     from 1000 → 100 to keep the agent → gateway wire bandwidth
//     bounded (HLL inner-label fan-out dominates SDK output rate).
//
//   - `top_endpoint_qps` (CountSketch): per-event Counter labelled
//     with `endpoint` drawn Zipfian (s=1.2) over
//     EXPORTER_FIVE_SKETCH_ENDPOINTS (default 50). Heavy hitters
//     dominate, so top-K is meaningful and the count-sketch's
//     unbiased frequency estimator is the right oracle.
//
//   - `endpoint_request_freq` (CountMinSketch): per-event Counter
//     labelled with `endpoint`. Same Zipfian shape as
//     `top_endpoint_qps` but exercised through the frequency query
//     class (`rate(...[5m])`). CMS provides one-sided over-estimates
//     of per-key frequency, the natural oracle for that query.
//
// ## Cardinality / emission-rate knobs
//
// These respect the existing per-producer knobs so the demo's
// PER_AGENT_CARDINALITY × N_PRODUCERS arithmetic continues to make
// sense:
//
//	EXPORTER_FREQ_HZ                  per-series tick rate (shared with runSynthetic)
//	EXPORTER_CARDINALITY              upstream zone/rack/node/pod label set count
//	                                  (shared — drives the outer label set on every
//	                                  five-sketch metric so they fan out per-host).
//	EXPORTER_FIVE_SKETCH              on | off       (default on)
//	EXPORTER_FIVE_SKETCH_USER_POOL    HLL user-id cardinality       (default 100)
//	EXPORTER_FIVE_SKETCH_ENDPOINTS    Zipfian endpoint cardinality  (default 50)
//	EXPORTER_FIVE_SKETCH_ZIPF_S       Zipfian s parameter            (default 1.2)
//	EXPORTER_FIVE_SKETCH_USER_ROTATE  s — how often we rotate the user
//	                                   active window forward by one slot
//	                                   (default 60s — matches the
//	                                   "_per_min" semantic).
//
// Each five-sketch series ticks at the same EXPORTER_FREQ_HZ as the
// existing http_requests_total counter; per-series goroutines stagger
// their start so the wire pattern is smooth.
//
// To turn these emitters off (e.g. when running the cost-eval that
// only cares about DDSketch + Sum), set EXPORTER_FIVE_SKETCH=off.

package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// fiveSketchEnabled honours the explicit "off" string the spec calls
// out, falling back to envBool's default-on for any other value.
func fiveSketchEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("EXPORTER_FIVE_SKETCH")))
	if v == "off" || v == "0" || v == "false" || v == "no" {
		return false
	}
	return true
}

// startFiveSketchWorkload wires the four new instruments and starts
// their per-series goroutines. Returns a stop closure (joinable);
// returns a no-op closure when gated off so the caller can `defer`
// unconditionally.
//
// `outerLabels` is the same per-host label set that runSynthetic
// uses, so each five-sketch metric fans out across the same
// zone/rack/node/pod attribute schema. The inner attribute (user_id /
// endpoint) is appended per-event.
func startFiveSketchWorkload(
	ctx context.Context,
	meter metric.Meter,
	outerLabels [][]attribute.KeyValue,
	freqHz float64,
) (stop func()) {
	if !fiveSketchEnabled() {
		log.Printf("five-sketch workload disabled (EXPORTER_FIVE_SKETCH=off)")
		return func() {}
	}

	// Default lowered from 1000 → 100 (and floor lowered from 500 → 50)
	// to keep agent → gateway bandwidth bounded; HLL inner-label fan-out
	// (one series per user_id × outer label set) dominates SDK output.
	// Aggregate target with N_PRODUCERS=10 × PER_AGENT_CARDINALITY=500
	// is ~5K series at the gateway; userPool only widens the active
	// user-id bucket inside that fan-out, so 100 is plenty for HLL to
	// have something non-trivial to estimate.
	userPool := envInt("EXPORTER_FIVE_SKETCH_USER_POOL", 100)
	if userPool < 50 {
		userPool = 50
	}
	if userPool > 2000 {
		userPool = 2000
	}
	endpoints := envInt("EXPORTER_FIVE_SKETCH_ENDPOINTS", 50)
	if endpoints < 5 {
		endpoints = 5
	}
	zipfS := envFloat("EXPORTER_FIVE_SKETCH_ZIPF_S", 1.2)
	if zipfS <= 1.0 {
		// math/rand.Zipf requires s > 1 strictly.
		zipfS = 1.2
	}
	userRotate := envDuration("EXPORTER_FIVE_SKETCH_USER_ROTATE", 60*time.Second)

	// Per-series tick period — matches the Hz used by the rest of the
	// synthetic workload so the wire shape stays predictable.
	if freqHz <= 0 {
		freqHz = 10.0
	}
	period := time.Duration(float64(time.Second) / freqHz)

	log.Printf(
		"five-sketch workload starting: freq_hz=%.1f users=%d endpoints=%d zipf_s=%.2f rotate=%s outer_card=%d",
		freqHz, userPool, endpoints, zipfS, userRotate, len(outerLabels),
	)

	// ── instruments ────────────────────────────────────────────────
	// request_size_bytes — per-event Gauge of body size in bytes.
	// KLL targets rank error on the value distribution.
	sizeGauge, err := meter.Float64Gauge(
		"request_size_bytes",
		metric.WithDescription("Synthetic HTTP body size sample per event (log-normal 100B–10KB)"),
		metric.WithUnit("By"),
	)
	if err != nil {
		log.Fatalf("five-sketch: request_size_bytes gauge init: %v", err)
	}

	// unique_users_per_min — Counter labelled with rotating user_id.
	// HLL estimates the active-user cardinality.
	usersCounter, err := meter.Int64Counter(
		"unique_users_per_min",
		metric.WithDescription("Synthetic per-user request counter — rotating user-id pool drives HLL cardinality"),
	)
	if err != nil {
		log.Fatalf("five-sketch: unique_users_per_min counter init: %v", err)
	}

	// top_endpoint_qps — Counter labelled with Zipfian endpoint.
	// CountSketch backs top-K queries.
	endpointCounter, err := meter.Int64Counter(
		"top_endpoint_qps",
		metric.WithDescription("Synthetic per-endpoint request counter — Zipfian endpoint pool drives top-K"),
	)
	if err != nil {
		log.Fatalf("five-sketch: top_endpoint_qps counter init: %v", err)
	}

	// endpoint_request_freq — same shape as top_endpoint_qps but
	// emitted as a separate metric so the controller can plan a
	// CountMinSketch for it (CMS = frequency query class). Splitting
	// the counter rather than re-using top_endpoint_qps keeps the
	// per-family bytes accounting clean.
	freqCounter, err := meter.Int64Counter(
		"endpoint_request_freq",
		metric.WithDescription("Synthetic per-endpoint request counter — same Zipfian as top_endpoint_qps, used for CMS frequency"),
	)
	if err != nil {
		log.Fatalf("five-sketch: endpoint_request_freq counter init: %v", err)
	}

	// userBase rotates forward at userRotate cadence so the active
	// 1-minute window slides through the pool deterministically. The
	// active user-id at tick t is `(userBase + tick % poolSize) %
	// poolSize`. We expose userBase through atomic so multiple
	// goroutines see a consistent value mid-rotation.
	var userBase uint64
	wctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup

	// User-window rotator goroutine — bumps userBase every userRotate.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(userRotate)
		defer ticker.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-ticker.C:
				atomic.AddUint64(&userBase, 1)
			}
		}
	}()

	// One per-outer-series goroutine per metric × outer label set.
	// We share the goroutine across all four metrics so the inner
	// labels stay correlated (same RNG draw produces the matching
	// {endpoint, user, size, freq} tuple, matching what a real
	// HTTP server would emit per request).
	for i := 0; i < len(outerLabels); i++ {
		wg.Add(1)
		go func(seriesIdx int) {
			defer wg.Done()

			// Deterministic per-series RNG so reruns are reproducible.
			r := rand.New(rand.NewSource(int64(seriesIdx) * 2654435761))
			zipf := rand.NewZipf(r, zipfS, 1.0, uint64(endpoints-1))

			// Stagger start so all series don't fire simultaneously.
			startOffset := time.Duration(float64(seriesIdx%max(int(freqHz), 1))/
				float64(max(int(freqHz), 1))) * period
			time.Sleep(startOffset)

			ticker := time.NewTicker(period)
			defer ticker.Stop()

			outerAttrs := outerLabels[seriesIdx]

			for {
				select {
				case <-wctx.Done():
					return
				case <-ticker.C:
					// (1) request_size_bytes — log-normal 100..10000.
					// μ=6.5, σ=0.9 → median ≈665 B, p99 ≈ 5 KB,
					// clamp to [100, 10000].
					size := math.Exp(6.5 + 0.9*r.NormFloat64())
					if size < 100 {
						size = 100
					}
					if size > 10000 {
						size = 10000
					}
					sizeAttrs := append([]attribute.KeyValue{}, outerAttrs...)
					sizeGauge.Record(wctx, size, metric.WithAttributes(sizeAttrs...))

					// (2) unique_users_per_min — rotating pool.
					base := atomic.LoadUint64(&userBase)
					uid := (base + uint64(r.Intn(userPool))) % uint64(userPool)
					userAttrs := append([]attribute.KeyValue{}, outerAttrs...)
					userAttrs = append(userAttrs, attribute.String("user_id", fmt.Sprintf("u%05d", uid)))
					usersCounter.Add(wctx, 1, metric.WithAttributes(userAttrs...))

					// (3) top_endpoint_qps — Zipfian endpoint pick.
					// (4) endpoint_request_freq — same draw, separate
					// metric, so the CMS path gets its own series.
					ep := zipf.Uint64()
					epAttrs := append([]attribute.KeyValue{}, outerAttrs...)
					epAttrs = append(epAttrs, attribute.String("endpoint", fmt.Sprintf("/api/ep%03d", ep)))
					endpointCounter.Add(wctx, 1, metric.WithAttributes(epAttrs...))
					freqCounter.Add(wctx, 1, metric.WithAttributes(epAttrs...))
				}
			}
		}(i)
	}

	return func() {
		cancel()
		wg.Wait()
	}
}
