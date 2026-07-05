// Package otlpfilter implements a pre-decode OTLP wire-filter that thins the
// datapoints of warm-sketch metrics directly at the protobuf wire level —
// before the OTLP payload is decoded into pdata. The dropped fraction is
// wire-skipped and therefore never materialized (no attribute-map / value
// decode), which captures more of the decode cost than post-decode sampling
// can (see docs/distributed-nitrosketch-coordinated-sampling.md,
// "in-collector pre-decode shim").
//
// Sampling is CONSISTENT (design §3.1.1, single-location sampling): the
// per-datapoint decision is the stateless hash
//
//	admit(seed, occ, row) = common.ConsistentAdmit(...)
//	seed = common.SeedForMetric(name)          (canonical FNV-1a-64)
//	occ  = datapoint time_unix_nano / 1e6      (milliseconds — matches the
//	                                            decoded Observation.TimestampMs
//	                                            the collector wrapper sees)
//
// and a datapoint is dropped iff NONE of the target sketch's d rows admits
// (the R(x)=∅ event, probability (1−p)^d). The collector wrapper re-evaluates
// the identical per-row decisions on survivors and applies the 1/p weight —
// the filter and the wrapper are two views of ONE sampling decision, so the
// filter needs no RNG state, no mutexes, and re-evaluation anywhere is
// idempotent. A datapoint with no/zero timestamp is passed through (fail-open
// — the filter cannot decide without a wire identity); the wrapper then
// samples such points via its per-series occurrence-counter fallback, so they
// are still sampled exactly once end-to-end.
//
// The package is additive and self-contained: it touches no existing code and
// only depends on sketchlib-go/common (pdata appears in tests only).
package otlpfilter

import (
	"sync/atomic"

	"github.com/ProjectASAP/sketchlib-go/common"
)

// SampleParams configures consistent sampling for one warm metric.
type SampleParams struct {
	// P is the per-row admission probability in (0,1). Values outside the open
	// interval mean "not sampled" (the metric passes through untouched).
	P float64
	// Rows is the counter fan-out d of the target sketch: the datapoint is kept
	// iff at least one of rows 0..Rows-1 admits. Use the sketch's row count for
	// per-row families (CountSketch / CountMinSketch) and 1 for whole-item
	// families (DDSketch). Values < 1 are treated as 1.
	Rows int
}

// SampleState is the shared, in-process sampling configuration consulted by
// the wire-filter. It maps each warm-sketch metric name to its SampleParams.
// The map is published lock-free via an atomic pointer so the real wiring
// (asap_edge's OnGrant -> SetParams) can swap the whole map without blocking
// the hot read path. A metric absent from the map, or with P outside (0,1),
// is treated as "not sampled" (cold / raw) and passes through untouched.
//
// There is no per-metric sampler state: decisions are stateless hashes, so
// SampleState is safe for concurrent use by construction.
type SampleState struct {
	params atomic.Pointer[map[string]SampleParams]
}

// NewSampleState returns an empty SampleState in which nothing is sampled
// (every metric passes through). Call SetParams (or the SetP convenience) to
// install per-metric sampling.
func NewSampleState() *SampleState {
	s := &SampleState{}
	empty := map[string]SampleParams{}
	s.params.Store(&empty)
	return s
}

// SetParams atomically replaces the metric -> params map. This is the writer
// called by asap_edge's OnGrant in the real wiring. A copy of the supplied map
// is stored so the caller may mutate its argument afterwards.
func (s *SampleState) SetParams(m map[string]SampleParams) {
	cp := make(map[string]SampleParams, len(m))
	for k, v := range m {
		cp[k] = v
	}
	s.params.Store(&cp)
}

// SetP is the whole-item (Rows=1) convenience form of SetParams, kept for
// callers that predate per-row fan-out configuration.
func (s *SampleState) SetP(m map[string]float64) {
	cp := make(map[string]SampleParams, len(m))
	for k, v := range m {
		cp[k] = SampleParams{P: v, Rows: 1}
	}
	s.params.Store(&cp)
}

// paramsFor classifies a metric: (params, seed, true) when it is sampled, or
// (_, _, false) for cold/raw metrics. The seed is the canonical per-metric
// consistent-sampling seed shared with the collector wrapper.
func (s *SampleState) paramsFor(metric string) (SampleParams, uint64, bool) {
	mp := s.params.Load()
	if mp == nil {
		return SampleParams{}, 0, false
	}
	p, ok := (*mp)[metric]
	if !ok || p.P >= 1.0 || p.P <= 0 {
		return SampleParams{}, 0, false
	}
	if p.Rows < 1 {
		p.Rows = 1
	}
	return p, common.SeedForMetric(metric), true
}

// keepDataPoint evaluates the consistent whole-datapoint decision for one
// opaque datapoint of a sampled metric: keep iff at least one of the d rows
// admits at (seed, occ = time ms). A datapoint whose time_unix_nano is absent
// or zero is KEPT (fail-open — no wire identity to decide on; the wrapper
// samples such points via its occurrence-counter fallback instead, exactly
// once end-to-end).
func keepDataPoint(p SampleParams, seed uint64, dp []byte) bool {
	tNanos, ok := readDataPointTimeNanos(dp)
	if !ok || tNanos == 0 {
		return true
	}
	occ := tNanos / 1_000_000 // milliseconds — must match Observation.TimestampMs
	for r := 0; r < p.Rows; r++ {
		if common.ConsistentAdmit(seed, occ, r, p.P) {
			return true
		}
	}
	return false
}
