// Package otlpfilter implements a pre-decode OTLP wire-filter that
// geometric-samples the datapoints of warm-sketch metrics directly at the
// protobuf wire level — before the OTLP payload is decoded into pdata. The
// dropped fraction is wire-skipped and therefore never materialized (no
// attribute-map / value decode), which captures more of the decode cost than
// post-decode sampling can (see
// docs/distributed-nitrosketch-coordinated-sampling.md, "in-collector
// pre-decode shim").
//
// The package is additive and self-contained: it touches no existing code and
// only depends on OTLP pdata (for tests) and sketchlib-go's GeometricSampler.
package otlpfilter

import (
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/ProjectASAP/sketchlib-go/common"
)

// SampleState is the shared, in-process sampling state consulted by the
// wire-filter. It maps each warm-sketch metric name to an effective sampling
// probability p in (0,1] and keeps a per-metric geometric sampler.
//
// The p-map is published lock-free via an atomic pointer so the real wiring
// (asap_edge's OnGrant -> SetP) can swap the whole map without blocking the
// hot read path. A metric absent from the map, or with p >= 1.0, is treated as
// "not sampled" (cold / raw) and passes through untouched.
//
// The geometric samplers are NOT safe for concurrent use individually, so each
// metric's sampler is guarded by its own mutex (the filter is expected to run
// on a single receive path, but we make it safe regardless).
type SampleState struct {
	// pmap : metric-name -> effective p in (0,1]. nil/absent or p>=1 => not sampled.
	pmap atomic.Pointer[map[string]float64]

	// samplers : metric-name -> *metricSampler (lazily created on first admit).
	samplers sync.Map
}

// metricSampler couples a GeometricSampler with a mutex and the p it was built
// for, so SetP can detect a changed p and rebuild the sampler deterministically.
type metricSampler struct {
	mu sync.Mutex
	gs *common.GeometricSampler
	p  float64
}

// NewSampleState returns an empty SampleState in which nothing is sampled
// (every metric passes through). Call SetP to install per-metric probabilities.
func NewSampleState() *SampleState {
	s := &SampleState{}
	empty := map[string]float64{}
	s.pmap.Store(&empty)
	return s
}

// SetP atomically replaces the metric -> p map. This is the writer called by
// asap_edge's OnGrant in the real wiring. A copy of the supplied map is stored
// so the caller may mutate its argument afterwards. Per-metric samplers whose p
// changed are dropped so they are rebuilt with the new p (and a fresh
// deterministic seed) on the next admit.
func (s *SampleState) SetP(m map[string]float64) {
	cp := make(map[string]float64, len(m))
	for k, v := range m {
		cp[k] = v
	}
	s.pmap.Store(&cp)

	// Invalidate samplers whose effective p changed (or which are no longer
	// sampled), so admit() rebuilds them lazily and reproducibly.
	s.samplers.Range(func(key, val any) bool {
		name := key.(string)
		ms := val.(*metricSampler)
		newP, ok := cp[name]
		if !ok || newP >= 1.0 {
			s.samplers.Delete(name)
			return true
		}
		ms.mu.Lock()
		if ms.p != newP {
			s.samplers.Delete(name)
		}
		ms.mu.Unlock()
		return true
	})
}

// effectiveP returns the configured p for a metric and whether it is sampled.
// A metric absent from the map or with p >= 1.0 (or non-positive) is not sampled.
func (s *SampleState) effectiveP(metric string) (float64, bool) {
	mp := s.pmap.Load()
	if mp == nil {
		return 1.0, false
	}
	p, ok := (*mp)[metric]
	if !ok || p >= 1.0 || p <= 0 {
		return 1.0, false
	}
	return p, true
}

// metricSeed derives a deterministic 64-bit seed from the metric name so test
// runs are reproducible across processes. The geometric sampler reseeds its RNG
// from this value.
func metricSeed(metric string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(metric))
	return int64(h.Sum64())
}

// samplerFor returns (creating if needed) the per-metric geometric sampler for
// a sampled metric. The sampler is seeded deterministically from the metric
// name. Returns nil if the metric is not sampled.
func (s *SampleState) samplerFor(metric string, p float64) *metricSampler {
	if v, ok := s.samplers.Load(metric); ok {
		ms := v.(*metricSampler)
		ms.mu.Lock()
		if ms.p == p {
			ms.mu.Unlock()
			return ms
		}
		// p changed underneath us; rebuild.
		ms.gs = common.NewGeometricSampler(p, metricSeed(metric))
		ms.p = p
		ms.mu.Unlock()
		return ms
	}
	ms := &metricSampler{
		gs: common.NewGeometricSampler(p, metricSeed(metric)),
		p:  p,
	}
	actual, _ := s.samplers.LoadOrStore(metric, ms)
	return actual.(*metricSampler)
}

// admit classifies a metric and, when it is sampled, draws one geometric
// admission decision for the current datapoint.
//
//   - If the metric is not in the p-map or p >= 1 => (sampled=false, keep=true):
//     the metric passes through unchanged; the caller must NOT have called this
//     per-datapoint (it is called once per metric to learn it is cold).
//   - If the metric IS sampled => (sampled=true, keep=<geometric Admit()>): the
//     caller calls this once per datapoint and keeps the datapoint iff keep.
//
// admit consumes one RNG admission only when the metric is sampled, matching
// NitroSketch's per-update geometric skip.
func (s *SampleState) admit(metric string) (sampled bool, keep bool) {
	p, isSampled := s.effectiveP(metric)
	if !isSampled {
		return false, true
	}
	ms := s.samplerFor(metric, p)
	ms.mu.Lock()
	keep = ms.gs.Admit()
	ms.mu.Unlock()
	return true, keep
}
