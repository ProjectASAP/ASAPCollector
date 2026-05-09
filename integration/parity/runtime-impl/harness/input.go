// Package harness builds a deterministic, multi-metric pmetric.Metrics
// payload covering all five ASAP sketch domains (DDSketch, KLL, HLL,
// CountSketch, CountMinSketch), and runs that payload through both the
// asap-precompute-go runtime (Path A) and the legacy OTel processor
// stack (Path B), then reports byte-level divergences in the emitted
// SketchEnvelopes.
//
// Determinism: every pseudorandom source uses a seeded math/rand.Source.
// Two test runs of the same harness MUST produce byte-identical inputs.
package harness

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// MetricNames the harness emits, one per sketch type. Both Path A
// (runtime) and Path B (legacy) match on these names to scope the
// per-sketch pipeline.
const (
	MetricDDSketch    = "http_request_duration_ms"
	MetricKLL         = "query_duration_ms"
	MetricHLL         = "unique_users"
	MetricCountSketch = "error_codes"
	MetricCMS         = "request_paths"
)

// Resources used in the synthetic input. Two distinct ResourceMetrics
// blocks exercise the resource-key portion of SeriesKey.
var Resources = []map[string]string{
	{"service.name": "svc-a", "deployment.environment": "prod"},
	{"service.name": "svc-b", "deployment.environment": "prod"},
}

// Per-resource label sets — three distinct attribute sets per metric
// per resource exercise per-series accumulation.
var LabelSets = []map[string]string{
	{"region": "us-east-1", "tier": "edge"},
	{"region": "us-west-2", "tier": "core"},
	{"region": "eu-central-1", "tier": "edge"},
}

// SyntheticConfig pins the deterministic input parameters. All fields
// are deliberately fixed; do NOT introduce wall-clock randomness.
type SyntheticConfig struct {
	// Seed controls the per-metric pseudorandom value generator.
	Seed int64
	// StartTimestampMs is the Unix-millisecond timestamp of the first
	// sample. Subsequent samples increment by SampleIntervalMs.
	StartTimestampMs uint64
	// SampleIntervalMs spaces consecutive samples within a single
	// (metric, resource, label-set) series.
	SampleIntervalMs uint64
	// QuantileSamples is the per-(metric,resource,labelset) sample
	// count for DDSketch and KLL. 1k per the spec.
	QuantileSamples int
	// HLLSamples is the per-(metric,resource,labelset) sample count
	// for HLL — 5k samples ~500 distinct (k=10 reuse) per spec.
	HLLSamples int
	// HLLDistinct is the count of distinct hashed user-IDs to draw
	// from per HLL series.
	HLLDistinct int
	// CountSamples is the per-(metric,resource,labelset) sample count
	// for CountSketch / CMS — 5k Zipfian-distributed keys per spec.
	CountSamples int
	// ZipfS / ZipfV / ZipfImax control the Zipfian generator the
	// frequency-sketch metrics draw from.
	ZipfS    float64
	ZipfV    float64
	ZipfImax uint64
}

// DefaultSyntheticConfig returns the canonical harness input config.
// Changing these values changes the byte-level payloads — only adjust
// when intentionally re-baselining.
func DefaultSyntheticConfig() SyntheticConfig {
	return SyntheticConfig{
		Seed:             0xA5AC011EC701, // "ASAPColleC[t]01"
		StartTimestampMs: 1_700_000_000_000,
		SampleIntervalMs: 1_000,
		QuantileSamples:  1_000,
		HLLSamples:       5_000,
		HLLDistinct:      500,
		CountSamples:     5_000,
		ZipfS:            1.2,
		ZipfV:            1.0,
		ZipfImax:         200,
	}
}

// BuildInput constructs the canonical multi-metric pmetric.Metrics
// payload covering all five sketch domains. The same payload is fed
// to Path A and Path B; both code paths must observe identical
// observation streams (modulo their own input-decoding rules).
func BuildInput(cfg SyntheticConfig) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for ri, resAttrs := range Resources {
		rm := md.ResourceMetrics().AppendEmpty()
		setAttributes(rm.Resource().Attributes(), resAttrs)
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("asap.parity.harness")
		// Each metric gets its own per-(resource, labelset)
		// pseudorandom stream seeded deterministically from
		// Seed XOR resource-index XOR label-index. Without the
		// per-resource salt, both resources would observe
		// identical samples — which silently masks resource-key
		// bugs in either pipeline.
		for li, labels := range LabelSets {
			seedSalt := int64(ri)*1_000_003 + int64(li)*97
			emitGaugeFloats(
				sm.Metrics(), MetricDDSketch, "ms", labels,
				cfg.QuantileSamples, cfg.StartTimestampMs,
				cfg.SampleIntervalMs,
				lognormalStream(cfg.Seed^seedSalt^0xDD, 4.0, 0.5),
			)
			emitGaugeFloats(
				sm.Metrics(), MetricKLL, "ms", labels,
				cfg.QuantileSamples, cfg.StartTimestampMs,
				cfg.SampleIntervalMs,
				lognormalStream(cfg.Seed^seedSalt^0xCC, 5.0, 0.7),
			)
			// HLL ingests scalar gauge values; the legacy HLL
			// processor's UpdateValue hashes the float bytes,
			// so we feed a deterministic sequence of float
			// IDs drawn from a small distinct pool to land
			// at ~500 distinct over 5k samples.
			emitGaugeFloats(
				sm.Metrics(), MetricHLL, "1", labels,
				cfg.HLLSamples, cfg.StartTimestampMs,
				cfg.SampleIntervalMs,
				bucketedFloatStream(
					cfg.Seed^seedSalt^0xAA,
					cfg.HLLDistinct,
				),
			)
			// CountSketch / CMS ingest Sum data points; legacy
			// processors UpdateString on the metric name and
			// hash the data-point attribute set into the
			// frequency sketch. We add a 'key' attribute that
			// follows a Zipfian distribution so the underlying
			// frequency table sees a heavy-tailed input
			// (matches realistic top-k workloads).
			emitSumZipfianKeys(
				sm.Metrics(), MetricCountSketch, "1", labels,
				cfg.CountSamples, cfg.StartTimestampMs,
				cfg.SampleIntervalMs,
				cfg.Seed^seedSalt^0xBB,
				cfg.ZipfS, cfg.ZipfV, cfg.ZipfImax,
			)
			emitSumZipfianKeys(
				sm.Metrics(), MetricCMS, "1", labels,
				cfg.CountSamples, cfg.StartTimestampMs,
				cfg.SampleIntervalMs,
				cfg.Seed^seedSalt^0x99,
				cfg.ZipfS, cfg.ZipfV, cfg.ZipfImax,
			)
		}
	}
	return md
}

// CloneInput produces a deep copy of the harness input. The runtime
// path mutates internal state on Decode, but the legacy processor
// path may also mutate the pdata it receives (Capabilities.MutatesData
// is true for every legacy sketch processor). Feeding both the same
// physical pmetric.Metrics would therefore corrupt one of them. The
// harness gives each path its own fresh copy.
func CloneInput(src pmetric.Metrics) pmetric.Metrics {
	dst := pmetric.NewMetrics()
	src.CopyTo(dst)
	return dst
}

// emitGaugeFloats appends a Gauge metric with `count` float64 data
// points. Each data point's timestamp advances by `intervalMs` from
// `startMs`. Stream is a function returning the next sample value;
// ordering is preserved across data-point emission.
func emitGaugeFloats(
	metrics pmetric.MetricSlice,
	name, unit string,
	labels map[string]string,
	count int,
	startMs, intervalMs uint64,
	stream func() float64,
) {
	m := metrics.AppendEmpty()
	m.SetName(name)
	m.SetUnit(unit)
	g := m.SetEmptyGauge()
	for i := 0; i < count; i++ {
		dp := g.DataPoints().AppendEmpty()
		ts := pcommon.Timestamp(
			(startMs + uint64(i)*intervalMs) * 1_000_000,
		)
		dp.SetTimestamp(ts)
		// Use Timestamp as both start and end so the data point
		// carries an unambiguous wall-clock anchor for both
		// pipelines' window-boundary logic. Without StartTs the
		// runtime's WindowStartMs is zero on the first emission,
		// which the legacy processor doesn't replicate.
		dp.SetStartTimestamp(ts)
		setAttributes(dp.Attributes(), labels)
		dp.SetDoubleValue(stream())
	}
}

// emitSumZipfianKeys appends a Sum metric carrying `count` data
// points each tagged with a `key` attribute drawn from a Zipfian
// distribution. The data point's value is fixed at 1.0 so the
// frequency-sketch's "sample weight" is unambiguous — both Path A and
// Path B observe a count-of-events, not a value-weighted sum.
func emitSumZipfianKeys(
	metrics pmetric.MetricSlice,
	name, unit string,
	labels map[string]string,
	count int,
	startMs, intervalMs uint64,
	seed int64,
	s, v float64,
	imax uint64,
) {
	m := metrics.AppendEmpty()
	m.SetName(name)
	m.SetUnit(unit)
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	rng := rand.New(rand.NewSource(seed))
	zipf := rand.NewZipf(rng, s, v, imax)
	if zipf == nil {
		// rand.NewZipf returns nil when s ≤ 1; default config
		// sets s=1.2 so this branch is purely defensive.
		zipf = rand.NewZipf(rng, 1.2, 1.0, imax)
	}
	for i := 0; i < count; i++ {
		dp := sum.DataPoints().AppendEmpty()
		ts := pcommon.Timestamp(
			(startMs + uint64(i)*intervalMs) * 1_000_000,
		)
		dp.SetStartTimestamp(ts)
		dp.SetTimestamp(ts)
		// Clone the labels, then add a per-sample 'key' attr.
		setAttributes(dp.Attributes(), labels)
		dp.Attributes().PutStr(
			"key",
			"k"+strconv.FormatUint(zipf.Uint64(), 10),
		)
		dp.SetDoubleValue(1.0)
	}
}

// lognormalStream returns a deterministic stream of log-normal
// distributed samples seeded from `seed`. Used by DDSketch and KLL
// metrics; values stay positive (a hard requirement for DDSketch).
func lognormalStream(seed int64, mu, sigma float64) func() float64 {
	r := rand.New(rand.NewSource(seed))
	return func() float64 {
		// rand.NormFloat64 is the standard Box-Muller normal;
		// exponentiating gives a log-normal sample. We clamp to
		// >= 1e-9 so DDSketch never sees a zero (its log-bucket
		// indexer is undefined at 0).
		v := math.Exp(mu + sigma*r.NormFloat64())
		if v < 1e-9 {
			v = 1e-9
		}
		return v
	}
}

// bucketedFloatStream returns a deterministic stream of floats drawn
// uniformly from a fixed pool of `distinct` values. HLL's input is
// hashed by float-byte representation, so reusing the same float
// values gives a controllable cardinality target. Pool size = distinct.
func bucketedFloatStream(seed int64, distinct int) func() float64 {
	r := rand.New(rand.NewSource(seed))
	pool := make([]float64, distinct)
	for i := range pool {
		// Spread the pool across a wide-enough range that float
		// rounding doesn't collapse two adjacent entries to the
		// same hash bucket.
		pool[i] = float64(i) + 0.5
	}
	return func() float64 {
		return pool[r.Intn(len(pool))]
	}
}

// setAttributes copies a string-keyed map into an attribute map in
// sorted-key order so the on-the-wire ordering is byte-stable across
// runs. (pcommon.Map preserves insertion order.)
func setAttributes(dst pcommon.Map, src map[string]string) {
	keys := make([]string, 0, len(src))
	for k := range src {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		dst.PutStr(k, src[k])
	}
}

// FormatLabels renders a label map as a stable string for diff
// messages. Used only by the diff reporter.
func FormatLabels(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for _, k := range keys {
		out += fmt.Sprintf("%s=%s,", k, m[k])
	}
	return out
}
