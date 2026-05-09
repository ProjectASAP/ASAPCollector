// Package harness builds two deterministic, semantically-equivalent
// inputs — one as pmetric.Metrics, one as []telegraf.Metric — and runs
// each through the host-neutral asap-precompute-go runtime via the
// matching adapter (otel / telegraf). It then exposes the per-sketch
// SketchEnvelope payload bytes so a parity test can assert byte-equality
// regardless of which host shape produced them.
//
// Design choices:
//
//   - Telegraf is flat: it has no resource scope. To make the OTel and
//     Telegraf inputs carry the same observation set, the OTel input
//     uses an empty Resource() — all attribute information lives on the
//     data-point. The runtime's PrecomputeConfig leaves
//     OmitResourceAttrs at false; with an empty resource, the
//     resource-segment of the SeriesKey is the empty string on both
//     paths.
//
//   - Determinism is paramount: every pseudorandom source is seeded.
//     Two runs of the harness produce byte-identical inputs and
//     byte-identical envelopes.
//
//   - Per-sketch input streams are intentionally short (compared to
//     integration/parity/) — the cross-host check is about adapter
//     transparency, not statistical fidelity. 200 samples per series is
//     enough to fill a non-trivial sketch state without bloating the
//     test.
package harness

import (
	"math"
	"math/rand"
	"sort"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// MetricNames the harness emits, one per sketch domain. Both the OTel
// and Telegraf inputs reference these so the runtime path's metric
// matcher admits exactly one envelope set per name.
const (
	MetricDDSketch    = "http_request_duration_ms"
	MetricKLL         = "query_duration_ms"
	MetricHLL         = "unique_users"
	MetricCountSketch = "error_codes"
	MetricCMS         = "request_paths"
)

// LabelSets are the per-series data-point label sets. Three sets per
// metric exercise per-series accumulation. Telegraf carries these as
// tags; OTel as data-point attributes — the codecs project both into
// the runtime's host-neutral []KeyValue Labels in identical order.
var LabelSets = []map[string]string{
	{"region": "us-east-1", "tier": "edge"},
	{"region": "us-west-2", "tier": "core"},
	{"region": "eu-central-1", "tier": "edge"},
}

// SyntheticConfig pins the deterministic input parameters.
type SyntheticConfig struct {
	// Seed controls every per-(metric, labelset) pseudorandom stream.
	Seed int64
	// StartTimestampMs is the Unix-millisecond timestamp of the first
	// sample in any series.
	StartTimestampMs uint64
	// SampleIntervalMs spaces consecutive samples within a single series.
	SampleIntervalMs uint64
	// QuantileSamples per (metric, labelset) for DDSketch and KLL.
	QuantileSamples int
	// HLLSamples per (metric, labelset).
	HLLSamples int
	// HLLDistinct is the count of distinct float IDs HLL draws from.
	HLLDistinct int
	// CountSamples per (metric, labelset) for CountSketch / CMS.
	CountSamples int
	// Zipfian generator parameters (only s > 1 is valid for math/rand).
	ZipfS    float64
	ZipfV    float64
	ZipfImax uint64
}

// DefaultSyntheticConfig returns the canonical config. Sample counts
// are smaller than integration/parity/'s defaults because cross-host
// parity is a transparency check, not a fidelity benchmark.
func DefaultSyntheticConfig() SyntheticConfig {
	return SyntheticConfig{
		Seed:             0xC205510517,
		StartTimestampMs: 1_700_000_000_000,
		SampleIntervalMs: 1_000,
		QuantileSamples:  200,
		HLLSamples:       500,
		HLLDistinct:      80,
		CountSamples:     500,
		ZipfS:            1.2,
		ZipfV:            1.0,
		ZipfImax:         100,
	}
}

// BuildOTelInput constructs the canonical pmetric.Metrics. Resource
// is intentionally empty so the input shape mirrors Telegraf's
// flat-attribute model.
func BuildOTelInput(cfg SyntheticConfig) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	// Empty Resource — Telegraf has no resource scope, so to keep the
	// host-neutral SeriesKey identical on both sides the OTel input
	// also produces no resource labels.
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("asap.cross-host.harness")

	for li, labels := range LabelSets {
		seedSalt := int64(li) * 97
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
		emitGaugeFloats(
			sm.Metrics(), MetricHLL, "1", labels,
			cfg.HLLSamples, cfg.StartTimestampMs,
			cfg.SampleIntervalMs,
			bucketedFloatStream(
				cfg.Seed^seedSalt^0xAA,
				cfg.HLLDistinct,
			),
		)
		// CountSketch ingests scalar Sum data points; the per-sample
		// "key" attribute carries a Zipfian-distributed string. The
		// runtime's countSketchObserver then UpdateString(metricName,
		// value) — same shape both adapters drive.
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
	return md
}

// CloneOTelInput deep-copies a pmetric.Metrics so per-path mutation
// can't corrupt the other path's input. Mirrors integration/parity/'s
// CloneInput defense.
func CloneOTelInput(src pmetric.Metrics) pmetric.Metrics {
	dst := pmetric.NewMetrics()
	src.CopyTo(dst)
	return dst
}

// emitGaugeFloats appends a Gauge metric with `count` float64 data
// points. Each data point's timestamp advances by intervalMs from
// startMs.
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
		dp.SetStartTimestamp(ts)
		setAttributes(dp.Attributes(), labels)
		dp.SetDoubleValue(stream())
	}
}

// emitSumZipfianKeys appends a Sum metric carrying `count` data
// points each tagged with a per-sample 'key' attribute drawn from a
// Zipfian distribution. Value is fixed at 1.0 — the frequency-sketch
// sees a count-of-events rather than a value-weighted sum.
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
		zipf = rand.NewZipf(rng, 1.2, 1.0, imax)
	}
	for i := 0; i < count; i++ {
		dp := sum.DataPoints().AppendEmpty()
		ts := pcommon.Timestamp(
			(startMs + uint64(i)*intervalMs) * 1_000_000,
		)
		dp.SetStartTimestamp(ts)
		dp.SetTimestamp(ts)
		setAttributes(dp.Attributes(), labels)
		dp.Attributes().PutStr(
			"key",
			"k"+strconv.FormatUint(zipf.Uint64(), 10),
		)
		dp.SetDoubleValue(1.0)
	}
}

// lognormalStream returns a deterministic stream of log-normal samples
// seeded from `seed`. Values stay positive so DDSketch's log-bucket
// indexer is well-defined.
func lognormalStream(seed int64, mu, sigma float64) func() float64 {
	r := rand.New(rand.NewSource(seed))
	return func() float64 {
		v := math.Exp(mu + sigma*r.NormFloat64())
		if v < 1e-9 {
			v = 1e-9
		}
		return v
	}
}

// bucketedFloatStream returns a deterministic stream of floats drawn
// uniformly from a fixed pool of `distinct` values. HLL hashes the
// float bytes — reusing the same float values gives a controllable
// cardinality target.
func bucketedFloatStream(seed int64, distinct int) func() float64 {
	r := rand.New(rand.NewSource(seed))
	pool := make([]float64, distinct)
	for i := range pool {
		pool[i] = float64(i) + 0.5
	}
	return func() float64 {
		return pool[r.Intn(len(pool))]
	}
}

// setAttributes writes a string-keyed map into a pcommon.Map in
// sorted-key order so the wire-byte ordering is stable across runs.
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
