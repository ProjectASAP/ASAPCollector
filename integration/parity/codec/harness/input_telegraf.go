package harness

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/influxdata/telegraf"
	telegrafmetric "github.com/influxdata/telegraf/metric"
)

// BuildTelegrafInput constructs the canonical []telegraf.Metric for
// the cross-host parity harness. One telegraf.Metric is emitted per
// data point in the equivalent OTel input — Telegraf's data model is
// "one metric per observation", so the per-(metric, labelset, sample)
// expansion is mandatory.
//
// Determinism comes from feeding identical seeds to identical
// generators per (metric, labelset) — exactly the streams used by
// BuildOTelInput. Each sketch domain's iteration order
// (labelsets-outermost, samples-innermost) matches the OTel side
// verbatim, so the runtime sees an identical observation order on
// both paths.
//
// Per the design doc, Telegraf has no resource scope, so the harness
// projects all attribute information into per-metric tags. The OTel
// input uses an empty Resource() to mirror this — see input_otel.go.
//
// The 'value' field name matches the Telegraf codec's
// DefaultValueField; we don't override it on the Adapter side either
// (a nil AdapterConfig is supplied), so Decode reads metric.Fields()
// ["value"] on every sample.
func BuildTelegrafInput(cfg SyntheticConfig) []telegraf.Metric {
	out := make([]telegraf.Metric, 0,
		len(LabelSets)*(2*cfg.QuantileSamples+cfg.HLLSamples+2*cfg.CountSamples))

	for li, labels := range LabelSets {
		seedSalt := int64(li) * 97
		// DDSketch
		emitTGGaugeFloats(&out, MetricDDSketch, labels,
			cfg.QuantileSamples, cfg.StartTimestampMs,
			cfg.SampleIntervalMs,
			lognormalStream(cfg.Seed^seedSalt^0xDD, 4.0, 0.5),
		)
		// KLL
		emitTGGaugeFloats(&out, MetricKLL, labels,
			cfg.QuantileSamples, cfg.StartTimestampMs,
			cfg.SampleIntervalMs,
			lognormalStream(cfg.Seed^seedSalt^0xCC, 5.0, 0.7),
		)
		// HLL
		emitTGGaugeFloats(&out, MetricHLL, labels,
			cfg.HLLSamples, cfg.StartTimestampMs,
			cfg.SampleIntervalMs,
			bucketedFloatStream(cfg.Seed^seedSalt^0xAA, cfg.HLLDistinct),
		)
		// CountSketch — Zipfian per-sample 'key' tag, fixed value 1.0.
		emitTGSumZipfianKeys(&out, MetricCountSketch, labels,
			cfg.CountSamples, cfg.StartTimestampMs,
			cfg.SampleIntervalMs,
			cfg.Seed^seedSalt^0xBB,
			cfg.ZipfS, cfg.ZipfV, cfg.ZipfImax,
		)
		// CMS — same pattern, different seed salt.
		emitTGSumZipfianKeys(&out, MetricCMS, labels,
			cfg.CountSamples, cfg.StartTimestampMs,
			cfg.SampleIntervalMs,
			cfg.Seed^seedSalt^0x99,
			cfg.ZipfS, cfg.ZipfV, cfg.ZipfImax,
		)
	}
	return out
}

// emitTGGaugeFloats appends `count` telegraf.Metric instances each
// representing one Gauge data point. The value lives in field "value"
// (the codec's DefaultValueField) and the labelset becomes tags.
func emitTGGaugeFloats(
	out *[]telegraf.Metric,
	name string,
	labels map[string]string,
	count int,
	startMs, intervalMs uint64,
	stream func() float64,
) {
	for i := 0; i < count; i++ {
		ts := time.UnixMilli(int64(startMs + uint64(i)*intervalMs)).UTC()
		tags := copyMap(labels)
		fields := map[string]interface{}{"value": stream()}
		*out = append(*out, telegrafmetric.New(name, tags, fields, ts))
	}
}

// emitTGSumZipfianKeys appends `count` telegraf.Metric instances each
// carrying a per-sample 'key' tag drawn from a Zipfian distribution
// (matching the OTel-side Sum data point's per-sample 'key'
// attribute). Value is fixed at 1.0 — frequency-sketch consumers see
// a count-of-events rather than a value-weighted sum.
func emitTGSumZipfianKeys(
	out *[]telegraf.Metric,
	name string,
	labels map[string]string,
	count int,
	startMs, intervalMs uint64,
	seed int64,
	s, v float64,
	imax uint64,
) {
	rng := rand.New(rand.NewSource(seed))
	zipf := rand.NewZipf(rng, s, v, imax)
	if zipf == nil {
		zipf = rand.NewZipf(rng, 1.2, 1.0, imax)
	}
	for i := 0; i < count; i++ {
		ts := time.UnixMilli(int64(startMs + uint64(i)*intervalMs)).UTC()
		tags := copyMap(labels)
		tags["key"] = "k" + strconv.FormatUint(zipf.Uint64(), 10)
		fields := map[string]interface{}{"value": 1.0}
		*out = append(*out, telegrafmetric.New(name, tags, fields, ts))
	}
}

// copyMap shallow-copies a string-string map so mutations on the per-
// metric tag map can't bleed across emissions.
func copyMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
