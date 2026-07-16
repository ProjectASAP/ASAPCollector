// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"hash/maphash"
	"sync"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// attrMapPool reuses the decoded attribute map across samples (the shared
// decode: pcommon.Map -> map[string]string, used for the gorilla key +
// cold AddSample + sum, then returned).
var attrMapPool = sync.Pool{New: func() any { return make(map[string]string, 16) }}

func getAttrMap(src pcommon.Map) map[string]string {
	m := attrMapPool.Get().(map[string]string)
	for k := range m {
		delete(m, k)
	}
	src.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}

func putAttrMap(m map[string]string) { attrMapPool.Put(m) }

// rowSampledAdmittedRowsKey / rowSampledRowsKey / rowSampledSamplePKey mirror
// the reserved attribute keys the SDK's OTLP wire transform stamps on a
// RowSampledSketchDataPoint (see otlpmetricgrpc/otlpmetrichttp's
// internal/transform/metricdata.go). A data point carrying these attrs is
// not a normal aggregated sample — it is one individually admitted RAW
// occurrence, already row-sampled at Record() time by an OTel SDK running
// AggregationRowSampledSketch (NitroSketch-style skip sampling). They MUST
// be stripped before the remaining attributes are used as series identity
// (gorilla key / AggregateBy grouping / emitted labels) — they are
// wire-transport metadata, not series-identifying dimensions.
const (
	rowSampledAdmittedRowsKey = "__asap_row_sampled_admitted_rows"
	rowSampledRowsKey         = "__asap_row_sampled_rows"
	rowSampledSamplePKey      = "__asap_row_sampled_sample_p"
)

// extractRowSampledMeta reports whether dp is a row-sampled raw occurrence
// (signaled by the presence of rowSampledAdmittedRowsKey, which the SDK
// always stamps alongside the other two reserved keys) and, if so, decodes
// the admission bitmask and sample probability the SDK computed.
func extractRowSampledMeta(attrs pcommon.Map) (rowSampled bool, admittedRows uint64, sampleP float64) {
	v, ok := attrs.Get(rowSampledAdmittedRowsKey)
	if !ok {
		return false, 0, 0
	}
	admittedRows = uint64(v.Int())
	if p, ok := attrs.Get(rowSampledSamplePKey); ok {
		sampleP = p.Double()
	}
	return true, admittedRows, sampleP
}

func (p *asapEdgeProcessor) shardForKey(key string) int {
	if len(p.shards) <= 1 {
		return 0
	}
	var h maphash.Hash
	h.SetSeed(p.hashSeed)
	_, _ = h.WriteString(key)
	return int(h.Sum64() % uint64(len(p.shards)))
}

// ConsumeMetrics is the single decode pass: each data point's attributes are
// decoded once, the gorilla series key is built once (for shard selection), and
// the sample is dispatched to its shard's cold fragment encoder + (if
// configured) the metric's warm aggregator. DropOriginal controls raw
// passthrough; warm/cold output is emitted on the flush tick.
func (p *asapEdgeProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				p.consumeMetric(ms.At(k))
			}
		}
	}
	if !p.cfg.DropOriginal {
		return p.next.ConsumeMetrics(ctx, md) // forward everything raw
	}
	// DropOriginal: forward only UNCONFIGURED (passthrough) metrics — e.g.
	// freshness probes and any metric with no configured entry. The raw of any
	// CONFIGURED metric is dropped here regardless of tier: a warm/both metric's
	// sum/sketch output is emitted on the flush tick, and a cold/both metric's
	// raw is carried by the cold archive (added above). Unconfigured metrics
	// are still cold-archived and forwarded raw, unchanged.
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				// Never remove an unsupported metric type (Histogram/Summary/
				// ExponentialHistogram): we don't aggregate or archive it, so the
				// only place it survives is the passthrough stream. Removing it here
				// would be total data loss regardless of how it is configured.
				if isUnsupportedType(m.Type()) {
					return false
				}
				_, isConfigured := p.configured[m.Name()]
				return isConfigured
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
	if md.ResourceMetrics().Len() == 0 {
		return nil
	}
	return p.next.ConsumeMetrics(ctx, md)
}

func (p *asapEdgeProcessor) consumeMetric(m pmetric.Metric) {
	name := m.Name()
	// coldArchive: add raw samples to the cold gorilla stream unless this
	// metric is configured tier=warm. Unconfigured metrics and tier∈{both,cold}
	// are archived as before.
	_, coldSkip := p.coldSkip[name]
	coldArchive := !coldSkip

	var dps pmetric.NumberDataPointSlice
	switch m.Type() {
	case pmetric.MetricTypeSum:
		dps = m.Sum().DataPoints()
	case pmetric.MetricTypeGauge:
		dps = m.Gauge().DataPoints()
	default:
		// Histogram / Summary / ExponentialHistogram are not warm-aggregated yet.
		// They must NEVER be silently dropped: count them, log once, and let the
		// passthrough path forward them unchanged. Critically, such a metric is
		// removed from p.unsupportedPassthrough (see ConsumeMetrics) so it is
		// always forwarded even under drop_original/tier=cold — otherwise a
		// histogram configured drop_original would be removed from the output
		// stream AND never archived (total data loss).
		p.recordUnsupportedType(m)
		return
	}

	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		rowSampled, admittedRows, sampleP := extractRowSampledMeta(dp.Attributes())
		am := getAttrMap(dp.Attributes()) // shared decode (once)
		if rowSampled {
			delete(am, rowSampledAdmittedRowsKey)
			delete(am, rowSampledRowsKey)
			delete(am, rowSampledSamplePKey)
		}
		key := gorilla.SeriesKey(name, am, p.coldExtLabels)
		val := numberValue(dp)
		ts := dp.Timestamp().AsTime()
		p.observeMax(uint64(ts.UnixMilli()))

		tsMs := uint64(ts.UnixMilli())
		sh := p.shards[p.shardForKey(key)]
		sh.mu.Lock()
		// A row-sampled data point is one individually admitted raw
		// occurrence, not a normal aggregated sample — it is never
		// cold-archived (the cold path expects real aggregate samples, and
		// the whole point of SDK-side row sampling is fewer, not more, raw
		// points reaching the collector).
		if sh.cold != nil && coldArchive && !rowSampled {
			// The fragment encoder rekeys internally by (metric, attrs); the
			// shared SeriesKey above is kept for shard selection only.
			_ = sh.cold.AddSample(gorilla.TSDBSample{
				MetricName: name,
				Attributes: am,
				Timestamp:  ts,
				Value:      val,
			})
		}
		if sa := sh.sketchAggs[name]; sa != nil {
			sa.observe(am, val, tsMs, rowSampled, admittedRows, sampleP)
		}
		sh.mu.Unlock()
		putAttrMap(am)
	}
}

// isUnsupportedType reports whether the metric type is one the warm/cold
// aggregation path does not handle (so it must be forwarded raw, never
// dropped).
func isUnsupportedType(t pmetric.MetricType) bool {
	switch t {
	case pmetric.MetricTypeSum, pmetric.MetricTypeGauge:
		return false
	default:
		return true
	}
}

// recordUnsupportedType increments the unsupported-type data-point counter and
// logs a single warning the first time an unsupported type is seen. The metric
// itself is left untouched in md so the passthrough path forwards it unchanged.
func (p *asapEdgeProcessor) recordUnsupportedType(m pmetric.Metric) {
	n := uint64(1)
	switch m.Type() {
	case pmetric.MetricTypeHistogram:
		n = uint64(m.Histogram().DataPoints().Len())
	case pmetric.MetricTypeExponentialHistogram:
		n = uint64(m.ExponentialHistogram().DataPoints().Len())
	case pmetric.MetricTypeSummary:
		n = uint64(m.Summary().DataPoints().Len())
	}
	if n == 0 {
		n = 1
	}
	p.unsupportedTypeCount.Add(n)
	if p.loggedUnsupported.CompareAndSwap(false, true) {
		p.logger.Warn("asap_edge: metric type not warm-aggregated; forwarding raw unchanged (logged once)",
			zap.String("metric", m.Name()), zap.String("type", m.Type().String()))
	}
}

func (p *asapEdgeProcessor) observeMax(tsMs uint64) {
	for {
		cur := p.maxObservedMs.Load()
		if tsMs <= cur || p.maxObservedMs.CompareAndSwap(cur, tsMs) {
			return
		}
	}
}

// resetMaxObserved lowers the max-observed watermark to the new window
// boundary at flush time. Without it, observeMax only ever RAISES the value, so
// a single future-timestamped sample would permanently skew every later
// window's emit-time end timestamp. It runs from the single flush goroutine;
// the watermark only bounds the emitted point's [start,end] (it is not part of
// the summed value), so a best-effort Store racing a concurrent observeMax CAS
// is acceptable — at worst one window's end is off by one late sample.
func (p *asapEdgeProcessor) resetMaxObserved(windowEndMs uint64) {
	p.maxObservedMs.Store(windowEndMs)
}

func numberValue(dp pmetric.NumberDataPoint) float64 {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(dp.IntValue())
	}
	return dp.DoubleValue()
}
