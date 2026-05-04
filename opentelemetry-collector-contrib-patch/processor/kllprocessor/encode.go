// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kllprocessor

import (
	"time"

	kll "github.com/ProjectASAP/sketchlib-go/sketches/KLL"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// encodeEnvelopes materializes envelopes into the legacy pmetric
// output shape. TransmitSketch=true emits a typed KLLSketch metric
// per (input metric, attrs); TransmitSketch=false emits one Gauge
// per configured quantile, deserializing the envelope payload to
// query CDF.
//
// The runtime's otel.Encode supports only the TransmitSketch=true
// envelope shape today (Phase-2.5 follow-up). The shim owns the
// full encode path so it can also emit per-quantile gauges, which
// the legacy KLL processor produced when TransmitSketch was off.
func (p *kllProcessor) encodeEnvelopes(metrics pmetric.MetricSlice, inputName string, envs []*precompute.SketchEnvelope) {
	now := pcommon.NewTimestampFromTime(time.Now())
	if p.cfg.TransmitSketch {
		p.encodeTypedSketch(metrics, inputName, envs, now)
		return
	}
	p.encodeQuantileGauges(metrics, inputName, envs, now)
}

func (p *kllProcessor) encodeTypedSketch(metrics pmetric.MetricSlice, inputName string, envs []*precompute.SketchEnvelope, now pcommon.Timestamp) {
	m := metrics.AppendEmpty()
	m.SetName(p.sketchMetricName(inputName))
	m.SetEmptyKLLSketch().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	for _, env := range envs {
		if env == nil || len(env.Payload) == 0 {
			continue
		}
		dp := m.KLLSketch().DataPoints().AppendEmpty()
		labelsToAttrs(env.Labels, dp.Attributes())
		dp.Attributes().PutInt("kll.k", int64(p.cfg.K))
		dp.SetTimestamp(now)
		dp.SetCount(env.Count)
		dp.SetSketch(env.Payload)
		dp.SetEncoding(pmetric.KLLSketchEncodingProto)
	}
}

func (p *kllProcessor) encodeQuantileGauges(metrics pmetric.MetricSlice, inputName string, envs []*precompute.SketchEnvelope, now pcommon.Timestamp) {
	type qSeries struct {
		attrs pcommon.Map
		val   float64
	}
	per := make(map[float64][]qSeries, len(p.cfg.Quantiles))
	for _, env := range envs {
		if env == nil || len(env.Payload) == 0 {
			continue
		}
		sk, err := kll.DeserializeKLLSketchFromProtoBytes(env.Payload)
		if err != nil || sk == nil || sk.GetSize() == 0 {
			continue
		}
		cdf := sk.CDF()
		for _, q := range p.cfg.Quantiles {
			if _, ok := p.cfg.suffixes[q]; !ok {
				continue
			}
			attrs := pcommon.NewMap()
			labelsToAttrs(env.Labels, attrs)
			per[q] = append(per[q], qSeries{attrs: attrs, val: cdf.Query(q)})
		}
	}
	for _, q := range p.cfg.Quantiles {
		dps, ok := per[q]
		if !ok || len(dps) == 0 {
			continue
		}
		suffix := p.cfg.suffixes[q]
		name := inputName + suffix
		if p.cfg.MetricSuffix != "" {
			name = inputName + p.cfg.MetricSuffix + suffix
		}
		m := metrics.AppendEmpty()
		m.SetName(name)
		g := m.SetEmptyGauge()
		for _, d := range dps {
			dp := g.DataPoints().AppendEmpty()
			d.attrs.CopyTo(dp.Attributes())
			dp.SetTimestamp(now)
			dp.SetDoubleValue(d.val)
		}
	}
}

// sketchMetricName returns the output metric name for the
// TransmitSketch=true path. Mirrors the legacy emit's behavior:
// MetricSuffix overrides the implicit "_kll" suffix entirely.
func (p *kllProcessor) sketchMetricName(base string) string {
	if p.cfg.MetricSuffix != "" {
		return base + p.cfg.MetricSuffix
	}
	return base + "_kll"
}

// labelsToAttrs copies host-neutral KeyValues into a pcommon.Map.
func labelsToAttrs(kvs []precompute.KeyValue, dst pcommon.Map) {
	for _, kv := range kvs {
		dst.PutStr(kv.Key, kv.Value)
	}
}

// appendMetrics moves all ResourceMetrics from src into dst. Used by
// the batch-mode ConsumeMetrics path to graft the synthesized output
// onto the input md before forwarding (preserves the legacy "single
// downstream call carries both" contract).
func appendMetrics(dst, src pmetric.Metrics) {
	srcRMs := src.ResourceMetrics()
	for i := 0; i < srcRMs.Len(); i++ {
		srcRMs.At(i).MoveTo(dst.ResourceMetrics().AppendEmpty())
	}
}
