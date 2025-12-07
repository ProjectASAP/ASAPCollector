// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ddsketchprocessor

import (
	"context"
	"sort"
	"strings"

	"github.com/DataDog/sketches-go/ddsketch"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type ddsketchProcessor struct {
	cfg    *Config
	logger *zap.Logger
}

func newProcessor(cfg *Config, logger *zap.Logger) *ddsketchProcessor {
	return &ddsketchProcessor{cfg: cfg, logger: logger}
}

func (p *ddsketchProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			p.processScopeMetrics(sms.At(j))
		}
	}
	return md, nil
}

func (p *ddsketchProcessor) processScopeMetrics(sm pmetric.ScopeMetrics) {
	metrics := sm.Metrics()
	originalLen := metrics.Len()
	for i := 0; i < originalLen; i++ {
		metric := metrics.At(i)
		if newMetric, ok := p.buildDDSketchMetric(metric); ok {
			newMetric.CopyTo(metrics.AppendEmpty())
		}
	}
}

type sketchSeries struct {
	attrs  pcommon.Map
	sketch *ddsketch.DDSketch
	count  uint64
	sum    float64
	start  pcommon.Timestamp
	end    pcommon.Timestamp
}

func (p *ddsketchProcessor) buildDDSketchMetric(src pmetric.Metric) (pmetric.Metric, bool) {
	var series map[string]*sketchSeries
	switch src.Type() {
	case pmetric.MetricTypeSum:
		series = p.consumeNumberDataPoints(src.Sum().DataPoints())
	case pmetric.MetricTypeGauge:
		series = p.consumeNumberDataPoints(src.Gauge().DataPoints())
	default:
		return pmetric.Metric{}, false
	}

	if len(series) == 0 {
		return pmetric.Metric{}, false
	}

	out := pmetric.NewMetric()
	out.SetName(src.Name() + p.cfg.MetricSuffix)
	out.SetDescription("DDSketch summary for " + src.Name())
	out.SetUnit(src.Unit())
	summary := out.SetEmptySummary()

	dps := summary.DataPoints()
	for _, s := range series {
		if s.count == 0 {
			continue
		}
		dp := dps.AppendEmpty()
		s.attrs.CopyTo(dp.Attributes())
		dp.SetCount(s.count)
		dp.SetSum(s.sum)
		dp.SetStartTimestamp(s.start)
		dp.SetTimestamp(s.end)
		quantiles := dp.QuantileValues()
		for _, q := range p.cfg.Quantiles {
			v, err := s.sketch.GetValueAtQuantile(q)
			if err != nil {
				if p.logger != nil {
					p.logger.Debug("failed to read quantile", zap.Float64("quantile", q), zap.Error(err))
				}
				continue
			}
			qv := quantiles.AppendEmpty()
			qv.SetQuantile(q)
			qv.SetValue(v)
		}
	}

	return out, true
}

func (p *ddsketchProcessor) consumeNumberDataPoints(dps pmetric.NumberDataPointSlice) map[string]*sketchSeries {
	if dps.Len() == 0 {
		return nil
	}
	result := make(map[string]*sketchSeries)
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		value, ok := numberValue(dp)
		if !ok {
			continue
		}
		key := attributesKey(dp.Attributes())
		series := result[key]
		if series == nil {
			series = newSketchSeries(dp.Attributes(), dp.StartTimestamp(), dp.Timestamp(), p.cfg.RelativeAccuracy, p.logger)
			if series == nil {
				continue
			}
			result[key] = series
		}
		series.observe(value, dp.StartTimestamp(), dp.Timestamp())
	}
	return result
}

func numberValue(dp pmetric.NumberDataPoint) (float64, bool) {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue()), true
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue(), true
	default:
		return 0, false
	}
}

func newSketchSeries(attrs pcommon.Map, start, ts pcommon.Timestamp, accuracy float64, logger *zap.Logger) *sketchSeries {
	sk, err := ddsketch.NewDefaultDDSketch(accuracy)
	if err != nil {
		if logger != nil {
			logger.Error("failed to allocate DDSketch", zap.Error(err))
		}
		return nil
	}
	attrCopy := pcommon.NewMap()
	attrs.CopyTo(attrCopy)
	series := &sketchSeries{
		attrs:  attrCopy,
		sketch: sk,
		start:  start,
		end:    ts,
	}
	return series
}

func (s *sketchSeries) observe(value float64, start, ts pcommon.Timestamp) {
	_ = s.sketch.Add(value)
	s.count++
	s.sum += value
	if s.start == 0 || (start != 0 && start < s.start) {
		s.start = start
	}
	if ts > s.end {
		s.end = ts
	}
}

func attributesKey(attrs pcommon.Map) string {
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	b := strings.Builder{}
	for _, k := range keys {
		v, ok := attrs.Get(k)
		if !ok {
			continue
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(v.AsString())
		b.WriteByte(';')
	}
	return b.String()
}
