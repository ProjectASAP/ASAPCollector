// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sketchmetricsprocessor

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// processor maintains Count-Min sketches keyed by metric name, group-by tag values, and tag key.
// Each incoming datapoint updates the relevant sketches; the processor then emits a synthetic
// metric (configured via Measurement) containing the serialized sketches and optional top-k
// entries while optionally forwarding the original data.
type sketchProcessor struct {
	cfg         Config
	groupByKeys map[string]struct{}
	cache       map[string]*aggregate
	mu          sync.Mutex
	logger      *zap.Logger
}

type aggregate struct {
	measurement string
	groupTags   map[string]string
	sketches    map[string]*CountMinSketch
}

func newProcessor(cfg *Config, logger *zap.Logger) (*sketchProcessor, error) {
	if cfg == nil {
		return nil, errInvalidConfig
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	normalized := *cfg
	if normalized.Measurement == "" {
		normalized.Measurement = "countmin"
	}
	if normalized.Rows <= 0 {
		normalized.Rows = defaultRows
	}
	if normalized.Columns <= 0 {
		normalized.Columns = defaultColumns
	}
	if normalized.Seed == 0 {
		normalized.Seed = defaultSeed
	}

	groupBy := make(map[string]struct{}, len(normalized.GroupBy))
	for _, key := range normalized.GroupBy {
		groupBy[key] = struct{}{}
	}

	return &sketchProcessor{
		cfg:         normalized,
		groupByKeys: groupBy,
		cache:       make(map[string]*aggregate),
		logger:      logger,
	}, nil
}

func (p *sketchProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.consume(md)

	if p.cfg.DropOriginal {
		md = pmetric.NewMetrics()
	}
	p.appendSketchMetrics(md)
	return md, nil
}

func (p *sketchProcessor) consume(md pmetric.Metrics) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		baseTags := map[string]string{}
		copyAttributes(rm.Resource().Attributes(), baseTags)

		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				p.consumeMetric(metrics.At(k), baseTags)
			}
		}
	}
}

func (p *sketchProcessor) consumeMetric(metric pmetric.Metric, baseTags map[string]string) {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dps := metric.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			p.consumeNumberDataPoint(metric.Name(), dps.At(i), baseTags)
		}
	case pmetric.MetricTypeSum:
		dps := metric.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			p.consumeNumberDataPoint(metric.Name(), dps.At(i), baseTags)
		}
	default:
		// Other metric types are ignored.
	}
}

func (p *sketchProcessor) consumeNumberDataPoint(metricName string, dp pmetric.NumberDataPoint, baseTags map[string]string) {
	value, ok := numberValue(dp)
	if !ok {
		return
	}

	tags := make(map[string]string, len(baseTags)+dp.Attributes().Len())
	for k, v := range baseTags {
		tags[k] = v
	}
	copyAttributes(dp.Attributes(), tags)

	groupTags := make(map[string]string, len(p.groupByKeys))
	for key := range p.groupByKeys {
		if v, ok := tags[key]; ok {
			groupTags[key] = v
		}
	}
	cacheKey := aggregateKey(metricName, groupTags)
	agg, ok := p.cache[cacheKey]
	if !ok {
		agg = &aggregate{
			measurement: metricName,
			groupTags:   copyStringMap(groupTags),
			sketches:    make(map[string]*CountMinSketch),
		}
		p.cache[cacheKey] = agg
	}

	keys := p.effectiveTagKeys(tags)
	if len(keys) == 0 {
		return
	}
	valueKey := joinTagValues(tags, keys)

	for _, tagKey := range keys {
		sk, ok := agg.sketches[tagKey]
		if !ok {
			derived := deriveSeed(p.cfg.Seed, agg.measurement, tagKey)
			var err error
			sk, err = NewCountMinSketch(p.cfg.Rows, p.cfg.Columns, derived, p.cfg.TopK)
			if err != nil {
				p.logger.Warn("sketchmetrics: failed to create sketch", zap.String("measurement", agg.measurement), zap.String("tag_key", tagKey), zap.Error(err))
				continue
			}
			agg.sketches[tagKey] = sk
		}
		sk.Insert(valueKey, value)
	}
}

func (p *sketchProcessor) appendSketchMetrics(md pmetric.Metrics) {
	if len(p.cache) == 0 {
		return
	}

	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("sketchmetricsprocessor")
	metric := sm.Metrics().AppendEmpty()
	metric.SetName(p.cfg.Measurement)
	gauge := metric.SetEmptyGauge()
	dps := gauge.DataPoints()
	now := pcommon.NewTimestampFromTime(time.Now())

	for _, agg := range p.cache {
		for tagKey, sketch := range agg.sketches {
			dp := dps.AppendEmpty()
			dp.SetTimestamp(now)
			dp.SetDoubleValue(sketch.Total())
			attrs := dp.Attributes()
			for k, v := range agg.groupTags {
				attrs.PutStr(k, v)
			}
			attrs.PutStr("source_measurement", agg.measurement)
			attrs.PutStr("tag_key", tagKey)
			attrs.PutInt("rows", int64(sketch.Rows()))
			attrs.PutInt("columns", int64(sketch.Columns()))
			attrs.PutDouble("count", sketch.Total())

			if payload, err := sketch.MarshalBinary(); err != nil {
				p.logger.Warn("sketchmetrics: failed to serialize sketch", zap.String("measurement", agg.measurement), zap.String("tag_key", tagKey), zap.Error(err))
			} else {
				attrs.PutEmptyBytes("countmin").FromRaw(payload)
			}
			if top := sketch.TopKEntries(); len(top) > 0 {
				if encoded, err := json.Marshal(top); err != nil {
					p.logger.Warn("sketchmetrics: failed to serialize topk", zap.String("measurement", agg.measurement), zap.String("tag_key", tagKey), zap.Error(err))
				} else {
					attrs.PutEmptyBytes("topk").FromRaw(encoded)
				}
			}
		}
	}
}

func (p *sketchProcessor) effectiveTagKeys(all map[string]string) []string {
	if len(p.cfg.TagKeys) > 0 {
		return p.cfg.TagKeys
	}
	keys := make([]string, 0, len(all))
	for key := range all {
		if _, skip := p.groupByKeys[key]; skip {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func numberValue(dp pmetric.NumberDataPoint) (float64, bool) {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue(), true
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue()), true
	default:
		return 0, false
	}
}

func copyAttributes(attrs pcommon.Map, dst map[string]string) {
	attrs.Range(func(k string, v pcommon.Value) bool {
		dst[k] = valueAsString(v)
		return true
	})
}

func valueAsString(v pcommon.Value) string {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeBool:
		return strconv.FormatBool(v.Bool())
	case pcommon.ValueTypeInt:
		return strconv.FormatInt(v.Int(), 10)
	case pcommon.ValueTypeDouble:
		return strconv.FormatFloat(v.Double(), 'f', -1, 64)
	case pcommon.ValueTypeBytes:
		return string(v.Bytes().AsRaw())
	default:
		return v.AsString()
	}
}

func aggregateKey(measurement string, tags map[string]string) string {
	if len(tags) == 0 {
		return measurement
	}
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(measurement)
	for _, k := range keys {
		b.WriteString(",")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(tags[k])
	}
	return b.String()
}

func copyStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func joinTagValues(tags map[string]string, keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	var b strings.Builder
	for i, key := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(key)
		b.WriteString("=")
		if v, ok := tags[key]; ok {
			b.WriteString(v)
		}
	}
	return b.String()
}
