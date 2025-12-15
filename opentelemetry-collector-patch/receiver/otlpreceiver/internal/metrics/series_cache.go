package metrics

import (
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

const defaultSeriesCacheTTL = time.Hour

type seriesCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	sources map[string]*seriesSource
}

type seriesSource struct {
	mu     sync.RWMutex
	series map[uint64]*seriesRecord
}

type seriesRecord struct {
	metricName string
	dataType   pmetric.MetricType
	attrs      pcommon.Map
	lastSeen   time.Time
}

func newSeriesCache(ttl time.Duration) *seriesCache {
	return &seriesCache{
		ttl:     ttl,
		sources: make(map[string]*seriesSource),
	}
}

// rehydrate ensures every data point has attributes populated, either by
// storing newly seen (series_id, attributes) pairs or by copying cached
// attributes back onto ID-only points before the batch is consumed.
func (sc *seriesCache) rehydrate(md pmetric.Metrics) {
	if sc == nil {
		return
	}

	now := time.Now()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		source := sc.getOrCreateSource(buildResourceKey(rm))
		scopeMetrics := rm.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			metrics := scopeMetrics.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				sc.rehydrateMetricLocked(source, metrics.At(k), now)
			}
		}
	}
	sc.evictLocked(now)
}

func (sc *seriesCache) rehydrateMetricLocked(src *seriesSource, m pmetric.Metric, now time.Time) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		sc.rehydrateNumberDataPointsLocked(src, m.Name(), m.Type(), m.Gauge().DataPoints(), now)
	case pmetric.MetricTypeSum:
		sc.rehydrateNumberDataPointsLocked(src, m.Name(), m.Type(), m.Sum().DataPoints(), now)
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, m.Name(), m.Type(), dp.SeriesID(), dp.Attributes(), now)
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, m.Name(), m.Type(), dp.SeriesID(), dp.Attributes(), now)
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, m.Name(), m.Type(), dp.SeriesID(), dp.Attributes(), now)
		}
	case pmetric.MetricTypeDDSketch:
		dps := m.DDSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, m.Name(), m.Type(), dp.SeriesID(), dp.Attributes(), now)
		}
	default:
	}
}

func (sc *seriesCache) rehydrateNumberDataPointsLocked(src *seriesSource, metricName string, dataType pmetric.MetricType, dps pmetric.NumberDataPointSlice, now time.Time) {
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		sc.rehydrateDataPointLocked(src, metricName, dataType, dp.SeriesID(), dp.Attributes(), now)
	}
}

func (sc *seriesCache) rehydrateDataPointLocked(src *seriesSource, metricName string, dataType pmetric.MetricType, seriesID uint64, attrs pcommon.Map, now time.Time) {
	if attrs.Len() > 0 {
		if seriesID == 0 {
			return
		}
		storeSeries(src, seriesID, metricName, dataType, attrs, now)
		return
	}

	if seriesID == 0 {
		return
	}

	if !hydrateFromSeries(src, seriesID, metricName, dataType, attrs, now) {
		zap.L().Debug("series id not found", zap.Uint64("series_id", seriesID), zap.String("metric", metricName))
	}
}

func (sc *seriesCache) ensureSourceLocked(sourceKey string) *seriesSource {
	src := sc.sources[sourceKey]
	if src == nil {
		src = &seriesSource{
			series: make(map[uint64]*seriesRecord),
		}
		sc.sources[sourceKey] = src
	}
	return src
}

func (sc *seriesCache) getOrCreateSource(sourceKey string) *seriesSource {
	sc.mu.RLock()
	src := sc.sources[sourceKey]
	sc.mu.RUnlock()
	if src != nil {
		return src
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.ensureSourceLocked(sourceKey)
}

func storeSeries(src *seriesSource, id uint64, metricName string, dataType pmetric.MetricType, attrs pcommon.Map, now time.Time) {
	src.mu.Lock()
	defer src.mu.Unlock()

	cloned := pcommon.NewMap()
	attrs.CopyTo(cloned)
	if src.series == nil {
		src.series = make(map[uint64]*seriesRecord)
	}
	src.series[id] = &seriesRecord{
		metricName: metricName,
		dataType:   dataType,
		attrs:      cloned,
		lastSeen:   now,
	}
}

func hydrateFromSeries(src *seriesSource, id uint64, metricName string, dataType pmetric.MetricType, attrs pcommon.Map, now time.Time) bool {
	src.mu.Lock()
	defer src.mu.Unlock()

	rec, ok := src.series[id]
	if !ok || rec.metricName != metricName || rec.dataType != dataType {
		return false
	}

	rec.lastSeen = now
	attrs.Clear()
	rec.attrs.CopyTo(attrs)
	return true
}

func (sc *seriesCache) evictLocked(now time.Time) {
	if sc.ttl <= 0 {
		return
	}
	cutoff := now.Add(-sc.ttl)
	sc.mu.Lock()
	defer sc.mu.Unlock()

	for key, src := range sc.sources {
		src.mu.Lock()
		for id, rec := range src.series {
			if rec.lastSeen.Before(cutoff) {
				delete(src.series, id)
			}
		}
		empty := len(src.series) == 0
		src.mu.Unlock()
		if empty {
			delete(sc.sources, key)
		}
	}
}

func buildResourceKey(rm pmetric.ResourceMetrics) string {
	builder := strings.Builder{}
	builder.WriteString(rm.SchemaUrl())
	builder.WriteByte('|')
	appendSortedAttributes(&builder, rm.Resource().Attributes())
	return builder.String()
}

func appendSortedAttributes(builder *strings.Builder, attrs pcommon.Map) {
	if attrs.Len() == 0 {
		return
	}
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	for _, k := range keys {
		val, ok := attrs.Get(k)
		if !ok {
			continue
		}
		builder.WriteString(k)
		builder.WriteByte('=')
		builder.WriteString(val.AsString())
		builder.WriteByte('|')
	}
}
