package metrics

import (
	"encoding/base64"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const defaultSeriesCacheTTL = time.Hour

type seriesCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	sources map[string]*seriesSource
}

type seriesSource struct {
	mu     sync.RWMutex
	nextID uint64
	series map[uint64]*seriesRecord
}

type seriesRecord struct {
	metricName string
	dataType   pmetric.MetricType
	attrs      pcommon.Map
	lastSeen   time.Time
}

type seriesDataPoint interface {
	Attributes() pcommon.Map
	SeriesID() uint64
	SetSeriesID(uint64)
}

type seriesAssignment struct {
	resourceKey           string
	scopeKey              string
	metricName            string
	metricType            string
	attributesFingerprint string
	seriesID              uint64
}

const (
	metricTypeGaugeInt             = "gauge_int"
	metricTypeGaugeDouble          = "gauge_double"
	metricTypeSumInt               = "sum_int"
	metricTypeSumDouble            = "sum_double"
	metricTypeHistogram            = "histogram"
	metricTypeExponentialHistogram = "exponential_histogram"
	metricTypeSummary              = "summary"
	metricTypeDDSketchInt          = "ddsketch_int"
	metricTypeDDSketchDouble       = "ddsketch_double"
	metricTypeDDSketchUnspecified  = "ddsketch"
	metricTypeHLLSketch            = "hllsketch"
	metricTypeKLLSketch            = "kllsketch"
	metricTypeCountSketch          = "countsketch"
	metricTypeCountMinSketch       = "countminsketch"
)

func newSeriesCache(ttl time.Duration) *seriesCache {
	return &seriesCache{
		ttl:     ttl,
		sources: make(map[string]*seriesSource),
	}
}

// rehydrate ensures every data point has attributes populated, either by
// storing newly seen (series_id, attributes) pairs or by copying cached
// attributes back onto ID-only points before the batch is consumed.
func (sc *seriesCache) rehydrate(md pmetric.Metrics) []seriesAssignment {
	if sc == nil {
		return nil
	}

	var assignments []seriesAssignment
	now := time.Now()
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resourceKey := buildResourceKey(rm)
		source := sc.getOrCreateSource(resourceKey)
		scopeMetrics := rm.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			scope := scopeMetrics.At(j)
			scopeKey := buildScopeKey(scope)
			metrics := scope.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				sc.rehydrateMetricLocked(source, resourceKey, scopeKey, metrics.At(k), &assignments, now)
			}
		}
	}
	sc.evictLocked(now)
	return assignments
}

func (sc *seriesCache) rehydrateMetricLocked(src *seriesSource, resourceKey, scopeKey string, m pmetric.Metric, assignments *[]seriesAssignment, now time.Time) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		if dps.Len() == 0 {
			return
		}
		metricType := metricTypeStringForNumber(metricTypeGaugeInt, metricTypeGaugeDouble, dps.At(0).ValueType())
		if metricType == "" {
			return
		}
		sc.rehydrateNumberDataPointsLocked(src, resourceKey, scopeKey, m.Name(), metricType, m.Type(), dps, assignments, now)
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		if dps.Len() == 0 {
			return
		}
		metricType := metricTypeStringForNumber(metricTypeSumInt, metricTypeSumDouble, dps.At(0).ValueType())
		if metricType == "" {
			return
		}
		sc.rehydrateNumberDataPointsLocked(src, resourceKey, scopeKey, m.Name(), metricType, m.Type(), dps, assignments, now)
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, resourceKey, scopeKey, m.Name(), metricTypeHistogram, m.Type(), dp, assignments, now)
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, resourceKey, scopeKey, m.Name(), metricTypeExponentialHistogram, m.Type(), dp, assignments, now)
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, resourceKey, scopeKey, m.Name(), metricTypeSummary, m.Type(), dp, assignments, now)
		}
	case pmetric.MetricTypeDDSketch:
		dps := m.DDSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			metricType := metricTypeStringForDDSketch(dp)
			if metricType == "" {
				metricType = metricTypeDDSketchUnspecified
			}
			sc.rehydrateDataPointLocked(src, resourceKey, scopeKey, m.Name(), metricType, m.Type(), dp, assignments, now)
		}
	case pmetric.MetricTypeHLLSketch:
		dps := m.HLLSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, resourceKey, scopeKey, m.Name(), metricTypeHLLSketch, m.Type(), dp, assignments, now)
		}
	case pmetric.MetricTypeKLLSketch:
		dps := m.KLLSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, resourceKey, scopeKey, m.Name(), metricTypeKLLSketch, m.Type(), dp, assignments, now)
		}
	case pmetric.MetricTypeCountSketch:
		dps := m.CountSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, resourceKey, scopeKey, m.Name(), metricTypeCountSketch, m.Type(), dp, assignments, now)
		}
	case pmetric.MetricTypeCountMinSketch:
		dps := m.CountMinSketch().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			dp := dps.At(i)
			sc.rehydrateDataPointLocked(src, resourceKey, scopeKey, m.Name(), metricTypeCountMinSketch, m.Type(), dp, assignments, now)
		}
	default:
	}
}

func (sc *seriesCache) rehydrateNumberDataPointsLocked(src *seriesSource, resourceKey, scopeKey, metricName, metricType string, dataType pmetric.MetricType, dps pmetric.NumberDataPointSlice, assignments *[]seriesAssignment, now time.Time) {
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		sc.rehydrateDataPointLocked(src, resourceKey, scopeKey, metricName, metricType, dataType, dp, assignments, now)
	}
}

func (sc *seriesCache) rehydrateDataPointLocked(src *seriesSource, resourceKey, scopeKey, metricName, metricType string, dataType pmetric.MetricType, dp seriesDataPoint, assignments *[]seriesAssignment, now time.Time) {
	attrs := dp.Attributes()
	seriesID := dp.SeriesID()
	if attrs.Len() > 0 {
		if seriesID == 0 {
			id := assignSeries(src, metricName, dataType, attrs, now)
			if id == 0 {
				return
			}
			dp.SetSeriesID(id)
			*assignments = append(*assignments, seriesAssignment{
				resourceKey:           resourceKey,
				scopeKey:              scopeKey,
				metricName:            metricName,
				metricType:            metricType,
				attributesFingerprint: fingerprintAttributes(attrs),
				seriesID:              id,
			})
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
			nextID: 1,
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
	if id >= src.nextID {
		src.nextID = id + 1
	}
	src.series[id] = &seriesRecord{
		metricName: metricName,
		dataType:   dataType,
		attrs:      cloned,
		lastSeen:   now,
	}
}

func assignSeries(src *seriesSource, metricName string, dataType pmetric.MetricType, attrs pcommon.Map, now time.Time) uint64 {
	src.mu.Lock()
	defer src.mu.Unlock()

	if src.series == nil {
		src.series = make(map[uint64]*seriesRecord)
	}
	if src.nextID == 0 {
		src.nextID = 1
	}
	id := src.nextID
	src.nextID++

	cloned := pcommon.NewMap()
	attrs.CopyTo(cloned)
	src.series[id] = &seriesRecord{
		metricName: metricName,
		dataType:   dataType,
		attrs:      cloned,
		lastSeen:   now,
	}
	return id
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

func buildScopeKey(scopeMetrics pmetric.ScopeMetrics) string {
	scope := scopeMetrics.Scope()
	builder := strings.Builder{}
	builder.WriteString(scope.Name())
	builder.WriteByte('|')
	builder.WriteString(scope.Version())
	builder.WriteByte('|')
	builder.WriteString(scopeMetrics.SchemaUrl())
	builder.WriteByte('|')
	appendSortedAttributes(&builder, scope.Attributes())
	return builder.String()
}

func fingerprintAttributes(attrs pcommon.Map) string {
	if attrs.Len() == 0 {
		return ""
	}
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	builder := strings.Builder{}
	for _, k := range keys {
		val, ok := attrs.Get(k)
		if !ok {
			continue
		}
		builder.WriteString(k)
		builder.WriteByte('=')
		builder.WriteString(valueString(val))
		builder.WriteByte('|')
	}
	return builder.String()
}

func valueString(val pcommon.Value) string {
	switch val.Type() {
	case pcommon.ValueTypeStr:
		return val.Str()
	case pcommon.ValueTypeBool:
		if val.Bool() {
			return "true"
		}
		return "false"
	case pcommon.ValueTypeInt:
		return strconv.FormatInt(val.Int(), 10)
	case pcommon.ValueTypeDouble:
		return formatFloat64(val.Double())
	case pcommon.ValueTypeBytes:
		return base64.StdEncoding.EncodeToString(val.Bytes().AsRaw())
	case pcommon.ValueTypeSlice:
		slc := val.Slice()
		builder := strings.Builder{}
		builder.WriteByte('[')
		for i := 0; i < slc.Len(); i++ {
			if i > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(valueString(slc.At(i)))
		}
		builder.WriteByte(']')
		return builder.String()
	case pcommon.ValueTypeMap:
		m := val.Map()
		keys := make([]string, 0, m.Len())
		m.Range(func(k string, _ pcommon.Value) bool {
			keys = append(keys, k)
			return true
		})
		sort.Strings(keys)
		builder := strings.Builder{}
		builder.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(k)
			builder.WriteByte('=')
			v, _ := m.Get(k)
			builder.WriteString(valueString(v))
		}
		builder.WriteByte('}')
		return builder.String()
	default:
		return ""
	}
}

func formatFloat64(f float64) string {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return "json: unsupported value: " + strconv.FormatFloat(f, 'g', -1, 64)
	}
	scratch := [64]byte{}
	b := scratch[:0]
	abs := math.Abs(f)
	fmt := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		fmt = 'e'
	}
	b = strconv.AppendFloat(b, f, fmt, -1, 64)
	if fmt == 'e' {
		n := len(b)
		if n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}
	return string(b)
}

func metricTypeStringForNumber(intKey, doubleKey string, valueType pmetric.NumberDataPointValueType) string {
	switch valueType {
	case pmetric.NumberDataPointValueTypeInt:
		return intKey
	case pmetric.NumberDataPointValueTypeDouble:
		return doubleKey
	default:
		return ""
	}
}

func metricTypeStringForDDSketch(dp pmetric.DDSketchDataPoint) string {
	switch dp.SumType() {
	case pmetric.DDSketchDataPointSumTypeInt:
		return metricTypeDDSketchInt
	case pmetric.DDSketchDataPointSumTypeDouble:
		return metricTypeDDSketchDouble
	}
	switch dp.MinType() {
	case pmetric.DDSketchDataPointMinTypeInt:
		return metricTypeDDSketchInt
	case pmetric.DDSketchDataPointMinTypeDouble:
		return metricTypeDDSketchDouble
	}
	switch dp.MaxType() {
	case pmetric.DDSketchDataPointMaxTypeInt:
		return metricTypeDDSketchInt
	case pmetric.DDSketchDataPointMaxTypeDouble:
		return metricTypeDDSketchDouble
	}
	return ""
}
