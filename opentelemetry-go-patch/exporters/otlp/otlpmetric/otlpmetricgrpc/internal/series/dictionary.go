package series

import (
	"slices"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// Dictionary maintains per-resource mappings of metric descriptors to stable series IDs.
type Dictionary struct {
	mu      sync.Mutex
	sources map[string]*sourceState
}

type sourceState struct {
	mu      sync.Mutex
	nextID  uint64
	entries map[string]*seriesEntry
}

type seriesEntry struct {
	id         uint64
	registered bool
}

// NewDictionary constructs an empty dictionary.
func NewDictionary() *Dictionary {
	return &Dictionary{
		sources: make(map[string]*sourceState),
	}
}

// Annotate walks every metric data point in rm and assigns SeriesID values.
func (d *Dictionary) Annotate(rm *metricdata.ResourceMetrics) {
	if rm == nil {
		return
	}
	sourceKey := resourceKey(rm.Resource)
	source := d.getOrCreateSource(sourceKey)
	for i := range rm.ScopeMetrics {
		scope := rm.ScopeMetrics[i].Scope
		scopeKey := scopeKey(scope)
		metrics := rm.ScopeMetrics[i].Metrics
		for j := range metrics {
			d.annotateMetric(source, scopeKey, &metrics[j])
		}
	}
}

func (d *Dictionary) getOrCreateSource(key string) *sourceState {
	d.mu.Lock()
	defer d.mu.Unlock()
	if src, ok := d.sources[key]; ok {
		return src
	}
	src := &sourceState{
		nextID:  1,
		entries: make(map[string]*seriesEntry),
	}
	d.sources[key] = src
	return src
}

func (d *Dictionary) annotateMetric(src *sourceState, scopeKey string, m *metricdata.Metrics) {
	switch data := m.Data.(type) {
	case metricdata.Gauge[int64]:
		annotateNumberDataPoints(src, scopeKey, m.Name, "gauge_i64", data.DataPoints)
		m.Data = data
	case metricdata.Gauge[float64]:
		annotateNumberDataPoints(src, scopeKey, m.Name, "gauge_f64", data.DataPoints)
		m.Data = data
	case metricdata.Sum[int64]:
		annotateNumberDataPoints(src, scopeKey, m.Name, "sum_i64", data.DataPoints)
		m.Data = data
	case metricdata.Sum[float64]:
		annotateNumberDataPoints(src, scopeKey, m.Name, "sum_f64", data.DataPoints)
		m.Data = data
	case metricdata.Histogram[int64]:
		annotateHistogramDataPoints(src, scopeKey, m.Name, "histogram_i64", data.DataPoints)
		m.Data = data
	case metricdata.Histogram[float64]:
		annotateHistogramDataPoints(src, scopeKey, m.Name, "histogram_f64", data.DataPoints)
		m.Data = data
	case metricdata.ExponentialHistogram[int64]:
		annotateExponentialDataPoints(src, scopeKey, m.Name, "exphist_i64", data.DataPoints)
		m.Data = data
	case metricdata.ExponentialHistogram[float64]:
		annotateExponentialDataPoints(src, scopeKey, m.Name, "exphist_f64", data.DataPoints)
		m.Data = data
	case metricdata.DDSketch[int64]:
		annotateDDSketchDataPoints(src, scopeKey, m.Name, "ddsketch_i64", data.DataPoints)
		m.Data = data
	case metricdata.DDSketch[float64]:
		annotateDDSketchDataPoints(src, scopeKey, m.Name, "ddsketch_f64", data.DataPoints)
		m.Data = data
	case metricdata.Summary:
		annotateSummaryDataPoints(src, scopeKey, m.Name, "summary", data.DataPoints)
		m.Data = data
	default:
	}
}

func annotateNumberDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, dps []metricdata.DataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, &dp.SeriesID, &dp.Attributes)
	}
}

func annotateHistogramDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, dps []metricdata.HistogramDataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, &dp.SeriesID, &dp.Attributes)
	}
}

func annotateExponentialDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, dps []metricdata.ExponentialHistogramDataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, &dp.SeriesID, &dp.Attributes)
	}
}

func annotateSummaryDataPoints(src *sourceState, scopeKey, metricName, metricType string, dps []metricdata.SummaryDataPoint) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, &dp.SeriesID, &dp.Attributes)
	}
}

func annotateDDSketchDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, dps []metricdata.DDSketchDataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, &dp.SeriesID, &dp.Attributes)
	}
}

func assignSeriesID(src *sourceState, scopeKey, metricName, metricType string, seriesID *uint64, attrs *attribute.Set) {
	key := descriptorKey(scopeKey, metricName, metricType, *attrs)
	entry := src.lookup(key)
	*seriesID = entry.id
	if entry.registered {
		*attrs = attribute.Set{}
		return
	}
	entry.registered = true
}

func (s *sourceState) lookup(key string) *seriesEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.entries[key]; ok {
		return entry
	}
	id := s.nextID
	s.nextID++
	if s.entries == nil {
		s.entries = make(map[string]*seriesEntry)
	}
	entry := &seriesEntry{id: id}
	s.entries[key] = entry
	return entry
}

func descriptorKey(scopeKey, metricName, metricType string, attrs attribute.Set) string {
	builder := strings.Builder{}
	builder.WriteString(scopeKey)
	builder.WriteByte('|')
	builder.WriteString(metricType)
	builder.WriteByte('|')
	builder.WriteString(metricName)
	builder.WriteByte('|')
	appendAttributes(&builder, attrs)
	return builder.String()
}

func resourceKey(res *resource.Resource) string {
	builder := strings.Builder{}
	builder.WriteString(resSchema(res))
	builder.WriteByte('|')
	appendAttributeSlice(&builder, res.Attributes())
	return builder.String()
}

func scopeKey(scope instrumentation.Scope) string {
	builder := strings.Builder{}
	builder.WriteString(scope.Name)
	builder.WriteByte('|')
	builder.WriteString(scope.Version)
	builder.WriteByte('|')
	builder.WriteString(scope.SchemaURL)
	builder.WriteByte('|')
	appendAttributes(&builder, scope.Attributes)
	return builder.String()
}

func resSchema(res *resource.Resource) string {
	if res == nil {
		return ""
	}
	return res.SchemaURL()
}

func appendAttributes(builder *strings.Builder, attrs attribute.Set) {
	iter := attrs.Iter()
	if iter.Len() == 0 {
		return
	}
	for iter.Next() {
		kv := iter.Attribute()
		builder.WriteString(string(kv.Key))
		builder.WriteByte('=')
		builder.WriteString(kv.Value.Emit())
		builder.WriteByte('|')
	}
}

func appendAttributeSlice(builder *strings.Builder, attrs []attribute.KeyValue) {
	if len(attrs) == 0 {
		return
	}
	slices.SortFunc(attrs, func(a, b attribute.KeyValue) int {
		return strings.Compare(string(a.Key), string(b.Key))
	})
	for _, kv := range attrs {
		builder.WriteString(string(kv.Key))
		builder.WriteByte('=')
		builder.WriteString(kv.Value.Emit())
		builder.WriteByte('|')
	}
}
