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
	mu      sync.RWMutex
	sources map[string]*sourceState
}

type sourceState struct {
	mu      sync.RWMutex
	nextID  uint64
	entries map[string]*seriesEntry
}

type seriesEntry struct {
	id         uint64
	registered bool
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
)

// Assignment mirrors the collector-provided series mapping.
type Assignment struct {
	ResourceKey           string
	ScopeKey              string
	MetricName            string
	MetricType            string
	AttributesFingerprint string
	SeriesID              uint64
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

// Apply registers the provided assignments in the dictionary.
func (d *Dictionary) Apply(assignments []Assignment) {
	if len(assignments) == 0 {
		return
	}
	d.mu.Lock()
	for _, asg := range assignments {
		src, ok := d.sources[asg.ResourceKey]
		if !ok {
			src = &sourceState{
				nextID:  1,
				entries: make(map[string]*seriesEntry),
			}
			d.sources[asg.ResourceKey] = src
		}
		src.applyAssignment(descriptorKeyFromParts(asg.ScopeKey, asg.MetricType, asg.MetricName, asg.AttributesFingerprint), asg.SeriesID)
	}
	d.mu.Unlock()
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
		annotateNumberDataPoints(src, scopeKey, m.Name, metricTypeGaugeInt, data.DataPoints)
		m.Data = data
	case metricdata.Gauge[float64]:
		annotateNumberDataPoints(src, scopeKey, m.Name, metricTypeGaugeDouble, data.DataPoints)
		m.Data = data
	case metricdata.Sum[int64]:
		annotateNumberDataPoints(src, scopeKey, m.Name, metricTypeSumInt, data.DataPoints)
		m.Data = data
	case metricdata.Sum[float64]:
		annotateNumberDataPoints(src, scopeKey, m.Name, metricTypeSumDouble, data.DataPoints)
		m.Data = data
	case metricdata.Histogram[int64]:
		annotateHistogramDataPoints(src, scopeKey, m.Name, metricTypeHistogram, data.DataPoints)
		m.Data = data
	case metricdata.Histogram[float64]:
		annotateHistogramDataPoints(src, scopeKey, m.Name, metricTypeHistogram, data.DataPoints)
		m.Data = data
	case metricdata.ExponentialHistogram[int64]:
		annotateExponentialDataPoints(src, scopeKey, m.Name, metricTypeExponentialHistogram, data.DataPoints)
		m.Data = data
	case metricdata.ExponentialHistogram[float64]:
		annotateExponentialDataPoints(src, scopeKey, m.Name, metricTypeExponentialHistogram, data.DataPoints)
		m.Data = data
	case metricdata.DDSketch[int64]:
		annotateDDSketchDataPoints(src, scopeKey, m.Name, metricTypeDDSketchInt, data.DataPoints)
		m.Data = data
	case metricdata.DDSketch[float64]:
		annotateDDSketchDataPoints(src, scopeKey, m.Name, metricTypeDDSketchDouble, data.DataPoints)
		m.Data = data
	case metricdata.Summary:
		annotateSummaryDataPoints(src, scopeKey, m.Name, metricTypeSummary, data.DataPoints)
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
	return descriptorKeyFromParts(scopeKey, metricType, metricName, attributesFingerprint(attrs))
}

func descriptorKeyFromParts(scopeKey, metricType, metricName, attrsFingerprint string) string {
	builder := strings.Builder{}
	builder.WriteString(scopeKey)
	builder.WriteByte('|')
	builder.WriteString(metricType)
	builder.WriteByte('|')
	builder.WriteString(metricName)
	builder.WriteByte('|')
	builder.WriteString(attrsFingerprint)
	return builder.String()
}

func (s *sourceState) applyAssignment(descriptor string, id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = make(map[string]*seriesEntry)
	}
	entry, ok := s.entries[descriptor]
	if !ok {
		entry = &seriesEntry{}
		s.entries[descriptor] = entry
	}
	entry.id = id
	entry.registered = true
	if id >= s.nextID {
		s.nextID = id + 1
	}
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
	builder.WriteString(attributesFingerprint(attrs))
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

func attributesFingerprint(attrs attribute.Set) string {
	iter := attrs.Iter()
	if iter.Len() == 0 {
		return ""
	}
	pairs := make([]attribute.KeyValue, 0, iter.Len())
	for iter.Next() {
		kv := iter.Attribute()
		pairs = append(pairs, kv)
	}
	slices.SortFunc(pairs, func(a, b attribute.KeyValue) int {
		if c := strings.Compare(string(a.Key), string(b.Key)); c != 0 {
			return c
		}
		return strings.Compare(a.Value.Emit(), b.Value.Emit())
	})
	builder := strings.Builder{}
	for _, kv := range pairs {
		builder.WriteString(string(kv.Key))
		builder.WriteByte('=')
		builder.WriteString(kv.Value.Emit())
		builder.WriteByte('|')
	}
	return builder.String()
}
