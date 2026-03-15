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

// builderPool recycles strings.Builder instances to avoid per-call heap
// allocations when building descriptor keys and attribute fingerprints.
var builderPool = sync.Pool{New: func() any { return new(strings.Builder) }}

// entryPool recycles *seriesEntry instances to reduce GC pressure in
// high-cardinality or high-churn deployments.
var entryPool = sync.Pool{New: func() any { return new(seriesEntry) }}

// staleGenerations is the number of consecutive Annotate calls during which a
// series entry must be absent before it is evicted from the Dictionary.
// Increase this value to tolerate longer gaps between exports for slow series.
const staleGenerations uint64 = 5

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
	metricTypeKLLSketchInt         = "kllsketch_int"
	metricTypeKLLSketchDouble      = "kllsketch_double"
	metricTypeCountSketchInt       = "countsketch_int"
	metricTypeCountSketchDouble    = "countsketch_double"
	metricTypeCountMinSketchInt    = "countminsketch_int"
	metricTypeCountMinSketchDouble = "countminsketch_double"
	metricTypeHLLSketch            = "hllsketch"
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

// Dictionary maintains per-resource mappings of metric descriptors to stable series IDs.
type Dictionary struct {
	mu         sync.RWMutex
	generation uint64 // incremented on every Annotate call; used for staleness detection
	sources    map[string]*sourceState
}

type sourceState struct {
	mu      sync.RWMutex
	nextID  uint64
	entries map[string]*seriesEntry
}

type seriesEntry struct {
	id         uint64
	registered bool
	lastGen    uint64 // Dictionary.generation when this entry was last accessed
}

// NewDictionary constructs an empty dictionary.
func NewDictionary() *Dictionary {
	return &Dictionary{
		sources: make(map[string]*sourceState),
	}
}

// Annotate walks every metric data point in rm and assigns SeriesID values.
// It increments an internal generation counter and, after annotation, sweeps
// entries that have not been seen for staleGenerations cycles.
func (d *Dictionary) Annotate(rm *metricdata.ResourceMetrics) {
	if rm == nil {
		return
	}

	d.mu.Lock()
	d.generation++
	gen := d.generation
	d.mu.Unlock()

	sourceKey := resourceKey(rm.Resource)
	source := d.getOrCreateSource(sourceKey)
	for i := range rm.ScopeMetrics {
		scope := rm.ScopeMetrics[i].Scope
		sk := scopeKey(scope)
		metrics := rm.ScopeMetrics[i].Metrics
		for j := range metrics {
			d.annotateMetric(source, sk, gen, &metrics[j])
		}
	}

	d.sweep(gen)
}

// sweep removes entries from all sources that have not been accessed for
// staleGenerations export cycles, returning their memory to the pool.
func (d *Dictionary) sweep(currentGen uint64) {
	if currentGen <= staleGenerations {
		return
	}
	cutoff := currentGen - staleGenerations

	d.mu.RLock()
	sources := d.sources
	d.mu.RUnlock()

	for _, src := range sources {
		src.mu.Lock()
		for key, entry := range src.entries {
			if entry.lastGen < cutoff {
				*entry = seriesEntry{} // zero before returning to pool
				entryPool.Put(entry)
				delete(src.entries, key)
			}
		}
		src.mu.Unlock()
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

func (d *Dictionary) annotateMetric(src *sourceState, sk string, gen uint64, m *metricdata.Metrics) {
	switch data := m.Data.(type) {
	case metricdata.Gauge[int64]:
		annotateNumberDataPoints(src, sk, m.Name, metricTypeGaugeInt, gen, data.DataPoints)
		m.Data = data
	case metricdata.Gauge[float64]:
		annotateNumberDataPoints(src, sk, m.Name, metricTypeGaugeDouble, gen, data.DataPoints)
		m.Data = data
	case metricdata.Sum[int64]:
		annotateNumberDataPoints(src, sk, m.Name, metricTypeSumInt, gen, data.DataPoints)
		m.Data = data
	case metricdata.Sum[float64]:
		annotateNumberDataPoints(src, sk, m.Name, metricTypeSumDouble, gen, data.DataPoints)
		m.Data = data
	case metricdata.Histogram[int64]:
		annotateHistogramDataPoints(src, sk, m.Name, metricTypeHistogram, gen, data.DataPoints)
		m.Data = data
	case metricdata.Histogram[float64]:
		annotateHistogramDataPoints(src, sk, m.Name, metricTypeHistogram, gen, data.DataPoints)
		m.Data = data
	case metricdata.ExponentialHistogram[int64]:
		annotateExponentialDataPoints(src, sk, m.Name, metricTypeExponentialHistogram, gen, data.DataPoints)
		m.Data = data
	case metricdata.ExponentialHistogram[float64]:
		annotateExponentialDataPoints(src, sk, m.Name, metricTypeExponentialHistogram, gen, data.DataPoints)
		m.Data = data
	case metricdata.DDSketch[int64]:
		annotateDDSketchDataPoints(src, sk, m.Name, metricTypeDDSketchInt, gen, data.DataPoints)
		m.Data = data
	case metricdata.DDSketch[float64]:
		annotateDDSketchDataPoints(src, sk, m.Name, metricTypeDDSketchDouble, gen, data.DataPoints)
		m.Data = data
	case metricdata.KLLSketch[int64]:
		annotateKLLSketchDataPoints(src, sk, m.Name, metricTypeKLLSketchInt, gen, data.DataPoints)
		m.Data = data
	case metricdata.KLLSketch[float64]:
		annotateKLLSketchDataPoints(src, sk, m.Name, metricTypeKLLSketchDouble, gen, data.DataPoints)
		m.Data = data
	case metricdata.CountSketch[int64]:
		annotateCountSketchDataPoints(src, sk, m.Name, metricTypeCountSketchInt, gen, data.DataPoints)
		m.Data = data
	case metricdata.CountSketch[float64]:
		annotateCountSketchDataPoints(src, sk, m.Name, metricTypeCountSketchDouble, gen, data.DataPoints)
		m.Data = data
	case metricdata.CountMinSketch[int64]:
		annotateCountMinSketchDataPoints(src, sk, m.Name, metricTypeCountMinSketchInt, gen, data.DataPoints)
		m.Data = data
	case metricdata.CountMinSketch[float64]:
		annotateCountMinSketchDataPoints(src, sk, m.Name, metricTypeCountMinSketchDouble, gen, data.DataPoints)
		m.Data = data
	case metricdata.HLLSketch:
		annotateHLLSketchDataPoints(src, sk, m.Name, metricTypeHLLSketch, gen, data.DataPoints)
		m.Data = data
	case metricdata.Summary:
		annotateSummaryDataPoints(src, sk, m.Name, metricTypeSummary, gen, data.DataPoints)
		m.Data = data
	default:
	}
}

func annotateNumberDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, gen uint64, dps []metricdata.DataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, gen, &dp.SeriesID, &dp.Attributes)
	}
}

func annotateHistogramDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, gen uint64, dps []metricdata.HistogramDataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, gen, &dp.SeriesID, &dp.Attributes)
	}
}

func annotateExponentialDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, gen uint64, dps []metricdata.ExponentialHistogramDataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, gen, &dp.SeriesID, &dp.Attributes)
	}
}

func annotateSummaryDataPoints(src *sourceState, scopeKey, metricName, metricType string, gen uint64, dps []metricdata.SummaryDataPoint) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, gen, &dp.SeriesID, &dp.Attributes)
	}
}

func annotateDDSketchDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, gen uint64, dps []metricdata.DDSketchDataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, gen, &dp.SeriesID, &dp.Attributes)
		if dp.SeriesIDSink != nil {
			*dp.SeriesIDSink = dp.SeriesID
			dp.SeriesIDSink = nil
		}
		if dp.AttrsClearer != nil {
			*dp.AttrsClearer = attribute.Set{}
			dp.AttrsClearer = nil
		}
	}
}

func annotateKLLSketchDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, gen uint64, dps []metricdata.KLLSketchDataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, gen, &dp.SeriesID, &dp.Attributes)
		if dp.SeriesIDSink != nil {
			*dp.SeriesIDSink = dp.SeriesID
			dp.SeriesIDSink = nil
		}
		if dp.AttrsClearer != nil {
			*dp.AttrsClearer = attribute.Set{}
			dp.AttrsClearer = nil
		}
	}
}

func annotateCountSketchDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, gen uint64, dps []metricdata.CountSketchDataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, gen, &dp.SeriesID, &dp.Attributes)
		if dp.SeriesIDSink != nil {
			*dp.SeriesIDSink = dp.SeriesID
			dp.SeriesIDSink = nil
		}
		if dp.AttrsClearer != nil {
			*dp.AttrsClearer = attribute.Set{}
			dp.AttrsClearer = nil
		}
	}
}

func annotateCountMinSketchDataPoints[N int64 | float64](src *sourceState, scopeKey, metricName, metricType string, gen uint64, dps []metricdata.CountMinSketchDataPoint[N]) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, gen, &dp.SeriesID, &dp.Attributes)
		if dp.SeriesIDSink != nil {
			*dp.SeriesIDSink = dp.SeriesID
			dp.SeriesIDSink = nil
		}
		if dp.AttrsClearer != nil {
			*dp.AttrsClearer = attribute.Set{}
			dp.AttrsClearer = nil
		}
	}
}

func annotateHLLSketchDataPoints(src *sourceState, scopeKey, metricName, metricType string, gen uint64, dps []metricdata.HLLSketchDataPoint) {
	for i := range dps {
		dp := &dps[i]
		assignSeriesID(src, scopeKey, metricName, metricType, gen, &dp.SeriesID, &dp.Attributes)
		if dp.SeriesIDSink != nil {
			*dp.SeriesIDSink = dp.SeriesID
			dp.SeriesIDSink = nil
		}
		if dp.AttrsClearer != nil {
			*dp.AttrsClearer = attribute.Set{}
			dp.AttrsClearer = nil
		}
	}
}

func assignSeriesID(src *sourceState, scopeKey, metricName, metricType string, gen uint64, seriesID *uint64, attrs *attribute.Set) {
	// Fast path: series ID already cached in the aggregator series struct.
	if *seriesID != 0 {
		return
	}
	key := descriptorKey(scopeKey, metricName, metricType, *attrs)
	entry := src.lookup(key, gen)
	*seriesID = entry.id
	if entry.registered {
		*attrs = attribute.Set{}
		return
	}
	entry.registered = true
}

func (s *sourceState) lookup(key string, gen uint64) *seriesEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.entries[key]; ok {
		entry.lastGen = gen
		return entry
	}
	e := entryPool.Get().(*seriesEntry)
	*e = seriesEntry{id: s.nextID, lastGen: gen}
	s.nextID++
	if s.entries == nil {
		s.entries = make(map[string]*seriesEntry)
	}
	s.entries[key] = e
	return e
}

func descriptorKey(scopeKey, metricName, metricType string, attrs attribute.Set) string {
	return descriptorKeyFromParts(scopeKey, metricType, metricName, attributesFingerprint(attrs))
}

func descriptorKeyFromParts(scopeKey, metricType, metricName, attrsFingerprint string) string {
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	b.WriteString(scopeKey)
	b.WriteByte('|')
	b.WriteString(metricType)
	b.WriteByte('|')
	b.WriteString(metricName)
	b.WriteByte('|')
	b.WriteString(attrsFingerprint)
	s := b.String()
	builderPool.Put(b)
	return s
}

func (s *sourceState) applyAssignment(descriptor string, id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = make(map[string]*seriesEntry)
	}
	entry, ok := s.entries[descriptor]
	if !ok {
		e := entryPool.Get().(*seriesEntry)
		*e = seriesEntry{}
		entry = e
		s.entries[descriptor] = entry
	}
	entry.id = id
	entry.registered = true
	if id >= s.nextID {
		s.nextID = id + 1
	}
}

func resourceKey(res *resource.Resource) string {
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	b.WriteString(resSchema(res))
	b.WriteByte('|')
	appendAttributeSlice(b, res.Attributes())
	s := b.String()
	builderPool.Put(b)
	return s
}

func scopeKey(scope instrumentation.Scope) string {
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	b.WriteString(scope.Name)
	b.WriteByte('|')
	b.WriteString(scope.Version)
	b.WriteByte('|')
	b.WriteString(scope.SchemaURL)
	b.WriteByte('|')
	appendAttributes(b, scope.Attributes)
	s := b.String()
	builderPool.Put(b)
	return s
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
		pairs = append(pairs, iter.Attribute())
	}
	slices.SortFunc(pairs, func(a, b attribute.KeyValue) int {
		if c := strings.Compare(string(a.Key), string(b.Key)); c != 0 {
			return c
		}
		return strings.Compare(a.Value.Emit(), b.Value.Emit())
	})
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	for _, kv := range pairs {
		b.WriteString(string(kv.Key))
		b.WriteByte('=')
		b.WriteString(kv.Value.Emit())
		b.WriteByte('|')
	}
	s := b.String()
	builderPool.Put(b)
	return s
}
