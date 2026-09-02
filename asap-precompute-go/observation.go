// Package precompute is the host-neutral edge precompute runtime
// described in `docs/design-asap-edge-framework.md` §6.
//
// This package owns the windowing, snapshot caching, and delta
// encoding runtime logic that today lives inside each OTel
// processor in `opentelemetry-collector-contrib-patch/processor/`.
// Per-platform Adapter implementations (the Layer-4 shims) translate
// their host's native event into Observation, hand it to a
// Precompute, and translate the runtime's emitted SketchEnvelope
// back to the host's native event.
package precompute

// Observation is the host-neutral input to Precompute.Observe.
//
// Adapters decode their native event (currently pmetric.NumberDataPoint) into
// one of these. The runtime never sees host-specific types; it only
// sees Observation and SketchEnvelope.
//
// Labels are []KeyValue rather than pcommon.Map because pcommon
// is host-specific (OTel-only).
type Observation struct {
	// TimestampMs is the observation's wall-clock timestamp in
	// milliseconds since the Unix epoch. Used for window assignment
	// and late-data detection.
	TimestampMs uint64
	// Metric is the metric name (e.g. "http_requests_total"). May
	// be empty if the matchers don't key off metric name.
	Metric string
	// ResourceLabels are the (key,value) pairs from the host's
	// resource scope (e.g. pmetric.ResourceMetrics.Resource()
	// attributes on OTel; analogous fields on Telegraf / Vector /
	// OTAP). Today's OTel processors include resource attrs in the
	// series key (see ddsketchprocessor::accumulateIntoWindow's
	// `attributesKey(rm.Resource().Attributes())`), so the runtime
	// must too — preserved here as a separate slice so adapters
	// don't have to flatten.
	ResourceLabels []KeyValue
	// Labels are the (key,value) pairs identifying this
	// observation's data-point-level series. Order is not
	// significant for matching but the SeriesKey helper sorts by
	// key for stable hashing.
	Labels []KeyValue
	// Value carries the actual measurement. Exactly one of the
	// Float / Hash / Bytes / Envelope fields inside is meaningful
	// per Kind.
	Value ObservationValue
}

// KeyValue is a single string-string label pair. Mirrors the
// `repeated KeyValue` field in the SketchEnvelope proto.
type KeyValue struct {
	Key   string
	Value string
}

// ObservationValue is a kind-discriminated union over the four
// observation value shapes the runtime supports. Go does not have
// a clean tagged-union type; the invariant is that exactly one of
// {Float, Hash, Bytes, Envelope} is meaningful for each Kind:
//
//	KindFloat    → Float is meaningful; the rest are zero.
//	KindHash     → Hash is meaningful (cardinality / topk inputs).
//	KindBytes    → Bytes is meaningful (opaque keys for set aggregator).
//	KindEnvelope → Envelope is non-nil; this is a pre-aggregated
//	               sketch from upstream that should be merged or
//	               delta-applied via Precompute.ObserveEnvelope.
type ObservationValue struct {
	Kind     ObservationValueKind
	Float    float64
	Hash     uint64
	Bytes    []byte
	Envelope *SketchEnvelope

	// RowSampled, when true, indicates this ObservationValue is a
	// pre-sampled RAW occurrence: an OTel SDK already ran row-admission
	// (NitroSketch-style skip sampling) at Record() time — see
	// AggregationRowSampledSketch in the SDK — and AdmittedRows/SampleP
	// below record that decision. SketchObserver implementations that
	// support admitted-occurrence application (CMSObserver,
	// CountSketchObserver) must route through Sketch.ApplyAdmittedOccurrence
	// instead of the plain insert path, so the SDK's row-selection is
	// applied verbatim rather than re-derived by a second, independent
	// collector-side sampler. Observers with no *AtRows sketchlib primitive
	// (DDSketch/KLL/HLL aren't row-replicated matrices) do not support this
	// and must reject it rather than silently ignore it.
	RowSampled bool
	// AdmittedRows is the bitmask over the target sketch's rows (bit r set
	// => row r admits this occurrence). Meaningful only when RowSampled.
	AdmittedRows uint64
	// SampleP is the admission probability in effect when the SDK made the
	// row-admission decision. Meaningful only when RowSampled.
	SampleP float64
}

// ObservationValueKind is the Kind discriminator on
// ObservationValue.
type ObservationValueKind uint8

const (
	// KindFloat indicates Float is the meaningful field.
	KindFloat ObservationValueKind = iota
	// KindHash indicates Hash is the meaningful field; used by
	// cardinality (HLL) and top-k (CountSketch) observations.
	KindHash
	// KindBytes indicates Bytes is the meaningful field; used by
	// opaque-key set-aggregator observations.
	KindBytes
	// KindEnvelope indicates Envelope is the meaningful field; the
	// observation carries a pre-aggregated upstream sketch and
	// should be routed through Precompute.ObserveEnvelope, NOT
	// expanded to scalar samples (see design doc §5.2).
	KindEnvelope
)

// String returns a debug-friendly name for the kind.
func (k ObservationValueKind) String() string {
	switch k {
	case KindFloat:
		return "Float"
	case KindHash:
		return "Hash"
	case KindBytes:
		return "Bytes"
	case KindEnvelope:
		return "Envelope"
	}
	return "unknown"
}

// FloatValue returns ObservationValue with Kind=KindFloat.
func FloatValue(v float64) ObservationValue {
	return ObservationValue{Kind: KindFloat, Float: v}
}

// HashValue returns ObservationValue with Kind=KindHash.
func HashValue(h uint64) ObservationValue {
	return ObservationValue{Kind: KindHash, Hash: h}
}

// BytesValue returns ObservationValue with Kind=KindBytes.
func BytesValue(b []byte) ObservationValue {
	return ObservationValue{Kind: KindBytes, Bytes: b}
}

// EnvelopeValue returns ObservationValue with Kind=KindEnvelope.
func EnvelopeValue(env *SketchEnvelope) ObservationValue {
	return ObservationValue{Kind: KindEnvelope, Envelope: env}
}
