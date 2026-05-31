package precompute

import (
	commonpb "github.com/ProjectASAP/sketchlib-go/proto/common"
)

// SketchType identifies which sketch algorithm produced the envelope's
// payload bytes. Mirrors the design-doc §5.1 SketchEnvelope.sketch_type
// enum. The sketchlib-go proto today carries the sketch type implicitly
// via its `sketch_state` oneof, but the Layer-3 runtime keeps it as an
// explicit tag because envelopes flow through code paths that don't
// always unmarshal the inner state.
type SketchType uint8

const (
	// SketchTypeUnspecified means the type was not set; reject at
	// decode boundaries.
	SketchTypeUnspecified SketchType = iota
	// SketchTypeDDSketch identifies a DDSketch quantile sketch.
	SketchTypeDDSketch
	// SketchTypeKLLSketch identifies a KLL quantile sketch.
	SketchTypeKLLSketch
	// SketchTypeHLLSketch identifies a HyperLogLog cardinality sketch.
	SketchTypeHLLSketch
	// SketchTypeCountSketch identifies a Count Sketch frequency sketch.
	SketchTypeCountSketch
	// SketchTypeCountMinSketch identifies a Count-Min Sketch
	// frequency sketch.
	SketchTypeCountMinSketch
)

// String returns the canonical name for the sketch type, matching
// the well-known string spellings used by Strategy-B adapters
// (see ADR-0003 §4 standardized keys).
func (s SketchType) String() string {
	switch s {
	case SketchTypeDDSketch:
		return "DDSketch"
	case SketchTypeKLLSketch:
		return "KLLSketch"
	case SketchTypeHLLSketch:
		return "HLLSketch"
	case SketchTypeCountSketch:
		return "CountSketch"
	case SketchTypeCountMinSketch:
		return "CountMinSketch"
	}
	return "Unspecified"
}

// AggregationKind is the umbrella over WHAT kind of aggregate an envelope
// carries: a Sketch (whose SketchType sub-tag names the algorithm —
// DDSketch/KLL/HLL/CountSketch/CountMinSketch) or a scalar Sum. This mirrors
// the ASAPQuery backend's AggKind { Sketch | ExactAgg } split: "sketch" is
// ONE aggregation kind and "sum" is a sibling, NOT a SketchType.
//
// Backward compatibility: every producer that predates this field emits the
// zero value (AggKindUnspecified). On decode, an Unspecified AggKind paired
// with a real SketchType is read as AggKindSketch (see EffectiveAggKind), so
// existing sketch envelopes stay byte-identical and decode unchanged.
type AggregationKind uint8

const (
	// AggKindUnspecified is the proto3 zero value; resolved via
	// EffectiveAggKind (Unspecified + a real SketchType => Sketch).
	AggKindUnspecified AggregationKind = iota
	// AggKindSketch: the payload is a sketch; SketchType names which.
	AggKindSketch
	// AggKindSum: the payload is a scalar Sum aggregate ({sum,count}).
	AggKindSum
)

// String returns the canonical aggregation-kind name.
func (a AggregationKind) String() string {
	switch a {
	case AggKindSketch:
		return "Sketch"
	case AggKindSum:
		return "Sum"
	}
	return "Unspecified"
}

// Encoding describes how the bytes in SketchEnvelope.Payload are
// encoded. Mirrors the design-doc §5.1 SketchEnvelope.encoding enum.
type Encoding uint8

const (
	// EncodingUnspecified means encoding was not set; treated as
	// PROTO_FULL for backwards-compatibility but adapters should
	// reject at strict-mode boundaries.
	EncodingUnspecified Encoding = iota
	// EncodingProtoFull means Payload is a proto-encoded full
	// SketchEnvelope state.
	EncodingProtoFull
	// EncodingProtoDelta means Payload is a proto-encoded sparse
	// delta against the receiver's cached snapshot of this series.
	EncodingProtoDelta
	// EncodingMsgpack means Payload is a msgpack-encoded full state
	// (some sketches expose msgpack as a faster wire format).
	EncodingMsgpack
	// EncodingMsgpackDelta means Payload is a msgpack-encoded sparse
	// delta (the DELTA-HEAP wire form for the heap-bearing CountSketch:
	// a sparse signed matrix delta plus the full top-k heap, applied
	// against the receiver's per-window-rotated base).
	EncodingMsgpackDelta
)

// String returns the canonical encoding name.
func (e Encoding) String() string {
	switch e {
	case EncodingProtoFull:
		return "PROTO_FULL"
	case EncodingProtoDelta:
		return "PROTO_DELTA"
	case EncodingMsgpack:
		return "MSGPACK"
	case EncodingMsgpackDelta:
		return "MSGPACK_DELTA"
	}
	return "UNSPECIFIED"
}

// SketchEnvelope is the Go-side runtime view of the sketchlib-go
// SketchEnvelope proto plus the surrounding metadata that today's
// OTel modified-OTLP variants and tomorrow's Strategy-B adapters
// both need (window bounds, agg_id, encoding tag, sketch_type tag,
// host-neutral labels).
//
// The actual on-the-wire representation depends on the platform's
// encoding strategy (see design-doc §7.2):
//   - Strategy A (OTel today): Payload rides a typed
//     pmetric.Metric.data oneof variant (DDSketch / KLLSketch / …).
//   - Strategy B (Telegraf / Vector / OTAP): the whole struct is
//     decomposed into the well-known `_asap_envelope` /
//     `_asap_sketch_type` / `_asap_agg_id` / etc. carrier keys.
//
// Either way Payload is byte-identical and is what
// Precompute.ObserveEnvelope deserializes.
type SketchEnvelope struct {
	// SchemaVersion is the design-doc SketchEnvelope.schema_version
	// field. Adapters reject envelopes whose version exceeds the
	// highest version they understand.
	SchemaVersion uint32
	// SketchType identifies which sketch algorithm produced Payload.
	SketchType SketchType
	// AggKind is the umbrella aggregation kind (Sketch vs Sum). The zero
	// value (AggKindUnspecified) is resolved by EffectiveAggKind to
	// AggKindSketch whenever SketchType is set, so every pre-existing
	// (sketch-only) producer is byte-for-byte unaffected.
	AggKind AggregationKind
	// AggID is the controller-plan join key. Pairs the envelope to
	// a specific PrecomputeConfig.
	AggID AggId
	// ResourceLabels are the resource-scope attributes captured at
	// observation time (for OTel: pmetric.ResourceMetrics.Resource()
	// attrs). Carried in-process so the adapter's Encode can faithfully
	// reconstruct the host's resource hierarchy on emission. NOT on
	// the proto wire today: proto's `repeated KeyValue labels` carries
	// only data-point labels; cross-host hops that need to preserve
	// resource scope can prefix-encode (e.g. `resource.region=…`) into
	// Labels at encode time. Strategy-B hosts (Telegraf / Vector /
	// flat-attribute platforms) flatten resource into Labels.
	ResourceLabels []KeyValue
	// Labels are the host-neutral string-string label pairs that
	// identify the data-point-level (metric × attrs) series.
	Labels []KeyValue
	// WindowStartMs is the inclusive lower bound of the window the
	// payload summarizes (Unix milliseconds).
	WindowStartMs uint64
	// WindowEndMs is the exclusive upper bound of the window the
	// payload summarizes (Unix milliseconds).
	WindowEndMs uint64
	// Encoding describes how Payload is laid out.
	Encoding Encoding
	// Payload is the proto-encoded sketch state or delta. This is
	// the bandwidth-invariant blob (design-doc §5.2).
	Payload []byte
	// HashSpec is the determinism contract for cross-language
	// reconstruction. Comes from sketchlib-go's `commonpb.HashSpec`.
	HashSpec *commonpb.HashSpec
	// MetricName is the metric name the envelope was emitted for.
	// Today's per-processor flushWindow builds the output
	// pmetric.Metric and sets its Name() from a config field; once
	// the shims delegate to this runtime, the runtime needs to know
	// the metric name per-emission so Adapter.Encode can stamp it
	// onto the synthesized output Metric. This is an in-process Go
	// field — NOT a proto wire field. The canonical wire bytes
	// remain Payload (ADR-0002 §"Behavior preservation").
	MetricName string
	// Count is the total observation count this envelope represents
	// (sum of dp.Count() for the producing window's input samples).
	// The OTel adapter copies it into output Sum data points via
	// dp.SetCount(); some downstream consumers use it as a sanity
	// field. In-process only.
	Count uint64
	// AggregationTemporality is the OTel temporality enum stored as
	// an int32 to keep the runtime host-neutral (no pmetric import
	// here): 0 = unspecified, 1 = delta, 2 = cumulative. The OTel
	// adapter encode-side reads this to set
	// Sum.SetAggregationTemporality(...). In-process only.
	AggregationTemporality int32
	// RelativeAccuracy is the DDSketch alpha (relative accuracy) the
	// producing sketch was built with — non-zero only for
	// SketchType==DDSketch. In-process only (NOT a proto wire field): the
	// OTel adapter's Encode stamps it onto the output pmetric.DDSketch
	// container's relative_accuracy so the backend registers a non-zero ε.
	// A 0.0 here leaves the container at its zero value, which the backend
	// treats as a degenerate (exact, no-bucket) DDSketch — quantile queries
	// then capability-miss to the archive and return empty.
	RelativeAccuracy float64
}

// EffectiveAggKind resolves the envelope's aggregation kind, applying the
// backward-compat default: an unset AggKind on an envelope that carries a
// real SketchType is treated as AggKindSketch (every producer emitted before
// AggKind existed predates the field and only ever produced sketches). A Sum
// producer sets AggKind = AggKindSum explicitly.
func (e *SketchEnvelope) EffectiveAggKind() AggregationKind {
	if e.AggKind == AggKindUnspecified && e.SketchType != SketchTypeUnspecified {
		return AggKindSketch
	}
	return e.AggKind
}
