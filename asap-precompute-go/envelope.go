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
}
