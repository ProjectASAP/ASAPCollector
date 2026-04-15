// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

import "go.opentelemetry.io/collector/pdata/internal"

// DDSketchEncoding identifies how the DDSketch payload bytes are encoded.
type DDSketchEncoding int32

const (
	// DDSketchEncodingUnspecified indicates the encoding is not specified.
	DDSketchEncodingUnspecified = DDSketchEncoding(internal.DDSketchEncoding_DDSKETCH_ENCODING_UNSPECIFIED)
	// DDSketchEncodingProto indicates the payload is encoded as the DataDog sketch proto.
	DDSketchEncodingProto = DDSketchEncoding(internal.DDSketchEncoding_DDSKETCH_ENCODING_PROTO)
	// DDSketchEncodingProtoDelta indicates the payload is encoded as a delta-compressed DataDog sketch proto.
	DDSketchEncodingProtoDelta = DDSketchEncoding(internal.DDSketchEncoding_DDSKETCH_ENCODING_PROTO_DELTA)
	// DDSketchEncodingMsgpack indicates the payload is encoded as the
	// cross-language sketch-core MessagePack wire format — matching
	// sketchlib-go's `wire/asapmsgpack.MarshalDDSketch` output and
	// ASAPQuery-backend's `sketch_core::dd_sketch::DdSketch::deserialize_msgpack`
	// consumer. Note: the DataDog DDSketch library's internal state does
	// not map directly to sketchlib-go's cross-language wire format —
	// producers using this encoding need to convert gamma → alpha and
	// extract bucket counts / offsets explicitly.
	DDSketchEncodingMsgpack = DDSketchEncoding(internal.DDSketchEncoding_DDSKETCH_ENCODING_MSGPACK)
	// DDSketchEncodingMsgpackDelta reserves the delta variant; not yet
	// wired end-to-end (Rust decoder rejects it pending `apply_delta` API).
	DDSketchEncodingMsgpackDelta = DDSketchEncoding(internal.DDSketchEncoding_DDSKETCH_ENCODING_MSGPACK_DELTA)
)

// String returns the string representation of the DDSketchEncoding.
func (e DDSketchEncoding) String() string {
	switch e {
	case DDSketchEncodingUnspecified:
		return "Unspecified"
	case DDSketchEncodingProto:
		return "Proto"
	case DDSketchEncodingProtoDelta:
		return "ProtoDelta"
	case DDSketchEncodingMsgpack:
		return "Msgpack"
	case DDSketchEncodingMsgpackDelta:
		return "MsgpackDelta"
	}
	return ""
}
