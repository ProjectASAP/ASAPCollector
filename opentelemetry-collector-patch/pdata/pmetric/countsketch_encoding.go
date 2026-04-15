// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

import "go.opentelemetry.io/collector/pdata/internal"

// CountSketchEncoding identifies how the CountSketch payload bytes are encoded.
type CountSketchEncoding int32

const (
	// CountSketchEncodingUnspecified indicates the encoding is not specified.
	CountSketchEncodingUnspecified = CountSketchEncoding(internal.CountSketchEncoding_COUNT_SKETCH_ENCODING_UNSPECIFIED)
	// CountSketchEncodingProto indicates the payload is proto-encoded.
	CountSketchEncodingProto = CountSketchEncoding(internal.CountSketchEncoding_COUNT_SKETCH_ENCODING_PROTO)
	// CountSketchEncodingDelta indicates the payload is delta-encoded (proto diff).
	CountSketchEncodingDelta = CountSketchEncoding(internal.CountSketchEncoding_COUNT_SKETCH_ENCODING_DELTA)
	// CountSketchEncodingMsgpack indicates the payload is encoded as the
	// cross-language sketch-core MessagePack wire format — matching
	// sketchlib-go's `wire/asapmsgpack.MarshalCountSketch` output and
	// ASAPQuery-backend's `sketch_core::count_sketch::CountSketch::deserialize_msgpack`
	// consumer.
	CountSketchEncodingMsgpack = CountSketchEncoding(internal.CountSketchEncoding_COUNT_SKETCH_ENCODING_MSGPACK)
	// CountSketchEncodingMsgpackDelta reserves the delta variant; not yet
	// wired end-to-end (Rust decoder rejects it pending `apply_delta` API).
	CountSketchEncodingMsgpackDelta = CountSketchEncoding(internal.CountSketchEncoding_COUNT_SKETCH_ENCODING_MSGPACK_DELTA)
)

// String returns the string representation of the CountSketchEncoding.
func (e CountSketchEncoding) String() string {
	switch e {
	case CountSketchEncodingUnspecified:
		return "Unspecified"
	case CountSketchEncodingProto:
		return "Proto"
	case CountSketchEncodingDelta:
		return "Delta"
	case CountSketchEncodingMsgpack:
		return "Msgpack"
	case CountSketchEncodingMsgpackDelta:
		return "MsgpackDelta"
	}
	return ""
}
