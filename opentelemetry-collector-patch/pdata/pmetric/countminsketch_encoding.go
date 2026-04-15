// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

import "go.opentelemetry.io/collector/pdata/internal"

// CountMinSketchEncoding identifies how the CountMinSketch payload bytes are encoded.
type CountMinSketchEncoding int32

const (
	// CountMinSketchEncodingUnspecified indicates the encoding is not specified.
	CountMinSketchEncodingUnspecified = CountMinSketchEncoding(internal.CountMinSketchEncoding_COUNT_MIN_SKETCH_ENCODING_UNSPECIFIED)
	// CountMinSketchEncodingProto indicates the payload is proto-encoded.
	CountMinSketchEncodingProto = CountMinSketchEncoding(internal.CountMinSketchEncoding_COUNT_MIN_SKETCH_ENCODING_PROTO)
	// CountMinSketchEncodingDelta indicates the payload is delta-encoded (proto diff).
	CountMinSketchEncodingDelta = CountMinSketchEncoding(internal.CountMinSketchEncoding_COUNT_MIN_SKETCH_ENCODING_DELTA)
	// CountMinSketchEncodingMsgpack indicates the payload is encoded as the
	// cross-language sketch-core MessagePack wire format — matching
	// sketchlib-go's `wire/asapmsgpack.MarshalCountMinSketch` output and
	// ASAPQuery-backend's `sketch_core::count_min::CountMinSketch::deserialize_msgpack`
	// consumer. Used by DataCollector PR flow (ASAPQuery PRs #9-#12).
	CountMinSketchEncodingMsgpack = CountMinSketchEncoding(internal.CountMinSketchEncoding_COUNT_MIN_SKETCH_ENCODING_MSGPACK)
	// CountMinSketchEncodingMsgpackDelta reserves the delta variant of the
	// msgpack wire format. Not yet implemented end-to-end — the Rust decoder
	// currently rejects it and falls through to the §5.2 fallback. Will be
	// wired once sketch-core grows an `apply_delta` API.
	CountMinSketchEncodingMsgpackDelta = CountMinSketchEncoding(internal.CountMinSketchEncoding_COUNT_MIN_SKETCH_ENCODING_MSGPACK_DELTA)
)

// String returns the string representation of the CountMinSketchEncoding.
func (e CountMinSketchEncoding) String() string {
	switch e {
	case CountMinSketchEncodingUnspecified:
		return "Unspecified"
	case CountMinSketchEncodingProto:
		return "Proto"
	case CountMinSketchEncodingDelta:
		return "Delta"
	case CountMinSketchEncodingMsgpack:
		return "Msgpack"
	case CountMinSketchEncodingMsgpackDelta:
		return "MsgpackDelta"
	}
	return ""
}
