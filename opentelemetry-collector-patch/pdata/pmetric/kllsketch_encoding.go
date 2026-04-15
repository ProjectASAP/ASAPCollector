// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

import "go.opentelemetry.io/collector/pdata/internal"

// KLLSketchEncoding identifies how the KLL sketch payload bytes are encoded.
type KLLSketchEncoding int32

const (
	// KLLSketchEncodingUnspecified indicates the encoding is not specified.
	KLLSketchEncodingUnspecified = KLLSketchEncoding(internal.KLLSketchEncoding_KLL_SKETCH_ENCODING_UNSPECIFIED)
	// KLLSketchEncodingProto indicates the payload is proto-encoded.
	KLLSketchEncodingProto = KLLSketchEncoding(internal.KLLSketchEncoding_KLL_SKETCH_ENCODING_PROTO)
	// KLLSketchEncodingMsgpack reserves the MessagePack wire format for KLL.
	// Currently not implementable end-to-end: sketchlib-go's KLL and
	// ASAPQuery-backend's sketch-core KLL do not share a byte-level backend
	// serialization (see sketchlib-go PR #50 §out-of-scope and PR #51's
	// KLL omission note). Producers emitting KLL should keep using PROTO.
	// The enum value is reserved here for parity with the other sketch
	// encoding enums and for future use once a shared backend exists.
	KLLSketchEncodingMsgpack = KLLSketchEncoding(internal.KLLSketchEncoding_KLL_SKETCH_ENCODING_MSGPACK)
	// KLLSketchEncodingMsgpackDelta reserves the delta variant of KLL
	// msgpack encoding. Same status as KLLSketchEncodingMsgpack.
	KLLSketchEncodingMsgpackDelta = KLLSketchEncoding(internal.KLLSketchEncoding_KLL_SKETCH_ENCODING_MSGPACK_DELTA)
)

// String returns the string representation of the KLLSketchEncoding.
func (e KLLSketchEncoding) String() string {
	switch e {
	case KLLSketchEncodingUnspecified:
		return "Unspecified"
	case KLLSketchEncodingProto:
		return "Proto"
	case KLLSketchEncodingMsgpack:
		return "Msgpack"
	case KLLSketchEncodingMsgpackDelta:
		return "MsgpackDelta"
	}
	return ""
}
