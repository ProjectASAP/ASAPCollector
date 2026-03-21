// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

import "go.opentelemetry.io/collector/pdata/internal"

// HLLSketchEncoding identifies how the HLL sketch payload bytes are encoded.
type HLLSketchEncoding int32

const (
	// HLLSketchEncodingUnspecified indicates the encoding is not specified.
	HLLSketchEncodingUnspecified = HLLSketchEncoding(internal.HLLSketchEncoding_HLL_SKETCH_ENCODING_UNSPECIFIED)
	// HLLSketchEncodingProto indicates the payload is proto-encoded (SerializeProtoBytes).
	HLLSketchEncodingProto = HLLSketchEncoding(internal.HLLSketchEncoding_HLL_SKETCH_ENCODING_PROTO)
	// HLLSketchEncodingDelta indicates the payload is delta-encoded.
	HLLSketchEncodingDelta = HLLSketchEncoding(internal.HLLSketchEncoding_HLL_SKETCH_ENCODING_DELTA)
)

// String returns the string representation of the HLLSketchEncoding.
func (e HLLSketchEncoding) String() string {
	switch e {
	case HLLSketchEncodingUnspecified:
		return "Unspecified"
	case HLLSketchEncodingProto:
		return "Proto"
	case HLLSketchEncodingDelta:
		return "Delta"
	}
	return ""
}
