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
	// CountSketchEncodingDelta indicates the payload is delta-encoded.
	CountSketchEncodingDelta = CountSketchEncoding(internal.CountSketchEncoding_COUNT_SKETCH_ENCODING_DELTA)
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
	}
	return ""
}
