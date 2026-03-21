// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

import "go.opentelemetry.io/collector/pdata/internal"

// CountMinSketchEncoding identifies how the CountMinSketch payload bytes are encoded.
type CountMinSketchEncoding int32

const (
	// CountMinSketchEncodingUnspecified indicates the encoding is not specified.
	CountMinSketchEncodingUnspecified = CountMinSketchEncoding(internal.CountMinSketchEncoding_COUNT_MIN_SKETCH_ENCODING_UNSPECIFIED)
	// CountMinSketchEncodingGob indicates the payload is gob-encoded.
	CountMinSketchEncodingGob = CountMinSketchEncoding(internal.CountMinSketchEncoding_COUNT_MIN_SKETCH_ENCODING_GOB)
	// CountMinSketchEncodingDelta indicates the payload is delta-encoded.
	CountMinSketchEncodingDelta = CountMinSketchEncoding(internal.CountMinSketchEncoding_COUNT_MIN_SKETCH_ENCODING_DELTA)
)

// String returns the string representation of the CountMinSketchEncoding.
func (e CountMinSketchEncoding) String() string {
	switch e {
	case CountMinSketchEncodingUnspecified:
		return "Unspecified"
	case CountMinSketchEncodingGob:
		return "Gob"
	case CountMinSketchEncodingDelta:
		return "Delta"
	}
	return ""
}
