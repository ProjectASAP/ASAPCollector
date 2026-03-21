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
)

// String returns the string representation of the KLLSketchEncoding.
func (e KLLSketchEncoding) String() string {
	switch e {
	case KLLSketchEncodingUnspecified:
		return "Unspecified"
	case KLLSketchEncodingProto:
		return "Proto"
	}
	return ""
}
