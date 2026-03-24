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
	}
	return ""
}
