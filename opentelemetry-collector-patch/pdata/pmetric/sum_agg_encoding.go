// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

import "go.opentelemetry.io/collector/pdata/internal"

// SumAggEncoding identifies how the SumAgg payload bytes are encoded.
type SumAggEncoding int32

const (
	// SumAggEncodingUnspecified indicates the encoding is not specified.
	SumAggEncodingUnspecified = SumAggEncoding(internal.SumAggEncoding_SUM_AGG_ENCODING_UNSPECIFIED)
	// SumAggEncodingProto indicates the payload is the sketchlib SketchEnvelope
	// proto carrying a SumState{sum,count} (the asap-precompute-go SumWrapper
	// full-state form; decoded by the backend's SumAccumulator).
	SumAggEncodingProto = SumAggEncoding(internal.SumAggEncoding_SUM_AGG_ENCODING_PROTO)
	// SumAggEncodingProtoDelta reserves the per-window proto delta form.
	SumAggEncodingProtoDelta = SumAggEncoding(internal.SumAggEncoding_SUM_AGG_ENCODING_PROTO_DELTA)
	// SumAggEncodingMsgpack reserves a msgpack full-state form.
	SumAggEncodingMsgpack = SumAggEncoding(internal.SumAggEncoding_SUM_AGG_ENCODING_MSGPACK)
	// SumAggEncodingMsgpackDelta reserves a msgpack delta form.
	SumAggEncodingMsgpackDelta = SumAggEncoding(internal.SumAggEncoding_SUM_AGG_ENCODING_MSGPACK_DELTA)
)

// String returns the string representation of the SumAggEncoding.
func (e SumAggEncoding) String() string {
	switch e {
	case SumAggEncodingUnspecified:
		return "Unspecified"
	case SumAggEncodingProto:
		return "Proto"
	case SumAggEncodingProtoDelta:
		return "ProtoDelta"
	case SumAggEncodingMsgpack:
		return "Msgpack"
	case SumAggEncodingMsgpackDelta:
		return "MsgpackDelta"
	}
	return ""
}
