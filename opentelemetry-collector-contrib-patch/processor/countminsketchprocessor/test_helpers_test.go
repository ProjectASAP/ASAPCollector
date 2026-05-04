// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
)

// serializeCMS / deserializeCMS are test-only helpers consumed by
// processor_test.go's TestRoundTripIngestProtoSketch. They mirror the
// emit-side serializer (SerializeProtoBytesFO) so wire bytes stay
// byte-identical with what the production sketches.CMSWrapper emits.
// Production code constructs sketches via
// asap-precompute-go/sketches.NewCMSWrapper.
func serializeCMS(s *cms.CountMinSketch) ([]byte, error) {
	if s == nil {
		return nil, nil
	}
	return s.SerializeProtoBytesFO()
}

func deserializeCMS(data []byte) (*cms.CountMinSketch, error) {
	return cms.DeserializeCountMinSketchFromProtoBytes(data)
}
