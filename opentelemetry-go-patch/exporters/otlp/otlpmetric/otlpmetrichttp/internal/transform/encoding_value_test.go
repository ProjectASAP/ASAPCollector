// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Tests for the *EncodingValue helpers that map metricdata-typed
// encoding strings to the OTLP proto enum. Splits out from the
// auto-generated metricdata_test.go (which is regenerated from
// internal/shared/otlp/otlpmetric/transform/metricdata_test.go.tmpl)
// so the new cases survive a re-template.
//
// Background: PR #262 introduced metricdata.DDSketchEncodingProtoDelta
// alongside the pre-existing CountSketch / CountMinSketch / HLLSketch
// delta encodings, but DDSketchEncodingValue's switch was not extended
// to cover it. The OTLP exporter then rejected EXPORTER_SDK_AGG=dd-delta
// with `unknown ddsketch encoding: ddsketch_proto_delta`. These tests
// pin the post-#262 absorption.

package transform

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	mpb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestDDSketchEncodingValue(t *testing.T) {
	tests := []struct {
		name string
		in   metricdata.DDSketchEncoding
		want mpb.DDSketchEncoding
	}{
		{"proto_full", metricdata.DDSketchEncodingProto, mpb.DDSketchEncoding_DDSKETCH_ENCODING_PROTO},
		{"proto_delta", metricdata.DDSketchEncodingProtoDelta, mpb.DDSketchEncoding_DDSKETCH_ENCODING_PROTO_DELTA},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DDSketchEncodingValue(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestDDSketchEncodingValue_Unknown(t *testing.T) {
	got, err := DDSketchEncodingValue(metricdata.DDSketchEncoding("not-a-real-encoding"))
	require.Error(t, err)
	assert.Equal(t, mpb.DDSketchEncoding_DDSKETCH_ENCODING_UNSPECIFIED, got)
}

// Sibling sketches: confirm Delta + Proto map cleanly today so we
// catch regressions the next time someone touches the switch.

func TestKLLSketchEncodingValue_Proto(t *testing.T) {
	got, err := KLLSketchEncodingValue(metricdata.KLLSketchEncodingProto)
	require.NoError(t, err)
	assert.Equal(t, mpb.KLLSketchEncoding_KLL_SKETCH_ENCODING_PROTO, got)
}

func TestCountSketchEncodingValue(t *testing.T) {
	tests := []struct {
		name string
		in   metricdata.CountSketchEncoding
		want mpb.CountSketchEncoding
	}{
		{"proto", metricdata.CountSketchEncodingProto, mpb.CountSketchEncoding_COUNT_SKETCH_ENCODING_PROTO},
		{"delta", metricdata.CountSketchEncodingDelta, mpb.CountSketchEncoding_COUNT_SKETCH_ENCODING_DELTA},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CountSketchEncodingValue(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCountMinSketchEncodingValue(t *testing.T) {
	tests := []struct {
		name string
		in   metricdata.CountMinSketchEncoding
		want mpb.CountMinSketchEncoding
	}{
		{"proto", metricdata.CountMinSketchEncodingProto, mpb.CountMinSketchEncoding_COUNT_MIN_SKETCH_ENCODING_PROTO},
		{"delta", metricdata.CountMinSketchEncodingDelta, mpb.CountMinSketchEncoding_COUNT_MIN_SKETCH_ENCODING_DELTA},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CountMinSketchEncodingValue(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestHLLSketchEncodingValue(t *testing.T) {
	tests := []struct {
		name string
		in   metricdata.HLLSketchEncoding
		want mpb.HLLSketchEncoding
	}{
		{"proto", metricdata.HLLSketchEncodingProto, mpb.HLLSketchEncoding_HLL_SKETCH_ENCODING_PROTO},
		{"delta", metricdata.HLLSketchEncodingDelta, mpb.HLLSketchEncoding_HLL_SKETCH_ENCODING_DELTA},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := HLLSketchEncodingValue(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestDDSketchDataPointsCarriesProtoDeltaEncoding round-trips a
// dd-delta-encoded SDK data point through DDSketchDataPoints and
// asserts the OTLP enum lands at PROTO_DELTA. This is the path
// EXPORTER_SDK_AGG=dd-delta exercises end-to-end.
func TestDDSketchDataPointsCarriesProtoDeltaEncoding(t *testing.T) {
	in := []metricdata.DDSketchDataPoint[float64]{
		{
			Sketch:   []byte{0x01, 0x02, 0x03},
			Encoding: metricdata.DDSketchEncodingProtoDelta,
		},
	}
	out, err := DDSketchDataPoints(in)
	require.NoError(t, err)
	require.Len(t, out, 1)
	assert.Equal(t, mpb.DDSketchEncoding_DDSKETCH_ENCODING_PROTO_DELTA, out[0].Encoding)
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, out[0].Sketch)
}
