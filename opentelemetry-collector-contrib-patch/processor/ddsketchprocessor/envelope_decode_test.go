// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// envelope_decode_test.go pins the wire-format invariants the
// patched DDSketch processor's decoder relies on. Background:
// post-#262, the SDK's metricdata.DDSketchEncodingProtoDelta path
// landed but the OTLP encoding-string switch was incomplete and
// the SDK's DDSketch aggregator (internal/aggregate/ddsketch.go)
// still serializes via DataDog `sketchpb.DDSketch` — a different
// proto schema than the sketchlib-go `SketchEnvelope{DDSketchState}`
// the agent decoder expects. The structural mismatch surfaces as
// `DDSketchState.alpha: invalid wire type: LengthDelimited (expected
// SixtyFourBit)` on the consuming side. These tests pin the
// decoder's expected-input contract so the next time someone touches
// the encoder we catch a re-introduction of the same drift.

package ddsketchprocessor

import (
	"testing"

	commonpb "github.com/ProjectASAP/sketchlib-go/proto/common"
	ddpb "github.com/ProjectASAP/sketchlib-go/proto/ddsketch"
	envpb "github.com/ProjectASAP/sketchlib-go/proto/sketch_envelope"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestDecodeDDSketchEnvelope_RoundTripFull confirms a happy-path
// SerializePortable → proto.Marshal → decodeDDSketchEnvelope round
// trip succeeds and the recovered sketch matches the input. This is
// the wire-format contract the EXPORTER_SDK_AGG=dd-full path
// requires once the SDK encoder is migrated off DataDog's
// sketchpb.DDSketch onto sketchlib-go's portable shape.
func TestDecodeDDSketchEnvelope_RoundTripFull(t *testing.T) {
	src := ddsketch.NewDDSketch(0.01)
	for _, v := range []float64{1.5, 2.5, 3.5, 100.0, 99.0} {
		src.Update(v)
	}
	env, err := src.SerializePortable()
	require.NoError(t, err)
	bytes, err := proto.Marshal(env)
	require.NoError(t, err)

	recovered, err := decodeDDSketchEnvelope(bytes)
	require.NoError(t, err)
	require.NotNil(t, recovered)
	assert.Equal(t, src.GetCount(), recovered.GetCount())
}

// TestDecodeDDSketchEnvelope_BareStateRejected confirms that a bare
// DDSketchState (no envelope wrapper) is rejected. proto.Unmarshal
// trips on the field-number / wire-type clash between
// SketchEnvelope.format_version (varint, field 1) and
// DDSketchState.alpha (fixed64, field 1), surfacing as a generic
// `cannot parse invalid wire-format data` error. The decoder
// returns this as-is without reaching the GetDdsketch sentinel —
// the runtime's dispatcher then falls through to bare-state and
// delta decode paths, which is the documented contract.
func TestDecodeDDSketchEnvelope_BareStateRejected(t *testing.T) {
	// Refactor-2026-05: per-DP metric scalars (Count/Sum/Min/Max) were
	// removed from DDSketchState — the count is recoverable by summing
	// bucket store counts and min/max/quantiles derive from the bucket
	// distribution. The bare state now carries only alpha + bucket store.
	bare := &ddpb.DDSketchState{
		Alpha:       0.01,
		StoreCounts: []uint64{1, 2, 3},
		StoreOffset: 0,
	}
	bytes, err := proto.Marshal(bare)
	require.NoError(t, err)

	got, err := decodeDDSketchEnvelope(bytes)
	require.Error(t, err)
	assert.Nil(t, got)
}

// TestDecodeDDSketchEnvelope_EmptyEnvelopeRejected pins that an
// envelope with no oneof variant set (e.g. an envelope built without
// a sketch_state field, which is exactly the wire-shape the runtime
// produces when `proto.Unmarshal` is fed bytes from a different proto
// schema entirely — like DataDog's sketchpb.DDSketch) trips the same
// "did not carry DDSketchState" error path. This is the class of
// failure the sweep agent's `EXPORTER_SDK_AGG=dd-full` cell hit; the
// fix lives in the SDK encoder (out of this PR's scope) and is
// tracked as sweep-blocker-1 follow-up.
func TestDecodeDDSketchEnvelope_EmptyEnvelopeRejected(t *testing.T) {
	env := &envpb.SketchEnvelope{
		FormatVersion: 1,
		Producer: &commonpb.ProducerInfo{
			Library: "test",
			Version: "0",
		},
		// No sketch_state oneof set.
	}
	bytes, err := proto.Marshal(env)
	require.NoError(t, err)

	got, err := decodeDDSketchEnvelope(bytes)
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "did not carry DDSketchState")
}
