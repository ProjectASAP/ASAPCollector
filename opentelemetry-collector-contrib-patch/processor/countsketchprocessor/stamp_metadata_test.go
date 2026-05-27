// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countsketchprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	otelpre "github.com/ProjectASAP/asap-precompute-go/otel"
)

// TestEnvelopesInEncodeOrder_GroupsByResourceLabels verifies the shim's
// reconstruction of the encoder's grouping: envelopes with the same
// ResourceLabels are contiguous, group order is first-seen, and nils drop.
func TestEnvelopesInEncodeOrder_GroupsByResourceLabels(t *testing.T) {
	rl := func(host string) []precompute.KeyValue {
		return []precompute.KeyValue{{Key: "host.name", Value: host}}
	}
	a1 := &precompute.SketchEnvelope{ResourceLabels: rl("A"), Encoding: precompute.EncodingProtoFull}
	b1 := &precompute.SketchEnvelope{ResourceLabels: rl("B"), Encoding: precompute.EncodingProtoDelta}
	a2 := &precompute.SketchEnvelope{ResourceLabels: rl("A"), Encoding: precompute.EncodingProtoDelta}

	// Input order interleaves the two groups; encode order must coalesce
	// each group (A's both before B, first-seen group order A then B).
	got := envelopesInEncodeOrder([]*precompute.SketchEnvelope{a1, b1, nil, a2})
	require.Len(t, got, 3)
	assert.Same(t, a1, got[0])
	assert.Same(t, a2, got[1])
	assert.Same(t, b1, got[2])
}

// TestStampDPMetadata_PairsEncodingAcrossResourceGroups builds two
// envelopes in distinct ResourceLabels groups with DIFFERENT encodings,
// encodes them via the same adapter the processor uses, and asserts
// stampDPMetadata stamps each DP with ITS OWN envelope's encoding — the
// scenario the old flat-idx walk could mis-pair after the encoder regroups.
func TestStampDPMetadata_PairsEncodingAcrossResourceGroups(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		Epsilon:        0.01,
		Delta:          0.99,
		TransmitSketch: true,
		DropOriginal:   true,
	}
	require.NoError(t, cfg.Validate())
	proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))

	rl := func(host string) []precompute.KeyValue {
		return []precompute.KeyValue{{Key: "host.name", Value: host}}
	}
	// Minimal valid CountSketch payloads aren't needed for the stamp test
	// (stamp only reads Encoding/Temporality/labels); a non-nil payload
	// keeps the encoder happy.
	payload := []byte{0x01}
	full := &precompute.SketchEnvelope{
		SchemaVersion:  1,
		SketchType:     precompute.SketchTypeCountSketch,
		ResourceLabels: rl("A"),
		MetricName:     "top_endpoint_qps",
		Encoding:       precompute.EncodingProtoFull,
		Payload:        payload,
	}
	delta := &precompute.SketchEnvelope{
		SchemaVersion:  1,
		SketchType:     precompute.SketchTypeCountSketch,
		ResourceLabels: rl("B"),
		MetricName:     "top_endpoint_qps",
		Encoding:       precompute.EncodingProtoDelta,
		Payload:        payload,
	}

	// Pass them in an order that, after the encoder's group-by-resource,
	// is preserved as two single-envelope groups [A=full, B=delta].
	envs := []*precompute.SketchEnvelope{full, delta}

	encoded, err := otelpre.New(&otelpre.AdapterConfig{ScopeName: "otelcol/countsketch"}, nil).Encode(envs)
	require.NoError(t, err)
	md := encoded.(pmetric.Metrics)
	proc.stampDPMetadata(md, envs)

	// Walk output and map each DP's resource host -> encoding.
	encByHost := map[string]pmetric.CountSketchEncoding{}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		host, ok := rms.At(i).Resource().Attributes().Get("host.name")
		require.True(t, ok)
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				require.Equal(t, pmetric.MetricTypeCountSketch, m.Type())
				dps := m.CountSketch().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					encByHost[host.AsString()] = dps.At(l).Encoding()
				}
			}
		}
	}
	assert.Equal(t, pmetric.CountSketchEncodingProto, encByHost["A"], "host A must keep proto_full")
	assert.Equal(t, pmetric.CountSketchEncodingDelta, encByHost["B"], "host B must keep proto_delta")
}
