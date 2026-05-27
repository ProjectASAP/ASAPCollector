// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countsketchprocessor

import (
	"context"
	"testing"

	"github.com/ProjectASAP/sketchlib-go/wire/asapmsgpack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// TestEmitHeap_EndToEndBackendReadable drives the processor end-to-end
// with emit_heap=true + item_label=endpoint and asserts the emitted
// CountSketch data point (a) is tagged msgpack encoding and (b) carries a
// heap-bearing payload that round-trips through the SAME wire decode the
// ASAPQuery backend uses (CountMinSketchWithHeap::from_msgpack, mirrored
// by asapmsgpack.UnmarshalCountSketchWithHeap) with a NON-EMPTY heap that
// ranks the heaviest endpoint first — the gate that makes a
// `topk(top_endpoint_qps)` query route to FrequencyTopk instead of
// returning "No result".
func TestEmitHeap_EndToEndBackendReadable(t *testing.T) {
	cfg := &Config{
		Mode:           ModeBatch,
		Epsilon:        0.001,
		Delta:          0.99,
		TransmitSketch: true,
		DropOriginal:   true,
		EmitHeap:       true,
		ItemLabel:      "endpoint",
		MetricName:     "top_endpoint_qps",
	}
	require.NoError(t, cfg.Validate())
	// Validate must have forced msgpack encoding and a heap size.
	require.Equal(t, EncodingMsgpack, cfg.Encoding)
	require.Equal(t, 100, cfg.HeapSize)

	proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))

	md := makeEndpointQPS("top_endpoint_qps", map[string]int{
		"/checkout": 100,
		"/cart":     40,
		"/home":     10,
	})
	out, err := proc.ProcessMetrics(context.Background(), md)
	require.NoError(t, err)

	// Find the emitted CountSketch DP, assert msgpack encoding, decode the heap.
	var (
		found   bool
		rawPay  []byte
		encEnum pmetric.CountSketchEncoding
	)
	rms := out.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Type() != pmetric.MetricTypeCountSketch {
					continue
				}
				dps := m.CountSketch().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					dp := dps.At(l)
					rawPay = dp.Sketch()
					encEnum = dp.Encoding()
					found = true
				}
			}
		}
	}
	require.True(t, found, "expected a CountSketch data point")
	assert.Equal(t, pmetric.CountSketchEncodingMsgpack, encEnum, "heap variant must be tagged msgpack")

	rows, cols, _, heap, heapSize, err := asapmsgpack.UnmarshalCountSketchWithHeap(rawPay)
	require.NoError(t, err, "payload must decode via the backend's heap wire format")
	assert.Greater(t, rows, uint64(0))
	assert.Greater(t, cols, uint64(0))
	assert.Equal(t, uint64(100), heapSize)
	require.NotEmpty(t, heap, "heap must be non-empty (backend promotion gate to CountSketchWithHeap)")

	// Heaviest endpoint must be ranked first (we emit highest-count first).
	assert.Equal(t, "/checkout", heap[0].Key)
}

// TestEmitHeap_AllowsDeltaTransmission asserts emit_heap + delta_transmission
// now coexist: the DELTA-HEAP wire form (sparse matrix delta + full top-k
// heap, encoding tag MSGPACK_DELTA) lets the heap path participate in delta
// transmission. Validate() must accept the combo and force msgpack encoding.
func TestEmitHeap_AllowsDeltaTransmission(t *testing.T) {
	cfg := &Config{
		Mode:              ModeBatch,
		Epsilon:           0.01,
		Delta:             0.99,
		TransmitSketch:    true,
		EmitHeap:          true,
		DeltaTransmission: true,
	}
	require.NoError(t, cfg.Validate())
	require.Equal(t, EncodingMsgpack, cfg.Encoding, "emit_heap forces msgpack encoding")
	require.True(t, cfg.DeltaTransmission)
}
