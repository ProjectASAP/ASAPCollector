// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package hllprocessor

import (
	"context"
	"fmt"
	"testing"
	"time"

	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// makeHLLGaugeMetrics builds a simple Gauge metric with distinct float values
// so each value contributes to the HLL cardinality estimate.
func makeHLLGaugeMetrics(name string, start, count int) pmetric.Metrics {
	vals := make([]float64, count)
	for i := range vals {
		vals[i] = float64(start + i)
	}
	return makeGaugeMetrics(name, vals)
}

// hllWindowConfig returns a Config for window mode with DeltaTransmission enabled.
func hllWindowConfig(windowDur time.Duration) *Config {
	return &Config{
		Mode:              ModeWindow,
		WindowDuration:    windowDur,
		TransmitSketch:    true,
		DeltaTransmission: true,
	}
}

// collectHLLDataPoints gathers all gauge data points from metrics emitted to sink
// that belong to the given metric name prefix.
func collectHLLDataPoints(allMetrics []pmetric.Metrics, metricName string) []pmetric.NumberDataPoint {
	var dps []pmetric.NumberDataPoint
	for _, md := range allMetrics {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == metricName {
						pts := ms.At(k).Gauge().DataPoints()
						for l := 0; l < pts.Len(); l++ {
							dps = append(dps, pts.At(l))
						}
					}
				}
			}
		}
	}
	return dps
}

// assertHLLRegistersEqual fails the test if two HyperLogLog sketches have
// different register arrays.
func assertHLLRegistersEqual(t *testing.T, label string, want, got *hll.HyperLogLog) {
	t.Helper()
	wr := want.RegisterSlice()
	gr := got.RegisterSlice()
	require.Equal(t, len(wr), len(gr), "%s: register count mismatch", label)
	for i := range wr {
		if wr[i] != gr[i] {
			t.Fatalf("%s: register[%d] want %d got %d", label, i, wr[i], gr[i])
		}
	}
}

// TestHLLDelta_FirstWindowSendsFullSketch verifies that the first window flush
// with DeltaTransmission=true sends a full proto sketch (no hll.encoding attribute).
func TestHLLDelta_FirstWindowSendsFullSketch(t *testing.T) {
	const winDur = 200 * time.Millisecond
	cfg := hllWindowConfig(winDur)
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)
	require.NoError(t, proc.Start(context.Background(), nil))
	defer proc.Shutdown(context.Background())

	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("events", 0, 100)))

	time.Sleep(winDur + 80*time.Millisecond)

	dps := collectHLLDataPoints(sink.AllMetrics(), "events_hll_cardinality")
	require.Len(t, dps, 1, "expected exactly one data point after first flush")

	// Full sketch: hll.sketch_payload present, hll.encoding NOT present (or not "proto_delta").
	_, hasPayload := dps[0].Attributes().Get("hll.sketch_payload")
	assert.True(t, hasPayload, "hll.sketch_payload must be present")

	encVal, hasEnc := dps[0].Attributes().Get("hll.encoding")
	if hasEnc {
		assert.NotEqual(t, "proto_delta", encVal.Str(), "first window must not be proto_delta")
	}
}

// TestHLLDelta_SubsequentWindowsSendDelta verifies that the second and later
// window flushes include hll.encoding=proto_delta once a snapshot exists.
func TestHLLDelta_SubsequentWindowsSendDelta(t *testing.T) {
	const winDur = 200 * time.Millisecond
	cfg := hllWindowConfig(winDur)
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)
	require.NoError(t, proc.Start(context.Background(), nil))
	defer proc.Shutdown(context.Background())

	// Window 1.
	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("hits", 0, 50)))
	time.Sleep(winDur + 80*time.Millisecond)

	sink.Reset()

	// Window 2.
	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("hits", 50, 50)))
	time.Sleep(winDur + 80*time.Millisecond)

	dps := collectHLLDataPoints(sink.AllMetrics(), "hits_hll_cardinality")
	require.Len(t, dps, 1, "expected one data point for window 2")

	encVal, ok := dps[0].Attributes().Get("hll.encoding")
	require.True(t, ok, "hll.encoding attribute must be present in window 2")
	assert.Equal(t, "proto_delta", encVal.Str(), "second window must be proto_delta")

	payloadVal, ok := dps[0].Attributes().Get("hll.sketch_payload")
	require.True(t, ok)
	_, err := hll.DeserializeRegisterDelta(payloadVal.Bytes().AsRaw())
	require.NoError(t, err, "proto_delta payload must deserialize as a RegisterDelta")
}

// TestHLLDelta_RoundTrip verifies that applying the delta from window 2 onto a
// clone of the full snapshot from window 1 reconstructs the exact same register
// state as what the processor had after window 2.
func TestHLLDelta_RoundTrip(t *testing.T) {
	const winDur = 200 * time.Millisecond
	cfg := hllWindowConfig(winDur)
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)
	require.NoError(t, proc.Start(context.Background(), nil))
	defer proc.Shutdown(context.Background())

	// Window 1: values 0..99 → snapshot.
	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("req", 0, 100)))
	time.Sleep(winDur + 80*time.Millisecond)

	dps1 := collectHLLDataPoints(sink.AllMetrics(), "req_hll_cardinality")
	require.Len(t, dps1, 1)
	rawFull := dps1[0].Attributes().AsRaw()["hll.sketch_payload"].([]byte)
	snapSketch, err := hll.DeserializeHyperLogLogFromProtoBytes(rawFull)
	require.NoError(t, err)

	sink.Reset()

	// Window 2: additional values 100..149 → delta.
	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("req", 100, 50)))
	time.Sleep(winDur + 80*time.Millisecond)

	dps2 := collectHLLDataPoints(sink.AllMetrics(), "req_hll_cardinality")
	require.Len(t, dps2, 1)

	encVal, ok := dps2[0].Attributes().Get("hll.encoding")
	require.True(t, ok)
	require.Equal(t, "proto_delta", encVal.Str())

	rawDelta := dps2[0].Attributes().AsRaw()["hll.sketch_payload"].([]byte)
	deltaMsg, err := hll.DeserializeRegisterDelta(rawDelta)
	require.NoError(t, err)

	// Apply delta onto clone of window-1 snapshot → reconstructed.
	reconstructed := cloneHLL(snapSketch)
	require.NotNil(t, reconstructed)
	hll.ApplyRegisterDelta(reconstructed, deltaMsg)

	// Build expected: merge the window-1 snapshot with the HLL for window-2 data.
	// The processor's window-2 internal sketch is max(snap, w2_only), which is
	// exactly cloneHLL(snap) merged with an HLL built from values 100..149.
	// We obtain the window-2 HLL via a fresh batch-mode processor.
	batchCfg := &Config{Mode: ModeBatch, TransmitSketch: true}
	require.NoError(t, batchCfg.Validate())
	batchSink := new(consumertest.MetricsSink)
	batchProc := newProcessor(batchCfg, zap.NewNop(), batchSink)
	require.NoError(t, batchProc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("req", 100, 50)))
	batchOut := batchSink.AllMetrics()
	require.Len(t, batchOut, 1)
	batchDPs := collectHLLDataPoints(batchOut, "req_hll_cardinality")
	require.Len(t, batchDPs, 1)
	rawW2 := batchDPs[0].Attributes().AsRaw()["hll.sketch_payload"].([]byte)
	w2Sketch, err := hll.DeserializeHyperLogLogFromProtoBytes(rawW2)
	require.NoError(t, err)

	expected := cloneHLL(snapSketch)
	require.NotNil(t, expected)
	require.NoError(t, expected.Merge(w2Sketch))

	assertHLLRegistersEqual(t, "RoundTrip", expected, reconstructed)
}

// TestHLLDelta_MaxSemanticsIdempotent verifies that re-applying the same delta
// payload twice yields the same result as applying it once (HLL max semantics).
func TestHLLDelta_MaxSemanticsIdempotent(t *testing.T) {
	const winDur = 200 * time.Millisecond
	cfg := hllWindowConfig(winDur)
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)
	require.NoError(t, proc.Start(context.Background(), nil))
	defer proc.Shutdown(context.Background())

	// Window 1.
	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("ev", 0, 80)))
	time.Sleep(winDur + 80*time.Millisecond)

	snapDPs := collectHLLDataPoints(sink.AllMetrics(), "ev_hll_cardinality")
	require.Len(t, snapDPs, 1)
	rawFull := snapDPs[0].Attributes().AsRaw()["hll.sketch_payload"].([]byte)
	snapSketch, err := hll.DeserializeHyperLogLogFromProtoBytes(rawFull)
	require.NoError(t, err)

	sink.Reset()

	// Window 2.
	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("ev", 80, 40)))
	time.Sleep(winDur + 80*time.Millisecond)

	dps2 := collectHLLDataPoints(sink.AllMetrics(), "ev_hll_cardinality")
	require.Len(t, dps2, 1)
	rawDelta := dps2[0].Attributes().AsRaw()["hll.sketch_payload"].([]byte)

	// Apply once.
	recv1 := cloneHLL(snapSketch)
	require.NotNil(t, recv1)
	d1, err := hll.DeserializeRegisterDelta(rawDelta)
	require.NoError(t, err)
	hll.ApplyRegisterDelta(recv1, d1)

	// Apply twice (idempotent due to max semantics).
	d2, err := hll.DeserializeRegisterDelta(rawDelta)
	require.NoError(t, err)
	hll.ApplyRegisterDelta(recv1, d2)

	// Apply-once reference.
	recv2 := cloneHLL(snapSketch)
	require.NotNil(t, recv2)
	d3, err := hll.DeserializeRegisterDelta(rawDelta)
	require.NoError(t, err)
	hll.ApplyRegisterDelta(recv2, d3)

	assertHLLRegistersEqual(t, "MaxSemanticsIdempotent", recv2, recv1)
}

// TestHLLDelta_MultipleWindowsConvergence verifies that 5 consecutive window
// deltas can be applied in order to reconstruct the state after each window.
func TestHLLDelta_MultipleWindowsConvergence(t *testing.T) {
	const winDur = 200 * time.Millisecond
	cfg := hllWindowConfig(winDur)
	require.NoError(t, cfg.Validate())

	sink := new(consumertest.MetricsSink)
	proc := newProcessor(cfg, zap.NewNop(), sink)
	require.NoError(t, proc.Start(context.Background(), nil))
	defer proc.Shutdown(context.Background())

	var prevSnap *hll.HyperLogLog

	for w := 0; w < 5; w++ {
		sink.Reset()

		start := w * 50
		require.NoError(t, proc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("stream", start, 50)))
		time.Sleep(winDur + 80*time.Millisecond)

		dps := collectHLLDataPoints(sink.AllMetrics(), "stream_hll_cardinality")
		require.Len(t, dps, 1, "window %d: expected 1 data point", w)

		attrs := dps[0].Attributes().AsRaw()
		rawPayload := attrs["hll.sketch_payload"].([]byte)
		encAttr, hasEnc := attrs["hll.encoding"]
		enc, _ := encAttr.(string)

		var currentHLL *hll.HyperLogLog
		if !hasEnc || enc != "proto_delta" {
			// Full sketch.
			var ferr error
			currentHLL, ferr = hll.DeserializeHyperLogLogFromProtoBytes(rawPayload)
			require.NoError(t, ferr, "window %d: full deserialize", w)
		} else {
			require.NotNil(t, prevSnap, "window %d: delta before full snapshot", w)
			deltaMsg, derr := hll.DeserializeRegisterDelta(rawPayload)
			require.NoError(t, derr, "window %d: delta deserialize", w)
			currentHLL = cloneHLL(prevSnap)
			require.NotNil(t, currentHLL)
			hll.ApplyRegisterDelta(currentHLL, deltaMsg)
		}
		prevSnap = cloneHLL(currentHLL)

		// Cardinality estimate must be non-zero after each window.
		cardAttr, ok := dps[0].Attributes().Get("hll.cardinality")
		require.True(t, ok, "window %d: hll.cardinality attr missing", w)
		assert.Greater(t, cardAttr.Int(), int64(0), fmt.Sprintf("window %d: cardinality must be positive", w))
	}
}
