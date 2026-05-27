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

// collectHLLDataPoints gathers all HLLSketch data points from metrics emitted
// to sink that belong to the given metric name.
func collectHLLDataPoints(allMetrics []pmetric.Metrics, metricName string) []pmetric.HLLSketchDataPoint {
	var dps []pmetric.HLLSketchDataPoint
	for _, md := range allMetrics {
		rms := md.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				ms := sms.At(j).Metrics()
				for k := 0; k < ms.Len(); k++ {
					if ms.At(k).Name() == metricName && ms.At(k).Type() == pmetric.MetricTypeHLLSketch {
						pts := ms.At(k).HLLSketch().DataPoints()
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
// with DeltaTransmission=true sends a full proto sketch (HLLSketchEncodingProto).
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

	dps := collectHLLDataPoints(sink.AllMetrics(), "events")
	require.Len(t, dps, 1, "expected exactly one data point after first flush")

	// Full sketch: encoding must be Proto, not Delta.
	assert.Equal(t, pmetric.HLLSketchEncodingProto, dps[0].Encoding(),
		"first window must use Proto encoding, not Delta")
	assert.Greater(t, len(dps[0].Sketch()), 0, "sketch bytes must be present")
}

// TestHLLDelta_SubsequentWindowsSendDelta verifies that the second and later
// window flushes use HLLSketchEncodingDelta once a snapshot exists.
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

	dps := collectHLLDataPoints(sink.AllMetrics(), "hits")
	require.Len(t, dps, 1, "expected one data point for window 2")

	assert.Equal(t, pmetric.HLLSketchEncodingDelta, dps[0].Encoding(),
		"second window must use Delta encoding")

	_, err := hll.DeserializeRegisterDelta(dps[0].Sketch())
	require.NoError(t, err, "Delta payload must deserialize as a RegisterDelta")
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

	dps1 := collectHLLDataPoints(sink.AllMetrics(), "req")
	require.Len(t, dps1, 1)
	require.Equal(t, pmetric.HLLSketchEncodingProto, dps1[0].Encoding())
	snapSketch, err := hll.DeserializeHyperLogLogFromProtoBytes(dps1[0].Sketch())
	require.NoError(t, err)

	sink.Reset()

	// Window 2: additional values 100..149 → delta.
	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("req", 100, 50)))
	time.Sleep(winDur + 80*time.Millisecond)

	dps2 := collectHLLDataPoints(sink.AllMetrics(), "req")
	require.Len(t, dps2, 1)

	require.Equal(t, pmetric.HLLSketchEncodingDelta, dps2[0].Encoding())

	deltaMsg, err := hll.DeserializeRegisterDelta(dps2[0].Sketch())
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
	batchDPs := collectHLLDataPoints(batchOut, "req")
	require.Len(t, batchDPs, 1)
	w2Sketch, err := hll.DeserializeHyperLogLogFromProtoBytes(batchDPs[0].Sketch())
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

	snapDPs := collectHLLDataPoints(sink.AllMetrics(), "ev")
	require.Len(t, snapDPs, 1)
	snapSketch, err := hll.DeserializeHyperLogLogFromProtoBytes(snapDPs[0].Sketch())
	require.NoError(t, err)

	sink.Reset()

	// Window 2.
	require.NoError(t, proc.ConsumeMetrics(context.Background(), makeHLLGaugeMetrics("ev", 80, 40)))
	time.Sleep(winDur + 80*time.Millisecond)

	dps2 := collectHLLDataPoints(sink.AllMetrics(), "ev")
	require.Len(t, dps2, 1)
	rawDelta := dps2[0].Sketch()

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

		dps := collectHLLDataPoints(sink.AllMetrics(), "stream")
		require.Len(t, dps, 1, "window %d: expected 1 data point", w)

		rawPayload := dps[0].Sketch()
		enc := dps[0].Encoding()

		var currentHLL *hll.HyperLogLog
		if enc != pmetric.HLLSketchEncodingDelta {
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

		// Refactor-2026-05: per-DP Cardinality was removed from
		// HLLSketchDataPoint; the receiver evaluates cardinality on demand
		// from the (reconstructed) sketch state. Mirror that here: the
		// estimate from the reconstructed sketch must be non-zero after
		// each window.
		assert.Greater(t, currentHLL.Estimate(), 0,
			fmt.Sprintf("window %d: reconstructed cardinality must be positive", w))
	}
}
