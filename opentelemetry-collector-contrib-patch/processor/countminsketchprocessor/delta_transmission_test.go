// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"context"
	"fmt"
	"testing"

	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// cloneCMSTest deep-copies a CountMinSketch for use as a receiver snapshot.
func cloneCMSTest(s *cms.CountMinSketch) *cms.CountMinSketch {
	data, err := s.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	clone, err := cms.DeserializeCountMinSketchFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return clone
}

// assertCMSCellsEqual fails the test if two CMS sketches differ in any cell.
func assertCMSCellsEqual(t *testing.T, label string, want, got *cms.CountMinSketch) {
	t.Helper()
	require.Equal(t, want.Rows, got.Rows, "%s: rows mismatch", label)
	require.Equal(t, want.Cols, got.Cols, "%s: cols mismatch", label)
	for r := 0; r < want.Rows; r++ {
		for c := 0; c < want.Cols; c++ {
			if want.Count[r][c] != got.Count[r][c] {
				t.Fatalf("%s: Count[%d][%d] want %v got %v", label, r, c, want.Count[r][c], got.Count[r][c])
			}
		}
	}
}

// deltaTestConfig returns a batch-mode Config with DeltaTransmission enabled.
func deltaTestConfig() *Config {
	return &Config{
		Mode:              ModeBatch,
		MetricName:        "cms_delta_test",
		Rows:              5,
		Columns:           128,
		TransmitSketch:    true,
		DropOriginal:      true,
		DeltaTransmission: true,
		DeltaThreshold:    1.0,
	}
}

// referenceConfig returns a batch-mode Config without DeltaTransmission for
// building reference sketches.
func referenceConfig() *Config {
	return &Config{
		Mode:           ModeBatch,
		MetricName:     "cms_delta_test",
		Rows:           5,
		Columns:        128,
		TransmitSketch: true,
		DropOriginal:   true,
	}
}

// extractSketchDP returns the single expected sketch data point from the output.
func extractSketchDP(t *testing.T, out interface{ ResourceMetrics() interface{ Len() int } }) interface{} {
	return nil // unused shim; use getAllDataPoints directly
}

// TestCMSDelta_FirstWindowSendsFullSketch verifies that the very first batch
// with DeltaTransmission=true sends a proto_full payload (no prior snapshot).
func TestCMSDelta_FirstWindowSendsFullSketch(t *testing.T) {
	cfg := deltaTestConfig()
	require.NoError(t, cfg.Validate())

	proc := newProcessor(cfg, &mockConsumer{}, zap.NewNop())

	out, err := proc.ConsumeMetrics(context.Background(), generateMetrics("svc", 10))
	require.NoError(t, err)

	dps := getAllDataPoints(out)
	require.Len(t, dps, 1, "expected exactly one output data point")

	encVal, ok := dps[0].Attributes().Get("encoding")
	require.True(t, ok, "encoding attribute must be present")
	assert.Equal(t, "proto_full", encVal.Str(), "first window must send proto_full")

	payloadVal, ok := dps[0].Attributes().Get("sketch_payload")
	require.True(t, ok)
	rawBytes := payloadVal.Bytes().AsRaw()
	require.NotEmpty(t, rawBytes)

	sketch, err := cms.DeserializeCountMinSketchFromProtoBytes(rawBytes)
	require.NoError(t, err, "proto_full payload must deserialize as a CountMinSketch")
	assert.Equal(t, 5, sketch.Rows)
	assert.Equal(t, 128, sketch.Cols)
}

// TestCMSDelta_SubsequentWindowsSendDelta verifies that the second and later
// batches send proto_delta payloads once a snapshot exists.
func TestCMSDelta_SubsequentWindowsSendDelta(t *testing.T) {
	cfg := deltaTestConfig()
	require.NoError(t, cfg.Validate())

	proc := newProcessor(cfg, &mockConsumer{}, zap.NewNop())

	// Window 1 — establishes snapshot.
	_, err := proc.ConsumeMetrics(context.Background(), generateMetrics("svc", 10))
	require.NoError(t, err)

	// Window 2 — should send delta.
	out2, err := proc.ConsumeMetrics(context.Background(), generateMetrics("svc", 5))
	require.NoError(t, err)

	dps := getAllDataPoints(out2)
	require.Len(t, dps, 1)

	encVal, ok := dps[0].Attributes().Get("encoding")
	require.True(t, ok)
	assert.Equal(t, "proto_delta", encVal.Str(), "second window must send proto_delta")

	// Delta payload must parse without error.
	payloadVal, ok := dps[0].Attributes().Get("sketch_payload")
	require.True(t, ok)
	_, err = cms.DeserializeDelta(payloadVal.Bytes().AsRaw())
	require.NoError(t, err, "proto_delta payload must deserialize as a Delta")
}

// TestCMSDelta_RoundTrip verifies that applying the delta payload from window 2
// onto the full sketch from window 1 reconstructs the same state as what the
// processor computed for window 2, using an independent reference processor.
//
// Both windows use the same service name so they share a partition key, which
// ensures the snapshot from window 1 is reused and a real delta is computed.
func TestCMSDelta_RoundTrip(t *testing.T) {
	cfg := deltaTestConfig()
	require.NoError(t, cfg.Validate())

	proc := newProcessor(cfg, &mockConsumer{}, zap.NewNop())

	// Window 1: 50 insertions for "svc" → snapshot created.
	out1, err := proc.ConsumeMetrics(context.Background(), generateMetrics("svc", 50))
	require.NoError(t, err)
	dps1 := getAllDataPoints(out1)
	require.Len(t, dps1, 1)
	rawFull := dps1[0].Attributes().AsRaw()["sketch_payload"].([]byte)
	snapSketch, err := cms.DeserializeCountMinSketchFromProtoBytes(rawFull)
	require.NoError(t, err)

	// Window 2: 30 insertions for the same "svc" → delta against window-1 snapshot.
	md2 := generateMetrics("svc", 30)
	out2, err := proc.ConsumeMetrics(context.Background(), md2)
	require.NoError(t, err)
	dps2 := getAllDataPoints(out2)
	require.Len(t, dps2, 1)

	encVal, ok := dps2[0].Attributes().Get("encoding")
	require.True(t, ok)
	require.Equal(t, "proto_delta", encVal.Str(), "window 2 must be proto_delta (same partition key)")

	rawDelta := dps2[0].Attributes().AsRaw()["sketch_payload"].([]byte)
	deltaMsg, err := cms.DeserializeDelta(rawDelta)
	require.NoError(t, err)

	// Apply delta to clone of snapshot → reconstructed.
	reconstructed := cloneCMSTest(snapSketch)
	require.NotNil(t, reconstructed)
	cms.ApplyDelta(reconstructed, deltaMsg)

	// Reference: fresh no-delta processor with exactly window-2 data.
	refProc := newProcessor(referenceConfig(), &mockConsumer{}, zap.NewNop())
	refOut, err := refProc.ConsumeMetrics(context.Background(), md2)
	require.NoError(t, err)
	refDps := getAllDataPoints(refOut)
	require.Len(t, refDps, 1)
	rawRef := refDps[0].Attributes().AsRaw()["sketch_payload"].([]byte)
	refSketch, err := cms.DeserializeCountMinSketchFromProtoBytes(rawRef)
	require.NoError(t, err)

	assertCMSCellsEqual(t, "RoundTrip", refSketch, reconstructed)
}

// TestCMSDelta_MultipleWindowsConvergence simulates 5 consecutive delta windows
// and verifies that a receiver reconstructing the state via Apply matches an
// independent reference processor for each window.
func TestCMSDelta_MultipleWindowsConvergence(t *testing.T) {
	cfg := deltaTestConfig()
	require.NoError(t, cfg.Validate())

	proc := newProcessor(cfg, &mockConsumer{}, zap.NewNop())

	var prevSnap *cms.CountMinSketch // receiver's current reconstructed state

	for w := 0; w < 5; w++ {
		// Same service name across all windows → same partition key → snapshot is reused.
		md := generateMetrics("svc", 20*(w+1))

		out, err := proc.ConsumeMetrics(context.Background(), md)
		require.NoError(t, err, "window %d", w)

		dps := getAllDataPoints(out)
		require.Len(t, dps, 1, "window %d: expected 1 data point", w)

		encAttr := dps[0].Attributes().AsRaw()
		rawPayload := encAttr["sketch_payload"].([]byte)
		enc, _ := encAttr["encoding"].(string)

		// Receiver reconstructs current state.
		var currentCMS *cms.CountMinSketch
		if enc == "proto_full" {
			currentCMS, err = cms.DeserializeCountMinSketchFromProtoBytes(rawPayload)
			require.NoError(t, err, "window %d: full deserialize", w)
		} else {
			require.Equal(t, "proto_delta", enc, "window %d: unexpected encoding", w)
			require.NotNil(t, prevSnap, "window %d: delta before full snapshot", w)
			deltaMsg, derr := cms.DeserializeDelta(rawPayload)
			require.NoError(t, derr, "window %d: delta deserialize", w)
			currentCMS = cloneCMSTest(prevSnap)
			require.NotNil(t, currentCMS)
			cms.ApplyDelta(currentCMS, deltaMsg)
		}
		prevSnap = currentCMS

		// Reference: fresh processor with only this window's data.
		refProc := newProcessor(referenceConfig(), &mockConsumer{}, zap.NewNop())
		refOut, err := refProc.ConsumeMetrics(context.Background(), md)
		require.NoError(t, err, "window %d: reference proc", w)
		refDps := getAllDataPoints(refOut)
		require.Len(t, refDps, 1, "window %d: reference output", w)
		rawRef := refDps[0].Attributes().AsRaw()["sketch_payload"].([]byte)
		refSketch, err := cms.DeserializeCountMinSketchFromProtoBytes(rawRef)
		require.NoError(t, err, "window %d: reference deserialize", w)

		assertCMSCellsEqual(t, fmt.Sprintf("window %d", w), refSketch, currentCMS)
	}
}

// TestCMSDelta_DisabledAlwaysSendsFullSketch verifies that without
// DeltaTransmission every window always sends proto_full.
func TestCMSDelta_DisabledAlwaysSendsFullSketch(t *testing.T) {
	cfg := referenceConfig()
	require.NoError(t, cfg.Validate())

	proc := newProcessor(cfg, &mockConsumer{}, zap.NewNop())

	for i := 0; i < 3; i++ {
		out, err := proc.ConsumeMetrics(context.Background(), generateMetrics("svc", 5))
		require.NoError(t, err)
		dps := getAllDataPoints(out)
		require.Len(t, dps, 1, "window %d", i)
		encVal, ok := dps[0].Attributes().Get("encoding")
		require.True(t, ok, "window %d: encoding attr missing", i)
		assert.Equal(t, "proto_full", encVal.Str(), "window %d: expected proto_full", i)
	}
}
