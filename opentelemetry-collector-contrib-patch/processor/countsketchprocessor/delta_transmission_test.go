// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countsketchprocessor

import (
	"context"
	"fmt"
	"testing"

	countsketch "github.com/ProjectASAP/sketchlib-go/sketches/CountSketch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// makeCSGaugeMetrics builds a simple Gauge metrics payload with count data points
// labelled with the given service name.
func makeCSGaugeMetrics(service string, count int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http_requests")
	m.SetEmptyGauge()
	for i := 0; i < count; i++ {
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetDoubleValue(1.0)
		dp.Attributes().PutStr("service.name", service)
	}
	return md
}

// csTestDataPoint is a test-only adapter that flattens either a
// `CountSketchDataPoint` (the typed emission path) or a
// `NumberDataPoint` (legacy Gauge path kept for non-TransmitSketch
// mode) into a single `pcommon.Map` so existing test assertions that
// read `sketch_payload` / `encoding` / `sample_count` via
// `Attributes().Get(...)` keep compiling without per-site rewrites.
//
// `doubleValue` is only populated for the non-TransmitSketch Gauge
// path; the typed path leaves it zero and unused.
type csTestDataPoint struct {
	attributes  pcommon.Map
	doubleValue float64
}

func (d csTestDataPoint) Attributes() pcommon.Map { return d.attributes }
func (d csTestDataPoint) DoubleValue() float64    { return d.doubleValue }

// csEncodingToLegacyString maps the proto enum back to the
// "proto_full" / "proto_delta" strings existing test assertions
// compare against.
func csEncodingToLegacyString(enc pmetric.CountSketchEncoding) string {
	switch enc {
	case pmetric.CountSketchEncodingProto:
		return "proto_full"
	case pmetric.CountSketchEncodingDelta:
		return "proto_delta"
	}
	return "unknown"
}

// getCSOutputDPs walks the output metrics and returns a flat slice
// of test adapters, one per sketch data point (typed or legacy
// Gauge). For typed `CountSketchDataPoint`s the helper synthesizes
// the legacy attribute keys (`sketch_payload`, `encoding`,
// `sample_count`, `partition_key`, `epsilon`, `delta`,
// `window_duration_seconds`) from the typed fields so downstream
// assertions in this file don't need per-site rewrites.
func getCSOutputDPs(md pmetric.Metrics) []csTestDataPoint {
	var dps []csTestDataPoint
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				switch m.Type() {
				case pmetric.MetricTypeCountSketch:
					pts := m.CountSketch().DataPoints()
					for l := 0; l < pts.Len(); l++ {
						dp := pts.At(l)
						attrs := pcommon.NewMap()
						dp.Attributes().CopyTo(attrs)
						// The typed DP has a dedicated
						// `dimension` field that carries
						// the partition key; mirror it
						// back into the legacy attribute
						// name.
						attrs.PutStr("partition_key", dp.Dimension())
						attrs.PutDouble("epsilon", dp.Epsilon())
						attrs.PutDouble("delta", dp.Delta())
						attrs.PutEmptyBytes("sketch_payload").FromRaw(dp.Sketch())
						attrs.PutStr("encoding", csEncodingToLegacyString(dp.Encoding()))
						dps = append(dps, csTestDataPoint{attributes: attrs})
					}
				case pmetric.MetricTypeGauge:
					pts := m.Gauge().DataPoints()
					for l := 0; l < pts.Len(); l++ {
						dp := pts.At(l)
						attrs := pcommon.NewMap()
						dp.Attributes().CopyTo(attrs)
						dps = append(dps, csTestDataPoint{
							attributes:  attrs,
							doubleValue: dp.DoubleValue(),
						})
					}
				}
			}
		}
	}
	return dps
}

// cloneCSTest deep-copies a CountSketch via proto round-trip.
func cloneCSTest(cs *countsketch.CountSketch) *countsketch.CountSketch {
	data, err := cs.SerializeProtoBytes()
	if err != nil {
		return nil
	}
	clone, err := countsketch.DeserializeCountSketchFromProtoBytes(data)
	if err != nil {
		return nil
	}
	return clone
}

// assertCSCellsEqual fails the test if two CountSketches differ in any cell.
func assertCSCellsEqual(t *testing.T, label string, want, got *countsketch.CountSketch) {
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

// deltaCSConfig returns a batch-mode Config with DeltaTransmission enabled.
func deltaCSConfig() *Config {
	return &Config{
		Mode:              ModeBatch,
		Epsilon:           0.01,
		Delta:             0.99,
		TransmitSketch:    true,
		DropOriginal:      true,
		DeltaTransmission: true,
		DeltaThreshold:    1.0,
	}
}

// refCSConfig returns a batch-mode Config without DeltaTransmission for
// building reference sketches.
func refCSConfig() *Config {
	return &Config{
		Mode:           ModeBatch,
		Epsilon:        0.01,
		Delta:          0.99,
		TransmitSketch: true,
		DropOriginal:   true,
	}
}

// TestCSDelta_FirstWindowSendsFullSketch verifies that the very first batch
// with DeltaTransmission=true emits encoding=proto_full.
func TestCSDelta_FirstWindowSendsFullSketch(t *testing.T) {
	cfg := deltaCSConfig()
	require.NoError(t, cfg.Validate())

	proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))

	out, err := proc.processMetrics(context.Background(), makeCSGaugeMetrics("svc", 10))
	require.NoError(t, err)

	dps := getCSOutputDPs(out)
	require.Len(t, dps, 1, "expected exactly one output data point")

	encVal, ok := dps[0].Attributes().Get("encoding")
	require.True(t, ok, "encoding attribute must be present")
	assert.Equal(t, "proto_full", encVal.Str(), "first window must send proto_full")

	payloadVal, ok := dps[0].Attributes().Get("sketch_payload")
	require.True(t, ok)
	rawBytes := payloadVal.Bytes().AsRaw()
	require.NotEmpty(t, rawBytes)

	sketch, err := countsketch.DeserializeCountSketchFromProtoBytes(rawBytes)
	require.NoError(t, err, "proto_full payload must deserialize as a CountSketch")
	assert.Greater(t, sketch.Rows, 0)
	assert.Greater(t, sketch.Cols, 0)
}

// TestCSDelta_SubsequentWindowsSendDelta verifies that the second batch sends
// a proto_delta payload once a snapshot has been established.
func TestCSDelta_SubsequentWindowsSendDelta(t *testing.T) {
	cfg := deltaCSConfig()
	require.NoError(t, cfg.Validate())

	proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))

	// Window 1 — establishes snapshot.
	_, err := proc.processMetrics(context.Background(), makeCSGaugeMetrics("svc", 10))
	require.NoError(t, err)

	// Window 2 — should send delta.
	out2, err := proc.processMetrics(context.Background(), makeCSGaugeMetrics("svc", 5))
	require.NoError(t, err)

	dps := getCSOutputDPs(out2)
	require.Len(t, dps, 1)

	encVal, ok := dps[0].Attributes().Get("encoding")
	require.True(t, ok)
	assert.Equal(t, "proto_delta", encVal.Str(), "second window must send proto_delta")

	// Delta payload must parse without error.
	payloadVal, ok := dps[0].Attributes().Get("sketch_payload")
	require.True(t, ok)
	_, err = countsketch.DeserializeDelta(payloadVal.Bytes().AsRaw())
	require.NoError(t, err, "proto_delta payload must deserialize as a Delta")
}

// TestCSDelta_RoundTrip verifies that applying the delta from window 2 onto the
// full sketch from window 1 produces the same state as an independent reference
// processor run on window-2 data.
//
// Both windows use the same service name so they share a partition key.
func TestCSDelta_RoundTrip(t *testing.T) {
	cfg := deltaCSConfig()
	require.NoError(t, cfg.Validate())

	proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))

	// Window 1: 50 insertions → snapshot created.
	out1, err := proc.processMetrics(context.Background(), makeCSGaugeMetrics("svc", 50))
	require.NoError(t, err)
	dps1 := getCSOutputDPs(out1)
	require.Len(t, dps1, 1)
	rawFull := dps1[0].Attributes().AsRaw()["sketch_payload"].([]byte)
	snapSketch, err := countsketch.DeserializeCountSketchFromProtoBytes(rawFull)
	require.NoError(t, err)

	// Window 2: 30 insertions of the same key → delta against window-1 snapshot.
	md2 := makeCSGaugeMetrics("svc", 30)
	out2, err := proc.processMetrics(context.Background(), md2)
	require.NoError(t, err)
	dps2 := getCSOutputDPs(out2)
	require.Len(t, dps2, 1)

	encVal, ok := dps2[0].Attributes().Get("encoding")
	require.True(t, ok)
	require.Equal(t, "proto_delta", encVal.Str(), "window 2 must be proto_delta")

	rawDelta := dps2[0].Attributes().AsRaw()["sketch_payload"].([]byte)
	deltaMsg, err := countsketch.DeserializeDelta(rawDelta)
	require.NoError(t, err)

	// Apply delta to clone of snapshot → reconstructed.
	reconstructed := cloneCSTest(snapSketch)
	require.NotNil(t, reconstructed)
	countsketch.ApplyDelta(reconstructed, deltaMsg)

	// Reference: fresh no-delta processor with exactly window-2 data.
	refProc := newProcessor(zap.NewNop(), refCSConfig(), new(consumertest.MetricsSink))
	require.NoError(t, refCSConfig().Validate())
	refOut, err := refProc.processMetrics(context.Background(), md2)
	require.NoError(t, err)
	refDps := getCSOutputDPs(refOut)
	require.Len(t, refDps, 1)
	rawRef := refDps[0].Attributes().AsRaw()["sketch_payload"].([]byte)
	refSketch, err := countsketch.DeserializeCountSketchFromProtoBytes(rawRef)
	require.NoError(t, err)

	assertCSCellsEqual(t, "RoundTrip", refSketch, reconstructed)
}

// TestCSDelta_MultipleWindowsConvergence simulates 5 consecutive delta windows
// and verifies that the receiver's reconstructed sketch matches an independent
// reference processor for each window.
func TestCSDelta_MultipleWindowsConvergence(t *testing.T) {
	cfg := deltaCSConfig()
	require.NoError(t, cfg.Validate())

	proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))

	var prevSnap *countsketch.CountSketch

	for w := 0; w < 5; w++ {
		insertCount := 20 * (w + 1)
		md := makeCSGaugeMetrics("svc", insertCount)

		out, err := proc.processMetrics(context.Background(), md)
		require.NoError(t, err, "window %d", w)

		dps := getCSOutputDPs(out)
		require.Len(t, dps, 1, "window %d: expected 1 data point", w)

		attrs := dps[0].Attributes().AsRaw()
		rawPayload := attrs["sketch_payload"].([]byte)
		enc, _ := attrs["encoding"].(string)

		var currentCS *countsketch.CountSketch
		if enc == "proto_full" {
			currentCS, err = countsketch.DeserializeCountSketchFromProtoBytes(rawPayload)
			require.NoError(t, err, "window %d: full deserialize", w)
		} else {
			require.Equal(t, "proto_delta", enc, "window %d: unexpected encoding", w)
			require.NotNil(t, prevSnap, "window %d: delta before full snapshot", w)
			deltaMsg, derr := countsketch.DeserializeDelta(rawPayload)
			require.NoError(t, derr, "window %d: delta deserialize", w)
			currentCS = cloneCSTest(prevSnap)
			require.NotNil(t, currentCS)
			countsketch.ApplyDelta(currentCS, deltaMsg)
		}
		prevSnap = currentCS

		// Reference: fresh no-delta processor with only this window's data.
		refProc := newProcessor(zap.NewNop(), refCSConfig(), new(consumertest.MetricsSink))
		refOut, err := refProc.processMetrics(context.Background(), md)
		require.NoError(t, err, "window %d: reference proc", w)
		refDps := getCSOutputDPs(refOut)
		require.Len(t, refDps, 1, "window %d: reference output", w)
		rawRef := refDps[0].Attributes().AsRaw()["sketch_payload"].([]byte)
		refSketch, err := countsketch.DeserializeCountSketchFromProtoBytes(rawRef)
		require.NoError(t, err, "window %d: reference deserialize", w)

		assertCSCellsEqual(t, fmt.Sprintf("window %d", w), refSketch, currentCS)
	}
}

// TestCSDelta_DisabledAlwaysSendsFullSketch verifies that without
// DeltaTransmission every window always sends proto_full.
func TestCSDelta_DisabledAlwaysSendsFullSketch(t *testing.T) {
	cfg := refCSConfig()
	require.NoError(t, cfg.Validate())

	proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))

	for i := 0; i < 3; i++ {
		out, err := proc.processMetrics(context.Background(), makeCSGaugeMetrics("svc", 5))
		require.NoError(t, err)
		dps := getCSOutputDPs(out)
		require.Len(t, dps, 1, "window %d", i)
		encVal, ok := dps[0].Attributes().Get("encoding")
		require.True(t, ok, "window %d: encoding attr missing", i)
		assert.Equal(t, "proto_full", encVal.Str(), "window %d: expected proto_full", i)
	}
}

// TestCSDelta_PartitionKeyIsolation verifies that two different series (partition
// keys) each get their own snapshot and independently send full→delta.
func TestCSDelta_PartitionKeyIsolation(t *testing.T) {
	cfg := deltaCSConfig()
	cfg.AggregateBy = []string{"service.name"}
	require.NoError(t, cfg.Validate())

	proc := newProcessor(zap.NewNop(), cfg, new(consumertest.MetricsSink))

	// Build a batch with two services.
	buildTwoService := func(nA, nB int) pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		sm := rm.ScopeMetrics().AppendEmpty()
		m := sm.Metrics().AppendEmpty()
		m.SetName("req")
		m.SetEmptyGauge()
		for i := 0; i < nA; i++ {
			dp := m.Gauge().DataPoints().AppendEmpty()
			dp.SetDoubleValue(1)
			dp.Attributes().PutStr("service.name", "alpha")
		}
		for i := 0; i < nB; i++ {
			dp := m.Gauge().DataPoints().AppendEmpty()
			dp.SetDoubleValue(1)
			dp.Attributes().PutStr("service.name", "beta")
		}
		return md
	}

	// Window 1: both services → both send proto_full (no prior snapshot).
	out1, err := proc.processMetrics(context.Background(), buildTwoService(30, 20))
	require.NoError(t, err)
	dps1 := getCSOutputDPs(out1)
	require.Len(t, dps1, 2, "window 1: expected 2 partition data points")
	for _, dp := range dps1 {
		enc, _ := dp.Attributes().AsRaw()["encoding"].(string)
		assert.Equal(t, "proto_full", enc, "window 1: all partitions must be proto_full")
	}

	// Window 2: both services → both send proto_delta.
	out2, err := proc.processMetrics(context.Background(), buildTwoService(15, 10))
	require.NoError(t, err)
	dps2 := getCSOutputDPs(out2)
	require.Len(t, dps2, 2, "window 2: expected 2 partition data points")

	// Collect encodings by partition key attribute.
	encByKey := make(map[string]string)
	for _, dp := range dps2 {
		attrs := dp.Attributes()
		var key string
		attrs.Range(func(k string, v pcommon.Value) bool {
			if k == "partition_key" {
				key = v.AsString()
			}
			return true
		})
		enc, _ := attrs.AsRaw()["encoding"].(string)
		encByKey[key] = enc
	}
	for k, enc := range encByKey {
		assert.Equal(t, "proto_delta", enc, "partition %q: window 2 must be proto_delta", k)
	}
}
