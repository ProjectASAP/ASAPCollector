// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allsketches

import (
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/influxdata/telegraf"
	telegrafmetric "github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/testutil"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	telegrafcodec "github.com/ProjectASAP/asap-precompute-go/telegraf"
)

// newPlugin returns an AllSketches with the codec field defaults set,
// so each test only fills in what it actually exercises.
func newPlugin(sketchType string, window string) *AllSketches {
	return &AllSketches{
		SketchType:       sketchType,
		WindowSize:       window,
		ValueField:       telegrafcodec.DefaultValueField,
		EnvelopeField:    telegrafcodec.DefaultEnvelopeField,
		OutputMetricName: "test_metric",
		Log:              testutil.Logger{},
	}
}

// newScalarMetric builds a telegraf.Metric carrying a single float
// value field — the canonical scalar input the codec consumes.
func newScalarMetric(name string, val float64, ts time.Time) telegraf.Metric {
	return telegrafmetric.New(name,
		map[string]string{"host": "h1"},
		map[string]interface{}{"value": val},
		ts)
}

// TestStart_InvalidSketchType asserts Start surfaces a clear error
// for unknown sketch_type spellings instead of failing later in
// Add(). This is the cheapest test that exercises the resolveSketchType
// dispatch path.
func TestStart_InvalidSketchType(t *testing.T) {
	a := newPlugin("notarealsketch", "10s")
	acc := &testutil.Accumulator{}
	if err := a.Start(acc); err == nil {
		t.Fatalf("Start: want error for unknown sketch_type, got nil")
	}
}

// TestStart_InvalidWindow asserts the duration-parse path is wired up
// correctly — a malformed window_size must reject in Start, not panic
// in the ticker goroutine.
func TestStart_InvalidWindow(t *testing.T) {
	a := newPlugin("ddsketch", "not-a-duration")
	acc := &testutil.Accumulator{}
	if err := a.Start(acc); err == nil {
		t.Fatalf("Start: want error for unparsable window_size, got nil")
	}
}

// TestDDSketch_HappyPath drives Start → N-metric Add → Stop and
// asserts the accumulator received envelope-bearing metrics. The
// final flush in Stop is what produces the output: we don't depend
// on the ticker firing, which keeps the test deterministic.
func TestDDSketch_HappyPath(t *testing.T) {
	a := newPlugin("ddsketch", "1h") // long window so the only flush is the Stop drain
	a.Alpha = 0.01
	acc := &testutil.Accumulator{}
	if err := a.Start(acc); err != nil {
		t.Fatalf("Start: %v", err)
	}

	now := time.Now()
	for i := 0; i < 32; i++ {
		m := newScalarMetric("http_request_duration_ms", float64(i), now)
		if err := a.Add(m, acc); err != nil {
			t.Fatalf("Add[%d]: %v", i, err)
		}
	}

	a.Stop()

	out := acc.GetTelegrafMetrics()
	if len(out) == 0 {
		t.Fatalf("expected at least one envelope metric on the accumulator, got 0")
	}
	for i, m := range out {
		raw, ok := m.GetField(telegrafcodec.DefaultEnvelopeField)
		if !ok {
			t.Fatalf("metric[%d]: missing envelope field %q", i, telegrafcodec.DefaultEnvelopeField)
		}
		s, ok := raw.(string)
		if !ok || s == "" {
			t.Fatalf("metric[%d]: envelope field empty or wrong type: %T", i, raw)
		}
		if _, err := base64.StdEncoding.DecodeString(s); err != nil {
			t.Fatalf("metric[%d]: envelope field is not base64: %v", i, err)
		}
	}
}

// TestEnvelopeShortcut feeds the plugin a metric carrying a
// pre-aggregated envelope on the well-known field. The decode path
// must take the envelope shortcut (not try to read value_field) and
// not error.
func TestEnvelopeShortcut(t *testing.T) {
	a := newPlugin("ddsketch", "1h")
	a.Alpha = 0.01
	acc := &testutil.Accumulator{}
	if err := a.Start(acc); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer a.Stop()

	// Build a plausible upstream envelope. The runtime will route it
	// through ObserveEnvelope path via the codec's KindEnvelope.
	env := &precompute.SketchEnvelope{
		MetricName:    "upstream_metric",
		SketchType:    precompute.SketchTypeDDSketch,
		Encoding:      precompute.EncodingProtoFull,
		WindowStartMs: uint64(time.Now().Add(-time.Second).UnixMilli()),
		WindowEndMs:   uint64(time.Now().UnixMilli()),
		Payload:       []byte{},
	}
	jsonBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("json.Marshal envelope: %v", err)
	}
	b64 := base64.StdEncoding.EncodeToString(jsonBytes)

	m := telegrafmetric.New("upstream_metric",
		map[string]string{"host": "h1"},
		map[string]interface{}{telegrafcodec.DefaultEnvelopeField: b64},
		time.Now())

	// Add must not error: the envelope-shortcut path must be exercised
	// without a value_field on the metric.
	if err := a.Add(m, acc); err != nil {
		t.Fatalf("Add envelope-shortcut: %v", err)
	}
	// We don't assert on Errors() here — the runtime may still reject
	// the empty payload as invalid, but the codec layer's job (decode
	// without a value field) must succeed, which is the assertion above.
}

// TestStopWithoutStart asserts Stop is a safe no-op when Start was
// never called — important because Telegraf's reload machinery can
// invoke Stop on a plugin instance whose Start failed earlier.
func TestStopWithoutStart(t *testing.T) {
	a := newPlugin("ddsketch", "10s")
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Stop without Start panicked: %v", r)
		}
	}()
	a.Stop()
}

// TestAllSketchTypes loops over every supported sketch_type and
// asserts each one constructs cleanly and accepts a few scalar
// observations without erroring. Per-sketch correctness is covered
// by the asap-precompute-go/sketches/* unit tests; this test just
// verifies the dispatch table is wired up end-to-end.
func TestAllSketchTypes(t *testing.T) {
	cases := []struct {
		name       string
		sketchType string
		setup      func(*AllSketches)
	}{
		{"ddsketch", SketchTypeDDSketch, func(p *AllSketches) { p.Alpha = 0.01 }},
		{"kll", SketchTypeKLL, func(p *AllSketches) { p.K = 200 }},
		{"hll", SketchTypeHLL, func(p *AllSketches) {}},
		{"countsketch", SketchTypeCountSketch, func(p *AllSketches) { p.Width = 256; p.Depth = 4 }},
		{"countminsketch", SketchTypeCountMinSketch, func(p *AllSketches) { p.Width = 256; p.Depth = 4 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newPlugin(tc.sketchType, "1h")
			tc.setup(a)
			acc := &testutil.Accumulator{}
			if err := a.Start(acc); err != nil {
				t.Fatalf("Start(%s): %v", tc.sketchType, err)
			}
			now := time.Now()
			for i := 0; i < 8; i++ {
				m := newScalarMetric("m_"+tc.name, float64(i+1), now)
				if err := a.Add(m, acc); err != nil {
					t.Fatalf("Add(%s)[%d]: %v", tc.sketchType, i, err)
				}
			}
			a.Stop()
			// Every sketch type should at least produce one envelope
			// on the final drain. We don't introspect payload bytes
			// here — that's the per-sketch wrapper's contract.
			if len(acc.GetTelegrafMetrics()) == 0 {
				t.Fatalf("Stop(%s): expected drain to emit at least one metric", tc.sketchType)
			}
		})
	}
}
