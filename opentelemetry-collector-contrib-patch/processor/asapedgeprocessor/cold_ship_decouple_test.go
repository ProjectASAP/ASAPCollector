// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// TestColdShipsWithControlChannelDisabled pins the cold-ship/control-channel
// DECOUPLING invariant: the cold-fragment shipper must activate whenever
// cold.enabled is true, INDEPENDENT of the control channel. The control channel
// only carries coordinated-sampling config updates (PrecomputeConfigSet); it
// must never gate whether cold fragments are shipped.
//
// This is the static-config / "Fig 7 cold arm" scenario: the edge runs with
//
//	cold:            {enabled: true, ship_endpoint: <merger>}
//	control_channel: {enabled: false}            // explicitly OFF
//
// and cold fragments must still reach the HTTP sink. The processor is driven
// through its real lifecycle — Start() (which spawns the ship worker AND the
// no-op control plane), ConsumeMetrics, flushAll, Shutdown — rather than poking
// the encoder directly, so a regression that re-couples shipping to the control
// channel (e.g. only starting the ship worker when ctrlChan != nil) would fail
// here.
func TestColdShipsWithControlChannelDisabled(t *testing.T) {
	var (
		mu       sync.Mutex
		bodies   [][]byte
		ctHeader string
		ceHeader string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, raw)
		ctHeader = r.Header.Get("Content-Type")
		ceHeader = r.Header.Get("Content-Encoding")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := &Config{
		ShardCount:     2,
		WindowDuration: time.Hour, // no auto-flush; we drive flushAll explicitly
		DropOriginal:   true,
		Cold: ColdConfig{
			Enabled:      true,
			ShipEndpoint: srv.URL,
			ExternalLabels: map[string]string{
				"agent": "edge-static",
			},
		},
		// The crux: the control channel is EXPLICITLY disabled. Cold shipping
		// must not depend on it.
		ControlChannel: ControlChannelConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	// Sanity: the config we are exercising really is the decoupled scenario.
	if !cfg.Cold.Enabled {
		t.Fatal("precondition: cold must be enabled")
	}
	if cfg.ControlChannel.enabled() {
		t.Fatal("precondition: control channel must be disabled")
	}

	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	// With the control channel disabled, the poll channel must be nil — proving
	// the control plane is genuinely off in this run.
	if p.ctrlChan != nil {
		t.Fatal("ctrlChan must be nil when control_channel is disabled")
	}
	if p.shipper.noop() {
		t.Fatal("shipper must be active (non-noop) when cold.ship_endpoint is set")
	}

	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })

	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("node_cpu_seconds_total")
	g := m.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	for i := 0; i < 8; i++ {
		dp := g.DataPoints().AppendEmpty()
		dp.Attributes().PutStr("core", "0")
		dp.SetDoubleValue(float64(i))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Second)))
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	p.flushAll(context.Background())

	// The ship is async (decoupled from flush). Poll for the worker's POST.
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		n := len(bodies)
		mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(bodies) == 0 {
		t.Fatal("cold shipper POSTed nothing with control_channel disabled — shipping is (wrongly) gated on the control channel")
	}
	if ctHeader != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want application/octet-stream", ctHeader)
	}
	if ceHeader != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", ceHeader)
	}
	frags, err := gorilla.DecodeFragmentBatch(gunzip(t, bodies[0]))
	if err != nil {
		t.Fatalf("DecodeFragmentBatch: %v", err)
	}
	if len(frags) == 0 {
		t.Fatal("decoded 0 cold fragments, want >= 1")
	}
	for _, f := range frags {
		if f.MetricName != "node_cpu_seconds_total" {
			t.Fatalf("fragment metric = %q, want node_cpu_seconds_total", f.MetricName)
		}
		if f.Source != "edge-static" {
			t.Fatalf("fragment source = %q, want edge-static (from external_labels)", f.Source)
		}
		if f.Count == 0 || len(f.Data) == 0 {
			t.Fatalf("fragment has no samples/data: %+v", f)
		}
	}
}
