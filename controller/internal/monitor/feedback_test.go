package monitor_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ProjectASAP/controller/internal/monitor"
)

// metricsServer returns an httptest.Server that serves a fixed Prometheus payload.
func metricsServer(payload string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, payload)
	}))
}

const normalPayload = `
# HELP otelcol_sketch_size_bytes Current sketch size in bytes
# TYPE otelcol_sketch_size_bytes gauge
otelcol_sketch_size_bytes 1048576
# HELP process_cpu_seconds_total Total CPU seconds
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 0.5
# HELP otelcol_processor_accepted_metric_points Accepted samples
# TYPE otelcol_processor_accepted_metric_points counter
otelcol_processor_accepted_metric_points 100000
# HELP otelcol_sketch_error_rate Error rate
# TYPE otelcol_sketch_error_rate gauge
otelcol_sketch_error_rate 0.001
`

const highBandwidthPayload = `
otelcol_sketch_size_bytes 10485760
process_cpu_seconds_total 1.0
otelcol_processor_accepted_metric_points 200000
otelcol_sketch_error_rate 0.001
`

const highErrorRatePayload = `
otelcol_sketch_size_bytes 512000
process_cpu_seconds_total 1.0
otelcol_processor_accepted_metric_points 200000
otelcol_sketch_error_rate 0.05
`

func TestScraper_NoViolationOnNormalMetrics(t *testing.T) {
	srv := metricsServer(normalPayload)
	defer srv.Close()

	var violations []monitor.Violation
	scraper := monitor.NewScraper(
		[]monitor.Endpoint{{AgentID: "a1", MetricsURL: srv.URL}},
		monitor.DefaultThresholds,
		func(v monitor.Violation) { violations = append(violations, v) },
		time.Second,
		nil,
	)

	scraper.ScrapeOnce(context.Background())

	if len(violations) != 0 {
		t.Errorf("expected no violations, got %v", violations)
	}
}

func TestScraper_BandwidthViolation(t *testing.T) {
	srv := metricsServer(highBandwidthPayload)
	defer srv.Close()

	var violations []monitor.Violation
	scraper := monitor.NewScraper(
		[]monitor.Endpoint{{AgentID: "a1", MetricsURL: srv.URL}},
		monitor.DefaultThresholds, // MaxSketchSizeBytes = 5MB; payload = 10MB
		func(v monitor.Violation) { violations = append(violations, v) },
		time.Second,
		nil,
	)

	scraper.ScrapeOnce(context.Background())

	if len(violations) != 1 {
		t.Fatalf("expected 1 violation, got %d: %v", len(violations), violations)
	}
	if violations[0].Kind != monitor.ViolationBandwidth {
		t.Errorf("ViolationKind = %v, want Bandwidth", violations[0].Kind)
	}
	if violations[0].Observed <= violations[0].Threshold {
		t.Errorf("Observed (%f) should exceed Threshold (%f)", violations[0].Observed, violations[0].Threshold)
	}
}

func TestScraper_AccuracyViolation(t *testing.T) {
	srv := metricsServer(highErrorRatePayload)
	defer srv.Close()

	var violations []monitor.Violation
	scraper := monitor.NewScraper(
		[]monitor.Endpoint{{AgentID: "a1", MetricsURL: srv.URL}},
		monitor.DefaultThresholds, // MaxErrorRate = 2%; payload = 5%
		func(v monitor.Violation) { violations = append(violations, v) },
		time.Second,
		nil,
	)

	scraper.ScrapeOnce(context.Background())

	found := false
	for _, v := range violations {
		if v.Kind == monitor.ViolationAccuracy {
			found = true
			if v.AgentID != "a1" {
				t.Errorf("AgentID = %q, want a1", v.AgentID)
			}
		}
	}
	if !found {
		t.Error("expected an accuracy violation, got none")
	}
}

func TestScraper_CPUViolationOnDelta(t *testing.T) {
	// First scrape: baseline
	// Second scrape: high CPU usage relative to samples ingested
	call := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		if call == 1 {
			// Baseline: 0 CPU, 0 samples
			fmt.Fprint(w, `
process_cpu_seconds_total 0
otelcol_processor_accepted_metric_points 0
otelcol_sketch_size_bytes 100
otelcol_sketch_error_rate 0
`)
		} else {
			// 1 second later: 5ms CPU per sample → 50 μs/sample (above 5 μs threshold)
			fmt.Fprint(w, `
process_cpu_seconds_total 0.005
otelcol_processor_accepted_metric_points 100
otelcol_sketch_size_bytes 100
otelcol_sketch_error_rate 0
`)
		}
	}))
	defer srv.Close()

	var violations []monitor.Violation
	scraper := monitor.NewScraper(
		[]monitor.Endpoint{{AgentID: "a1", MetricsURL: srv.URL}},
		monitor.DefaultThresholds,
		func(v monitor.Violation) { violations = append(violations, v) },
		time.Second,
		nil,
	)

	ctx := context.Background()
	scraper.ScrapeOnce(ctx) // baseline
	scraper.ScrapeOnce(ctx) // second scrape — CPU delta triggers violation

	found := false
	for _, v := range violations {
		if v.Kind == monitor.ViolationCPU {
			found = true
		}
	}
	if !found {
		t.Error("expected a CPU violation from delta computation, got none")
	}
}

func TestScraper_MultipleEndpoints(t *testing.T) {
	srvOK := metricsServer(normalPayload)
	srvBad := metricsServer(highBandwidthPayload)
	defer srvOK.Close()
	defer srvBad.Close()

	var violations []monitor.Violation
	scraper := monitor.NewScraper(
		[]monitor.Endpoint{
			{AgentID: "ok", MetricsURL: srvOK.URL},
			{AgentID: "bad", MetricsURL: srvBad.URL},
		},
		monitor.DefaultThresholds,
		func(v monitor.Violation) { violations = append(violations, v) },
		time.Second,
		nil,
	)

	scraper.ScrapeOnce(context.Background())

	for _, v := range violations {
		if v.AgentID == "ok" {
			t.Errorf("unexpected violation from healthy agent: %v", v)
		}
	}
	found := false
	for _, v := range violations {
		if v.AgentID == "bad" {
			found = true
		}
	}
	if !found {
		t.Error("expected violation from bad agent, got none")
	}
}

func TestScraper_UnreachableEndpoint(t *testing.T) {
	var violations []monitor.Violation
	scraper := monitor.NewScraper(
		[]monitor.Endpoint{{AgentID: "a1", MetricsURL: "http://127.0.0.1:1"}},
		monitor.DefaultThresholds,
		func(v monitor.Violation) { violations = append(violations, v) },
		time.Second,
		nil,
	)

	// Should not panic; just log the error.
	scraper.ScrapeOnce(context.Background())

	if len(violations) != 0 {
		t.Errorf("unreachable endpoint should produce no violations, got %v", violations)
	}
}

func TestViolationKind_String(t *testing.T) {
	cases := []struct {
		k    monitor.ViolationKind
		want string
	}{
		{monitor.ViolationBandwidth, "bandwidth"},
		{monitor.ViolationAccuracy, "accuracy"},
		{monitor.ViolationCPU, "cpu"},
	}
	for _, tc := range cases {
		if tc.k.String() != tc.want {
			t.Errorf("ViolationKind(%d).String() = %q, want %q", tc.k, tc.k.String(), tc.want)
		}
	}
}
