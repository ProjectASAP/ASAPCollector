// Package warmpathe2e is the warm-path end-to-end integration test
// for the ASAP warm-tier sketch pipeline:
//
//	fake-driver  ─OTLP─▶  asap-otel + ddsketchprocessor (window 10s)
//	                          │  emits typed DDSketchDataPoint
//	                          ▼
//	                       data plane (asap/data-plane:dev)
//	                          │  warm-tier OTLP receiver +
//	                          │  DDSketchAccumulator
//	                          ▼
//	                       PromQL HTTP /api/v1/query
//
// The test asserts:
//
//   1. The PromQL HTTP response is shape-valid (status 200, "data" +
//      "resultType" populated).
//   2. The response carries `data_source: sketch_warm_tier` (or the
//      backend's current warm-tier marker — see assertion comment).
//   3. The returned p99 estimate is within DDSketch's `ε=0.01` of the
//      offline-computed exact p99 from the 100-sample fixture.
//
// `TestWarmPathE2E` is the orchestrator. It brings up a minimal
// docker stack (asap-otel agent + backend) on a separate compose
// project (`asap-warm-e2e`) and tears it down at the end. The test
// is gated on `WARM_E2E_LIVE=1`; default `go test ./...` skips so
// CI without docker stays green.
package warmpathe2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// -----------------------------------------------------------------
// Sweep-coordination + setup helpers.
// -----------------------------------------------------------------

const defaultSweepPID = 2862786

// composeProject is the compose project name for the ephemeral
// e2e stack. Pinned so a local `docker compose -p asap-warm-e2e
// down -v` will tear it down even if the test panicked.
const composeProject = "asap-warm-e2e"

// Host-side ports. Match docker-compose/e2e-overlay.yml.
const (
	hostPortAgentOTLP    = 24317
	hostPortBackendQuery = 29191
)

// liveOrSkip is the gate every "needs-docker" subtest goes through.
func liveOrSkip(t *testing.T) bool {
	t.Helper()

	if v := os.Getenv("WARM_E2E_LIVE"); v != "1" {
		t.Skipf("WARM_E2E_LIVE != 1; set WARM_E2E_LIVE=1 to run the live " +
			"docker-compose path. Defaults skip so `go test ./...` from a fresh " +
			"checkout never tries to bring up containers.")
		return false
	}
	if force := os.Getenv("WARM_E2E_FORCE"); force != "1" {
		if running, pid := isSweepRunning(); running {
			t.Skipf("sweep process PID=%d is alive; the live e2e path competes "+
				"with it for docker / CPU / RAM. Run after the sweep finishes, "+
				"or override with WARM_E2E_FORCE=1.", pid)
			return false
		}
	}
	return true
}

func isSweepRunning() (bool, int) {
	pidStr := os.Getenv("WARM_E2E_SWEEP_PID")
	if pidStr == "" {
		pidStr = strconv.Itoa(defaultSweepPID)
	}
	if pidStr == "0" || pidStr == "none" {
		return false, 0
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil || pid <= 0 {
		return false, 0
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); err == nil {
		return true, pid
	}
	return false, pid
}

// -----------------------------------------------------------------
// Top-level orchestrator.
// -----------------------------------------------------------------

// TestWarmPathE2E runs the live warm-path pipeline.
func TestWarmPathE2E(t *testing.T) {
	if !liveOrSkip(t) {
		return
	}

	repoRoot, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	composeFile := filepath.Join(repoRoot,
		"integration", "e2e-warm-path", "docker-compose", "e2e-overlay.yml")
	if _, err := os.Stat(composeFile); err != nil {
		t.Fatalf("compose overlay missing at %s: %v", composeFile, err)
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker not on PATH (%v) — live e2e requires Docker Engine + compose v2", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Logf("bringing up compose project %s with %s", composeProject, composeFile)
	upCmd := exec.CommandContext(ctx, "docker", "compose",
		"-p", composeProject, "-f", composeFile, "up", "-d")
	upCmd.Stdout = testLogWriter{t}
	upCmd.Stderr = testLogWriter{t}
	if err := upCmd.Run(); err != nil {
		t.Fatalf("docker compose up: %v", err)
	}
	t.Cleanup(func() {
		downCtx, downCancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer downCancel()
		downCmd := exec.CommandContext(downCtx, "docker", "compose",
			"-p", composeProject, "-f", composeFile, "down", "-v")
		downCmd.Stdout = testLogWriter{t}
		downCmd.Stderr = testLogWriter{t}
		_ = downCmd.Run()
	})

	if err := waitForTCP(ctx, fmt.Sprintf("127.0.0.1:%d", hostPortAgentOTLP), 90*time.Second); err != nil {
		t.Fatalf("asap-otel-warm OTLP gRPC port not reachable: %v", err)
	}
	if err := waitForTCP(ctx, fmt.Sprintf("127.0.0.1:%d", hostPortBackendQuery), 90*time.Second); err != nil {
		t.Fatalf("backend-warm query port not reachable: %v", err)
	}

	fixture, err := loadFixture(repoRoot)
	if err != nil {
		t.Fatalf("loadFixture: %v", err)
	}

	now := time.Now().UnixNano()
	if err := pushSamplesOTLPHTTP(ctx, hostPortAgentOTLP+1 /* :4318 → host 24318 */, fixture, now); err != nil {
		t.Fatalf("pushSamplesOTLPHTTP: %v", err)
	}

	// ddsketchprocessor flushes once per `window_duration` (10s).
	// Wait one window + a small slack for the OTLP forward + backend
	// ingest to land.
	t.Logf("waiting 15s for the ddsketch window flush + backend ingest")
	select {
	case <-ctx.Done():
		t.Fatalf("context expired before window flush: %v", ctx.Err())
	case <-time.After(15 * time.Second):
	}

	// Issue the p99 PromQL query against the warm-tier surface.
	resp, err := promQLQuantileQuery(ctx, fixture.MetricName, 0.99)
	if err != nil {
		t.Fatalf("promQLQuantileQuery: %v", err)
	}

	// 1) Shape-valid response.
	if status, _ := resp["status"].(string); status != "success" {
		t.Fatalf("PromQL response status != success: %v", resp)
	}
	data, ok := resp["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("PromQL response missing data: %v", resp)
	}
	if rt, _ := data["resultType"].(string); rt == "" {
		t.Fatalf("PromQL response missing resultType: %v", resp)
	}

	// 2) data_source marker. The backend stamps this either at the
	// top level or under data["data_source"] depending on response
	// shape evolution; accept either.
	dataSource := extractDataSource(resp)
	if dataSource == "" {
		t.Errorf("PromQL response is missing the data_source marker; "+
			"expected `sketch_warm_tier` (or whatever the backend currently "+
			"stamps for warm-tier responses). Full response: %v", resp)
	} else if dataSource != "sketch_warm_tier" {
		// Tolerate near-name variants (e.g. backend evolution to
		// `sketch_warm_tier_ddsketch`) but log them so the canonical
		// name surfaces in the test output for the next regen.
		if !strings.Contains(dataSource, "warm") {
			t.Errorf("data_source=%q; expected something matching `sketch_warm_tier`", dataSource)
		} else {
			t.Logf("data_source=%q (warm-tier match; canonical name has drifted from `sketch_warm_tier`)", dataSource)
		}
	} else {
		t.Logf("data_source=%q (canonical warm-tier marker)", dataSource)
	}

	// 3) p99 estimate within ε=0.01 of the exact value.
	p99, err := extractScalarOrFirstVector(data)
	if err != nil {
		t.Fatalf("extract p99 from response: %v (full response: %v)", err, resp)
	}
	exact := fixture.ExpectedP99
	eps := fixture.DDSketchRelativeAccuracy
	if eps <= 0 {
		eps = 0.01
	}
	// DDSketch relative-accuracy: |estimate - exact| / exact <= eps.
	relErr := math.Abs(p99-exact) / exact
	if relErr > eps {
		t.Fatalf("p99 estimate %v exceeds DDSketch ε=%v bound around exact=%v "+
			"(relative error %v)", p99, eps, exact, relErr)
	}
	t.Logf("WarmPathE2E: p99 estimate=%v exact=%v relErr=%v (within ε=%v)",
		p99, exact, relErr, eps)
}

// -----------------------------------------------------------------
// Sibling fixture-only test — always runs, no docker required.
// -----------------------------------------------------------------

// TestGoldenInputFixtureWellFormed pins the shape of the input
// fixture so a typo in `golden/input_samples.json` surfaces here
// rather than mid-pipeline as an opaque decode error.
func TestGoldenInputFixtureWellFormed(t *testing.T) {
	repoRoot, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	fx, err := loadFixture(repoRoot)
	if err != nil {
		t.Fatalf("loadFixture: %v", err)
	}
	if fx.MetricName == "" {
		t.Errorf("metric_name empty")
	}
	if len(fx.Values) != fx.ExpectedCount {
		t.Errorf("len(values)=%d ≠ expected_count=%d", len(fx.Values), fx.ExpectedCount)
	}
	if fx.ExpectedP99 <= 0 {
		t.Errorf("expected_p99 must be > 0")
	}
	if fx.DDSketchRelativeAccuracy <= 0 || fx.DDSketchRelativeAccuracy >= 1 {
		t.Errorf("ddsketch_relative_accuracy out of (0,1): %v", fx.DDSketchRelativeAccuracy)
	}
}

// -----------------------------------------------------------------
// Fixture loader + input model.
// -----------------------------------------------------------------

type inputFixture struct {
	MetricName               string            `json:"metric_name"`
	Labels                   map[string]string `json:"labels"`
	IntervalNs               int64             `json:"interval_ns"`
	Values                   []float64         `json:"values"`
	ExpectedCount            int               `json:"expected_count"`
	ExpectedP99              float64           `json:"expected_p99"`
	DDSketchRelativeAccuracy float64           `json:"ddsketch_relative_accuracy"`
}

func loadFixture(repoRoot string) (inputFixture, error) {
	path := filepath.Join(repoRoot,
		"integration", "e2e-warm-path", "golden", "input_samples.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return inputFixture{}, fmt.Errorf("read fixture %s: %w", path, err)
	}
	var fx inputFixture
	if err := json.Unmarshal(b, &fx); err != nil {
		return inputFixture{}, fmt.Errorf("parse fixture %s: %w", path, err)
	}
	return fx, nil
}

func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	cur := wd
	for {
		if _, err := os.Stat(filepath.Join(cur, "PROGRESS.md")); err == nil {
			return cur, nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return "", fmt.Errorf("could not find PROGRESS.md from %s", wd)
		}
		cur = parent
	}
}

// -----------------------------------------------------------------
// OTLP/HTTP push (no client lib).
// -----------------------------------------------------------------

// pushSamplesOTLPHTTP serializes the fixture as an OTLP/HTTP
// `ExportMetricsServiceRequest` JSON body and POSTs it to the
// agent's :4318/v1/metrics endpoint. Uses Gauge data points (the
// DDSketch processor's window mode reads any Gauge / Sum series).
func pushSamplesOTLPHTTP(ctx context.Context, hostPort int, fx inputFixture, baseTSNs int64) error {
	type otlpKV struct {
		Key   string                 `json:"key"`
		Value map[string]interface{} `json:"value"`
	}
	type otlpDataPoint struct {
		Attributes   []otlpKV `json:"attributes,omitempty"`
		StartTimeNs  string   `json:"startTimeUnixNano"`
		TimeUnixNano string   `json:"timeUnixNano"`
		AsDouble     float64  `json:"asDouble"`
	}
	type otlpGauge struct {
		DataPoints []otlpDataPoint `json:"dataPoints"`
	}
	type otlpMetric struct {
		Name  string    `json:"name"`
		Gauge otlpGauge `json:"gauge"`
	}
	type otlpScopeMetrics struct {
		Metrics []otlpMetric `json:"metrics"`
	}
	type otlpResourceMetrics struct {
		ScopeMetrics []otlpScopeMetrics `json:"scopeMetrics"`
	}
	type otlpReq struct {
		ResourceMetrics []otlpResourceMetrics `json:"resourceMetrics"`
	}

	dps := make([]otlpDataPoint, len(fx.Values))
	for i, v := range fx.Values {
		ts := baseTSNs + int64(i)*fx.IntervalNs
		attrs := make([]otlpKV, 0, len(fx.Labels))
		for k, val := range fx.Labels {
			attrs = append(attrs, otlpKV{
				Key:   k,
				Value: map[string]interface{}{"stringValue": val},
			})
		}
		dps[i] = otlpDataPoint{
			Attributes:   attrs,
			StartTimeNs:  strconv.FormatInt(baseTSNs, 10),
			TimeUnixNano: strconv.FormatInt(ts, 10),
			AsDouble:     v,
		}
	}
	req := otlpReq{
		ResourceMetrics: []otlpResourceMetrics{{
			ScopeMetrics: []otlpScopeMetrics{{
				Metrics: []otlpMetric{{
					Name:  fx.MetricName,
					Gauge: otlpGauge{DataPoints: dps},
				}},
			}},
		}},
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal OTLP req: %w", err)
	}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/v1/metrics", hostPort)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build POST: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("POST OTLP: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("OTLP HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// -----------------------------------------------------------------
// Backend PromQL probe.
// -----------------------------------------------------------------

// promQLQuantileQuery hits the backend's /api/v1/query endpoint with
// `quantile_over_time(<q>, <metric>[30s])` and returns the parsed
// JSON response.
func promQLQuantileQuery(ctx context.Context, metric string, q float64) (map[string]interface{}, error) {
	expr := fmt.Sprintf("quantile_over_time(%v, %s[30s])", q, metric)
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/api/v1/query?query=%s",
		hostPortBackendQuery, url.QueryEscape(expr))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("backend HTTP %d: %s", resp.StatusCode, string(body))
	}
	var out map[string]interface{}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("parse PromQL response: %w; body=%s", err, string(body))
	}
	return out, nil
}

// extractDataSource pulls the data_source marker out of the PromQL
// response. Different backend versions stamp it at slightly different
// paths — accept the common ones.
func extractDataSource(resp map[string]interface{}) string {
	if s, ok := resp["data_source"].(string); ok {
		return s
	}
	if data, ok := resp["data"].(map[string]interface{}); ok {
		if s, ok := data["data_source"].(string); ok {
			return s
		}
	}
	// Some shapes carry it under data.infos[] as a "data_source: X"
	// line; scan that too.
	if data, ok := resp["data"].(map[string]interface{}); ok {
		if infos, ok := data["infos"].([]interface{}); ok {
			for _, i := range infos {
				if s, ok := i.(string); ok && strings.HasPrefix(s, "data_source:") {
					return strings.TrimSpace(strings.TrimPrefix(s, "data_source:"))
				}
			}
		}
	}
	return ""
}

// extractScalarOrFirstVector returns the numeric value from a PromQL
// response. Handles both `resultType=scalar` (data.result is
// [ts, "value"]) and `resultType=vector` (data.result is array of
// {metric, value: [ts, "value"]}). Returns the first vector entry if
// multiple series are present.
func extractScalarOrFirstVector(data map[string]interface{}) (float64, error) {
	rt, _ := data["resultType"].(string)
	switch rt {
	case "scalar":
		arr, ok := data["result"].([]interface{})
		if !ok || len(arr) < 2 {
			return 0, fmt.Errorf("scalar result not [ts, value]: %v", data["result"])
		}
		s, _ := arr[1].(string)
		return strconv.ParseFloat(s, 64)
	case "vector":
		arr, ok := data["result"].([]interface{})
		if !ok || len(arr) == 0 {
			return 0, fmt.Errorf("vector result empty: %v", data["result"])
		}
		first, ok := arr[0].(map[string]interface{})
		if !ok {
			return 0, fmt.Errorf("vector entry not an object: %v", arr[0])
		}
		val, ok := first["value"].([]interface{})
		if !ok || len(val) < 2 {
			return 0, fmt.Errorf("vector entry value not [ts, value]: %v", first)
		}
		s, _ := val[1].(string)
		return strconv.ParseFloat(s, 64)
	default:
		return 0, fmt.Errorf("unsupported resultType=%q", rt)
	}
}

// -----------------------------------------------------------------
// Misc helpers.
// -----------------------------------------------------------------

// waitForTCP polls a host:port until it accepts a TCP connection or
// the deadline expires.
func waitForTCP(ctx context.Context, addr string, max time.Duration) error {
	deadline := time.Now().Add(max)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err == nil {
			_ = c.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("waitForTCP %s: deadline exceeded: %w", addr, err)
		}
		time.Sleep(1 * time.Second)
	}
}

// testLogWriter forwards writes to t.Logf so subprocess stdout/stderr
// shows up under the right subtest in `go test -v` output.
type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		w.t.Log(line)
	}
	return len(p), nil
}
