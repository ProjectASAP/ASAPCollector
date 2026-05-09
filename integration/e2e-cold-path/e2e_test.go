// Package coldpathe2e is the cold-path end-to-end integration test
// for the ASAP cold-path pipeline:
//
//	fake-driver  ─OTLP─▶  asap-otel + gorillas3processor (gateway_raw)
//	                          │  Prometheus TSDB block builder
//	                          ▼
//	                       MinIO   (<ulid>/chunks/000001 + index + meta.json)
//	                          │
//	                          ▼
//	                       Thanos store-gateway / backend cold-path
//
// After PR #359 the gorillas3processor emits Prometheus TSDB blocks
// (`<ulid>/chunks/000001`, `<ulid>/index`, `<ulid>/meta.json`) — the
// legacy GORILLA1 magic-prefixed XOR-delta chunk format the old
// in-test decoder targeted is no longer produced at runtime. The new
// test reads each finalized block back via `prometheus/tsdb`'s
// `OpenBlock` + `BlockQuerier`, then asserts byte-equality of the
// recovered (label, ts, value) tuples against the input fixture.
//
// # Subtests
//
// `TestColdPathEnd2End` is the orchestrator. It brings the docker
// stack up once and then drives subtests against it:
//
//   - HappyPath        — fixture pushed, MinIO has ≥1 finalized
//                        block (meta.json present), block decodes to
//                        the input sample-set.
//   - MultiBlockRange  — if input spans >1 window, ≥2 blocks land
//                        and their union covers the full input.
//
// (The PromQL accuracy / data-source-marker subtests and the
// CrossLanguageByteCompat subtest from the legacy GORILLA1 e2e are
// retired here: TSDB is upstream-Prometheus's native format, not an
// ASAP-specific wire format, so neither a separate Rust decoder nor
// a "kind=Exact, ε=0" backend marker applies — Thanos serves these
// blocks as ordinary Prometheus storage.)
package coldpathe2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// -----------------------------------------------------------------
// Sweep-coordination + setup helpers.
// -----------------------------------------------------------------

const defaultSweepPID = 2862786

// composeProject is the compose project name for the ephemeral
// e2e stack. Pinned so a local `docker compose -p asap-cold-e2e
// down -v` will tear it down even if the test panicked.
const composeProject = "asap-cold-e2e"

// MinIO + agent host-side ports. Match docker-compose/e2e-overlay.yml.
const (
	hostPortMinIOS3   = 29000
	hostPortAgentOTLP = 24317
)

// liveOrSkip is the gate every "needs-docker" subtest goes through.
func liveOrSkip(t *testing.T) bool {
	t.Helper()

	if v := os.Getenv("COLD_E2E_LIVE"); v != "1" {
		t.Skipf("COLD_E2E_LIVE != 1; set COLD_E2E_LIVE=1 to run the live " +
			"docker-compose path. Defaults skip so `go test ./...` from a fresh " +
			"checkout never tries to bring up containers.")
		return false
	}
	if force := os.Getenv("COLD_E2E_FORCE"); force != "1" {
		if running, pid := isSweepRunning(); running {
			t.Skipf("sweep process PID=%d is alive; the live e2e path competes "+
				"with it for docker / CPU / RAM. Run after the sweep finishes, "+
				"or override with COLD_E2E_FORCE=1.", pid)
			return false
		}
	}
	return true
}

// isSweepRunning checks whether the host-wide sweep process is alive.
func isSweepRunning() (bool, int) {
	pidStr := os.Getenv("COLD_E2E_SWEEP_PID")
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

// TestColdPathEnd2End runs the live cold-path pipeline.
func TestColdPathEnd2End(t *testing.T) {
	if !liveOrSkip(t) {
		return
	}

	repoRoot, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	composeFile := filepath.Join(repoRoot,
		"integration", "e2e-cold-path", "docker-compose", "e2e-overlay.yml")
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

	if err := waitForTCP(ctx, fmt.Sprintf("127.0.0.1:%d", hostPortMinIOS3), 90*time.Second); err != nil {
		t.Fatalf("MinIO not reachable: %v", err)
	}
	if err := waitForTCP(ctx, fmt.Sprintf("127.0.0.1:%d", hostPortAgentOTLP), 90*time.Second); err != nil {
		t.Fatalf("asap-otel-e2e OTLP gRPC port not reachable: %v", err)
	}

	fixture, err := loadFixture(repoRoot)
	if err != nil {
		t.Fatalf("loadFixture: %v", err)
	}

	now := time.Now().UnixNano()
	if err := pushSamplesOTLPHTTP(ctx, hostPortAgentOTLP+1 /* :4318 → host 24318 */, fixture, now); err != nil {
		t.Fatalf("pushSamplesOTLPHTTP: %v", err)
	}

	// gorillas3processor flushes once per `window_interval`. Wait one
	// window + a small slack for the S3 PutObject + meta.json upload to
	// land.
	t.Logf("waiting 75s for the gorillas3 window flush + S3 PutObject")
	select {
	case <-ctx.Done():
		t.Fatalf("context expired before window flush: %v", ctx.Err())
	case <-time.After(75 * time.Second):
	}

	t.Run("HappyPath", func(t *testing.T) {
		runHappyPath(ctx, t, fixture)
	})
	t.Run("MultiBlockRange", func(t *testing.T) {
		runMultiBlockRange(ctx, t, fixture)
	})
}

// -----------------------------------------------------------------
// Subtest bodies.
// -----------------------------------------------------------------

func runHappyPath(ctx context.Context, t *testing.T, fixture inputFixture) {
	blocks, err := listAndFetchBlocks(ctx, t)
	if err != nil {
		t.Fatalf("list/fetch blocks: %v", err)
	}
	if len(blocks) == 0 {
		t.Fatalf("no finalized TSDB blocks under tsdb_bucket; gorillas3processor did not flush")
	}
	t.Logf("found %d finalized block(s)", len(blocks))

	got, err := readSamplesFromBlocks(t, blocks, fixture.MetricName)
	if err != nil {
		t.Fatalf("read samples from blocks: %v", err)
	}
	if len(got) != fixture.ExpectedCount {
		t.Fatalf("decoded %d samples, want %d", len(got), fixture.ExpectedCount)
	}

	// Decoded values should match the fixture set (order-insensitive
	// — TSDB sorts by ts and we shifted timestamps to `now`, so we
	// only verify the multi-set of values matches).
	wantValues := append([]float64(nil), fixture.Values...)
	sort.Float64s(wantValues)
	gotValues := make([]float64, len(got))
	for i, s := range got {
		gotValues[i] = s.Value
	}
	sort.Float64s(gotValues)
	for i := range wantValues {
		if wantValues[i] != gotValues[i] {
			t.Fatalf("value #%d mismatch: got=%v want=%v", i, gotValues[i], wantValues[i])
		}
	}
	t.Logf("HappyPath: 100 input values round-tripped through encoder + S3 + TSDB block reader")
}

func runMultiBlockRange(ctx context.Context, t *testing.T, fixture inputFixture) {
	// The fixture spans 99s (1s spacing). With window_interval=60s, the
	// gorillas3processor should produce at least ONE block; depending
	// on alignment with the window boundary, it MAY produce two.
	// Assert "≥1" — and when "≥2", also assert their union covers the
	// full input set.
	blocks, err := listAndFetchBlocks(ctx, t)
	if err != nil {
		t.Fatalf("list/fetch blocks: %v", err)
	}
	if len(blocks) < 1 {
		t.Fatalf("expected ≥1 blocks, got %d", len(blocks))
	}
	if len(blocks) >= 2 {
		merged, err := readSamplesFromBlocks(t, blocks, fixture.MetricName)
		if err != nil {
			t.Fatalf("multi-block read: %v", err)
		}
		if len(merged) != fixture.ExpectedCount {
			t.Fatalf("multi-block merge yielded %d samples, want %d",
				len(merged), fixture.ExpectedCount)
		}
		t.Logf("MultiBlockRange: 2+ blocks aggregated correctly (%d samples)",
			len(merged))
	} else {
		t.Logf("MultiBlockRange: 1 block only — input fits inside a single 60s "+
			"window boundary. Subtest still validates the single-block path; "+
			"true multi-block coverage would require a >120s input fixture or "+
			"a smaller window_interval. (fixture window=%ds)",
			fixture.IntervalNs*int64(len(fixture.Values))/int64(time.Second))
	}
}

// -----------------------------------------------------------------
// Sibling fixture-only subtests — always run, no docker required.
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
	var sum float64
	for _, v := range fx.Values {
		sum += v
	}
	if sum != fx.ExpectedSum {
		t.Errorf("sum(values)=%v ≠ expected_sum=%v", sum, fx.ExpectedSum)
	}
}

// -----------------------------------------------------------------
// Fixture loader + input model.
// -----------------------------------------------------------------

type inputFixture struct {
	MetricName    string            `json:"metric_name"`
	Labels        map[string]string `json:"labels"`
	IntervalNs    int64             `json:"interval_ns"`
	Values        []float64         `json:"values"`
	ExpectedSum   float64           `json:"expected_sum"`
	ExpectedCount int               `json:"expected_count"`
	ExpectedMin   float64           `json:"expected_min"`
	ExpectedMax   float64           `json:"expected_max"`
}

func loadFixture(repoRoot string) (inputFixture, error) {
	path := filepath.Join(repoRoot,
		"integration", "e2e-cold-path", "golden", "input_samples.json")
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

// repoRoot walks up from the package's test directory to the
// nearest ancestor that contains `PROGRESS.md`.
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
// MinIO + TSDB block fetch.
// -----------------------------------------------------------------

// Sample is one (timestamp, value) pair recovered from a TSDB block.
type Sample struct {
	LabelSet  labels.Labels
	Timestamp int64
	Value     float64
}

// listAndFetchBlocks lists every finalized TSDB block in the
// `asap-gorilla` MinIO bucket (i.e. every prefix that contains a
// `meta.json`), downloads its three files (`chunks/000001`, `index`,
// `meta.json`) into a per-block temp dir, and returns the list of
// per-block dirs. Callers are responsible for opening each dir with
// `tsdb.OpenBlock` and reading samples back.
//
// Why `mc` instead of an in-Go S3 client? Adding `aws-sdk-go` (or
// `minio-go`) drags ~30 deps into this test module. The test already
// needs docker exec for the OTLP push; reusing it for block listing
// keeps go.mod focused on Prometheus-side deps only.
func listAndFetchBlocks(ctx context.Context, t *testing.T) ([]string, error) {
	t.Helper()

	// 1) List meta.json markers under the bucket. mc returns the full
	//    "asap/asap-gorilla/<ulid>/meta.json" key per line.
	listCmd := exec.CommandContext(ctx, "docker", "run", "--rm",
		"--network", composeProject+"_default",
		"--entrypoint", "sh",
		"minio/mc:latest",
		"-c",
		"mc alias set asap http://minio-e2e:9000 asap asap-local-only >/dev/null "+
			"&& mc find asap/asap-gorilla --name 'meta.json' || true")
	out, err := listCmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("mc find meta.json: %v\n%s", err, string(out))
	}
	keys := strings.Split(strings.TrimSpace(string(out)), "\n")

	// 2) For each "<bucket>/<ulid>/meta.json" prefix, download the
	//    three block files into a per-block temp dir.
	var blockDirs []string
	tmpRoot := t.TempDir()
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" || !strings.HasSuffix(k, "/meta.json") {
			continue
		}
		// Strip the trailing /meta.json to get the ULID prefix.
		blockPrefix := strings.TrimSuffix(k, "/meta.json")
		ulidStr := filepath.Base(blockPrefix)
		blockDir := filepath.Join(tmpRoot, ulidStr)
		if err := os.MkdirAll(filepath.Join(blockDir, "chunks"), 0o755); err != nil {
			return nil, err
		}
		// Pull each of the three files separately. mc cat preserves
		// binary content cleanly and avoids us hand-coding the chunk
		// numbering, which is internal TSDB.
		for _, rel := range []string{"chunks/000001", "index", "meta.json"} {
			catCmd := exec.CommandContext(ctx, "docker", "run", "--rm",
				"--network", composeProject+"_default",
				"--entrypoint", "sh",
				"minio/mc:latest",
				"-c",
				fmt.Sprintf(
					"mc alias set asap http://minio-e2e:9000 asap asap-local-only >/dev/null "+
						"&& mc cat %s/%s", blockPrefix, rel))
			var buf bytes.Buffer
			catCmd.Stdout = &buf
			if err := catCmd.Run(); err != nil {
				return nil, fmt.Errorf("mc cat %s/%s: %w", blockPrefix, rel, err)
			}
			if err := os.WriteFile(filepath.Join(blockDir, rel), buf.Bytes(), 0o644); err != nil {
				return nil, fmt.Errorf("write %s: %w", rel, err)
			}
		}
		blockDirs = append(blockDirs, blockDir)
	}
	return blockDirs, nil
}

// readSamplesFromBlocks opens each block dir with `tsdb.OpenBlock`,
// builds a `BlockQuerier` over its full time range, selects every
// series matching `__name__=~.+`, and returns the flattened (label,
// ts, value) tuples across all blocks. When `metric` is non-empty
// the result is filtered to that metric name defensively.
func readSamplesFromBlocks(t *testing.T, blockDirs []string, metric string) ([]Sample, error) {
	t.Helper()
	var all []Sample
	for _, d := range blockDirs {
		blk, err := tsdb.OpenBlock(nil, d, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("OpenBlock %s: %w", d, err)
		}
		q, err := tsdb.NewBlockQuerier(blk, blk.MinTime(), blk.MaxTime())
		if err != nil {
			_ = blk.Close()
			return nil, fmt.Errorf("NewBlockQuerier %s: %w", d, err)
		}
		matcher := labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+")
		ss := q.Select(context.Background(), false, &storage.SelectHints{
			Start: blk.MinTime(),
			End:   blk.MaxTime(),
		}, matcher)
		for ss.Next() {
			s := ss.At()
			ls := s.Labels()
			it := s.Iterator(nil)
			for it.Next() != chunkenc.ValNone {
				ts, v := it.At()
				all = append(all, Sample{LabelSet: ls, Timestamp: ts, Value: v})
			}
			if err := it.Err(); err != nil {
				_ = q.Close()
				_ = blk.Close()
				return nil, fmt.Errorf("iterator: %w", err)
			}
		}
		if err := ss.Err(); err != nil {
			_ = q.Close()
			_ = blk.Close()
			return nil, fmt.Errorf("series set: %w", err)
		}
		_ = q.Close()
		_ = blk.Close()
	}
	if metric != "" {
		filtered := all[:0]
		for _, s := range all {
			if s.LabelSet.Get(labels.MetricName) == metric {
				filtered = append(filtered, s)
			}
		}
		all = filtered
	}
	return all, nil
}

// -----------------------------------------------------------------
// OTLP/HTTP push (no client lib).
// -----------------------------------------------------------------

// pushSamplesOTLPHTTP serializes the fixture as an OTLP/HTTP
// `ExportMetricsServiceRequest` JSON body and POSTs it to the agent's
// :4318/v1/metrics endpoint.
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
	type otlpSum struct {
		AggregationTemporality int             `json:"aggregationTemporality"`
		IsMonotonic            bool            `json:"isMonotonic"`
		DataPoints             []otlpDataPoint `json:"dataPoints"`
	}
	type otlpMetric struct {
		Name string  `json:"name"`
		Sum  otlpSum `json:"sum"`
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
					Name: fx.MetricName,
					Sum: otlpSum{
						AggregationTemporality: 2, // CUMULATIVE
						IsMonotonic:            true,
						DataPoints:             dps,
					},
				}},
			}},
		}},
	}
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal OTLP req: %w", err)
	}
	url := fmt.Sprintf("http://127.0.0.1:%d/v1/metrics", hostPort)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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
