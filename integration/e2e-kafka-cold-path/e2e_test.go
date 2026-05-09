// Package kafkae2e is the end-to-end integration test for the
// Kafka-bridged cold path (durable_fragment delivery mode):
//
//	test driver  ──OTLP/HTTP──▶  agent-kafka
//	                                  │  gorillas3 (role=agent,
//	                                  │   delivery_mode=durable_fragment,
//	                                  │   drop_original=true)
//	                                  ▼
//	                              kafkaexporter
//	                                  │  topic asap.gorilla.fragments
//	                                  ▼
//	                                kafka
//	                                  │
//	                                  ▼
//	                              kafkareceiver
//	                                  │
//	                                  ▼
//	                              gateway-kafka
//	                                  │  gorillas3 (role=gateway_fragment,
//	                                  │   delivery_mode=durable_fragment)
//	                                  ▼
//	                              MinIO bucket asap-gorilla-tsdb
//	                                  │
//	                                  ▼
//	                              test driver lists + downloads each
//	                              <ulid>/{chunks/000001, index, meta.json}
//	                              opens the block via prometheus/tsdb,
//	                              streams (label, ts, value) tuples,
//	                              asserts byte-equal vs the input set.
//
// # Composition
//
//   - `e2e_test.go`              — this file; orchestrates docker compose
//                                   lifecycle, OTLP push, MinIO list/get,
//                                   TSDB block read-back, and assertion.
//   - `golden/input_samples.json` — 100 deterministic samples, copied
//                                   verbatim from
//                                   integration/gorilla_s3_e2e/golden/
//                                   to keep the byte-format pin
//                                   consistent across the two cold-path
//                                   tests.
//
// # Coordination strategy
//
// Mirrors the gorilla_s3_e2e test's posture:
//
//   - Live test gated on `KAFKA_E2E_LIVE=1`. Defaults skip so a fresh
//     `go test ./...` from a clean checkout does not bring up docker.
//   - Sweep-coordination is intentionally simpler than gorilla_s3_e2e —
//     this test runs on its own compose project (`asap-kafka-cold`)
//     and on its own host port range (34xxx). The MVP demo at 19xxx
//     and the gorilla_s3_e2e overlay at 29xxx do not collide. The
//     test still respects `KAFKA_E2E_FORCE=1` for environments where
//     a known sweep is alive but the operator wants to override.
//
// # Test runtime budget
//
// ~120s end-to-end:
//
//   - 30s for compose up + kafka health,
//   - 5s OTLP push,
//   - 75-90s window flush + Kafka transit + gateway finalize → S3,
//   - 5s S3 list/get + TSDB block parse,
//   - 10s compose down -v.
package kafkae2e

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/prometheus/prometheus/tsdb"
)

// -----------------------------------------------------------------
// Constants — port choices, project name, topic / bucket names.
// -----------------------------------------------------------------

// composeProject pins the compose project so a stale stack from a
// crashed test can be torn down with `docker compose -p
// asap-kafka-cold down -v` even after the test process exited.
const composeProject = "asap-kafka-cold"

// Host-side ports.
//
//	hostPortAgentOTLPGRPC 34317  — agent-kafka's OTLP gRPC port,
//	                                exposed via mvp-kafka-cold-path.yml.
//	hostPortMinIO         19000  — MinIO from base.yml. We list /
//	                                download the TSDB blocks through
//	                                this port (no S3 SDK in this
//	                                module — we shell out to `mc`
//	                                inside a transient container).
//
// 34xxx is chosen to avoid collision with:
//
//	mvp-multi-stage demo (19090/19091),
//	gorilla_s3_e2e overlay (29xxx),
//	base.yml gateway/backend (14317/14318/19091).
const (
	hostPortAgentOTLPGRPC = 34317
	hostPortAgentOTLPHTTP = 34318 // not actually exposed in the overlay
	hostPortMinIO         = 19000
)

// Kafka topic + S3 bucket names. Pinned in three places (agent YAML,
// gateway YAML, this file). Kept in sync via the brief's contract.
const (
	kafkaTopic       = "asap.gorilla.fragments"
	bucketTSDB       = "asap-gorilla-tsdb"
	bucketFragments  = "asap-gorilla-fragments"
	minioAccessKey   = "asap"
	minioSecretKey   = "asap-local-only"
	minioServiceName = "minio" // the docker-internal hostname from base.yml
)

// -----------------------------------------------------------------
// Test entry point.
// -----------------------------------------------------------------

// liveOrSkip gates the live docker path. Mirrors gorilla_s3_e2e's
// `liveOrSkip`.
func liveOrSkip(t *testing.T) bool {
	t.Helper()
	if os.Getenv("KAFKA_E2E_LIVE") != "1" {
		t.Skip("KAFKA_E2E_LIVE != 1; set KAFKA_E2E_LIVE=1 to run the docker path. " +
			"Defaults skip so `go test ./...` from a fresh checkout never tries " +
			"to bring up containers.")
		return false
	}
	return true
}

// TestKafkaColdPathE2E is the orchestrator. By default it SKIPs (CI-
// friendly posture); set `KAFKA_E2E_LIVE=1` and ensure docker + the
// `asap/asap-otel:dev` image are available to actually run.
func TestKafkaColdPathE2E(t *testing.T) {
	if !liveOrSkip(t) {
		return
	}

	repoRoot, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	baseFile := filepath.Join(repoRoot, "deploy", "docker-compose", "base.yml")
	overlayFile := filepath.Join(repoRoot, "deploy", "docker-compose", "mvp-kafka-cold-path.yml")
	for _, p := range []string{baseFile, overlayFile} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("compose file missing at %s: %v", p, err)
		}
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker not on PATH (%v) — Kafka cold-path e2e requires Docker Engine + compose v2", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Logf("bringing up compose project %s with %s + %s", composeProject, baseFile, overlayFile)
	upCmd := exec.CommandContext(ctx, "docker", "compose",
		"-p", composeProject,
		"-f", baseFile,
		"-f", overlayFile,
		"up", "-d")
	upCmd.Stdout = testLogWriter{t}
	upCmd.Stderr = testLogWriter{t}
	if err := upCmd.Run(); err != nil {
		t.Fatalf("docker compose up: %v", err)
	}
	t.Cleanup(func() {
		downCtx, downCancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer downCancel()
		downCmd := exec.CommandContext(downCtx, "docker", "compose",
			"-p", composeProject,
			"-f", baseFile,
			"-f", overlayFile,
			"down", "-v")
		downCmd.Stdout = testLogWriter{t}
		downCmd.Stderr = testLogWriter{t}
		_ = downCmd.Run()
	})

	// Wait for the agent's OTLP gRPC port to come up (proxy for the
	// agent being ready to accept pushes). The gateway's readiness is
	// inferred from the kafkareceiver consuming from the topic; the
	// flush wait below dominates anyway, so we don't poll the
	// gateway separately.
	if err := waitForTCP(ctx, fmt.Sprintf("127.0.0.1:%d", hostPortAgentOTLPGRPC), 90*time.Second); err != nil {
		t.Fatalf("agent-kafka OTLP gRPC port not reachable: %v", err)
	}
	// Also wait for MinIO so the tail-end S3 listing has a live target.
	if err := waitForTCP(ctx, fmt.Sprintf("127.0.0.1:%d", hostPortMinIO), 90*time.Second); err != nil {
		t.Fatalf("MinIO not reachable: %v", err)
	}
	// Wait for the gateway to be healthy by checking the gateway's
	// existence (the compose project's `gateway-kafka` service is the
	// load-bearing service for the cold-path; if it failed to start
	// the test would never see TSDB blocks). `docker compose ps` is
	// the cheapest portable check.
	if err := waitForGatewayKafka(ctx, t, baseFile, overlayFile); err != nil {
		t.Fatalf("gateway-kafka not running: %v", err)
	}

	fixture, err := loadFixture(repoRoot)
	if err != nil {
		t.Fatalf("loadFixture: %v", err)
	}

	// Push 100 samples via OTLP/HTTP to the agent. The overlay only
	// exposes the gRPC port (34317), so we go through the in-cluster
	// HTTP receiver via `docker exec` on the agent — there's no host-
	// mapped HTTP port to keep the host-port footprint minimal.
	now := time.Now().UnixNano()
	if err := pushSamplesOTLPHTTPViaExec(ctx, t, baseFile, overlayFile, fixture, now); err != nil {
		t.Fatalf("pushSamplesOTLPHTTPViaExec: %v", err)
	}

	// Wait for the agent's window flush + Kafka transit + gateway
	// finalize → S3. Window is 60s default; agent flush + Kafka
	// produce + gateway consume + finalize + PUT typically lands
	// under ~75s. Add 15s slack for the e2e overhead.
	flushWait := 90 * time.Second
	if v := os.Getenv("KAFKA_E2E_FLUSH_WAIT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			flushWait = d
		}
	}
	t.Logf("waiting %s for window flush + Kafka transit + gateway finalize", flushWait)
	select {
	case <-ctx.Done():
		t.Fatalf("context expired before window flush: %v", ctx.Err())
	case <-time.After(flushWait):
	}

	// List blocks under bucketTSDB. The gateway writes one block per
	// flush as `<ulid>/{chunks/000001, index, meta.json}`.
	blocks, err := listTSDBBlocks(ctx, t, bucketTSDB)
	if err != nil {
		t.Fatalf("listTSDBBlocks: %v", err)
	}
	if len(blocks) == 0 {
		t.Fatalf("no TSDB blocks found in bucket %s; gateway never finalized", bucketTSDB)
	}
	t.Logf("found %d TSDB block(s): %v", len(blocks), blocks)

	// For each block download every file under <ulid>/ to a temp dir
	// and open the block with prometheus/tsdb. Stream all
	// (label_set, ts, value) tuples and accumulate.
	tmpRoot := t.TempDir()
	var got []readSample
	for _, blockULID := range blocks {
		blockDir := filepath.Join(tmpRoot, blockULID)
		if err := downloadBlock(ctx, t, bucketTSDB, blockULID, blockDir); err != nil {
			t.Fatalf("downloadBlock %s: %v", blockULID, err)
		}
		samples, err := readBlockSamples(ctx, blockDir, fixture.MetricName)
		if err != nil {
			t.Fatalf("readBlockSamples %s: %v", blockULID, err)
		}
		got = append(got, samples...)
	}

	// Assert byte-equality on (label_set, timestamp, value).
	assertSamplesMatch(t, fixture, got, now)
}

// -----------------------------------------------------------------
// Fixture loader (shape mirrors gorilla_s3_e2e's loader).
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
		"integration", "e2e-kafka-cold-path", "golden", "input_samples.json")
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

// repoRoot walks up from the test working directory to the nearest
// ancestor containing PROGRESS.md (the canonical ASAPCollector repo
// marker). Mirrors gorilla_s3_e2e/e2e_test.go's repoRoot.
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

// TestGoldenInputFixtureWellFormed pins the shape of the input
// fixture so a typo surfaces here rather than as an opaque mid-pipeline
// decode error. Always runs (no docker required).
func TestGoldenInputFixtureWellFormed(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("repoRoot: %v", err)
	}
	fx, err := loadFixture(root)
	if err != nil {
		t.Fatalf("loadFixture: %v", err)
	}
	if fx.MetricName == "" {
		t.Errorf("metric_name empty")
	}
	if len(fx.Values) != fx.ExpectedCount {
		t.Errorf("len(values)=%d != expected_count=%d", len(fx.Values), fx.ExpectedCount)
	}
	var sum float64
	for _, v := range fx.Values {
		sum += v
	}
	if sum != fx.ExpectedSum {
		t.Errorf("sum(values)=%v != expected_sum=%v", sum, fx.ExpectedSum)
	}
}

// -----------------------------------------------------------------
// OTLP/HTTP push via `docker exec` on the agent.
// -----------------------------------------------------------------

// pushSamplesOTLPHTTPViaExec serializes the fixture as an OTLP/HTTP
// JSON body and POSTs it to the agent's :4318 in-cluster receiver via
// `docker exec`. We avoid host-mapping the HTTP port to keep the
// overlay's host-port footprint minimal — the only externally
// reachable port is the gRPC :34317 (which we treat as a readiness
// proxy).
func pushSamplesOTLPHTTPViaExec(
	ctx context.Context,
	t *testing.T,
	baseFile, overlayFile string,
	fx inputFixture,
	baseTSNs int64,
) error {
	body, err := buildOTLPMetricsJSON(fx, baseTSNs)
	if err != nil {
		return err
	}

	// Write body to a temp file on the host, then `docker compose cp`
	// it into the agent container's /tmp/. `curl` is generally not in
	// `asap/asap-otel:dev`, so we fall back to a transient
	// `curlimages/curl` sidecar attached to the same docker network.
	tmp := t.TempDir()
	bodyPath := filepath.Join(tmp, "otlp.json")
	if err := os.WriteFile(bodyPath, body, 0o644); err != nil {
		return fmt.Errorf("write otlp body: %w", err)
	}

	// Compose project's default network name follows the pattern
	// `<project>_default`. For `-p asap-kafka-cold` that's
	// `asap-kafka-cold_default`.
	netName := composeProject + "_default"

	curlCmd := exec.CommandContext(ctx, "docker", "run", "--rm",
		"--network", netName,
		"-v", bodyPath+":/tmp/otlp.json:ro",
		"curlimages/curl:latest",
		"-sS", "-X", "POST",
		"-H", "Content-Type: application/json",
		"--data-binary", "@/tmp/otlp.json",
		"http://agent-kafka:4318/v1/metrics")
	out, err := curlCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("curl POST OTLP: %w\nout=%s", err, string(out))
	}
	t.Logf("OTLP push response: %s", strings.TrimSpace(string(out)))
	return nil
}

// buildOTLPMetricsJSON produces an OTLP/HTTP+JSON
// ExportMetricsServiceRequest body for the fixture. Shape mirrors
// gorilla_s3_e2e's pushSamplesOTLPHTTP — single counter named
// `fx.MetricName` with `len(fx.Values)` data points 1s apart starting
// at `baseTSNs`.
func buildOTLPMetricsJSON(fx inputFixture, baseTSNs int64) ([]byte, error) {
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

	// Sort label keys so the JSON body is deterministic across runs.
	labelKeys := make([]string, 0, len(fx.Labels))
	for k := range fx.Labels {
		labelKeys = append(labelKeys, k)
	}
	sort.Strings(labelKeys)
	attrs := make([]otlpKV, 0, len(labelKeys))
	for _, k := range labelKeys {
		attrs = append(attrs, otlpKV{
			Key:   k,
			Value: map[string]interface{}{"stringValue": fx.Labels[k]},
		})
	}

	dps := make([]otlpDataPoint, len(fx.Values))
	for i, v := range fx.Values {
		ts := baseTSNs + int64(i)*fx.IntervalNs
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
	return json.Marshal(req)
}

// -----------------------------------------------------------------
// MinIO list / get via `mc` inside a transient container.
// -----------------------------------------------------------------

// listTSDBBlocks returns the block ULIDs (top-level <ulid>/ prefixes)
// under `bucket`. Implementation: shell out to `mc ls --recursive
// asap/<bucket>` inside a transient `minio/mc:latest` container that
// joins the compose project's docker network so it can reach the
// `minio` service hostname.
func listTSDBBlocks(ctx context.Context, t *testing.T, bucket string) ([]string, error) {
	out, err := mcRun(ctx,
		fmt.Sprintf("mc ls --recursive asap/%s", bucket))
	if err != nil {
		return nil, err
	}
	// mc output line shape (recursive):
	//   [DATE TIME TZ]   SIZE B/KiB/MiB STANDARD <ulid>/<file>
	// We only need the <ulid> directory part of the last column.
	seen := map[string]struct{}{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 1 {
			continue
		}
		// Last whitespace-separated token is the relative path from
		// the bucket root, e.g. `01HK.../meta.json`.
		rel := fields[len(fields)-1]
		// First path segment is the block ULID dir. Skip lines that
		// aren't under a <ulid>/ prefix (e.g. mc summary footer).
		slash := strings.Index(rel, "/")
		if slash <= 0 {
			continue
		}
		ulid := rel[:slash]
		seen[ulid] = struct{}{}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

// downloadBlock fetches every object under `<bucket>/<blockULID>/` and
// writes them to `<destDir>/<relPath>` preserving the relative path
// inside the block (chunks/000001, index, meta.json).
func downloadBlock(ctx context.Context, t *testing.T, bucket, blockULID, destDir string) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	// `mc cp --recursive --quiet asap/<bucket>/<ulid>/ /out/`. The
	// transient mc container mounts `destDir` at `/out`.
	bindSrc := destDir
	mcCmd := mcContainerCmd(ctx, []string{"-v", bindSrc + ":/out"},
		fmt.Sprintf("mc alias set asap http://%s:9000 %s %s >/dev/null && "+
			"mc cp --recursive --quiet asap/%s/%s/ /out/",
			minioServiceName, minioAccessKey, minioSecretKey, bucket, blockULID))
	out, err := mcCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mc cp recursive: %w\nout=%s", err, string(out))
	}
	// Sanity-check meta.json exists; if not, the gateway PUT failed
	// midway and the block is incomplete.
	if _, err := os.Stat(filepath.Join(destDir, "meta.json")); err != nil {
		return fmt.Errorf("downloaded block %s missing meta.json: %w", blockULID, err)
	}
	return nil
}

// mcRun runs an `mc` script inside a transient minio/mc container
// attached to the compose project's default network. Returns the
// container's combined stdout/stderr as a string.
func mcRun(ctx context.Context, script string) (string, error) {
	cmd := mcContainerCmd(ctx, nil, fmt.Sprintf(
		"mc alias set asap http://%s:9000 %s %s >/dev/null && %s",
		minioServiceName, minioAccessKey, minioSecretKey, script))
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("mc script %q: %w\nout=%s", script, err, string(out))
	}
	return string(out), nil
}

// mcContainerCmd builds the `docker run --rm --network ... mc:latest`
// command with optional extra docker args (e.g. `-v`).
func mcContainerCmd(ctx context.Context, extra []string, script string) *exec.Cmd {
	netName := composeProject + "_default"
	args := []string{"run", "--rm", "--network", netName}
	args = append(args, extra...)
	args = append(args,
		"--entrypoint", "sh",
		"minio/mc:latest",
		"-c", script)
	return exec.CommandContext(ctx, "docker", args...)
}

// -----------------------------------------------------------------
// TSDB block reader.
// -----------------------------------------------------------------

// readSample is the (label_set, ts, value) tuple read from a TSDB
// block. `Labels` is the canonical labels-as-string for set-equality
// in the assertion (`MetricsName{k1=\"v1\",k2=\"v2\",...}`).
type readSample struct {
	Labels string
	TS     int64 // ms (Prometheus convention)
	Value  float64
}

// readBlockSamples opens `blockDir` as a Prometheus TSDB block and
// streams every (label, ts, value) tuple where `__name__` matches the
// metric name. Mirrors the reader pattern documented in the brief.
func readBlockSamples(ctx context.Context, blockDir, metricName string) ([]readSample, error) {
	blk, err := tsdb.OpenBlock(nil, blockDir, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("OpenBlock %s: %w", blockDir, err)
	}
	defer blk.Close()

	q, err := tsdb.NewBlockQuerier(blk, blk.MinTime(), blk.MaxTime())
	if err != nil {
		return nil, fmt.Errorf("NewBlockQuerier: %w", err)
	}
	defer q.Close()

	// Match every series whose __name__ is non-empty. We narrow to
	// the specific metric name post-iteration so we surface a clear
	// error if the gateway ever wrote some unexpected series along
	// with the metric we care about.
	matcher := labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".+")
	ss := q.Select(ctx, false, nil, matcher)

	var out []readSample
	for ss.Next() {
		series := ss.At()
		ls := series.Labels()
		// Drop the `__name__` from the comparison label set — both
		// the input fixture (which carries Labels but not metric
		// name) and the expected-set builder strip it for equality.
		// We retain a flag that the iterator saw the right metric.
		//
		// `labels.Labels` is an opaque struct in stringlabels-mode
		// (the default build of prometheus/model/labels), so we
		// iterate via `Range` rather than a slice range loop.
		metricSeen := ls.Get(labels.MetricName)
		if metricSeen != metricName {
			// Not our metric — skip.
			continue
		}
		userLabels := labels.NewBuilder(ls)
		userLabels.Del(labels.MetricName)
		canon := userLabels.Labels().String() // sorted, escaped
		it := series.Iterator(nil)
		for it.Next() != 0 {
			ts, val := it.At()
			out = append(out, readSample{
				Labels: canon,
				TS:     ts,
				Value:  val,
			})
		}
		if err := it.Err(); err != nil {
			return nil, fmt.Errorf("series iterator: %w", err)
		}
	}
	if err := ss.Err(); err != nil {
		return nil, fmt.Errorf("series-set: %w", err)
	}
	return out, nil
}

// -----------------------------------------------------------------
// Assertion helpers.
// -----------------------------------------------------------------

// assertSamplesMatch verifies every (label_set, ts, value) emitted by
// the input fixture appears in the read-back set, byte-equal.
//
// Comparison strategy:
//
//   - Build the EXPECTED set from the fixture: all (canon_labels,
//     baseTSNs/1e6 + i*intervalMs, values[i]) tuples.
//   - Build the GOT multi-set from `got`.
//   - Assert that the GOT set is a SUPERSET of the EXPECTED set on
//     (canon_labels, ts_ms, value). The gateway may emit additional
//     series (e.g. self-monitoring counters) that we don't care
//     about; the brief's assertion is byte-equality on the input
//     samples specifically.
//
// Timestamps: Prometheus TSDB stores ms since epoch. The input
// fixture's baseTSNs is ns; convert to ms via integer division.
// This matches how the gorillas3 processor lowers OTLP nanosecond
// timestamps into TSDB block samples (see processor's BlockWriter
// path which calls ts/1e6 on commit).
func assertSamplesMatch(t *testing.T, fx inputFixture, got []readSample, baseTSNs int64) {
	t.Helper()

	// Canonical labels for the fixture: sorted "{k1=\"v1\",...}"
	// matching prometheus/labels.Labels.String() format.
	canon := canonLabels(fx.Labels)

	// Build expected: for each i, (canon, baseMs + i*intervalMs, values[i]).
	intervalMs := fx.IntervalNs / int64(time.Millisecond)
	if intervalMs <= 0 {
		intervalMs = 1
	}
	baseMs := baseTSNs / int64(time.Millisecond)
	type key struct {
		ls    string
		ts    int64
		value float64
	}
	expected := make(map[key]struct{}, len(fx.Values))
	for i, v := range fx.Values {
		expected[key{ls: canon, ts: baseMs + int64(i)*intervalMs, value: v}] = struct{}{}
	}

	gotSet := make(map[key]struct{}, len(got))
	for _, s := range got {
		gotSet[key{ls: s.Labels, ts: s.TS, value: s.Value}] = struct{}{}
	}

	// Diff.
	var missing []key
	for k := range expected {
		if _, ok := gotSet[k]; !ok {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		// Limit the print to the first 5 to keep test output sane.
		head := missing
		if len(head) > 5 {
			head = head[:5]
		}
		t.Fatalf("missing %d expected sample(s) (showing first %d): %+v\n"+
			"got %d sample(s) total in TSDB block(s)",
			len(missing), len(head), head, len(got))
	}
	t.Logf("all %d input samples round-tripped through Kafka cold-path "+
		"and were read back from TSDB block byte-equal on (label_set, ts, value)",
		len(expected))
}

// canonLabels formats a label map the same way prometheus/labels
// formats it: sorted by key, each value double-quoted.
func canonLabels(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b := labels.NewBuilder(labels.EmptyLabels())
	for _, k := range keys {
		b.Set(k, m[k])
	}
	return b.Labels().String()
}

// -----------------------------------------------------------------
// Misc helpers — TCP wait, gateway readiness, stream logging.
// -----------------------------------------------------------------

// waitForTCP polls a host:port until it accepts a TCP connection or
// the deadline expires. Mirrors the gorilla_s3_e2e helper.
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

// waitForGatewayKafka polls `docker compose ps -q gateway-kafka`
// until the service has a non-empty container ID. Used as a
// readiness proxy for the gateway service — the kafkareceiver
// itself doesn't expose a TCP listener that's useful to dial.
func waitForGatewayKafka(ctx context.Context, t *testing.T, baseFile, overlayFile string) error {
	deadline := time.Now().Add(120 * time.Second)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cmd := exec.CommandContext(ctx, "docker", "compose",
			"-p", composeProject,
			"-f", baseFile,
			"-f", overlayFile,
			"ps", "-q", "gateway-kafka")
		out, err := cmd.Output()
		if err == nil && strings.TrimSpace(string(out)) != "" {
			return nil
		}
		if time.Now().After(deadline) {
			if err == nil {
				err = errors.New("gateway-kafka has no container id")
			}
			return fmt.Errorf("waitForGatewayKafka: %w", err)
		}
		time.Sleep(2 * time.Second)
	}
}

// testLogWriter forwards subprocess writes to t.Logf so docker /
// compose stdout/stderr lands under the right subtest in -v output.
type testLogWriter struct{ t *testing.T }

func (w testLogWriter) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		w.t.Log(line)
	}
	return len(p), nil
}

// Compile-time guards — keep the linter happy by referring to
// optional helpers that older Go versions may shake away.
var _ = bytes.NewReader
var _ = http.MethodGet
