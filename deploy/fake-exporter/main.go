// fake-exporter — OTLP metrics producer. Two modes:
//
//   1. Synthetic (default) — log-normal gauge + exponential counter.
//      Stands in for a real workload when you're iterating on the
//      stack; reproducible by setting the RNG seed.
//
//   2. Trace replay — reads a CSV of recorded (ts_ms, series_id,
//      value) rows and emits at the recorded pace. The paper's §6.1
//      workload credibility datapoint. Files in the Google 2019
//      cluster trace format can be preprocessed into the CSV schema
//      below; see `deploy/fake-exporter/traces/README.md`.
//
// Emitted metric families (both modes):
//   * <metric>_latency_ms : Gauge. Sketch baselines (DDSketch/HLL)
//     only process Gauge inputs; Counter/Sum payloads would be a
//     sketch-pipeline no-op.
//   * <metric> : Counter. Kept for the raw-baseline path so existing
//     ingest-rate dashboards still see a monotonic time series.
//
// Env config:
//
//   EXPORTER_TARGET          OTLP/gRPC endpoint (default gateway:4317)
//   EXPORTER_METRIC          base metric name (default http_requests_total)
//   EXPORTER_RATE            synthetic mode: emits/sec (default 10)
//   EXPORTER_CARDINALITY     synthetic mode: distinct label sets (default 100)
//   EXPORTER_TRACE_FILE      if set, switches to trace replay of this CSV
//   EXPORTER_TRACE_SCALE     trace mode: playback speed multiplier (default 1.0)
//   EXPORTER_TRACE_LOOP      trace mode: wrap around at EOF (default true)
//
// Trace CSV schema (header required):
//   timestamp_ms,series_id,value

package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	switch v {
	case "":
		return def
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

// traceRow is one row of the replay CSV — a single gauge observation
// for a single series at a recorded timestamp.
type traceRow struct {
	tsMs     int64
	seriesID string
	value    float64
}

// loadTraceCSV reads the replay CSV into memory, sorts by timestamp,
// and returns the full row list plus the unique series list. Memory
// footprint is ~40 bytes per row — a 1M-row trace fits in 40 MB.
func loadTraceCSV(path string) ([]traceRow, []string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = 3
	rows, err := r.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("read csv: %w", err)
	}
	if len(rows) < 2 {
		return nil, nil, fmt.Errorf("trace csv has no data rows")
	}
	if rows[0][0] != "timestamp_ms" || rows[0][1] != "series_id" || rows[0][2] != "value" {
		return nil, nil, fmt.Errorf(
			"trace csv header must be `timestamp_ms,series_id,value`, got %q",
			rows[0],
		)
	}

	out := make([]traceRow, 0, len(rows)-1)
	seriesSet := make(map[string]struct{})
	for i, row := range rows[1:] {
		ts, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("row %d: bad timestamp_ms %q: %w", i+2, row[0], err)
		}
		val, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return nil, nil, fmt.Errorf("row %d: bad value %q: %w", i+2, row[2], err)
		}
		out = append(out, traceRow{tsMs: ts, seriesID: row[1], value: val})
		seriesSet[row[1]] = struct{}{}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].tsMs < out[j].tsMs })

	seriesList := make([]string, 0, len(seriesSet))
	for s := range seriesSet {
		seriesList = append(seriesList, s)
	}
	sort.Strings(seriesList)
	return out, seriesList, nil
}

func main() {
	target := envOr("EXPORTER_TARGET", "gateway:4317")
	metricName := envOr("EXPORTER_METRIC", "http_requests_total")
	traceFile := os.Getenv("EXPORTER_TRACE_FILE")

	ctx := context.Background()
	exp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(target),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithCompressor("gzip"),
	)
	if err != nil {
		log.Fatalf("otlp exporter init: %v", err)
	}

	res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("fake-exporter"),
	))

	// Batch emissions at 1s ticks regardless of mode. Synthetic loop
	// emits every (1/rate)s; trace loop emits at recorded pace —
	// using a shared reader interval keeps downstream batching
	// predictable.
	reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(time.Second))
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	defer provider.Shutdown(ctx)
	otel.SetMeterProvider(provider)

	meter := provider.Meter("asap.fake-exporter")

	if traceFile != "" {
		runTraceReplay(ctx, meter, metricName, traceFile)
	} else {
		runSynthetic(ctx, meter, metricName)
	}

	// Silence unused-import when the SDK types are only referenced
	// indirectly via the meter provider.
	var _ metricdata.ResourceMetrics
}

// runSynthetic emits log-normal gauge + exponential counter samples
// at the configured rate × cardinality. Kept as the default so
// compose-up without an explicit EXPORTER_TRACE_FILE still produces
// the shape the existing baseline sweep expects.
func runSynthetic(ctx context.Context, meter metric.Meter, metricName string) {
	rate := envInt("EXPORTER_RATE", 10)
	cardinality := envInt("EXPORTER_CARDINALITY", 100)

	log.Printf("fake-exporter starting (synthetic): metric=%s rate=%d/sec cardinality=%d",
		metricName, rate, cardinality)

	counter, err := meter.Float64Counter(metricName,
		metric.WithDescription("Synthetic counter — raw-baseline signal"))
	if err != nil {
		log.Fatalf("counter init: %v", err)
	}
	latencyGauge, err := meter.Float64Gauge(metricName+"_latency_ms",
		metric.WithDescription("Synthetic log-normal latency — sketch-baseline signal"),
		metric.WithUnit("ms"))
	if err != nil {
		log.Fatalf("gauge init: %v", err)
	}

	labelSets := make([][]attribute.KeyValue, cardinality)
	for i := 0; i < cardinality; i++ {
		labelSets[i] = []attribute.KeyValue{
			attribute.String("zone", fmt.Sprintf("z%d", i%4)),
			attribute.String("pod", fmt.Sprintf("pod-%d", i)),
		}
	}

	values := make([]float64, cardinality)
	var mu sync.Mutex
	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()
	for range tick.C {
		mu.Lock()
		for i := 0; i < cardinality; i++ {
			values[i] += rand.ExpFloat64()
			counter.Add(ctx, values[i], metric.WithAttributes(labelSets[i]...))
			latencyMs := math.Exp(3.0 + 0.7*rand.NormFloat64())
			latencyGauge.Record(ctx, latencyMs, metric.WithAttributes(labelSets[i]...))
		}
		mu.Unlock()
	}
}

// runTraceReplay reads a CSV trace and emits its rows as gauges at
// the recorded pace. Each unique series_id in the CSV becomes a
// label set `{series_id=…}`; the gauge metric name is the
// configured base name + `_trace`. When EOF is reached the player
// wraps back to the first row (so a 10-minute trace loops
// indefinitely during long soaks).
func runTraceReplay(ctx context.Context, meter metric.Meter, metricName, path string) {
	scale := envFloat("EXPORTER_TRACE_SCALE", 1.0)
	loop := envBool("EXPORTER_TRACE_LOOP", true)

	rows, series, err := loadTraceCSV(path)
	if err != nil {
		log.Fatalf("trace load: %v", err)
	}
	log.Printf("fake-exporter starting (trace replay): metric=%s_trace rows=%d series=%d scale=%.2fx loop=%v",
		metricName, len(rows), len(series), scale, loop)

	gauge, err := meter.Float64Gauge(metricName+"_trace",
		metric.WithDescription("Trace replay gauge — cpu_usage or similar"))
	if err != nil {
		log.Fatalf("gauge init: %v", err)
	}

	// Pre-compute label sets per series so the hot loop is
	// allocation-free.
	labelSets := make(map[string][]attribute.KeyValue, len(series))
	for _, s := range series {
		labelSets[s] = []attribute.KeyValue{attribute.String("series_id", s)}
	}

	for {
		replayOnce(ctx, gauge, rows, labelSets, scale)
		if !loop {
			return
		}
		log.Printf("trace replay wrap — restarting from row 0")
	}
}

// replayOnce plays the rows list once at recorded pace scaled by
// `scale` (1.0 = real-time, 2.0 = 2× faster, 0.5 = half-speed).
// The first row anchors wallclock; subsequent rows sleep to align
// with `(row.tsMs - first.tsMs) / scale` wallclock offset.
func replayOnce(
	ctx context.Context,
	gauge metric.Float64Gauge,
	rows []traceRow,
	labelSets map[string][]attribute.KeyValue,
	scale float64,
) {
	if len(rows) == 0 {
		return
	}
	walkStart := time.Now()
	traceStart := rows[0].tsMs
	for _, r := range rows {
		targetOffsetMs := float64(r.tsMs-traceStart) / scale
		target := walkStart.Add(time.Duration(targetOffsetMs) * time.Millisecond)
		if sleep := time.Until(target); sleep > 0 {
			time.Sleep(sleep)
		}
		gauge.Record(ctx, r.value, metric.WithAttributes(labelSets[r.seriesID]...))
	}
}
