// fake-exporter — OTLP metrics producer for the ASAP three-axis
// SDK aggregation sweep (see docs/sdk-cost-evaluation.md).
//
// Two operating modes:
//
//  1. Synthetic (default) — log-normal gauge + event counter driven
//     by per-series goroutines firing at EXPORTER_FREQ_HZ. The app
//     layer produces raw events at `freq × cardinality × #instruments`
//     samples/sec; what makes it to the wire is entirely determined
//     by the SDK config below.
//
//  2. Trace replay — reads a CSV of recorded `(ts_ms, series_id,
//     value)` rows and emits at the recorded pace. The 
//     workload-credibility hook.
//
// Emitted metric families (both modes):
//
//   - <metric>            : Counter — incremented by 1 on each event.
//     Used by Sum / CountSketch / CountMinSketch / HLL aggregations.
//   - <metric>_latency_ms : Gauge — log-normal latency sample per event.
//     Used by DDSketch / KLLSketch / Histogram aggregations.
//
// The SDK config is identical for both instruments (a single View
// covers both), so one agg_type choice cleanly sweeps both signals.
//
// ## Three-axis env config
//
//	EXPORTER_SDK_WINDOW        PeriodicReader interval. 
//	                           Duration string. Default "15s".
//	EXPORTER_SDK_PROJECTION    Comma-separated attribute keys to keep
//	                           inside the SDK aggregator. Everything
//	                           not listed is dropped via View's
//	                           AttributeFilter. 
//	                              ""        keep all labels (orig card)
//	                              "zone"    keep only zone (reduces card)
//	                              "zone,rack,node,pod"  keep all four
//	                              "-"       drop all (single series)
//	                           Default "" (keep all).
//	EXPORTER_SDK_AGG           Aggregator kind. Encoding axis.
//	                              default | sum | raw-buffer |
//	                              dd-full | dd-delta |
//	                              kll |
//	                              cms-full | cms-delta |
//	                              cs-full  | cs-delta  |
//	                              hll-full | hll-delta
//	                           Default "default" (Sum for Counter,
//	                           LastValue for Gauge).
//
// ## Workload config (orthogonal to the three SDK axes)
//
//	EXPORTER_TARGET                OTLP/gRPC endpoint (default gateway:4317).
//	EXPORTER_METRIC                base metric name (default http_requests_total).
//	EXPORTER_CARDINALITY           synthetic: # distinct attribute sets (default 1000).
//	                               Max is 4 × 10 × 25 × 10 = 10000 under the
//	                               default schema; to go higher, widen the
//	                               per-dim value counts below.
//	EXPORTER_ZONE_VALS             # distinct zone values      (default 4).
//	EXPORTER_RACK_VALS             # distinct rack values      (default 10).
//	EXPORTER_NODE_VALS             # distinct node values      (default 25).
//	EXPORTER_POD_VALS              # distinct pod values       (default 10).
//	EXPORTER_FREQ_HZ               synthetic: per-series event rate (default 10 Hz).
//	                               Aggregate raw rate = FREQ × CARDINALITY × 2.
//	EXPORTER_MAX_BUFFER_PER_SERIES raw-buffer: per-series event buffer cap
//	                               (default 10000).
//
// ## Trace replay config
//
//	EXPORTER_TRACE_FILE      if set, switches to trace replay of this CSV.
//	EXPORTER_TRACE_SCALE     playback speed multiplier (default 1.0).
//	EXPORTER_TRACE_LOOP      wrap around at EOF (default true).
//
// Trace CSV schema (header required):
//
//	timestamp_ms,series_id,value
//
// ## Deprecated (ignored with warning)
//
//	EXPORTER_RATE      replaced by EXPORTER_FREQ_HZ. The old meaning
//	                   was "ticker at 1s/rate", which was
//	                   semantically a no-op given SDK aggregation.

package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	_ "net/http/pprof" // expose /debug/pprof/* on EXPORTER_PPROF_ADDR
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
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

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("warning: %s=%q is not a valid duration, using default %s", key, v, def)
	}
	return def
}

// parseAgg maps EXPORTER_SDK_AGG to a concrete sdkmetric.Aggregation.
// Unrecognised values fall back to AggregationDefault with a warning
// so experiments don't silently run the wrong shape.
func parseAgg(name string, maxBufferPerSeries int) sdkmetric.Aggregation {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "default":
		return sdkmetric.AggregationDefault{}
	case "sum":
		return sdkmetric.AggregationSum{}
	case "raw-buffer":
		return sdkmetric.AggregationRawBuffer{MaxEventsPerSeries: maxBufferPerSeries}
	case "dd-full", "ddsketch", "ddsketch-full":
		return sdkmetric.AggregationDDSketch{}
	case "dd-delta", "ddsketch-delta":
		return sdkmetric.AggregationDDSketch{DeltaTransmission: true}
	case "kll", "kll-full":
		return sdkmetric.AggregationKLLSketch{}
	case "cms-full", "count-min", "count-min-full":
		return sdkmetric.AggregationCountMinSketch{}
	case "cms-delta", "count-min-delta":
		return sdkmetric.AggregationCountMinSketch{DeltaTransmission: true}
	case "cs-full", "countsketch", "count-sketch", "count-sketch-full":
		return sdkmetric.AggregationCountSketch{}
	case "cs-delta", "count-sketch-delta":
		return sdkmetric.AggregationCountSketch{DeltaTransmission: true}
	case "hll-full", "hyperloglog", "hyperloglog-full":
		return sdkmetric.AggregationHLLSketch{}
	case "hll-delta", "hyperloglog-delta":
		return sdkmetric.AggregationHLLSketch{DeltaTransmission: true}
	default:
		log.Printf("warning: unknown EXPORTER_SDK_AGG=%q — falling back to default", name)
		return sdkmetric.AggregationDefault{}
	}
}

// parseProjection converts a comma-separated key list to an
// attribute.Filter. The special token "-" means "drop everything" so
// the aggregator bucket degenerates to a single series. Empty string
// (the default) keeps every attribute — no filter registered.
func parseProjection(spec string) attribute.Filter {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil
	}
	if spec == "-" {
		return func(attribute.KeyValue) bool { return false }
	}
	keep := make(map[string]struct{})
	for _, k := range strings.Split(spec, ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			keep[k] = struct{}{}
		}
	}
	return func(kv attribute.KeyValue) bool {
		_, ok := keep[string(kv.Key)]
		return ok
	}
}

// buildLabelSets generates the first `cardinality` distinct attribute
// sets under a 4-dim schema (`zone × rack × node × pod`). If
// cardinality exceeds the schema's product, extra entries alias onto
// earlier ones by modular wraparound (so callers always get exactly
// `cardinality` slices; use EXPORTER_*_VALS to widen the schema for
// larger experiments).
func buildLabelSets(cardinality, zoneVals, rackVals, nodeVals, podVals int) [][]attribute.KeyValue {
	out := make([][]attribute.KeyValue, cardinality)
	for i := 0; i < cardinality; i++ {
		z := i % zoneVals
		r := (i / zoneVals) % rackVals
		n := (i / (zoneVals * rackVals)) % nodeVals
		p := (i / (zoneVals * rackVals * nodeVals)) % podVals
		out[i] = []attribute.KeyValue{
			attribute.String("zone", fmt.Sprintf("z%d", z)),
			attribute.String("rack", fmt.Sprintf("r%02d", r)),
			attribute.String("node", fmt.Sprintf("n%02d", n)),
			attribute.String("pod", fmt.Sprintf("pod-%03d", p)),
		}
	}
	return out
}

// traceRow is one row of the replay CSV — a single gauge observation
// for a single series at a recorded timestamp.
type traceRow struct {
	tsMs     int64
	seriesID string
	value    float64
}

// loadTraceCSV reads the replay CSV into memory, sorts by timestamp,
// and returns the full row list plus the unique series list.
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

	// Optional pprof endpoint for producer-side profiling.
	// When EXPORTER_PPROF_ADDR is set (e.g. "0.0.0.0:6060"), serves
	// /debug/pprof/{profile,heap,goroutine,...} so external tools
	// can sample CPU / heap / goroutines while the producer runs.
	// Off by default.
	if addr := os.Getenv("EXPORTER_PPROF_ADDR"); addr != "" {
		go func() {
			log.Printf("pprof listening on %s", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				log.Printf("pprof server: %v", err)
			}
		}()
	}

	// Three-axis SDK config.
	window := envDuration("EXPORTER_SDK_WINDOW", 15*time.Second)
	projection := parseProjection(os.Getenv("EXPORTER_SDK_PROJECTION"))
	aggName := envOr("EXPORTER_SDK_AGG", "default")
	maxBufPerSeries := envInt("EXPORTER_MAX_BUFFER_PER_SERIES", 0)
	agg := parseAgg(aggName, maxBufPerSeries)

	// Deprecated env — warn loudly so stale compose files surface.
	if v := os.Getenv("EXPORTER_RATE"); v != "" {
		log.Printf(
			"warning: EXPORTER_RATE=%q is deprecated and ignored. "+
				"The old ticker-driven meaning was a no-op under SDK "+
				"aggregation. Use EXPORTER_FREQ_HZ for the app-level event "+
				"frequency and EXPORTER_SDK_WINDOW for the SDK emit interval.",
			v,
		)
	}

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

	reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(window))

	// One View covers every instrument this exporter emits (match-all
	// matches any instrument name). The stream config is what the
	// three-axis sweep actually varies — aggregation + attribute
	// filter.
	//
	// AttributeFilter is wrapped in a swappableFilter so the
	// controller can change the label projection L mid-run via
	// `POST /control/projection` (P2 of the e2e harness — see
	// swappable_filter.go). When EXPORTER_CONTROL_ADDR is unset the
	// HTTP control endpoint is not started, but the wrapper still
	// works as a static filter, so this is always the right wiring.
	swappable := newSwappableFilter(projection)
	stream := sdkmetric.Stream{Aggregation: agg, AttributeFilter: swappable.Filter()}
	view := sdkmetric.NewView(
		sdkmetric.Instrument{Name: "*"},
		stream,
	)
	if addr := os.Getenv("EXPORTER_CONTROL_ADDR"); addr != "" {
		log.Printf("control plane listening on %s (POST /control/projection)", addr)
		_ = installControlServer(addr, swappable)
	}

	// Step-1 of the JSONL deprecation removed the
	// `raw_tee.go` ground-truth JSONL writer + the
	// `EXPORTER_RAW_TEE_ROOT` env var. The §5.2
	// LocalFsColdStore that read from `raw/<metric>/...` was
	// deleted in the backend at the same commit; the surviving
	// archive ground-truth path is the agent's `gorillas3processor`
	// emitting Gorilla blocks to S3.

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(view),
	)
	defer provider.Shutdown(ctx)
	otel.SetMeterProvider(provider)

	meter := provider.Meter("asap.fake-exporter")

	log.Printf(
		"fake-exporter sdk config: window=%s agg=%s projection=%q",
		window, aggName, os.Getenv("EXPORTER_SDK_PROJECTION"),
	)

	// MVP v6 freshness probes — three timestamp-encoded counters that
	// run alongside the primary workload (synthetic or trace replay).
	// Disabled with EXPORTER_FRESHNESS_PROBES=off. See probes.go for
	// the protocol + the spec at
	// docs/spec-mvp-v6-controller-driven-multi-stage-demo.md §⑥.
	stopProbes := startFreshnessProbes(ctx, meter)
	defer stopProbes()

	if traceFile != "" {
		runTraceReplay(ctx, meter, metricName, traceFile)
	} else {
		runSynthetic(ctx, meter, metricName)
	}
}

// runSynthetic drives a synthetic workload at EXPORTER_FREQ_HZ per
// series. One goroutine per attribute set fires `Add(1)` on the
// counter and `Record(log-normal)` on the gauge every `1/freq` wall
// time. The raw event rate on the app side is thus `freq × cardinality
// × 2`; what becomes wire traffic is determined by the SDK View +
// PeriodicReader config set up in main.
func runSynthetic(ctx context.Context, meter metric.Meter, metricName string) {
	cardinality := envInt("EXPORTER_CARDINALITY", 1000)
	freqHz := envFloat("EXPORTER_FREQ_HZ", 10.0)
	zoneVals := envInt("EXPORTER_ZONE_VALS", 4)
	rackVals := envInt("EXPORTER_RACK_VALS", 10)
	nodeVals := envInt("EXPORTER_NODE_VALS", 25)
	podVals := envInt("EXPORTER_POD_VALS", 10)
	maxCard := zoneVals * rackVals * nodeVals * podVals
	if cardinality > maxCard {
		log.Printf(
			"warning: EXPORTER_CARDINALITY=%d exceeds schema product %d; "+
				"extra attribute sets alias onto earlier ones",
			cardinality, maxCard,
		)
	}

	log.Printf(
		"fake-exporter starting (synthetic): metric=%s cardinality=%d freq_hz=%.1f schema=%dx%dx%dx%d",
		metricName, cardinality, freqHz, zoneVals, rackVals, nodeVals, podVals,
	)

	counter, err := meter.Float64Counter(metricName,
		metric.WithDescription("Synthetic event counter — incremented by 1 per event"))
	if err != nil {
		log.Fatalf("counter init: %v", err)
	}
	latencyGauge, err := meter.Float64Gauge(metricName+"_latency_ms",
		metric.WithDescription("Synthetic log-normal latency sample per event"),
		metric.WithUnit("ms"))
	if err != nil {
		log.Fatalf("gauge init: %v", err)
	}

	labelSets := buildLabelSets(cardinality, zoneVals, rackVals, nodeVals, podVals)
	period := time.Duration(float64(time.Second) / freqHz)

	// Per-series goroutines mean each attribute set ticks on its own
	// cadence — if we ever want to stagger frequencies per series
	// (to model heterogeneous workloads) this is the natural hook.
	var wg sync.WaitGroup
	for i := 0; i < cardinality; i++ {
		wg.Add(1)
		go func(seriesIdx int) {
			defer wg.Done()
			// Stagger start so all series don't fire simultaneously
			// at tick zero — deterministic offset keyed on series.
			time.Sleep(time.Duration(seriesIdx%int(max64(freqHz, 1))) * period /
				time.Duration(max64(freqHz, 1)))

			ticker := time.NewTicker(period)
			defer ticker.Stop()
			attrs := metric.WithAttributes(labelSets[seriesIdx]...)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					counter.Add(ctx, 1, attrs)
					latVal := math.Exp(3.0 + 0.7*rand.NormFloat64())
					latencyGauge.Record(ctx, latVal, attrs)
				}
			}
		}(i)
	}
	wg.Wait()
}

func max64(a float64, b int) int {
	if int(a) < b {
		return b
	}
	return int(a)
}

// runTraceReplay reads a CSV trace and emits its rows as gauges at
// the recorded pace. Each unique series_id becomes label
// `{series_id=…}`; the SDK config set up in main (window /
// projection / agg) applies uniformly.
func runTraceReplay(ctx context.Context, meter metric.Meter, metricName, path string) {
	scale := envFloat("EXPORTER_TRACE_SCALE", 1.0)
	loop := envBool("EXPORTER_TRACE_LOOP", true)

	rows, series, err := loadTraceCSV(path)
	if err != nil {
		log.Fatalf("trace load: %v", err)
	}
	log.Printf(
		"fake-exporter starting (trace replay): metric=%s_trace rows=%d series=%d scale=%.2fx loop=%v",
		metricName, len(rows), len(series), scale, loop,
	)

	gauge, err := meter.Float64Gauge(metricName+"_trace",
		metric.WithDescription("Trace replay gauge"))
	if err != nil {
		log.Fatalf("gauge init: %v", err)
	}

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
