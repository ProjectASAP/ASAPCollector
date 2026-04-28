// fake-exporter — metrics producer for the ASAP source-side
// profiling sweeps.
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
// ## Client selection
//
//	EXPORTER_CLIENT            "otel" or "prometheus". Default "otel".
//
// ## OTel three-axis env config
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
// ## Prometheus client profiling config
//
//	EXPORTER_PROM_ADDR         HTTP listen address for /metrics and
//	                           /debug/pprof/* in Prometheus mode.
//	                           Default "0.0.0.0:8000".
//	EXPORTER_PROM_UPDATE_MODE  "cached" or "dynamic". Cached pre-creates
//	                           metric children and updates those handles;
//	                           dynamic calls WithLabelValues on every event.
//	                           Default "cached".
//	EXPORTER_PROM_INSTRUMENTS  Comma-separated subset of
//	                           counter,gauge,histogram. Default
//	                           "counter,gauge".
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

type promInstrumentSet struct {
	counter   bool
	gauge     bool
	histogram bool
}

type promWorkload struct {
	instruments promInstrumentSet
	updateMode  string
	counterVec  *prometheus.CounterVec
	gaugeVec    *prometheus.GaugeVec
	histVec     *prometheus.HistogramVec
	counter     []prometheus.Counter
	gauge       []prometheus.Gauge
	histogram   []prometheus.Observer
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
	client := strings.ToLower(strings.TrimSpace(envOr("EXPORTER_CLIENT", "otel")))

	if client == "prometheus" || client == "prom" {
		if traceFile != "" {
			log.Fatalf("EXPORTER_TRACE_FILE is only supported in EXPORTER_CLIENT=otel mode")
		}
		runPrometheusClient(context.Background(), metricName)
		return
	}
	if client != "otel" {
		log.Printf("warning: unknown EXPORTER_CLIENT=%q - using otel", client)
	}

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

	// Ground-truth tee: every app-level event is mirrored to a
	// hour-bucketed JSONL store under EXPORTER_RAW_TEE_ROOT
	// (P4 of the e2e harness — see raw_tee.go). Disabled when the
	// env is empty; in that case Tee() is a single bool check.
	rt := newRawTee(os.Getenv("EXPORTER_RAW_TEE_ROOT"))
	if rt.enabled {
		log.Printf("raw-tee writing ground truth under %s", rt.root)
		_ = rt.startBackgroundFlush()
		defer rt.Close()
	}

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

	if traceFile != "" {
		runTraceReplay(ctx, meter, metricName, traceFile, rt)
	} else {
		runSynthetic(ctx, meter, metricName, rt)
	}
}

func runPrometheusClient(ctx context.Context, metricName string) {
	cardinality := envInt("EXPORTER_CARDINALITY", 1000)
	freqHz := envFloat("EXPORTER_FREQ_HZ", 10.0)
	zoneVals := envInt("EXPORTER_ZONE_VALS", 4)
	rackVals := envInt("EXPORTER_RACK_VALS", 10)
	nodeVals := envInt("EXPORTER_NODE_VALS", 25)
	podVals := envInt("EXPORTER_POD_VALS", 10)
	addr := envOr("EXPORTER_PROM_ADDR", "0.0.0.0:8000")
	updateMode := strings.ToLower(strings.TrimSpace(envOr("EXPORTER_PROM_UPDATE_MODE", "cached")))
	instruments := parsePromInstruments(envOr("EXPORTER_PROM_INSTRUMENTS", "counter,gauge"))
	if updateMode != "cached" && updateMode != "dynamic" {
		log.Printf("warning: unknown EXPORTER_PROM_UPDATE_MODE=%q - using cached", updateMode)
		updateMode = "cached"
	}

	maxCard := zoneVals * rackVals * nodeVals * podVals
	if cardinality > maxCard {
		log.Printf(
			"warning: EXPORTER_CARDINALITY=%d exceeds schema product %d; "+
				"extra attribute sets alias onto earlier ones",
			cardinality, maxCard,
		)
	}

	reg := prometheus.NewRegistry()
	workload := newPromWorkload(metricName, instruments, updateMode, reg)
	labelValues := buildPromLabelValues(cardinality, zoneVals, rackVals, nodeVals, podVals)
	workload.preload(labelValues)

	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	go func() {
		log.Printf("prometheus client listening on %s (/metrics + /debug/pprof/*)", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("prometheus client server: %v", err)
		}
	}()

	log.Printf(
		"fake-exporter prometheus config: metric=%s cardinality=%d freq_hz=%.1f update_mode=%s instruments=%s schema=%dx%dx%dx%d",
		metricName, cardinality, freqHz, updateMode, envOr("EXPORTER_PROM_INSTRUMENTS", "counter,gauge"),
		zoneVals, rackVals, nodeVals, podVals,
	)

	if freqHz <= 0 {
		log.Printf("EXPORTER_FREQ_HZ=%.1f; preloaded series only, no update goroutines", freqHz)
		select {}
	}

	period := time.Duration(float64(time.Second) / freqHz)
	var wg sync.WaitGroup
	for i := 0; i < cardinality; i++ {
		wg.Add(1)
		go func(seriesIdx int) {
			defer wg.Done()
			time.Sleep(time.Duration(seriesIdx%int(max64(freqHz, 1))) * period /
				time.Duration(max64(freqHz, 1)))

			ticker := time.NewTicker(period)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					workload.observe(seriesIdx, labelValues[seriesIdx])
				}
			}
		}(i)
	}
	wg.Wait()
}

func parsePromInstruments(spec string) promInstrumentSet {
	var out promInstrumentSet
	for _, raw := range strings.Split(spec, ",") {
		switch strings.ToLower(strings.TrimSpace(raw)) {
		case "counter", "counters":
			out.counter = true
		case "gauge", "gauges":
			out.gauge = true
		case "histogram", "histograms":
			out.histogram = true
		case "":
		default:
			log.Printf("warning: unknown EXPORTER_PROM_INSTRUMENTS item %q ignored", raw)
		}
	}
	if !out.counter && !out.gauge && !out.histogram {
		out.counter = true
		out.gauge = true
	}
	return out
}

func newPromWorkload(
	metricName string,
	instruments promInstrumentSet,
	updateMode string,
	reg *prometheus.Registry,
) *promWorkload {
	constLabels := []string{"zone", "rack", "node", "pod"}
	w := &promWorkload{instruments: instruments, updateMode: updateMode}
	if instruments.counter {
		w.counterVec = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: metricName,
			Help: "Synthetic event counter incremented by 1 per event.",
		}, constLabels)
		reg.MustRegister(w.counterVec)
	}
	if instruments.gauge {
		w.gaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: metricName + "_latency_ms",
			Help: "Synthetic log-normal latency sample per event.",
		}, constLabels)
		reg.MustRegister(w.gaugeVec)
	}
	if instruments.histogram {
		w.histVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    metricName + "_latency_ms_histogram",
			Help:    "Synthetic log-normal latency histogram per event.",
			Buckets: prometheus.DefBuckets,
		}, constLabels)
		reg.MustRegister(w.histVec)
	}
	return w
}

func (w *promWorkload) preload(labelValues [][]string) {
	w.counter = make([]prometheus.Counter, len(labelValues))
	w.gauge = make([]prometheus.Gauge, len(labelValues))
	w.histogram = make([]prometheus.Observer, len(labelValues))
	for i, vals := range labelValues {
		if w.counterVec != nil {
			w.counter[i] = w.counterVec.WithLabelValues(vals...)
			w.counter[i].Add(0)
		}
		if w.gaugeVec != nil {
			w.gauge[i] = w.gaugeVec.WithLabelValues(vals...)
			w.gauge[i].Set(0)
		}
		if w.histVec != nil {
			w.histogram[i] = w.histVec.WithLabelValues(vals...)
		}
	}
}

func (w *promWorkload) observe(seriesIdx int, labels []string) {
	latency := math.Exp(3.0 + 0.7*rand.NormFloat64())
	if w.updateMode == "dynamic" {
		if w.counterVec != nil {
			w.counterVec.WithLabelValues(labels...).Inc()
		}
		if w.gaugeVec != nil {
			w.gaugeVec.WithLabelValues(labels...).Set(latency)
		}
		if w.histVec != nil {
			w.histVec.WithLabelValues(labels...).Observe(latency)
		}
		return
	}
	if w.counterVec != nil {
		w.counter[seriesIdx].Inc()
	}
	if w.gaugeVec != nil {
		w.gauge[seriesIdx].Set(latency)
	}
	if w.histVec != nil {
		w.histogram[seriesIdx].Observe(latency)
	}
}

func buildPromLabelValues(cardinality, zoneVals, rackVals, nodeVals, podVals int) [][]string {
	out := make([][]string, cardinality)
	for i := 0; i < cardinality; i++ {
		z := i % zoneVals
		r := (i / zoneVals) % rackVals
		n := (i / (zoneVals * rackVals)) % nodeVals
		p := (i / (zoneVals * rackVals * nodeVals)) % podVals
		out[i] = []string{
			fmt.Sprintf("z%d", z),
			fmt.Sprintf("r%02d", r),
			fmt.Sprintf("n%02d", n),
			fmt.Sprintf("pod-%03d", p),
		}
	}
	return out
}

// runSynthetic drives a synthetic workload at EXPORTER_FREQ_HZ per
// series. One goroutine per attribute set fires `Add(1)` on the
// counter and `Record(log-normal)` on the gauge every `1/freq` wall
// time. The raw event rate on the app side is thus `freq × cardinality
// × 2`; what becomes wire traffic is determined by the SDK View +
// PeriodicReader config set up in main.
func runSynthetic(ctx context.Context, meter metric.Meter, metricName string, tee *rawTee) {
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
					nowMs := time.Now().UnixMilli()
					counter.Add(ctx, 1, attrs)
					latVal := math.Exp(3.0 + 0.7*rand.NormFloat64())
					latencyGauge.Record(ctx, latVal, attrs)
					// Ground-truth mirror: same ts, same attrs, raw
					// values. Two events per tick (counter + gauge),
					// matching the SDK input-side rate.
					if tee.enabled {
						tee.Tee(metricName, nowMs, 1, labelSets[seriesIdx])
						tee.Tee(metricName+"_latency_ms", nowMs, latVal, labelSets[seriesIdx])
					}
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
func runTraceReplay(ctx context.Context, meter metric.Meter, metricName, path string, tee *rawTee) {
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
		replayOnce(ctx, gauge, rows, labelSets, scale, tee, metricName+"_trace")
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
	tee *rawTee,
	teeMetric string,
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
		if tee.enabled {
			// Tee uses wall-clock time, not the trace timestamp,
			// to match the SDK's view (the SDK stamps records at
			// emit time). This means the trace's logical timeline
			// is preserved in the order of writes, but the
			// hour-bucket key reflects when we replayed the row,
			// not when it was originally captured. The ASAP
			// query path consumes wall-clock-stamped data anyway.
			tee.Tee(teeMetric, time.Now().UnixMilli(), r.value, labelSets[r.seriesID])
		}
	}
}
