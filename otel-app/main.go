// otel-app — single-binary synthetic OTLP metrics producer.
//
// This binary is the consolidation of two former tools:
//
//   - the fakemetricload Zipf load generator and SDK pre-aggregation
//     sketch-emission value model.
//   - the OTLP producer with trace replay, freshness probes, a runtime
//     control channel, and the five-sketch workload.
//
// Value model:
//
//   - Synthetic latency values are drawn from a Zipf distribution
//     (zipf-s / zipf-v / zipf-max / zipf-mean), the credibility model
//     inherited from fakemetricload.
//   - The SDK pre-aggregates per series per export window. The chosen
//     aggregation (-agg / -sketch-type) is attached to the instrument
//     via a View, so the collector receives one compact sketch (or raw)
//     data point per series per window — the patched
//     AggregationDDSketch / KLLSketch / CountSketch / CountMinSketch /
//     HLLSketch / RawBuffer types live in the local opentelemetry-go
//     tree.
//
// Two operating modes:
//
//  1. Synthetic (default) — a Zipf-latency gauge + an event counter
//     driven by per-series goroutines firing at freq-hz, plus the
//     five-sketch MVP workload and the three freshness probes.
//
//  2. Trace replay (-trace-file) — reads a CSV of recorded
//     (timestamp_ms, series_id, value) rows and emits at the recorded
//     pace. The workload-credibility hook.
//
// Configuration: command-line flags + an optional YAML config file
// (-config). There are NO environment variables. Precedence is:
//
//	built-in defaults  <  YAML (-config <file>)  <  explicitly-set flags
//
// The override is implemented by parsing the YAML into the same Config
// struct that flag defaults populate, then re-applying only those
// fields whose flag was explicitly set on the command line (detected
// via flag.Visit). See loadConfig.

package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof" // expose /debug/pprof/* on -pprof-addr
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
	"go.yaml.in/yaml/v2"
)

// Config is the full producer configuration. Every field maps 1:1 to a
// command-line flag (registered in registerFlags) and a YAML key
// (the `yaml:"…"` tag). See the README for the configuration reference.
//
// Precedence: built-in defaults < YAML (-config) < explicitly-set flags.
type Config struct {
	// --- workload / exporter ---
	Target     string `yaml:"target"`      // OTLP/gRPC endpoint
	Metric     string `yaml:"metric"`      // base metric name
	PprofAddr  string `yaml:"pprof_addr"`  // pprof listen addr; "" = off
	ProducerID string `yaml:"producer_id"` // producer_id label; "" = none
	Seed       int64  `yaml:"seed"`        // per-series PRNG seed; 0 = off

	// --- three-axis SDK config ---
	SDKWindow          time.Duration `yaml:"sdk_window"`             // PeriodicReader interval
	SDKProjection      string        `yaml:"sdk_projection"`         // attribute keep-list
	SDKAgg             string        `yaml:"sdk_agg"`                // aggregation kind
	MaxBufferPerSeries int           `yaml:"max_buffer_per_series"` // raw-buffer cap

	// SketchType is the fakemetricload-style aggregation selector
	// (ddsketch|kll|countsketch|countminsketch|hll|baseline). It is a
	// convenience alias that maps onto an -agg value when -agg is left at
	// its default. -agg always wins when set explicitly.
	SketchType string `yaml:"sketch_type"`

	// --- synthetic workload ---
	Cardinality int     `yaml:"cardinality"` // # distinct attribute sets
	FreqHz      float64 `yaml:"freq_hz"`     // per-series event rate
	ZoneVals    int     `yaml:"zone_vals"`   //
	RackVals    int     `yaml:"rack_vals"`   //
	NodeVals    int     `yaml:"node_vals"`   //
	PodVals     int     `yaml:"pod_vals"`    //

	// --- Zipf value model (latency value distribution) ---
	ZipfS    float64 `yaml:"zipf_s"`    // shape, must be > 1
	ZipfV    float64 `yaml:"zipf_v"`    // shift, must be >= 1
	ZipfMax  uint64  `yaml:"zipf_max"`  // integer range upper bound
	ZipfMean float64 `yaml:"zipf_mean"` // target mean for value scaling

	// --- trace replay ---
	TraceFile  string  `yaml:"trace_file"`  // CSV path; non-empty = replay mode
	TraceScale float64 `yaml:"trace_scale"` // playback speed multiplier
	TraceLoop  bool    `yaml:"trace_loop"`  // wrap at EOF

	// --- freshness probes ---
	FreshnessProbes  bool    `yaml:"freshness_probes"`   // enable the three probes
	FreshnessProbeHz float64 `yaml:"freshness_probe_hz"` // tick rate Hz

	// --- five-sketch workload ---
	FiveSketchEnabled   bool          `yaml:"five_sketch_enabled"`    // emit the four extra sketch metrics (default true)
	FiveSketchUserPool  int           `yaml:"five_sketch_user_pool"`  // HLL user-id cardinality
	FiveSketchEndpoints int           `yaml:"five_sketch_endpoints"`  // Zipfian endpoint cardinality
	FiveSketchZipfS     float64       `yaml:"five_sketch_zipf_s"`     // Zipfian s
	FiveSketchUserRot   time.Duration `yaml:"five_sketch_user_rotate"` // user-window rotate cadence

	// --- control channel ---
	ControlAddr string `yaml:"control_addr"` // /control/projection listen addr; "" = off

	// --- run duration (fakemetricload heritage; 0 = run forever) ---
	Duration time.Duration `yaml:"duration"`

	// --- producer-side ("SDK") warm-part sampling ---
	// WarmSampleP is the admitted fraction p of the warm-sketch metric
	// (<metric>_latency_ms, a DDSketch). Each latency datapoint is
	// independently kept with probability p before it enters the SDK's
	// aggregation; the (1-p) dropped points are never sketched, exported,
	// or sent over the network. The Sum counter is left whole.
	// p=1.0 (default) = no sampling. DDSketch quantiles are
	// rank-preserving, so thinning preserves quantile shape with no 1/p
	// rescale needed for quantile accuracy.
	//
	// In trace-replay mode (-trace-file) the same admit-with-probability-p
	// gate is applied to the replayed warm gauge before it enters the SDK
	// aggregation, so the Google-cluster accuracy sweep exercises warm
	// sampling on the real replayed metric (not just the synthetic one).
	WarmSampleP float64 `yaml:"warm_sample_p"`

	// TraceMetricName, when non-empty, overrides the replay gauge's metric
	// name. By default the replay path emits `<metric>_trace`; setting this
	// (e.g. to `google_cluster_2019_cpu_rate`) lets the replayed series land
	// under the exact name a DDSketch streaming-config aggregation + the
	// query suite reference, so the warm DDSketch path engages on the trace.
	TraceMetricName string `yaml:"trace_metric_name"`

	// --- CDM coordinated sampling (Priority 2) ---
	// CoordinatorURL, when non-empty, makes the producer a CDM edge: it dials
	// the coordinator's MonitorService, reports its observed warm-metric rate
	// per window, and applies the coordinator-granted SampleP as the live
	// warm-sample-p at the NEXT window boundary (never mid-window). The static
	// -warm-sample-p is the bootstrap value used until the first grant arrives.
	CoordinatorURL string `yaml:"coordinator_url"`
	// MonitorAggID is the content-addressed agg_id this producer reports under;
	// it MUST match the `monitors:` entry agg_id in the coordinator's
	// streaming-config so the grant is routed back to this edge's metric.
	MonitorAggID uint64 `yaml:"monitor_agg_id"`
	// EdgeID is this producer's stable identity to the coordinator (the
	// coordinator coordinates p across distinct edge ids). Defaults to
	// ProducerID when empty.
	EdgeID string `yaml:"edge_id"`
	// MonitorKey, when non-empty, makes this a `cms_point` (heavy-hitter) edge:
	// only events whose series_id equals MonitorKey count toward the reported
	// per-window VALUE (f_i = the monitored key's frequency at this edge), while
	// EVERY event counts toward the reported RATE (rate_i = total updates). This
	// decouples f_i from rate_i so the coordinator's p_i ∝ √(f_i/rate_i) can
	// differentiate (e.g. a fixed-frequency key on a higher-rate edge → smaller
	// p). Empty ⇒ legacy `sum`-monitor behaviour (value ∝ rate ⇒ uniform p).
	MonitorKey string `yaml:"monitor_key"`
	// MonitorConfigURL, when set and MonitorKey is empty, is the data-plane
	// streaming-config endpoint (the document the controller PUSHES its
	// `monitors:` config to, e.g. http://data-plane:9091/api/v1/streaming-config).
	// The edge fetches it on startup, finds the `monitors[]` entry whose agg_id
	// matches MonitorAggID, and uses that entry's `key` as the monitored key —
	// so the cms_point key flows from the control plane instead of a static flag,
	// closing the loop with the controller's monitor emission.
	MonitorConfigURL string `yaml:"monitor_config_url"`
}

// defaultConfig returns the built-in defaults — the lowest-precedence
// layer. These match the historical producer defaults exactly.
func defaultConfig() Config {
	return Config{
		Target:     "gateway:4317",
		Metric:     "http_requests_total",
		PprofAddr:  "",
		ProducerID: "",
		Seed:       0,

		SDKWindow:          15 * time.Second,
		SDKProjection:      "",
		SDKAgg:             "default",
		MaxBufferPerSeries: 0,
		SketchType:         "",

		Cardinality: 500,
		FreqHz:      10.0,
		ZoneVals:    4,
		RackVals:    10,
		NodeVals:    25,
		PodVals:     10,

		ZipfS:    1.1,
		ZipfV:    1.0,
		ZipfMax:  500,
		ZipfMean: 250.0,

		TraceFile:  "",
		TraceScale: 1.0,
		TraceLoop:  true,

		FreshnessProbes:  true,
		FreshnessProbeHz: 1.0,

		FiveSketchEnabled:   true,
		FiveSketchUserPool:  100,
		FiveSketchEndpoints: 50,
		FiveSketchZipfS:     1.2,
		FiveSketchUserRot:   60 * time.Second,

		ControlAddr: "",

		Duration: 0,

		WarmSampleP:     1.0,
		TraceMetricName: "",

		CoordinatorURL: "",
		MonitorAggID:   0,
		EdgeID:         "",
	}
}

// cfg is the resolved configuration. main() populates it; the workload
// helpers (probes / five-sketch / synthetic / replay) read from it so
// they no longer depend on environment variables.
var cfg = defaultConfig()

// registerFlags binds every Config field to a flag on fs, using c as the
// default values. The pointers returned via fs target the fields of the
// passed *Config so a single struct holds the resolved values.
func registerFlags(fs *flag.FlagSet, c *Config) {
	fs.StringVar(&c.Target, "target", c.Target, "OTLP/gRPC endpoint")
	fs.StringVar(&c.Metric, "metric", c.Metric, "base metric name")
	fs.StringVar(&c.PprofAddr, "pprof-addr", c.PprofAddr, "pprof listen addr, e.g. 0.0.0.0:6060; empty = off")
	fs.StringVar(&c.ProducerID, "producer-id", c.ProducerID, "producer_id label prepended to every series; empty = none")
	fs.Int64Var(&c.Seed, "seed", c.Seed, "deterministic per-series PRNG base seed; 0 = auto-random global PRNG")

	fs.DurationVar(&c.SDKWindow, "sdk-window", c.SDKWindow, "SDK PeriodicReader export interval")
	fs.StringVar(&c.SDKProjection, "sdk-projection", c.SDKProjection, "attribute keep-list: comma-separated keys, \"\" keep-all, \"-\" drop-all")
	fs.StringVar(&c.SDKAgg, "agg", c.SDKAgg, "SDK aggregation: default|sum|raw-buffer|dd-full|dd-delta|kll|cms-full|cms-delta|cs-full|cs-delta|hll-full|hll-delta")
	fs.IntVar(&c.MaxBufferPerSeries, "max-buffer-per-series", c.MaxBufferPerSeries, "raw-buffer per-series event cap")
	fs.StringVar(&c.SketchType, "sketch-type", c.SketchType, "convenience aggregation alias: ddsketch|kll|countsketch|countminsketch|hll|baseline; ignored when -agg is set")

	fs.IntVar(&c.Cardinality, "cardinality", c.Cardinality, "synthetic: # distinct attribute sets")
	fs.Float64Var(&c.FreqHz, "freq-hz", c.FreqHz, "synthetic: per-series event rate in Hz")
	fs.IntVar(&c.ZoneVals, "zone-vals", c.ZoneVals, "# distinct zone values")
	fs.IntVar(&c.RackVals, "rack-vals", c.RackVals, "# distinct rack values")
	fs.IntVar(&c.NodeVals, "node-vals", c.NodeVals, "# distinct node values")
	fs.IntVar(&c.PodVals, "pod-vals", c.PodVals, "# distinct pod values")

	fs.Float64Var(&c.ZipfS, "zipf-s", c.ZipfS, "Zipf s shape parameter (must be > 1)")
	fs.Float64Var(&c.ZipfV, "zipf-v", c.ZipfV, "Zipf v shift parameter (must be >= 1)")
	fs.Uint64Var(&c.ZipfMax, "zipf-max", c.ZipfMax, "Zipf integer range upper bound (imax)")
	fs.Float64Var(&c.ZipfMean, "zipf-mean", c.ZipfMean, "target mean for scaling Zipf latency values")

	fs.StringVar(&c.TraceFile, "trace-file", c.TraceFile, "if set, switch to trace replay of this CSV")
	fs.Float64Var(&c.TraceScale, "trace-scale", c.TraceScale, "trace playback speed multiplier")
	fs.BoolVar(&c.TraceLoop, "trace-loop", c.TraceLoop, "wrap trace replay at EOF")

	fs.BoolVar(&c.FreshnessProbes, "freshness-probes", c.FreshnessProbes, "emit the three freshness probe counters")
	fs.Float64Var(&c.FreshnessProbeHz, "freshness-probe-hz", c.FreshnessProbeHz, "freshness probe tick rate in Hz")

	fs.BoolVar(&c.FiveSketchEnabled, "five-sketch", c.FiveSketchEnabled, "emit the four extra five-sketch metrics; set false to isolate the warm-sample metric on the wire")
	fs.IntVar(&c.FiveSketchUserPool, "five-sketch-user-pool", c.FiveSketchUserPool, "five-sketch HLL user-id pool size")
	fs.IntVar(&c.FiveSketchEndpoints, "five-sketch-endpoints", c.FiveSketchEndpoints, "five-sketch Zipfian endpoint cardinality")
	fs.Float64Var(&c.FiveSketchZipfS, "five-sketch-zipf-s", c.FiveSketchZipfS, "five-sketch Zipfian s parameter")
	fs.DurationVar(&c.FiveSketchUserRot, "five-sketch-user-rotate", c.FiveSketchUserRot, "five-sketch user-window rotate cadence")

	fs.StringVar(&c.ControlAddr, "control-addr", c.ControlAddr, "HTTP /control/projection listen addr; empty = off")

	fs.DurationVar(&c.Duration, "duration", c.Duration, "run duration; 0 = run until Ctrl+C")

	fs.Float64Var(&c.WarmSampleP, "warm-sample-p", c.WarmSampleP, "producer-side warm-sketch sampling: admitted fraction p of the warm gauge datapoints; (1-p) dropped before export; 1.0 = no sampling. Applies to the synthetic <metric>_latency_ms AND the replayed trace gauge.")
	fs.StringVar(&c.TraceMetricName, "trace-metric-name", c.TraceMetricName, "override the replay gauge metric name (default <metric>_trace); set to land the trace under a DDSketch-aggregated name")
	fs.StringVar(&c.CoordinatorURL, "coordinator-url", c.CoordinatorURL, "CDM coordinator MonitorService endpoint (host:port); non-empty makes this producer a coordinated edge whose warm-sample-p comes from the coordinator's grant")
	fs.StringVar(&c.MonitorKey, "monitor-key", c.MonitorKey, "OVERRIDE for the cms_point heavy-hitter key: only this series_id counts toward the reported value f_i (the key's frequency), every event counts toward rate_i, so p_i ∝ √(f_i/rate_i) differentiates. Normally LEFT EMPTY and learned from the controller's pushed monitor config via -monitor-config-url; empty + no config URL = sum-monitor (value ∝ rate ⇒ uniform p)")
	fs.StringVar(&c.MonitorConfigURL, "monitor-config-url", c.MonitorConfigURL, "data-plane streaming-config endpoint (where the controller pushes its monitors:) — when set and -monitor-key is empty, the edge learns its cms_point key from the monitors[] entry matching -monitor-agg-id")
	fs.Uint64Var(&c.MonitorAggID, "monitor-agg-id", c.MonitorAggID, "content-addressed agg_id reported to the coordinator; must match the monitors: entry agg_id")
	fs.StringVar(&c.EdgeID, "edge-id", c.EdgeID, "edge identity reported to the coordinator; defaults to -producer-id when empty")
}

// loadConfig resolves the configuration with the locked precedence:
//
//	built-in defaults  <  YAML (-config <file>)  <  explicitly-set flags
//
// Implementation: a dedicated FlagSet is registered over a defaults
// struct and parsed once to learn which flags were explicitly set
// (flag.Visit) and to read -config. If -config is given, the YAML is
// unmarshalled on top of the defaults struct (so unspecified YAML keys
// keep their defaults). Then we re-apply only the explicitly-set flags
// over the YAML-merged struct, so CLI flags win.
func loadConfig(args []string) (Config, error) {
	// Layer 1: defaults.
	merged := defaultConfig()

	fs := flag.NewFlagSet("otel-app", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to YAML config file; CLI flags override its values")
	// Register the rest over a scratch copy so we can detect which flags
	// were set and read their values without clobbering `merged` yet.
	var fromFlags = defaultConfig()
	registerFlags(fs, &fromFlags)
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	// Layer 2: YAML (over the defaults).
	if *configPath != "" {
		data, err := os.ReadFile(*configPath)
		if err != nil {
			return Config{}, fmt.Errorf("reading config %s: %w", *configPath, err)
		}
		if err := yaml.Unmarshal(data, &merged); err != nil {
			return Config{}, fmt.Errorf("parsing config %s: %w", *configPath, err)
		}
	}

	// Layer 3: explicitly-set flags win. flag.Visit only walks flags that
	// were actually present on the command line.
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	applyExplicitFlags(&merged, &fromFlags, set)

	return merged, nil
}

// applyExplicitFlags copies, from src into dst, only those fields whose
// corresponding flag name appears in set. This is the "CLI flags
// override YAML" step.
func applyExplicitFlags(dst, src *Config, set map[string]bool) {
	if set["target"] {
		dst.Target = src.Target
	}
	if set["metric"] {
		dst.Metric = src.Metric
	}
	if set["pprof-addr"] {
		dst.PprofAddr = src.PprofAddr
	}
	if set["producer-id"] {
		dst.ProducerID = src.ProducerID
	}
	if set["seed"] {
		dst.Seed = src.Seed
	}
	if set["sdk-window"] {
		dst.SDKWindow = src.SDKWindow
	}
	if set["sdk-projection"] {
		dst.SDKProjection = src.SDKProjection
	}
	if set["agg"] {
		dst.SDKAgg = src.SDKAgg
	}
	if set["max-buffer-per-series"] {
		dst.MaxBufferPerSeries = src.MaxBufferPerSeries
	}
	if set["sketch-type"] {
		dst.SketchType = src.SketchType
	}
	if set["cardinality"] {
		dst.Cardinality = src.Cardinality
	}
	if set["freq-hz"] {
		dst.FreqHz = src.FreqHz
	}
	if set["zone-vals"] {
		dst.ZoneVals = src.ZoneVals
	}
	if set["rack-vals"] {
		dst.RackVals = src.RackVals
	}
	if set["node-vals"] {
		dst.NodeVals = src.NodeVals
	}
	if set["pod-vals"] {
		dst.PodVals = src.PodVals
	}
	if set["zipf-s"] {
		dst.ZipfS = src.ZipfS
	}
	if set["zipf-v"] {
		dst.ZipfV = src.ZipfV
	}
	if set["zipf-max"] {
		dst.ZipfMax = src.ZipfMax
	}
	if set["zipf-mean"] {
		dst.ZipfMean = src.ZipfMean
	}
	if set["trace-file"] {
		dst.TraceFile = src.TraceFile
	}
	if set["trace-scale"] {
		dst.TraceScale = src.TraceScale
	}
	if set["trace-loop"] {
		dst.TraceLoop = src.TraceLoop
	}
	if set["freshness-probes"] {
		dst.FreshnessProbes = src.FreshnessProbes
	}
	if set["freshness-probe-hz"] {
		dst.FreshnessProbeHz = src.FreshnessProbeHz
	}
	if set["five-sketch"] {
		dst.FiveSketchEnabled = src.FiveSketchEnabled
	}
	if set["five-sketch-user-pool"] {
		dst.FiveSketchUserPool = src.FiveSketchUserPool
	}
	if set["five-sketch-endpoints"] {
		dst.FiveSketchEndpoints = src.FiveSketchEndpoints
	}
	if set["five-sketch-zipf-s"] {
		dst.FiveSketchZipfS = src.FiveSketchZipfS
	}
	if set["five-sketch-user-rotate"] {
		dst.FiveSketchUserRot = src.FiveSketchUserRot
	}
	if set["control-addr"] {
		dst.ControlAddr = src.ControlAddr
	}
	if set["duration"] {
		dst.Duration = src.Duration
	}
	if set["warm-sample-p"] {
		dst.WarmSampleP = src.WarmSampleP
	}
	if set["trace-metric-name"] {
		dst.TraceMetricName = src.TraceMetricName
	}
	if set["coordinator-url"] {
		dst.CoordinatorURL = src.CoordinatorURL
	}
	if set["monitor-agg-id"] {
		dst.MonitorAggID = src.MonitorAggID
	}
	if set["edge-id"] {
		dst.EdgeID = src.EdgeID
	}
}

// resolveAggName picks the effective aggregation name. -agg is
// authoritative; -sketch-type is a convenience alias consulted only when
// -agg is left at its "default" value. The mapping mirrors
// fakemetricload's sketch-type → SDK aggregation choice (all sketch types
// pre-aggregate via a View).
func resolveAggName(c Config) string {
	if c.SDKAgg != "" && strings.ToLower(strings.TrimSpace(c.SDKAgg)) != "default" {
		return c.SDKAgg
	}
	switch strings.ToLower(strings.TrimSpace(c.SketchType)) {
	case "":
		return c.SDKAgg // honour explicit/empty "default"
	case "ddsketch":
		return "dd-full"
	case "kll":
		return "kll"
	case "countsketch":
		return "cs-full"
	case "countminsketch":
		return "cms-full"
	case "hll":
		return "hll-full"
	case "baseline":
		return "default"
	default:
		log.Printf("warning: unknown -sketch-type %q — using -agg=%q", c.SketchType, c.SDKAgg)
		return c.SDKAgg
	}
}

// parseAgg maps an aggregation name to a concrete sdkmetric.Aggregation.
// Unrecognised values fall back to AggregationDefault with a warning so
// experiments don't silently run the wrong shape. (cold_density_test.go
// depends on this signature.)
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
		log.Printf("warning: unknown aggregation %q — falling back to default", name)
		return sdkmetric.AggregationDefault{}
	}
}

// parseProjection converts a comma-separated key list to an
// attribute.Filter. The special token "-" means "drop everything" so the
// aggregator bucket degenerates to a single series. Empty string keeps
// every attribute — no filter registered. (swappable_filter.go + tests
// depend on this.)
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
// sets under a 4-dim schema (zone × rack × node × pod). Extra entries
// alias onto earlier ones by modular wraparound.
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

// traceRow is one row of the replay CSV.
type traceRow struct {
	tsMs     int64
	seriesID string
	value    float64
}

// loadTraceCSV reads the replay CSV into memory, sorts by timestamp, and
// returns the full row list plus the unique series list.
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
	resolved, err := loadConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	cfg = resolved

	// Optional pprof endpoint for producer-side profiling.
	if cfg.PprofAddr != "" {
		go func() {
			log.Printf("pprof listening on %s", cfg.PprofAddr)
			if err := http.ListenAndServe(cfg.PprofAddr, nil); err != nil {
				log.Printf("pprof server: %v", err)
			}
		}()
	}

	// Three-axis SDK config.
	aggName := resolveAggName(cfg)
	projection := parseProjection(cfg.SDKProjection)
	agg := parseAgg(aggName, cfg.MaxBufferPerSeries)

	// Run-duration context (fakemetricload heritage). 0 = run forever.
	var ctx context.Context
	var cancel context.CancelFunc
	if cfg.Duration > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), cfg.Duration)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	exp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(cfg.Target),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithCompressor("gzip"),
	)
	if err != nil {
		log.Fatalf("otlp exporter init: %v", err)
	}

	res, _ := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("otel-app"),
	))

	reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(cfg.SDKWindow))

	// One View covers every instrument this producer emits. The stream
	// config is what the three-axis sweep varies — aggregation + attribute
	// filter. AttributeFilter is wrapped in a swappableFilter so the
	// controller can change the label projection L mid-run via
	// POST /control/projection (see swappable_filter.go).
	swappable := newSwappableFilter(projection)
	stream := sdkmetric.Stream{Aggregation: agg, AttributeFilter: swappable.Filter()}
	view := sdkmetric.NewView(sdkmetric.Instrument{Name: "*"}, stream)
	if cfg.ControlAddr != "" {
		log.Printf("control plane listening on %s (POST /control/projection)", cfg.ControlAddr)
		_ = installControlServer(cfg.ControlAddr, swappable)
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(view),
	)
	defer func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		if err := provider.Shutdown(shutCtx); err != nil {
			log.Printf("MeterProvider shutdown: %v", err)
		}
	}()
	otel.SetMeterProvider(provider)

	meter := provider.Meter("otel-app")

	log.Printf(
		"otel-app sdk config: target=%s window=%s agg=%s projection=%q duration=%v",
		cfg.Target, cfg.SDKWindow, aggName, cfg.SDKProjection, cfg.Duration,
	)

	// Freshness probes ride the same PeriodicReader / View as the rest.
	stopProbes := startFreshnessProbes(ctx, meter, cfg)
	defer stopProbes()

	if cfg.TraceFile != "" {
		runTraceReplay(ctx, meter, cfg.Metric, cfg.TraceFile)
	} else {
		runSynthetic(ctx, meter, cfg.Metric)
	}
}

// runSynthetic drives the synthetic workload at cfg.FreqHz per series.
// One goroutine per attribute set fires Add(1) on the counter and
// Record(zipf-latency) on the gauge every 1/freq wall time. The Zipf
// distribution (cfg.Zipf*) is the inherited fakemetricload value model.
func runSynthetic(ctx context.Context, meter metric.Meter, metricName string) {
	cardinality := cfg.Cardinality
	freqHz := cfg.FreqHz
	zoneVals := cfg.ZoneVals
	rackVals := cfg.RackVals
	nodeVals := cfg.NodeVals
	podVals := cfg.PodVals

	if cfg.ZipfS <= 1.0 {
		log.Fatalf("zipf-s must be > 1.0, got %.2f", cfg.ZipfS)
	}
	if cfg.ZipfV < 1.0 {
		log.Fatalf("zipf-v must be >= 1.0, got %.2f", cfg.ZipfV)
	}

	maxCard := zoneVals * rackVals * nodeVals * podVals
	if cardinality > maxCard {
		log.Printf(
			"warning: cardinality=%d exceeds schema product %d; extra attribute sets alias onto earlier ones",
			cardinality, maxCard,
		)
	}

	log.Printf(
		"otel-app starting (synthetic): metric=%s cardinality=%d freq_hz=%.1f schema=%dx%dx%dx%d zipf(s=%.2f,v=%.2f,max=%d,mean=%.1f)",
		metricName, cardinality, freqHz, zoneVals, rackVals, nodeVals, podVals,
		cfg.ZipfS, cfg.ZipfV, cfg.ZipfMax, cfg.ZipfMean,
	)
	log.Printf("otel-app warm-sample-p=%.3f (admitted fraction of %s_latency_ms; 1.0=no sampling)",
		cfg.WarmSampleP, metricName)

	counter, err := meter.Float64Counter(metricName,
		metric.WithDescription("Synthetic event counter — incremented by 1 per event"))
	if err != nil {
		log.Fatalf("counter init: %v", err)
	}
	latencyGauge, err := meter.Float64Gauge(metricName+"_latency_ms",
		metric.WithDescription("Synthetic Zipf-distributed latency sample per event"),
		metric.WithUnit("ms"))
	if err != nil {
		log.Fatalf("gauge init: %v", err)
	}

	labelSets := buildLabelSets(cardinality, zoneVals, rackVals, nodeVals, podVals)
	// Multi-producer demos: prepend producer-id as a label so each
	// producer's series stay distinct at the backend.
	if cfg.ProducerID != "" {
		for i := range labelSets {
			labelSets[i] = append(
				[]attribute.KeyValue{attribute.String("producer_id", cfg.ProducerID)},
				labelSets[i]...,
			)
		}
	}
	period := time.Duration(float64(time.Second) / freqHz)

	// Deterministic per-series PRNG seed for accuracy-comparison runs.
	// When -seed is set, each series gets a deterministic PRNG seeded as
	//   seed XOR hash(producer-id) XOR (seriesIdx+1) * <large prime>
	// so all producers across all arms with the same -seed emit identical
	// value sequences per (producer_id, series_idx). When unset, behavior
	// matches the legacy auto-random global PRNG (back-compat).
	baseSeed := cfg.Seed
	producerIDHash := int64(0)
	for _, b := range []byte(cfg.ProducerID) {
		producerIDHash = producerIDHash*131 + int64(b)
	}
	const seriesSeedPrime int64 = 2654435761
	useSeededRng := baseSeed != 0

	// Five-sketch MVP workload (issue #46) — emits the four new metrics
	// (request_size_bytes / unique_users_per_min / top_endpoint_qps /
	// endpoint_request_freq). Always emitted; reuses the same outer label
	// schema as the counter above.
	if cfg.FiveSketchEnabled {
		stopFiveSketch := startFiveSketchWorkload(ctx, meter, labelSets, freqHz, cfg)
		defer stopFiveSketch()
	}

	var wg sync.WaitGroup
	for i := 0; i < cardinality; i++ {
		wg.Add(1)
		go func(seriesIdx int) {
			defer wg.Done()
			// Stagger start so all series don't fire simultaneously.
			time.Sleep(time.Duration(seriesIdx%int(max64(freqHz, 1))) * period /
				time.Duration(max64(freqHz, 1)))

			ticker := time.NewTicker(period)
			defer ticker.Stop()
			attrs := metric.WithAttributes(labelSets[seriesIdx]...)

			// Per-series PRNG: when -seed is set, deterministic per
			// (producer_id, series_idx). Otherwise the global PRNG.
			var localRng *rand.Rand
			if useSeededRng {
				seriesSeed := baseSeed ^ producerIDHash ^ (int64(seriesIdx+1) * seriesSeedPrime)
				localRng = rand.New(rand.NewSource(seriesSeed))
			} else {
				localRng = rand.New(rand.NewSource(time.Now().UnixNano() + int64(seriesIdx)))
			}
			// Zipf generator over the per-series RNG (fakemetricload value
			// model). NewZipf requires s > 1 strictly, validated above.
			zipf := rand.NewZipf(localRng, cfg.ZipfS, cfg.ZipfV, cfg.ZipfMax)
			drawLat := func() float64 { return generateZipfValue(zipf) }

			// Producer-side warm-part sampling: keep each latency
			// datapoint with probability p. Dropped points never enter
			// the SDK aggregation, so they are never sketched/exported/
			// sent. The Sum counter (counter.Add) is always emitted.
			warmP := cfg.WarmSampleP
			keepWarm := func() bool {
				if warmP >= 1.0 {
					return true
				}
				if warmP <= 0.0 {
					return false
				}
				return localRng.Float64() < warmP
			}

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					counter.Add(ctx, 1, attrs)
					if keepWarm() {
						latencyGauge.Record(ctx, drawLat(), attrs)
					}
				}
			}
		}(i)
	}
	wg.Wait()
}

// generateZipfValue scales a Zipf draw so the value distribution centres
// near cfg.ZipfMean. Carried from fakemetricload.
func generateZipfValue(zipf *rand.Zipf) float64 {
	scaleFactor := cfg.ZipfMean / (float64(cfg.ZipfMax) / 2.0)
	return float64(zipf.Uint64()+1) * scaleFactor
}

func max64(a float64, b int) int {
	if int(a) < b {
		return b
	}
	return int(a)
}

// runTraceReplay reads a CSV trace and emits its rows as gauges at the
// recorded pace. Each unique series_id becomes label {series_id=…}; the
// SDK config (window / projection / agg) applies uniformly.
//
// Warm sampling: each replayed point is admitted with probability p before
// it enters the SDK aggregation, identical in spirit to the synthetic path
// — so the Google-cluster accuracy sweep thins the REAL replayed warm
// metric. p is either the static -warm-sample-p, or, when -coordinator-url
// is set, the live coordinator-granted p applied at window boundaries.
func runTraceReplay(ctx context.Context, meter metric.Meter, metricName, path string) {
	scale := cfg.TraceScale
	loop := cfg.TraceLoop

	rows, series, err := loadTraceCSV(path)
	if err != nil {
		log.Fatalf("trace load: %v", err)
	}

	emitName := metricName + "_trace"
	if cfg.TraceMetricName != "" {
		emitName = cfg.TraceMetricName
	}
	log.Printf(
		"otel-app starting (trace replay): metric=%s rows=%d series=%d scale=%.2fx loop=%v warm-sample-p=%.3f",
		emitName, len(rows), len(series), scale, loop, cfg.WarmSampleP,
	)

	gauge, err := meter.Float64Gauge(emitName,
		metric.WithDescription("Trace replay gauge"))
	if err != nil {
		log.Fatalf("gauge init: %v", err)
	}

	labelSets := make(map[string][]attribute.KeyValue, len(series))
	for _, s := range series {
		kv := []attribute.KeyValue{attribute.String("series_id", s)}
		if cfg.ProducerID != "" {
			kv = append([]attribute.KeyValue{attribute.String("producer_id", cfg.ProducerID)}, kv...)
		}
		labelSets[s] = kv
	}

	// Replay PRNG: deterministic when -seed set (so an accuracy sweep's arms
	// admit a reproducible subset per p), else time-seeded.
	var rng *rand.Rand
	if cfg.Seed != 0 {
		rng = rand.New(rand.NewSource(cfg.Seed))
	} else {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	// Admission accounting: count candidate vs admitted points so the
	// accuracy sweep can report ingest load (admitted ∝ p) without needing a
	// receiver-side metrics scrape. Logged at replay end (REPLAY_STATS line).
	replayStats = &replayCounters{}

	// CDM coordinated-sampling edge state (Priority 2). When CoordinatorURL is
	// set, sampleCtl exposes the live p (updated at window boundaries from the
	// coordinator grant) and reports the per-window admitted rate. When unset,
	// it is a static holder returning the bootstrap -warm-sample-p.
	sc := newSampleController(cfg, metricName)
	defer sc.close()

	for {
		replayOnce(ctx, gauge, rows, labelSets, scale, rng, sc)
		log.Printf("REPLAY_STATS candidate=%d admitted=%d p=%.4f",
			replayStats.candidate, replayStats.admitted, cfg.WarmSampleP)
		if !loop {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		log.Printf("trace replay wrap — restarting from row 0")
	}
}

// replayCounters tracks candidate vs admitted points across a replay pass.
type replayCounters struct {
	candidate uint64
	admitted  uint64
}

var replayStats *replayCounters

// replayOnce plays the rows list once at recorded pace scaled by `scale`
// (1.0 = real-time, 2.0 = 2× faster, 0.5 = half-speed). Each point is
// admitted into the SDK aggregation with the current warm-sample probability
// p (static or coordinator-granted via sc).
func replayOnce(
	ctx context.Context,
	gauge metric.Float64Gauge,
	rows []traceRow,
	labelSets map[string][]attribute.KeyValue,
	scale float64,
	rng *rand.Rand,
	sc *sampleController,
) {
	if len(rows) == 0 {
		return
	}
	walkStart := time.Now()
	traceStart := rows[0].tsMs
	for _, r := range rows {
		select {
		case <-ctx.Done():
			return
		default:
		}
		targetOffsetMs := float64(r.tsMs-traceStart) / scale
		target := walkStart.Add(time.Duration(targetOffsetMs) * time.Millisecond)
		if sleep := time.Until(target); sleep > 0 {
			t := time.NewTimer(sleep)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
		// Window boundary bookkeeping: at each SDK-window rotation the
		// coordinated edge re-reads the granted p and reports last window's
		// observed rate. No-op (returns the static p) when uncoordinated.
		p := sc.currentP()
		sc.observe(r.seriesID) // rate += 1 always; value += 1 iff seriesID==MonitorKey
		if replayStats != nil {
			replayStats.candidate++
		}
		if p < 1.0 && rng.Float64() >= p {
			continue // dropped: never enters the SDK aggregation / wire
		}
		if replayStats != nil {
			replayStats.admitted++
		}
		gauge.Record(ctx, r.value, metric.WithAttributes(labelSets[r.seriesID]...))
	}
}
