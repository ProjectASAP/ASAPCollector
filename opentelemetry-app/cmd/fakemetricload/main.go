package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

var (
	configPath = flag.String("config", "", "Path to YAML pipeline config file; when set, overrides sketch/interval flags")
	endpoint   = flag.String("endpoint", "localhost:4317", "OTLP gRPC endpoint")
	workers    = flag.Int("workers", 4, "Number of worker goroutines")
	hosts      = flag.Int("hosts", 10, "Hosts per worker")
	metrics    = flag.Int("metrics", 10, "Metrics per host")
	interval   = flag.Duration("interval", 10*time.Second, "SDK export interval — controls data points/sec to collector")
	duration   = flag.Duration("duration", 60*time.Second, "Run duration (0 = forever)")
	sketchArg  = flag.String("sketch-type", "ddsketch", "Sketch aggregation: ddsketch|kll|countsketch|countminsketch|hll|baseline")

	// How many Zipf values to record per series per export window.
	// For ddsketch this controls sketch richness; for others only the last value is kept (gauge).
	samplesPerInterval = flag.Int("samples-per-interval", 1,
		"Values recorded per series per export window (ddsketch: sketch richness; gauge types: last value wins)")

	// DDSketch
	ddsketchAccuracy = flag.Float64("ddsketch-accuracy", 0.01, "DDSketch relative accuracy (0,1)")

	// KLL
	kllK = flag.Int("kll-k", 256, "KLL sketch parameter k (>= 2)")

	// CountSketch
	csEpsilon = flag.Float64("countsketch-epsilon", 0.01, "CountSketch epsilon (0,1)")
	csDelta   = flag.Float64("countsketch-delta", 0.99, "CountSketch delta (0,1)")

	// CountMinSketch
	cmsRows = flag.Int("countmin-rows", 5, "CountMinSketch rows")
	cmsCols = flag.Int("countmin-cols", 2000, "CountMinSketch columns")

	// Zipf distribution
	zipfS    = flag.Float64("zipf-s", 1.1, "Zipf s parameter (must be > 1)")
	zipfV    = flag.Float64("zipf-v", 1.0, "Zipf v parameter (must be >= 1)")
	zipfMax  = flag.Uint64("zipf-max", 500, "Zipf imax")
	zipfMean = flag.Float64("zipf-mean", 250.0, "Target mean for scaling Zipf values")
)

// sketchMode describes how measurements are delivered to the collector:
//
//   - sdkSketch: Float64Histogram with a sketch aggregation view. The SDK
//     pre-aggregates all recorded values into a sketch before export. The
//     collector receives a single compressed data point per series per window.
//     Only ddsketch is supported here because the ddsketchprocessor explicitly
//     handles incoming DDSketch pdata; the other processors accept only Gauge.
//
//   - sdkGauge: Float64Gauge with default (LastValue) aggregation. The SDK
//     emits the last recorded value per series on each export tick. The
//     collector's sketch processor (KLL, CountSketch, CountMinSketch) then
//     performs the aggregation from these raw gauge data points.
type sketchMode int

const (
	sdkSketch sketchMode = iota
	sdkGauge
)

func main() {
	flag.Parse()

	// --- Config file path: when --config is set, load YAML and override flags ---
	var pipelineCfg *sdkmetric.PipelineConfig
	if *configPath != "" {
		var err error
		pipelineCfg, err = sdkmetric.LoadPipelineConfig(*configPath)
		if err != nil {
			log.Fatalf("failed to load config: %v", err)
		}
		// Propagate config values into flag variables so the rest of main is
		// driven by a single set of variables regardless of the source.
		*sketchArg = pipelineCfg.Sketch.Type
		*endpoint = pipelineCfg.Exporter.Endpoint
		*interval = pipelineCfg.ReaderInterval()
		if pipelineCfg.Load.Workers > 0 {
			*workers = pipelineCfg.Load.Workers
		}
		if pipelineCfg.Load.Duration > 0 {
			*duration = pipelineCfg.Load.Duration
		}
		if pipelineCfg.Load.Distribution.S > 0 {
			*zipfS = pipelineCfg.Load.Distribution.S
		}
		if pipelineCfg.Load.Distribution.V > 0 {
			*zipfV = pipelineCfg.Load.Distribution.V
		}
		if pipelineCfg.Load.Distribution.Max > 0 {
			*zipfMax = pipelineCfg.Load.Distribution.Max
		}
		if pipelineCfg.Load.Distribution.Mean > 0 {
			*zipfMean = pipelineCfg.Load.Distribution.Mean
		}
		// Sketch-specific params
		if pipelineCfg.Sketch.DDSketch.RelativeAccuracy > 0 {
			*ddsketchAccuracy = pipelineCfg.Sketch.DDSketch.RelativeAccuracy
		}
		if pipelineCfg.Sketch.KLL.K > 0 {
			*kllK = pipelineCfg.Sketch.KLL.K
		}
		if pipelineCfg.Sketch.CountSketch.Epsilon > 0 {
			*csEpsilon = pipelineCfg.Sketch.CountSketch.Epsilon
		}
		if pipelineCfg.Sketch.CountSketch.Delta > 0 {
			*csDelta = pipelineCfg.Sketch.CountSketch.Delta
		}
		if pipelineCfg.Sketch.CountMinSketch.Rows > 0 {
			*cmsRows = pipelineCfg.Sketch.CountMinSketch.Rows
		}
		if pipelineCfg.Sketch.CountMinSketch.Cols > 0 {
			*cmsCols = pipelineCfg.Sketch.CountMinSketch.Cols
		}
	}

	mode := strings.ToLower(*sketchArg)
	switch mode {
	case "ddsketch", "kll", "countsketch", "countminsketch", "hll", "baseline":
	default:
		log.Fatalf("invalid sketch-type %q — valid: ddsketch|kll|countsketch|countminsketch|hll|baseline", mode)
	}
	if *zipfS <= 1.0 {
		log.Fatalf("zipf-s must be > 1.0, got %.2f", *zipfS)
	}
	if *zipfV < 1.0 {
		log.Fatalf("zipf-v must be >= 1.0, got %.2f", *zipfV)
	}

	totalSeries := *workers * *hosts * *metrics
	throughput := float64(totalSeries) / interval.Seconds()

	fmt.Printf("--- SDK Sketch Load Generator ---\n")
	fmt.Printf("Sketch type:  %s\n", strings.ToUpper(mode))
	fmt.Printf("Target:       %s\n", *endpoint)
	fmt.Printf("Series:       %d workers × %d hosts × %d metrics = %d total\n",
		*workers, *hosts, *metrics, totalSeries)
	fmt.Printf("Interval:     %s  →  %.0f data-points/s to collector\n", *interval, throughput)
	fmt.Printf("Samples/win:  %d (values recorded per series per export window)\n", *samplesPerInterval)
	if *duration > 0 {
		fmt.Printf("Duration:     %v\n", *duration)
	} else {
		fmt.Printf("Duration:     Until Ctrl+C\n")
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if *duration > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), *duration)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	// Choose aggregation. For ddsketch the SDK pre-aggregates; for the rest we
	// fall back to gauge so the collector-side processor can do the aggregation.
	var delivery sketchMode
	var agg sdkmetric.Aggregation
	switch mode {
	case "ddsketch":
		delivery = sdkSketch
		agg = sdkmetric.AggregationDDSketch{RelativeAccuracy: *ddsketchAccuracy}
	case "kll":
		// SDK pre-aggregates values into a KLL sketch before export.
		delivery = sdkSketch
		agg = sdkmetric.AggregationKLLSketch{K: *kllK}
	case "countsketch":
		// SDK pre-aggregates values into a CountSketch before export.
		delivery = sdkSketch
		agg = sdkmetric.AggregationCountSketch{Epsilon: *csEpsilon, Delta: *csDelta}
	case "countminsketch":
		// SDK pre-aggregates values into a CountMinSketch before export.
		delivery = sdkSketch
		agg = sdkmetric.AggregationCountMinSketch{Rows: *cmsRows, Cols: *cmsCols}
	case "hll":
		// HLL: SDK pre-aggregates values into an HLL sketch before export, analogous
		// to DDSketch. The collector receives HLLSketch-typed data points.
		delivery = sdkSketch
		agg = sdkmetric.AggregationHLLSketch{}
	case "baseline":
		// Baseline: no sketch aggregation. SDK emits raw Float64Gauge (LastValue)
		// observations; the collector-side processor performs any aggregation.
		delivery = sdkGauge
	}

	exp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(*endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		log.Fatalf("failed to create OTLP exporter: %v", err)
	}

	// Build provider options. When a config file was loaded use its views
	// (which carry transmit_sketch semantics); otherwise build a single view
	// from the resolved flag values.
	providerOpts := []sdkmetric.Option{
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(*interval)),
		),
	}
	if pipelineCfg != nil {
		for _, v := range pipelineCfg.ToViews() {
			providerOpts = append(providerOpts, sdkmetric.WithView(v))
		}
	} else {
		providerOpts = append(providerOpts, sdkmetric.WithView(
			sdkmetric.NewView(
				sdkmetric.Instrument{Name: "benchmark.latency"},
				sdkmetric.Stream{Aggregation: agg},
			),
		))
	}

	provider := sdkmetric.NewMeterProvider(providerOpts...)
	defer func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		if err := provider.Shutdown(shutCtx); err != nil {
			log.Printf("MeterProvider shutdown: %v", err)
		}
	}()

	meter := provider.Meter("fakemetricload")

	var wg sync.WaitGroup
	switch delivery {
	case sdkSketch:
		// Pre-aggregated path: histogram with sketch view.
		hist, err := meter.Float64Histogram("benchmark.latency",
			metric.WithDescription("Benchmark latency measurement"),
			metric.WithUnit("ms"),
		)
		if err != nil {
			log.Fatalf("failed to create histogram: %v", err)
		}
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go runHistogramWorker(ctx, w, hist, mode, &wg)
		}

	case sdkGauge:
		// Raw-value path: gauge so the collector processor aggregates.
		gauge, err := meter.Float64Gauge("benchmark.latency",
			metric.WithDescription("Benchmark latency measurement"),
			metric.WithUnit("ms"),
		)
		if err != nil {
			log.Fatalf("failed to create gauge: %v", err)
		}
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go runGaugeWorker(ctx, w, gauge, mode, &wg)
		}
	}

	<-ctx.Done()
	fmt.Println("\nLoad generation complete. Flushing final export...")
	wg.Wait()
}

// runHistogramWorker records samplesPerInterval Zipf values for each series on
// every export tick. For the ddsketch path the SDK accumulates them all into a
// single sketch per series before the periodic export fires.
func runHistogramWorker(ctx context.Context, id int, hist metric.Float64Histogram, mode string, wg *sync.WaitGroup) {
	defer wg.Done()

	src := rand.NewSource(time.Now().UnixNano() + int64(id))
	rng := rand.New(src)
	zipf := rand.NewZipf(rng, *zipfS, *zipfV, *zipfMax)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for h := 0; h < *hosts; h++ {
				hostAttr := attribute.String("host.name", fmt.Sprintf("worker-%d-host-%02d", id, h))
				for m := 0; m < *metrics; m++ {
					metricAttr := attribute.String("metric.name", fmt.Sprintf("metric.%03d", m))
					attrs := metric.WithAttributes(hostAttr, metricAttr)
					for s := 0; s < *samplesPerInterval; s++ {
						hist.Record(ctx, generateZipfValue(zipf), attrs)
					}
				}
			}
			fmt.Printf("Worker %d: recorded batch (%s, %d samples/series)\n",
				id, mode, *samplesPerInterval)
		}
	}
}

// runGaugeWorker records one Zipf value per series per export tick. The SDK
// keeps the last observation and exports it as a Gauge data point; the
// collector-side KLL/CountSketch/CountMinSketch processor aggregates the stream.
func runGaugeWorker(ctx context.Context, id int, gauge metric.Float64Gauge, mode string, wg *sync.WaitGroup) {
	defer wg.Done()

	src := rand.NewSource(time.Now().UnixNano() + int64(id))
	rng := rand.New(src)
	zipf := rand.NewZipf(rng, *zipfS, *zipfV, *zipfMax)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for h := 0; h < *hosts; h++ {
				hostAttr := attribute.String("host.name", fmt.Sprintf("worker-%d-host-%02d", id, h))
				for m := 0; m < *metrics; m++ {
					metricAttr := attribute.String("metric.name", fmt.Sprintf("metric.%03d", m))
					gauge.Record(ctx, generateZipfValue(zipf),
						metric.WithAttributes(hostAttr, metricAttr),
					)
				}
			}
			fmt.Printf("Worker %d: recorded batch (%s)\n", id, mode)
		}
	}
}

func generateZipfValue(zipf *rand.Zipf) float64 {
	scaleFactor := *zipfMean / (float64(*zipfMax) / 2.0)
	return float64(zipf.Uint64()+1) * scaleFactor
}
