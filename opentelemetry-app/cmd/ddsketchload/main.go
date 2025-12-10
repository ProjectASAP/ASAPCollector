package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"google.golang.org/grpc"
)

type appConfig struct {
	endpoint         string
	insecure         bool
	duration         time.Duration
	exportInterval   time.Duration
	ratePerSecond    int
	workers          int
	ddsketchAccuracy float64
	ddsketchMetric   string
	histogramMetric  string
	requestsMetric   string
	serviceName      string
	latencyMean      float64
	latencyStdDev    float64
	metricUnit       string
	resourceAttrs    []attribute.KeyValue
	additionalAttrs  []attribute.KeyValue
}

func defaultConfig() appConfig {
	return appConfig{
		endpoint:         envOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		insecure:         true,
		duration:         5 * time.Minute,
		exportInterval:   5 * time.Second,
		ratePerSecond:    5000,
		workers:          4,
		ddsketchAccuracy: 0.01,
		ddsketchMetric:   "stress.ddsketch.latency",
		histogramMetric:  "stress.histogram.latency",
		requestsMetric:   "stress.requests.total",
		serviceName:      "ddsketch-stress",
		latencyMean:      250,
		latencyStdDev:    40,
		metricUnit:       "ms",
		resourceAttrs: []attribute.KeyValue{
			semconv.DeploymentEnvironmentName("development"),
		},
	}
}

func main() {
	cfg := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if cfg.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}

	if err := run(ctx, cfg); err != nil && err != context.Canceled {
		log.Fatalf("ddsketch load generator failed: %v", err)
	}
}

func parseFlags() appConfig {
	cfg := defaultConfig()

	flag.StringVar(&cfg.endpoint, "endpoint", cfg.endpoint, "OTLP gRPC endpoint (host:port)")
	flag.BoolVar(&cfg.insecure, "insecure", cfg.insecure, "Use insecure gRPC (no TLS) when connecting to the collector")
	flag.DurationVar(&cfg.duration, "duration", cfg.duration, "How long to run the generator (0 to run until interrupted)")
	flag.DurationVar(&cfg.exportInterval, "export-interval", cfg.exportInterval, "Interval between metric exports")
	flag.IntVar(&cfg.ratePerSecond, "rate", cfg.ratePerSecond, "Target measurements per second across all workers (0 for unthrottled)")
	flag.IntVar(&cfg.workers, "workers", cfg.workers, "Number of concurrent workers emitting samples")
	flag.Float64Var(&cfg.ddsketchAccuracy, "ddsketch-accuracy", cfg.ddsketchAccuracy, "Relative accuracy for DDSketch aggregation (0 lets the SDK use its default)")
	flag.StringVar(&cfg.ddsketchMetric, "ddsketch-metric", cfg.ddsketchMetric, "Metric name for the DDSketch-backed histogram")
	flag.StringVar(&cfg.histogramMetric, "histogram-metric", cfg.histogramMetric, "Metric name for the comparison histogram (default SDK aggregation)")
	flag.StringVar(&cfg.requestsMetric, "requests-metric", cfg.requestsMetric, "Metric name for the emitted request counter")
	flag.StringVar(&cfg.serviceName, "service", cfg.serviceName, "Service.name resource attribute")
	flag.Float64Var(&cfg.latencyMean, "latency-mean", cfg.latencyMean, "Mean latency in milliseconds for generated samples")
	flag.Float64Var(&cfg.latencyStdDev, "latency-stddev", cfg.latencyStdDev, "Latency standard deviation in milliseconds")
	flag.Parse()

	if cfg.workers <= 0 {
		cfg.workers = 1
	}
	if cfg.latencyStdDev < 0 {
		cfg.latencyStdDev = 0
	}

	cfg.resourceAttrs = append(cfg.resourceAttrs, semconv.ServiceName(cfg.serviceName))
	cfg.additionalAttrs = []attribute.KeyValue{
		attribute.String("app", "ddsketch-load"),
		attribute.String("env", "development"),
		attribute.String("region", "us-central1"),
		attribute.String("zone", "us-central1-a"),
		attribute.String("team", "telemetry"),
		attribute.String("version", "v1"),
	}

	return cfg
}

func run(ctx context.Context, cfg appConfig) error {
	mp, shutdown, err := buildMeterProvider(ctx, cfg)
	if err != nil {
		return err
	}
	defer shutdown()

	go reportRuntimeStats(ctx, 5*time.Second)

	meter := mp.Meter("ddsketch.load")

	log.Printf("starting DDSketch stress generator -> %s (workers=%d target-rate=%d/s)", cfg.endpoint, cfg.workers, cfg.ratePerSecond)
	if err := generateLoad(ctx, meter, cfg); err != nil {
		return err
	}
	log.Println("ddscketch generator stopped")
	return nil
}

func buildMeterProvider(ctx context.Context, cfg appConfig) (*sdkmetric.MeterProvider, func(), error) {
	clientOpts := []otlpmetricgrpc.Option{
		otlpmetricgrpc.WithEndpoint(cfg.endpoint),
		otlpmetricgrpc.WithDialOption(grpc.WithBlock()),
	}
	if cfg.insecure {
		clientOpts = append(clientOpts, otlpmetricgrpc.WithInsecure())
	}

	exp, err := otlpmetricgrpc.New(ctx, clientOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("build exporter: %w", err)
	}

	reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(cfg.exportInterval))
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, cfg.resourceAttrs...),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("build resource: %w", err)
	}

	ddsketchView := sdkmetric.NewView(
		sdkmetric.Instrument{
			Name: cfg.ddsketchMetric,
			Kind: sdkmetric.InstrumentKindHistogram,
		},
		sdkmetric.Stream{
			Aggregation: sdkmetric.AggregationDDSketch{RelativeAccuracy: cfg.ddsketchAccuracy},
		},
	)

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
		sdkmetric.WithView(ddsketchView),
	)
	otel.SetMeterProvider(mp)

	shutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := mp.Shutdown(ctx); err != nil {
			log.Printf("meter provider shutdown error: %v", err)
		}
	}
	return mp, shutdown, nil
}

func generateLoad(ctx context.Context, meter metric.Meter, cfg appConfig) error {
	// Metrics emitted:
	//   - cfg.ddsketchMetric: latency values aggregated with DDSketch via the view set in buildMeterProvider.
	//   - cfg.histogramMetric: the same latency stream using the default SDK histogram for comparison.
	//   - cfg.requestsMetric: a counter tracking how many synthetic requests (latency samples) each worker emits.
	ddsketchHist, err := meter.Float64Histogram(
		cfg.ddsketchMetric,
		metric.WithUnit(cfg.metricUnit),
		metric.WithDescription("Generated latency distribution using DDSketch aggregation"),
	)
	if err != nil {
		return fmt.Errorf("create ddsketch histogram: %w", err)
	}

	rawHist, err := meter.Float64Histogram(
		cfg.histogramMetric,
		metric.WithUnit(cfg.metricUnit),
		metric.WithDescription("Baseline histogram using SDK defaults"),
	)
	if err != nil {
		return fmt.Errorf("create histogram: %w", err)
	}

	reqCounter, err := meter.Int64Counter(
		cfg.requestsMetric,
		metric.WithDescription("Synthetic request counter to correlate with histograms"),
	)
	if err != nil {
		return fmt.Errorf("create counter: %w", err)
	}

	perWorkerRate := cfg.ratePerWorker()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < cfg.workers; i++ {
		workerID := i
		g.Go(func() error {
			return workerLoop(ctx, workerID, perWorkerRate, cfg, ddsketchHist, rawHist, reqCounter)
		})
	}
	return g.Wait()
}

func workerLoop(
	ctx context.Context,
	id int,
	perWorkerRate int,
	cfg appConfig,
	ddsketchHist metric.Float64Histogram,
	rawHist metric.Float64Histogram,
	counter metric.Int64Counter,
) error {
	var ticker *time.Ticker
	if perWorkerRate > 0 {
		interval := time.Second / time.Duration(perWorkerRate)
		if interval == 0 {
			interval = time.Nanosecond
		}
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}

	rnd := rand.New(rand.NewPCG(uint64(time.Now().UnixNano())+uint64(id*13), uint64(id*7+1)))
	attrs := append([]attribute.KeyValue{}, cfg.additionalAttrs...)
	attrs = append(attrs, attribute.String("worker.id", fmt.Sprintf("worker-%02d", id)))

	for {
		if ticker != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		value := sampleLatency(rnd, cfg.latencyMean, cfg.latencyStdDev)
		opts := metric.WithAttributes(attrs...)
		ddsketchHist.Record(ctx, value, opts)
		rawHist.Record(ctx, value, opts)
		counter.Add(ctx, 1, opts)
	}
}

func sampleLatency(r *rand.Rand, mean, stdDev float64) float64 {
	if stdDev <= 0 {
		return math.Max(mean, 0)
	}
	v := r.NormFloat64()*stdDev + mean
	if v < 0 {
		return 0
	}
	return v
}

func (c appConfig) ratePerWorker() int {
	if c.ratePerSecond <= 0 || c.workers <= 0 {
		return 0
	}
	rate := c.ratePerSecond / c.workers
	if rate == 0 {
		return 1
	}
	return rate
}

func envOrDefault(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func reportRuntimeStats(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var usage unix.Rusage
	lastCPU := time.Duration(0)
	lastWall := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)

			if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
				log.Printf("runtime stats: getrusage failed: %v", err)
				continue
			}

			cpuUser := time.Duration(usage.Utime.Sec)*time.Second + time.Duration(usage.Utime.Usec)*time.Microsecond
			cpuSys := time.Duration(usage.Stime.Sec)*time.Second + time.Duration(usage.Stime.Usec)*time.Microsecond
			totalCPU := cpuUser + cpuSys

			wallNow := time.Now()
			wallElapsed := wallNow.Sub(lastWall)
			cpuElapsed := totalCPU - lastCPU
			cpuPercent := 0.0
			if wallElapsed > 0 {
				cpuPercent = 100 * float64(cpuElapsed) / float64(wallElapsed)
			}

			log.Printf("runtime stats: heap_alloc=%.2fMB rss≈%.2fMB goroutines=%d cpu=%.1f%%",
				float64(mem.Alloc)/1024.0/1024.0,
				float64(mem.Sys)/1024.0/1024.0,
				runtime.NumGoroutine(),
				cpuPercent,
			)

			lastCPU = totalCPU
			lastWall = wallNow
		}
	}
}
