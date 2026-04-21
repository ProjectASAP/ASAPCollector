// fake-exporter — replay-style synthetic OTLP metrics producer.
//
// Feeds the ASAP agent/gateway stack at configurable rate and
// cardinality. Paper §6.1: observability-only workloads (see
// docs/paper-outline.md §non-goals), so this producer is a
// stand-in for a Google-cluster / Alibaba trace replayer until
// a real replayer lands.
//
// Emits two metrics per tick:
//
//   1. <metric>_latency_ms : Gauge. Log-normal distribution,
//      which is the canonical latency shape. DDSketch + HLL both
//      require Gauge input — Counter/Sum inputs would be
//      sketch-pipeline no-ops. This is what exercises the
//      quantile and distinct-count sketches for §5 figures.
//
//   2. <metric> : Counter. Preserved for backwards-compat so
//      existing dashboards / raw-ingest baselines still see a
//      monotonic time series.
//
// Env config:
//
//   EXPORTER_TARGET       — OTLP/gRPC endpoint (default gateway:4317)
//   EXPORTER_METRIC       — base metric name (default http_requests_total)
//   EXPORTER_RATE         — emits/sec per metric (default 10)
//   EXPORTER_CARDINALITY  — distinct label sets per emit (default 100)
//
// Fast to rebuild, small image (<30 MB runtime), fits cleanly
// into the paper's docker-compose / Helm eval stack.

package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
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

func main() {
	target := envOr("EXPORTER_TARGET", "gateway:4317")
	metricName := envOr("EXPORTER_METRIC", "http_requests_total")
	rate := envInt("EXPORTER_RATE", 10)
	cardinality := envInt("EXPORTER_CARDINALITY", 100)

	log.Printf("fake-exporter starting: target=%s metric=%s rate=%d/sec cardinality=%d",
		target, metricName, rate, cardinality)

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
	reader := sdkmetric.NewPeriodicReader(exp,
		sdkmetric.WithInterval(time.Second/time.Duration(rate)))
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	defer provider.Shutdown(ctx)
	otel.SetMeterProvider(provider)

	meter := provider.Meter("asap.fake-exporter")
	counter, err := meter.Float64Counter(metricName,
		metric.WithDescription("Synthetic counter — raw-baseline signal"))
	if err != nil {
		log.Fatalf("counter init: %v", err)
	}

	// DDSketch + HLL processors both accept only Gauge inputs;
	// Counter / Sum values would be pipeline no-ops. Register a
	// Float64Gauge we push per-tick with a log-normal draw, which
	// is the textbook latency-distribution shape.
	latencyName := metricName + "_latency_ms"
	latencyGauge, err := meter.Float64Gauge(latencyName,
		metric.WithDescription("Synthetic log-normal latency — sketch-baseline signal"),
		metric.WithUnit("ms"))
	if err != nil {
		log.Fatalf("gauge init: %v", err)
	}

	// Pre-compute the label sets so the hot loop is allocation-free.
	labelSets := make([][]attribute.KeyValue, cardinality)
	for i := 0; i < cardinality; i++ {
		labelSets[i] = []attribute.KeyValue{
			attribute.String("zone", fmt.Sprintf("z%d", i%4)),
			attribute.String("pod", fmt.Sprintf("pod-%d", i)),
		}
	}

	// Track periodically-advertised counter values; Poisson-ish spikes
	// per-label-set so Prometheus query output has some shape.
	values := make([]float64, cardinality)
	var mu sync.Mutex

	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()
	for range tick.C {
		mu.Lock()
		for i := 0; i < cardinality; i++ {
			values[i] += rand.ExpFloat64() // exponential increments
			counter.Add(ctx, values[i], metric.WithAttributes(labelSets[i]...))

			// Log-normal(mu=3, sigma=0.7) ≈ median ~20ms, P99 ~100ms —
			// typical of a web-service latency distribution. DDSketch
			// sees each sample individually; HLL sees each distinct
			// float64 once per window.
			latencyMs := math.Exp(3.0 + 0.7*rand.NormFloat64())
			latencyGauge.Record(ctx, latencyMs, metric.WithAttributes(labelSets[i]...))
		}
		mu.Unlock()
	}

	// Silence unused imports when GOCACHE / build cache changes
	// — the `metricdata` import wires through the SDK types we
	// reference implicitly via the meter provider.
	var _ metricdata.ResourceMetrics
}
