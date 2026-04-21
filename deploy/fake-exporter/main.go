// fake-exporter — replay-style synthetic OTLP metrics producer.
//
// Feeds the ASAP agent/gateway stack at configurable rate and
// cardinality. Paper §6.1: observability-only workloads (see
// docs/paper-outline.md §non-goals), so this producer is a
// stand-in for a Google-cluster / Alibaba trace replayer until
// a real replayer lands. The shape is what matters for §6
// bandwidth / CPU figures:
//
//   * One Gauge named EXPORTER_METRIC_NAME (default http_requests_total)
//   * CARDINALITY distinct {zone, pod} label sets per emit
//   * One emit per (1 / RATE) seconds, ticking forever
//
// Env config:
//
//   EXPORTER_TARGET       — OTLP/gRPC endpoint (default gateway:4317)
//   EXPORTER_METRIC       — metric name (default http_requests_total)
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
		metric.WithDescription("Synthetic metric produced by fake-exporter"))
	if err != nil {
		log.Fatalf("counter init: %v", err)
	}

	// Pre-compute the label sets so the hot loop is allocation-free.
	labelSets := make([][]attribute.KeyValue, cardinality)
	for i := 0; i < cardinality; i++ {
		labelSets[i] = []attribute.KeyValue{
			attribute.String("zone", fmt.Sprintf("z%d", i%4)),
			attribute.String("pod", fmt.Sprintf("pod-%d", i)),
		}
	}

	// Track periodically-advertised values; Poisson-ish spikes
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
		}
		mu.Unlock()
	}

	// Silence unused imports when GOCACHE / build cache changes
	// — the `metricdata` import wires through the SDK types we
	// reference implicitly via the meter provider.
	var _ metricdata.ResourceMetrics
}
