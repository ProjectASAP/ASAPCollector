package main

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	metricapi "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Minimal OTLP metric sender for exercising the sketchmetrics processor.
func main() {
	ctx := context.Background()

	exp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint("localhost:4317"),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		log.Fatalf("create exporter: %v", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("demo-service"),
			attribute.String("region", "us-east"),
		),
	)
	if err != nil {
		log.Fatalf("create resource: %v", err)
	}

	provider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(exp, metric.WithInterval(2*time.Second))),
	)
	defer provider.Shutdown(ctx) // best-effort shutdown

	otel.SetMeterProvider(provider)
	meter := provider.Meter("demo-sender")
	counter, err := meter.Float64Counter("requests_total")
	if err != nil {
		log.Fatalf("create counter: %v", err)
	}

	attrs := []attribute.KeyValue{
		attribute.String("method", "GET"),
		attribute.String("status", "200"),
	}
	counter.Add(ctx, 5, metricapi.WithAttributes(attrs...))

	// allow export to run
	time.Sleep(3 * time.Second)
}
