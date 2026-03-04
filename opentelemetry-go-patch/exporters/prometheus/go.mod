module go.opentelemetry.io/otel/exporters/prometheus

go 1.24.0

// v0.59.0 produces incorrect metric names when bracketed units are used.
// https://github.com/open-telemetry/opentelemetry-go/issues/7039
retract v0.59.0

replace go.opentelemetry.io/otel => ../..

replace go.opentelemetry.io/otel/sdk => ../../sdk

replace go.opentelemetry.io/otel/sdk/metric => ../../sdk/metric

replace go.opentelemetry.io/otel/trace => ../../trace

replace go.opentelemetry.io/otel/metric => ../../metric
