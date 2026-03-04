module go.opentelemetry.io/otel/exporters/stdout/stdouttrace

go 1.24.0

replace (
	go.opentelemetry.io/otel => ../../..
	go.opentelemetry.io/otel/sdk => ../../../sdk
)

replace go.opentelemetry.io/otel/trace => ../../../trace

replace go.opentelemetry.io/otel/metric => ../../../metric

replace go.opentelemetry.io/otel/sdk/metric => ../../../sdk/metric
