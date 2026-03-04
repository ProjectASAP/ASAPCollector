module github.com/approx-telemetry/opentelemetry-app

go 1.24.0

replace (
	go.opentelemetry.io/otel => ../opentelemetry-go
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc => ../opentelemetry-go/exporters/otlp/otlpmetric/otlpmetricgrpc
	go.opentelemetry.io/otel/metric => ../opentelemetry-go/metric
	go.opentelemetry.io/otel/sdk => ../opentelemetry-go/sdk
	go.opentelemetry.io/otel/sdk/metric => ../opentelemetry-go/sdk/metric
	go.opentelemetry.io/proto/otlp => ../opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp
)
