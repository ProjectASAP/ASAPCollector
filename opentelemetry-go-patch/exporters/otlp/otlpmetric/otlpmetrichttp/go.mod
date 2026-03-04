module go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp

go 1.24.0

retract v0.32.2 // Contains unresolvable dependencies.

require (
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/otel/sdk/metric v1.38.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.opentelemetry.io/otel => ../../../..

replace go.opentelemetry.io/otel/sdk => ../../../../sdk

replace go.opentelemetry.io/otel/sdk/metric => ../../../../sdk/metric

replace go.opentelemetry.io/otel/metric => ../../../../metric

replace go.opentelemetry.io/otel/trace => ../../../../trace

replace go.opentelemetry.io/proto/otlp => ../../../../../opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp
