module github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen

go 1.24.0

require (
	github.com/DataDog/sketches-go v1.4.7
	github.com/lightstep/go-expohisto v1.0.0
	github.com/spf13/pflag v1.0.10
	github.com/stretchr/testify v1.11.1
	go.uber.org/zap v1.27.1
	golang.org/x/time v0.13.0
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract (
	v0.76.2
	v0.76.1
	v0.65.0
)

replace (
	go.opentelemetry.io/otel => ../../../opentelemetry-go
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc => ../../../opentelemetry-go/exporters/otlp/otlpmetric/otlpmetricgrpc
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp => ../../../opentelemetry-go/exporters/otlp/otlpmetric/otlpmetrichttp
	go.opentelemetry.io/otel/sdk => ../../../opentelemetry-go/sdk
	go.opentelemetry.io/otel/sdk/metric => ../../../opentelemetry-go/sdk/metric
	go.opentelemetry.io/proto/otlp => ../../../opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp
)

// IMPORTANT NOTE: Do not add replace statements to this go.mod. This will break go install.
// See https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/27855.
