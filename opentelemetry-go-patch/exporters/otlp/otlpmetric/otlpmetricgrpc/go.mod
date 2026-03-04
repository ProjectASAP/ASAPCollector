module go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc

go 1.24.0

retract v0.32.2 // Contains unresolvable dependencies.

require (
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/otel/sdk/metric v1.38.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217
	google.golang.org/grpc v1.77.0
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/DataDog/sketches-go v1.4.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.opentelemetry.io/otel => ../../../..

replace go.opentelemetry.io/otel/sdk => ../../../../sdk

replace go.opentelemetry.io/otel/sdk/metric => ../../../../sdk/metric

replace go.opentelemetry.io/otel/metric => ../../../../metric

replace go.opentelemetry.io/otel/trace => ../../../../trace

replace go.opentelemetry.io/proto/otlp => ../../../../../opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp
