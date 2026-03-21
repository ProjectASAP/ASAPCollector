module github.com/approx-telemetry/opentelemetry-app

go 1.24.0

require (
	github.com/ProjectASAP/sketchlib-go v0.0.0-20260321024028-d20a9f9151b5
	github.com/cespare/xxhash/v2 v2.3.0
	go.opentelemetry.io/otel v1.41.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.38.0
	go.opentelemetry.io/otel/metric v1.41.0
	go.opentelemetry.io/otel/sdk/metric v1.41.0
	google.golang.org/grpc v1.79.1
)

require (
	github.com/DataDog/sketches-go v1.4.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.28.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/prometheus v0.307.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/sdk v1.41.0 // indirect
	go.opentelemetry.io/otel/trace v1.41.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260209200024-4cfbd4190f57 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260209200024-4cfbd4190f57 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace (
	go.opentelemetry.io/otel => ../opentelemetry-go
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc => ../opentelemetry-go/exporters/otlp/otlpmetric/otlpmetricgrpc
	go.opentelemetry.io/otel/metric => ../opentelemetry-go/metric
	go.opentelemetry.io/otel/sdk => ../opentelemetry-go/sdk
	go.opentelemetry.io/otel/sdk/metric => ../opentelemetry-go/sdk/metric
	go.opentelemetry.io/proto/otlp => ../opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp
)
