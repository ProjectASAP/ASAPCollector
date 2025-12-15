module github.com/approx-telemetry/opentelemetry-app

go 1.24.0

require (
	go.opentelemetry.io/otel v1.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.38.0
	go.opentelemetry.io/otel/metric v1.38.0
	go.opentelemetry.io/otel/sdk v1.38.0
	go.opentelemetry.io/otel/sdk/metric v1.38.0
	golang.org/x/sync v0.18.0
	golang.org/x/sys v0.38.0
	google.golang.org/grpc v1.77.0
)

require (
	github.com/DataDog/sketches-go v1.4.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.3 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/trace v1.39.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace (
	go.opentelemetry.io/otel => ../opentelemetry-go
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc => ../opentelemetry-go/exporters/otlp/otlpmetric/otlpmetricgrpc
	go.opentelemetry.io/otel/metric => ../opentelemetry-go/metric
	go.opentelemetry.io/otel/sdk => ../opentelemetry-go/sdk
	go.opentelemetry.io/otel/sdk/metric => ../opentelemetry-go/sdk/metric
	go.opentelemetry.io/proto/otlp => ../opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp
)
