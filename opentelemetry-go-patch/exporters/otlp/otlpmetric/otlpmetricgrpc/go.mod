module go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc

go 1.24.0

retract v0.32.2 // Contains unresolvable dependencies.

require (
	github.com/cenkalti/backoff/v5 v5.0.3
	github.com/google/go-cmp v0.7.0
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/otel v1.41.0
	go.opentelemetry.io/otel/sdk v1.41.0
	go.opentelemetry.io/otel/sdk/metric v1.39.0
	go.opentelemetry.io/proto/otlp v1.9.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260209200024-4cfbd4190f57
	google.golang.org/grpc v1.79.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/ProjectASAP/asap-precompute-go v0.0.0-00010101000000-000000000000 // indirect
	github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient v0.0.0-00010101000000-000000000000 // indirect
	github.com/ProjectASAP/sketchlib-go v0.0.0-20260903011836-148643fae428 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/prometheus v0.307.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.41.0 // indirect
	go.opentelemetry.io/otel/trace v1.41.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.opentelemetry.io/otel => ../../../..

replace go.opentelemetry.io/otel/sdk => ../../../../sdk

replace go.opentelemetry.io/otel/sdk/metric => ../../../../sdk/metric

replace go.opentelemetry.io/otel/metric => ../../../../metric

replace go.opentelemetry.io/otel/trace => ../../../../trace

replace go.opentelemetry.io/proto/otlp => ../../../../../opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp

replace github.com/ProjectASAP/sketchlib-go => ../../../../../../sketchlib-go

replace github.com/ProjectASAP/asap-precompute-go => ../../../../../asap-precompute-go

replace github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient => ../../../../../asap-precompute-go/monitor/grpcclient
