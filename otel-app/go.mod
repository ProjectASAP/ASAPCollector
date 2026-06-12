module github.com/approx-telemetry/otel-app

go 1.25.0

require (
	github.com/ProjectASAP/asap-precompute-go v0.0.0-00010101000000-000000000000
	github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.41.0
	go.opentelemetry.io/otel/metric v1.43.0
	go.opentelemetry.io/otel/sdk v1.41.0
	go.opentelemetry.io/otel/sdk/metric v1.41.0
	go.opentelemetry.io/proto/otlp v1.9.0
	go.yaml.in/yaml/v2 v2.4.3
	google.golang.org/grpc v1.79.1
	google.golang.org/protobuf v1.36.11
)

replace (
	github.com/ProjectASAP/asap-precompute-go => ../asap-precompute-go
	github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient => ../asap-precompute-go/monitor/grpcclient
)

require (
	github.com/ProjectASAP/sketchlib-go v0.0.0-20260527012450-98a522055fc7 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/prometheus v0.307.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260209200024-4cfbd4190f57 // indirect
)

// The `opentelemetry-go` submodule is kept in the repo with
// `opentelemetry-go-patch/` applied on top (via
// `restore_opentelemetry_go_patches.sh`). That combined tree is the
// source of truth — build against it directly. The patched sdk/metric
// carries AggregationDDSketch/KLLSketch/CountSketch/CountMinSketch/HLLSketch
// + AggregationRawBuffer + DeltaTransmission flags; see
// docs/sdk-cost-evaluation.md. This module lives one level under the repo
// root (otel-app/), a sibling of the opentelemetry-go / opentelemetry-proto
// submodules (so those replaces are a single `../` deep). sketchlib-go is a
// local checkout at the workspace root (one level above the repo root), so it
// is `../../sketchlib-go` — matching the path opentelemetry-app used.
replace (
	github.com/ProjectASAP/sketchlib-go => ../../sketchlib-go
	go.opentelemetry.io/otel => ../opentelemetry-go
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc => ../opentelemetry-go/exporters/otlp/otlpmetric/otlpmetricgrpc
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp => ../opentelemetry-go/exporters/otlp/otlpmetric/otlpmetrichttp
	go.opentelemetry.io/otel/metric => ../opentelemetry-go/metric
	go.opentelemetry.io/otel/sdk => ../opentelemetry-go/sdk
	go.opentelemetry.io/otel/sdk/metric => ../opentelemetry-go/sdk/metric
	go.opentelemetry.io/otel/trace => ../opentelemetry-go/trace
	// The patched OTLP proto bindings (mpb.DDSketch / KLLSketch / CountSketch
	// / CountMinSketch / HLLSketch types added on top of upstream v1.9.0)
	// are regenerated under opentelemetry-proto/gen/go/... by
	// restore_otel_proto_patches.sh. See opentelemetry-proto-patch/REGEN.md.
	go.opentelemetry.io/proto/otlp => ../opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp
)
