// Nested module: the CDM gRPC transport (monitorpb stubs + bidi client).
// Isolated from the core asap-precompute-go module so the gRPC dependency
// tree (envoy/spiffe/otel-sdk/...) never perturbs the runtime's carefully
// pinned graph (telegraf/pdata/sketchlib). The core runtime stays gRPC-free
// and talks to this transport through a small interface; the final collector
// binary wires the two together.
module github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient

go 1.24.0

require (
	google.golang.org/grpc v1.75.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/ProjectASAP/sketchlib-go v0.0.0-20260903015955-4b0919d3d653 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/prometheus v0.307.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
)

require (
	github.com/ProjectASAP/asap-precompute-go v0.0.0-00010101000000-000000000000
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250922171735-9219d122eba9 // indirect
)

replace github.com/ProjectASAP/asap-precompute-go => ../..

replace github.com/ProjectASAP/sketchlib-go => ../../../../sketchlib-go
