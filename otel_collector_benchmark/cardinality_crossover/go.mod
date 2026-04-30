module github.com/ProjectASAP/datacollector/otel_collector_benchmark/cardinality_crossover

go 1.24.0

require github.com/ProjectASAP/sketchlib-go v0.0.0-00010101000000-000000000000

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/prometheus v0.307.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

// Mirror the `replaces:` pattern from
// opentelemetry-collector-contrib-patch/cmd/countminsketchcol/builder-config.yaml:
//   - github.com/ProjectASAP/sketchlib-go => ../../../../../sketchlib-go
// Here the path is computed from THIS module's location.
replace github.com/ProjectASAP/sketchlib-go => ../../../sketchlib-go
