module github.com/ProjectASAP/asap-precompute-go

go 1.24.0

require (
	github.com/ProjectASAP/sketchlib-go v0.0.0-20260527012450-98a522055fc7
	github.com/influxdata/telegraf v1.34.4
	go.opentelemetry.io/collector/pdata v1.42.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/prometheus v0.307.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.47.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.30.0 // indirect
)

// Use the local sketchlib-go checkout — same approach the OTel
// processors use. Path is relative to asap-precompute-go.
replace github.com/ProjectASAP/sketchlib-go => ../../sketchlib-go

// Use the patched pdata that exposes the modified-OTLP sketch
// data variants (DDSketch / KLLSketch / HLLSketch / CountSketch /
// CountMinSketch). Path is relative to asap-precompute-go.
replace go.opentelemetry.io/collector/pdata => ../opentelemetry-collector/pdata
