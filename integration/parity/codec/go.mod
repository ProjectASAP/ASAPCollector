module github.com/ProjectASAP/ASAPCollector/integration/parity/codec

go 1.25.5

require (
	github.com/ProjectASAP/asap-precompute-go v0.0.0-00010101000000-000000000000
	github.com/ProjectASAP/sketchlib-go v0.0.0-20260328221809-b24e56e64e94
	github.com/influxdata/telegraf v1.34.4
	go.opentelemetry.io/collector/pdata v1.54.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/hashicorp/go-version v1.8.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/prometheus v0.307.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.opentelemetry.io/collector/featuregate v1.47.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/text v0.30.0 // indirect
)

// Local checkouts mirror the cascade used by
// integration/parity/runtime-impl/. Paths are relative to
// integration/parity/codec/.
replace github.com/ProjectASAP/asap-precompute-go => ../../../asap-precompute-go

replace github.com/ProjectASAP/sketchlib-go => ../../../../sketchlib-go

replace go.opentelemetry.io/collector/pdata => ../../../opentelemetry-collector/pdata
