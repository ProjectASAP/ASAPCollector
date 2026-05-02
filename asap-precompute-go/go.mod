module github.com/ProjectASAP/asap-precompute-go

go 1.24.0

require (
	github.com/ProjectASAP/sketchlib-go v0.0.0-20260328221809-b24e56e64e94
	go.opentelemetry.io/collector/pdata v0.0.0-00010101000000-000000000000
)

require (
	github.com/hashicorp/go-version v1.7.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	go.opentelemetry.io/collector/featuregate v1.47.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

// Use the local sketchlib-go checkout — same approach the OTel
// processors use. Path is relative to asap-precompute-go.
replace github.com/ProjectASAP/sketchlib-go => ../../sketchlib-go

// Use the patched pdata that exposes the modified-OTLP sketch
// data variants (DDSketch / KLLSketch / HLLSketch / CountSketch /
// CountMinSketch). Path is relative to asap-precompute-go.
replace go.opentelemetry.io/collector/pdata => ../opentelemetry-collector/pdata
