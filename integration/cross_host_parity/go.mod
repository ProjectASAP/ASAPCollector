module github.com/ProjectASAP/ASAPCollector/integration/cross_host_parity

go 1.25.5

// Phase 5 step E (cross-host envelope parity) deps.
//
// In fixture mode the test reproduces the cross-language gate (#243)
// canonical envelope bytes inline using sketchlib-go's
// SerializePortable* helpers — the same calls integration/parity/
// golden_test.go makes — and asserts byte-equality across agent pairs
// against those bytes. This makes the test self-contained: it does
// NOT depend on integration/parity/golden/*.bin being regenerated
// separately. (Optional: if those fixtures ARE present, the test
// also cross-checks that the inline regen agrees with the on-disk
// fixture, surfacing a fixture-drift regression early.)
//
// In binary mode the test reads per-agent capture files written by
// run_parity.sh and asserts equality.

require (
	github.com/ProjectASAP/sketchlib-go v0.0.0-20260328221809-b24e56e64e94
	google.golang.org/protobuf v1.36.11
)

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
)

// sketchlib-go is a private module; the local sibling checkout lives
// next to the ASAPCollector repo. Path is relative to this go.mod.
replace github.com/ProjectASAP/sketchlib-go => ../../../sketchlib-go
