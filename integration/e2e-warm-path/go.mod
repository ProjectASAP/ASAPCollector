module github.com/ProjectASAP/ASAPCollector/integration/e2e-warm-path

go 1.25.5

// Warm-path e2e integration test deps.
//
// Kept INTENTIONALLY MINIMAL — std library only — so this test is
// portable across CI environments and does not need the patched OTel
// collector replace cascade that integration/parity/runtime-impl/
// requires. The test pushes OTLP/HTTP+JSON directly (no client lib)
// and parses PromQL JSON responses from the backend's HTTP query
// surface (no Prometheus client lib).
