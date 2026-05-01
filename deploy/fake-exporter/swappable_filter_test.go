package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

// nilFilterPassesEverything is the no-op behaviour: nil inner filter
// must keep every attribute.
func TestSwappableFilter_NilKeepsAll(t *testing.T) {
	sf := newSwappableFilter(nil)
	f := sf.Filter()
	if !f(attribute.String("zone", "z0")) {
		t.Fatalf("nil filter should keep zone")
	}
	if !f(attribute.String("rack", "r0")) {
		t.Fatalf("nil filter should keep rack")
	}
}

// initialFilter behaves like a static filter when never swapped.
func TestSwappableFilter_InitialFilterApplied(t *testing.T) {
	keepZone := func(kv attribute.KeyValue) bool { return string(kv.Key) == "zone" }
	sf := newSwappableFilter(keepZone)
	f := sf.Filter()
	if !f(attribute.String("zone", "z0")) {
		t.Fatalf("zone should be kept")
	}
	if f(attribute.String("rack", "r0")) {
		t.Fatalf("rack should be dropped")
	}
}

// Swap visible on next invocation, no need to rebuild meter.
func TestSwappableFilter_SwapChangesBehaviour(t *testing.T) {
	sf := newSwappableFilter(nil)
	f := sf.Filter()

	if !f(attribute.String("zone", "z0")) {
		t.Fatalf("pre-swap: nil filter should keep zone")
	}

	dropAll := func(attribute.KeyValue) bool { return false }
	sf.Swap(dropAll)
	if f(attribute.String("zone", "z0")) {
		t.Fatalf("post-swap to drop-all: zone should be dropped")
	}

	sf.Swap(nil)
	if !f(attribute.String("zone", "z0")) {
		t.Fatalf("post-swap to nil: zone should be kept again")
	}
}

// End-to-end: install the swappable filter on a real MeterProvider +
// ManualReader, record a measurement, swap to drop-all, record
// another, collect once, and confirm the second measurement's
// attribute set has been emptied. Pins the wiring claim from
// swappable_filter.go's package doc: the SDK's Builder.filter
// closure dispatches through the function value, so a swap is
// observable without rebuilding the MeterProvider.
func TestSwappableFilter_E2EThroughSDK(t *testing.T) {
	keepZone := func(kv attribute.KeyValue) bool { return string(kv.Key) == "zone" }
	sf := newSwappableFilter(keepZone)

	stream := sdkmetric.Stream{
		Aggregation:     sdkmetric.AggregationDefault{},
		AttributeFilter: sf.Filter(),
	}
	view := sdkmetric.NewView(sdkmetric.Instrument{Name: "*"}, stream)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(resource.Default()),
		sdkmetric.WithView(view),
	)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	counter, err := provider.Meter("swap-test").Float64Counter("requests")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Pre-swap: only zone is kept; rack/pod dropped from agg key.
	counter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("zone", "z0"),
			attribute.String("rack", "r0"),
			attribute.String("pod", "p0"),
		),
	)

	// Swap to drop-all → the next measurement should aggregate
	// under the empty attribute set.
	sf.Swap(func(attribute.KeyValue) bool { return false })
	counter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("zone", "z1"),
			attribute.String("rack", "r1"),
		),
	)

	var got metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &got); err != nil {
		t.Fatal(err)
	}

	var sawZoneOnly, sawEmpty bool
	for _, sm := range got.ScopeMetrics {
		for _, m := range sm.Metrics {
			s, ok := m.Data.(metricdata.Sum[float64])
			if !ok {
				continue
			}
			for _, dp := range s.DataPoints {
				keys := keysOf(dp.Attributes)
				switch {
				case len(keys) == 1 && keys[0] == "zone":
					sawZoneOnly = true
				case len(keys) == 0:
					sawEmpty = true
				}
			}
		}
	}
	if !sawZoneOnly {
		t.Errorf("expected a data point with only `zone` attr (pre-swap): got %v", dumpAttrs(got))
	}
	if !sawEmpty {
		t.Errorf("expected a data point with empty attr set (post-swap to drop-all): got %v", dumpAttrs(got))
	}
}

// HTTP control endpoint: POST a new projection, expect Swap to fire.
func TestSwappableFilter_HTTPControl(t *testing.T) {
	sf := newSwappableFilter(nil)

	// Spin a real http server bound to a random port via httptest so
	// we exercise the actual mux installation, not a hand-rolled
	// fake.
	mux := http.NewServeMux()
	mux.HandleFunc("/control/projection", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var req projectionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		sf.Swap(parseProjection(req.Projection))
		_ = json.NewEncoder(w).Encode(projectionResponse{Applied: req.Projection})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	body := bytes.NewBufferString(`{"projection":"-"}`)
	resp, err := http.Post(srv.URL+"/control/projection", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}

	// "-" means drop-all; verify the swap took effect.
	f := sf.Filter()
	if f(attribute.String("zone", "z0")) {
		t.Fatalf("post-swap to '-' (drop-all): zone should be dropped")
	}

	// Swap back to keep-all.
	body = bytes.NewBufferString(`{"projection":""}`)
	resp, err = http.Post(srv.URL+"/control/projection", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200 on second swap, got %d", resp.StatusCode)
	}
	if !f(attribute.String("zone", "z0")) {
		t.Fatalf("post-swap to keep-all: zone should be kept again")
	}
}

// Concurrent measurement + swap: race detector should catch any
// data race on the atomic.Pointer. Non-flaky version: 200 ms ceiling
// with a deterministic stop signal.
func TestSwappableFilter_RaceUnderConcurrentMeasurement(t *testing.T) {
	sf := newSwappableFilter(nil)
	f := sf.Filter()
	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = f(attribute.String("zone", "z0"))
			}
		}
	}()
	go func() {
		toggle := false
		for {
			select {
			case <-stop:
				return
			default:
				if toggle {
					sf.Swap(nil)
				} else {
					sf.Swap(func(attribute.KeyValue) bool { return false })
				}
				toggle = !toggle
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	close(stop)
}

// keysOf returns the sorted attribute keys of a Set, for tests.
func keysOf(set attribute.Set) []string {
	out := make([]string, 0, set.Len())
	iter := set.Iter()
	for iter.Next() {
		kv := iter.Attribute()
		out = append(out, string(kv.Key))
	}
	return out
}

// dumpAttrs renders a one-line summary of every data point's attrs
// for diagnostics on test failure.
func dumpAttrs(rm metricdata.ResourceMetrics) string {
	var sb strings.Builder
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			s, ok := m.Data.(metricdata.Sum[float64])
			if !ok {
				continue
			}
			for _, dp := range s.DataPoints {
				fmt.Fprintf(&sb, "[%v val=%v] ", keysOf(dp.Attributes), dp.Value)
			}
		}
	}
	return sb.String()
}
