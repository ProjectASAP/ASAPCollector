// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	oteladapter "github.com/ProjectASAP/asap-precompute-go/otel"
	"github.com/ProjectASAP/sketchlib-go/wire/asapmsgpack"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// TestEmitHeap_CountSketchHeapRoundTrips drives the fused asap_edge warm
// CountSketch aggregator with emit_heap=true + item_label=endpoint and asserts
// the emitted CountSketch envelope (a) is tagged the MSGPACK encoding (so the
// otel adapter maps it to CountSketchEncodingMsgpack and the backend's
// sketch_kind_handle_for promotes the sid to CountSketchWithHeap /
// FrequencyTopk) and (b) carries a heap-bearing payload that round-trips
// through the SAME wire decode the ASAPQuery backend uses
// (CountMinSketchWithHeap::from_msgpack, mirrored Go-side by
// asapmsgpack.UnmarshalCountSketchWithHeap) with a NON-EMPTY heap that ranks
// the heaviest endpoint first — the gate that makes a warm
// `topk(top_endpoint_qps)` query resolve instead of returning "No result".
func TestEmitHeap_CountSketchHeapRoundTrips(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics: []MetricFamily{{
			Metric:    "top_endpoint_qps",
			Family:    FamilyCountSketch,
			EmitHeap:  true,
			ItemLabel: "endpoint",
			// Wide matrix to drive down the signed-CountSketch estimate variance
			// so the median-of-rows estimate ranks the items by true count. The
			// large count separation below (checkout >> cart >> home) keeps the
			// ranking stable against the residual CS noise. 4*log2(8192)=52 stays
			// within the 64-bit per-item row-hash budget NewCountSketchWrapper
			// enforces (5*13=65 would exceed it).
			Rows: 4,
			Cols: 8192,
		}},
		Cold: ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	// Validate must have defaulted the heap size.
	if cfg.Metrics[0].HeapSize != 100 {
		t.Fatalf("emit_heap should default heap_size to 100, got %d", cfg.Metrics[0].HeapSize)
	}

	sa, ok := newSketchAggregator("top_endpoint_qps", &cfg.Metrics[0], sketchOpts{window: time.Hour}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator(CountSketch+emit_heap) returned ok=false")
	}
	// The factory must build the heap-bearing config: msgpack encoding + the
	// item-keyed observe path. Without these the payload is plain proto and the
	// backend never promotes the sid.
	if sa.pcfg.Encoding != precompute.EncodingMsgpack {
		t.Fatalf("emit_heap must set PrecomputeConfig.Encoding=MSGPACK, got %v", sa.pcfg.Encoding)
	}
	if sa.obsKind != obsKindKeyedItem {
		t.Fatalf("emit_heap must use obsKindKeyedItem, got %v", sa.obsKind)
	}

	// Feed gauge points with distinct endpoint values; /checkout is hottest.
	// Fixed feed order (a slice, not a map) so the producer's Space-Saving
	// candidate retention is deterministic across runs.
	feed := []struct {
		ep string
		n  int
	}{{"/checkout", 100}, {"/cart", 40}, {"/home", 10}}
	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	tick := uint64(0)
	for _, f := range feed {
		for i := 0; i < f.n; i++ {
			sa.observe(map[string]string{"endpoint": f.ep}, 1.0, base+tick)
			tick++
		}
	}
	if sa.lastObserveErr != nil {
		t.Fatalf("observe errored — value-kind regressed: %v", sa.lastObserveErr)
	}

	envs := sa.pc.Drain()

	// Locate the CountSketch envelope and assert the host-neutral encoding tag.
	var heapEnv *precompute.SketchEnvelope
	for _, env := range envs {
		if env.SketchType == precompute.SketchTypeCountSketch && len(env.Payload) > 0 {
			heapEnv = env
			break
		}
	}
	if heapEnv == nil {
		t.Fatal("no CountSketch envelope with a non-empty payload was emitted")
	}
	// First window per series ships a FULL frame → MSGPACK (not MSGPACK_DELTA).
	if heapEnv.Encoding != precompute.EncodingMsgpack {
		t.Fatalf("first-window heap envelope must be tagged MSGPACK, got %v", heapEnv.Encoding)
	}

	// Round-trip the payload through the backend's heap wire decode.
	rows, cols, _, heap, heapSize, err := asapmsgpack.UnmarshalCountSketchWithHeap(heapEnv.Payload)
	if err != nil {
		t.Fatalf("payload must decode via the backend's heap wire format: %v", err)
	}
	if rows == 0 || cols == 0 {
		t.Fatalf("decoded matrix dims must be positive, got rows=%d cols=%d", rows, cols)
	}
	if heapSize != 100 {
		t.Fatalf("decoded heap_size = %d, want 100", heapSize)
	}
	if len(heap) == 0 {
		t.Fatal("heap must be non-empty (backend promotion gate to CountSketchWithHeap)")
	}
	// All three endpoints land in ONE heap (the series-collapse for emit_heap),
	// ranked descending by count: /checkout (100) > /cart (40) > /home (10). This
	// is the heavy-hitter ranking a warm topk(...) query reads.
	if len(heap) != 3 {
		t.Fatalf("heap must rank all 3 endpoints together, got %d items: %v", len(heap), heap)
	}
	if heap[0].Key != "/checkout" || heap[1].Key != "/cart" || heap[2].Key != "/home" {
		t.Fatalf("heap ranking = %v, want /checkout > /cart > /home", heap)
	}
	for i := 1; i < len(heap); i++ {
		if heap[i-1].Value < heap[i].Value {
			t.Fatalf("heap must be descending by count, got %v", heap)
		}
	}

	// End-to-end through the otel adapter: the emitted pmetric DP must be tagged
	// CountSketchEncodingMsgpack so the backend's modified-OTLP router reads it
	// as a heap-bearing CountSketch.
	md, err := oteladapter.Encode(envs, sa.enc)
	if err != nil {
		t.Fatalf("oteladapter.Encode: %v", err)
	}
	var (
		found   bool
		encEnum pmetric.CountSketchEncoding
	)
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				if m.Type() != pmetric.MetricTypeCountSketch {
					continue
				}
				dps := m.CountSketch().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					encEnum = dps.At(l).Encoding()
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("otel encode produced no CountSketch data point")
	}
	if encEnum != pmetric.CountSketchEncodingMsgpack {
		t.Fatalf("emitted CountSketch DP encoding = %v, want CountSketchEncodingMsgpack", encEnum)
	}
}

// TestEmitHeap_DeltaFrameAfterFirstWindow asserts the delta path: with
// emit_heap + delta_transmission, the first window ships a full MSGPACK heap
// frame and the second ships a MSGPACK_DELTA frame (sparse matrix delta + full
// top-k heap) that still carries a non-empty, correctly-ranked heap.
func TestEmitHeap_DeltaFrameAfterFirstWindow(t *testing.T) {
	deltaOn := true
	cfg := &Config{
		ShardCount:        1,
		WindowDuration:    time.Hour,
		DeltaTransmission: true,
		Metrics: []MetricFamily{{
			Metric:            "top_endpoint_qps",
			Family:            FamilyCountSketch,
			EmitHeap:          true,
			ItemLabel:         "endpoint",
			DeltaTransmission: &deltaOn,
		}},
		Cold: ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	opts := sketchOpts{window: time.Hour, delta: cfg.Metrics[0].effectiveDelta(cfg.DeltaTransmission)}
	if !opts.delta {
		t.Fatal("effectiveDelta should be true for CountSketch with delta_transmission")
	}
	sa, ok := newSketchAggregator("top_endpoint_qps", &cfg.Metrics[0], opts, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator returned ok=false")
	}

	feed := func(start uint64) {
		counts := map[string]int{"/checkout": 100, "/cart": 40, "/home": 10}
		tick := start
		for ep, n := range counts {
			for i := 0; i < n; i++ {
				sa.observe(map[string]string{"endpoint": ep}, 1.0, tick)
				tick++
			}
		}
	}

	base := uint64(time.Unix(1700000000, 0).UnixMilli())

	// Window 1: full MSGPACK heap frame.
	feed(base)
	env1 := drainOneCountSketch(t, sa.pc.Drain())
	if env1.Encoding != precompute.EncodingMsgpack {
		t.Fatalf("window 1 must be MSGPACK (full), got %v", env1.Encoding)
	}
	if _, _, _, heap, _, err := asapmsgpack.UnmarshalCountSketchWithHeap(env1.Payload); err != nil || len(heap) == 0 {
		t.Fatalf("window 1 full frame must round-trip with a non-empty heap (err=%v, heapLen=%d)", err, len(heap))
	}

	// Window 2: MSGPACK_DELTA frame (sparse matrix delta + full heap).
	feed(base + 1_000_000)
	env2 := drainOneCountSketch(t, sa.pc.Drain())
	if env2.Encoding != precompute.EncodingMsgpackDelta {
		t.Fatalf("window 2 must be MSGPACK_DELTA, got %v", env2.Encoding)
	}
}

func drainOneCountSketch(t *testing.T, envs []*precompute.SketchEnvelope) *precompute.SketchEnvelope {
	t.Helper()
	for _, env := range envs {
		if env.SketchType == precompute.SketchTypeCountSketch && len(env.Payload) > 0 {
			return env
		}
	}
	t.Fatal("no CountSketch envelope with a non-empty payload was emitted")
	return nil
}

// TestEmitHeap_RejectedOnNonCountSketch asserts emit_heap on a non-CountSketch
// family is rejected at validation (no silent ignore).
func TestEmitHeap_RejectedOnNonCountSketch(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics: []MetricFamily{{
			Metric:   "x",
			Family:   FamilyHLL,
			EmitHeap: true,
		}},
		Cold: ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to reject emit_heap on a non-CountSketch family")
	}
}

// TestEmitHeap_NonHeapCountSketchUnchanged is the no-regression guard: a
// CountSketch family WITHOUT emit_heap keeps the proto wire form (EncodingProtoFull)
// and the attribute-set frequency keying — byte-unchanged from before this change.
func TestEmitHeap_NonHeapCountSketchUnchanged(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "m", Family: FamilyCountSketch}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	sa, ok := newSketchAggregator("m", &cfg.Metrics[0], sketchOpts{window: time.Hour}, zap.NewNop())
	if !ok {
		t.Fatal("newSketchAggregator returned ok=false")
	}
	if sa.pcfg.Encoding != precompute.EncodingProtoFull {
		t.Fatalf("non-heap CountSketch must keep EncodingProtoFull, got %v", sa.pcfg.Encoding)
	}
	if sa.obsKind != obsKindKeyedFreq {
		t.Fatalf("non-heap CountSketch must keep obsKindKeyedFreq, got %v", sa.obsKind)
	}

	base := uint64(time.Unix(1700000000, 0).UnixMilli())
	for i := 0; i < 20; i++ {
		sa.observe(map[string]string{"endpoint": "/a"}, 1.0, base+uint64(i))
	}
	if sa.lastObserveErr != nil {
		t.Fatalf("observe errored: %v", sa.lastObserveErr)
	}
	env := drainOneCountSketch(t, sa.pc.Drain())
	if env.Encoding != precompute.EncodingProtoFull {
		t.Fatalf("non-heap CountSketch envelope must be EncodingProtoFull, got %v", env.Encoding)
	}
	// A proto frame must NOT decode as a heap-bearing msgpack frame.
	if _, _, _, _, _, err := asapmsgpack.UnmarshalCountSketchWithHeap(env.Payload); err == nil {
		t.Fatal("non-heap CountSketch payload must NOT round-trip as a heap msgpack frame")
	}
}
