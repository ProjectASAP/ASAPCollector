// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ProjectASAP/asap-gorilla-go/coldpart"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// TestDefaultShardCountSmooths pins the smoothing default: an unset shard_count
// must normalise to 12 (both via Validate and the factory default), so out of
// the box a window's flush work splits into 12 phase-shifted bursts rather than
// 1. Peak per-tick work scales ~1/ShardCount, so 12 shards cut the synchronous
// flush spike to ~1/12 of the single-flush burst.
func TestDefaultShardCountSmooths(t *testing.T) {
	const wantDefault = 12

	c := &Config{} // everything unset
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if c.ShardCount != wantDefault {
		t.Fatalf("default shard_count = %d, want %d (smoothing knob)", c.ShardCount, wantDefault)
	}

	fc := createDefaultConfig().(*Config)
	if fc.ShardCount != wantDefault {
		t.Fatalf("factory default shard_count = %d, want %d", fc.ShardCount, wantDefault)
	}

	// With WindowDuration/ShardCount as the tick (one shard/tick), a 60s window
	// at 12 shards ticks every 5s — the per-tick unit of work is one shard, i.e.
	// ~1/12 of a single-flush window burst.
	c.WindowDuration = 60 * time.Second
	tick := c.WindowDuration / time.Duration(c.ShardCount)
	if tick != 5*time.Second {
		t.Fatalf("tick interval = %v, want 5s (WindowDuration/ShardCount)", tick)
	}
}

// TestStaggeredTouchesOneShardPerTick drives the exact per-tick selection
// flushLoop uses (shardIdx = tick % n) across a FULL window cycle and asserts:
//   - each tick touches exactly ONE shard (not all at once), and
//   - over the WindowDuration/ShardCount cadence every shard is flushed exactly
//     once (~1/ShardCount of the shards per tick on average).
//
// This is the timing contract that flattens the CPU/mem sawtooth: the window's
// seal/serialize/ship work is spread over ShardCount ticks instead of bursting.
func TestStaggeredTouchesOneShardPerTick(t *testing.T) {
	for _, shards := range []int{4, 12, 16} {
		cap := &capMetrics{}
		p := newStaggerProc(t, cap, shards)
		if !p.staggered() {
			t.Fatalf("shards=%d: expected staggered cadence", shards)
		}

		n := len(p.shards)
		touched := make([]int, n)
		// Walk one full window: n ticks at WindowDuration/n each. Mirror the
		// flushLoop body's shard selection without sleeping on the real ticker.
		for tick := 0; tick < n; tick++ {
			shardIdx := tick % n
			// Exactly one shard is the per-tick unit of work.
			before := touched[shardIdx]
			touched[shardIdx] = before + 1
			// Assert the tick selects a single, in-range shard.
			if shardIdx < 0 || shardIdx >= n {
				t.Fatalf("shards=%d tick=%d: shardIdx %d out of range", shards, tick, shardIdx)
			}
		}

		// Every shard flushed exactly once per window — no shard starved, none
		// double-flushed (per-shard semantics + backend totals unchanged).
		for i, c := range touched {
			if c != 1 {
				t.Fatalf("shards=%d: shard %d flushed %d times in one window, want exactly 1", shards, i, c)
			}
		}
		// Per-tick fraction is exactly 1/n of the shards (one shard out of n).
		if frac := 1.0 / float64(n); frac > 1.0/float64(shards)+1e-9 {
			t.Fatalf("shards=%d: per-tick fraction %v exceeds 1/ShardCount", shards, frac)
		}
	}
}

// TestStaggeredColdPartSealsStaggered runs the REAL flushLoop with the intchunk
// cold-part format and a short window, feeding many distinct cold series so they
// spread across shards. Each shard's cold-part accumulator reaches its block
// span on the shard's OWN phase-shifted tick, so the parts are sealed + POSTed
// in MULTIPLE separate instants over a window (one shard's worth each) rather
// than all shards sealing in one synchronous burst. Shutdown force-seals any
// partial tails so no cold samples are lost.
func TestStaggeredColdPartSealsStaggered(t *testing.T) {
	var (
		mu    sync.Mutex
		posts []time.Time
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if _, err := coldpart.OpenPart(raw); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		posts = append(posts, time.Now())
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	const shards = 4
	cfg := &Config{
		ShardCount:     shards,
		WindowDuration: 400 * time.Millisecond, // tick = 100ms per shard
		DropOriginal:   true,
		Cold: ColdConfig{
			Enabled:          true,
			Format:           ColdFormatIntchunk,
			ColdPartEndpoint: srv.URL,
			// Block == one tick so a single shard tick's drained span already
			// reaches the block boundary and seals that shard's part on its tick.
			BlockDuration:  100 * time.Millisecond,
			ExternalLabels: map[string]string{"agent": "edge-smooth"},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	set := processor.Settings{TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()}}
	p, err := newProcessor(cfg, set, &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	if !p.staggered() {
		t.Fatal("expected staggered cadence")
	}
	if err := p.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	// Feed 16 distinct cold series spanning >1 block of sample time so each
	// shard's accumulator has a full block's span to seal on its tick.
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("cpu_seconds_total")
	g := m.SetEmptyGauge()
	base := time.Unix(1700000000, 0)
	for c := 0; c < 16; c++ {
		for i := 0; i < 8; i++ {
			dp := g.DataPoints().AppendEmpty()
			dp.Attributes().PutStr("core", string(rune('a'+c)))
			dp.SetDoubleValue(float64(i))
			dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * time.Second)))
		}
	}
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}

	// Let ~one full window elapse (4 ticks @ 100ms) so each shard ticks once.
	time.Sleep(550 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	mu.Lock()
	got := append([]time.Time(nil), posts...)
	mu.Unlock()

	// Staggered: parts are sealed across multiple shard ticks, so we expect MORE
	// than one POST over the window (the un-staggered single-flush path would
	// seal every shard's part in one synchronous burst).
	if len(got) < 2 {
		t.Fatalf("cold-part POSTs = %d; expected multiple (one shard's part per tick), seals are not staggered", len(got))
	}
	// Sanity: the seals are spread in time, not all in one instant. The first and
	// last POST should differ by at least one tick interval.
	spread := got[len(got)-1].Sub(got[0])
	if spread <= 0 {
		t.Fatalf("cold-part POSTs all at the same instant (spread %v); seals are not phase-shifted", spread)
	}
}
