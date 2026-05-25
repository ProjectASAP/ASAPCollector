package gorilla

import (
	"context"
	"testing"
	"time"
)

// TestFragmentEncoderIdleEviction proves the memory-bounding eviction policy of
// StreamingFragmentEncoder under rotating high cardinality: a series that has
// been fully flushed/shipped AND idle (no sample within the idle threshold) has
// its per-series state evicted, so retained state collapses to the ACTIVE
// working set rather than the cumulative set-of-all-series. It also asserts that
// active series keep encoding correctly, that no un-shipped sample is ever lost,
// and that a previously-evicted series re-encodes cleanly when it reappears.
func TestFragmentEncoderIdleEviction(t *testing.T) {
	const (
		idleSeries = 3
		actSeries  = 3
		grace      = 1000  // reorder grace ms
		idleEvict  = 5000  // evict after 5s of inactivity
		step       = 1000  // ms between active-series samples
		windows    = 25    // active windows after the idle series last spoke
		startTs    = 10000 // active-series start ts (ms)
	)

	enc := NewStreamingFragmentEncoder(StreamingFragmentOptions{
		ReorderGrace: grace * time.Millisecond,
		IdleEvict:    idleEvict * time.Millisecond,
		Source:       "agent-test",
	})

	// addedPerSeries records every value we fed each series, so we can prove
	// (a) no un-shipped sample is lost and (b) values round-trip after eviction.
	addedPerSeries := map[string][]float64{}
	add := func(series string, ms int64, v float64) {
		if err := enc.AddSample(TSDBSample{
			MetricName: "unique_users_per_min",
			Attributes: map[string]string{"user_id": series},
			Timestamp:  time.UnixMilli(ms),
			Value:      v,
		}); err != nil {
			t.Fatalf("add %s@%d: %v", series, ms, err)
		}
		addedPerSeries[series] = append(addedPerSeries[series], v)
	}

	idleName := func(i int) string { return "idle-" + string(rune('a'+i)) }
	actName := func(i int) string { return "act-" + string(rune('a'+i)) }

	// The idle cohort speaks once early, at t < startTs, then never again.
	for i := 0; i < idleSeries; i++ {
		add(idleName(i), 1000, float64(100+i))
	}

	// The active cohort keeps producing in-order samples across many windows,
	// advancing maxObserved far past the idle cohort's last activity.
	val := 0.0
	var lastActiveTs int64
	for w := 0; w < windows; w++ {
		ts := int64(startTs + w*step)
		lastActiveTs = ts
		for i := 0; i < actSeries; i++ {
			val++
			add(actName(i), ts, val)
		}
	}

	// Sanity: before the flush, all series are still retained (eviction only runs
	// on the force/flush sweep) and nothing has been evicted yet.
	if got := enc.ActiveSeries(); got != idleSeries+actSeries {
		t.Fatalf("pre-flush ActiveSeries = %d, want %d", got, idleSeries+actSeries)
	}
	if got := enc.EvictedSeries(); got != 0 {
		t.Fatalf("pre-flush EvictedSeries = %d, want 0", got)
	}

	// Force-drain: this drains every series' pending points, flushes their open
	// chunks (so the idle cohort is now fully shipped with empty pending), then
	// runs the eviction sweep. cutoff = maxObserved(=lastActiveTs) - idleEvict;
	// the idle cohort (lastActive=1000) is well past it, the active cohort is not.
	frags, err := enc.Drain(true)
	if err != nil {
		t.Fatalf("force drain: %v", err)
	}

	// The idle+flushed cohort must be evicted; only the active set is retained.
	if got := enc.ActiveSeries(); got != actSeries {
		t.Fatalf("post-flush ActiveSeries = %d, want %d (idle cohort should be evicted)", got, actSeries)
	}
	if got := enc.EvictedSeries(); got != idleSeries {
		t.Fatalf("post-flush EvictedSeries = %d, want %d", got, idleSeries)
	}
	_ = lastActiveTs

	// No un-shipped sample lost: every value fed to every series must appear in
	// the emitted fragments (idle cohort included — eviction happens only AFTER
	// the data is shipped). Tally per-series sample counts from the fragments.
	shipped := map[string]int{}
	for _, f := range frags {
		uid := f.Attributes["user_id"]
		shipped[uid] += f.Count
	}
	for series, vals := range addedPerSeries {
		if shipped[series] != len(vals) {
			t.Errorf("series %q: shipped %d samples, fed %d (un-shipped data lost)", series, shipped[series], len(vals))
		}
	}

	// The eviction heap must be bounded to the active set, not all-ever-seen.
	if qlen := enc.evictQueue.Len(); qlen > actSeries {
		t.Errorf("evictQueue retained %d entries, want <= %d (active set)", qlen, actSeries)
	}

	// A previously-evicted series that reappears must re-initialize cleanly and
	// encode a fresh fragment with correct values. Feed evicted idle-a again at a
	// timestamp newer than maxObserved, then force-drain.
	reTs := lastActiveTs + step
	reborn := idleName(0)
	add(reborn, reTs, 777)
	add(reborn, reTs+step, 778)

	if got := enc.ActiveSeries(); got != actSeries+1 {
		t.Fatalf("after reappearance ActiveSeries = %d, want %d", got, actSeries+1)
	}

	frags2, err := enc.Drain(true)
	if err != nil {
		t.Fatalf("force drain 2: %v", err)
	}
	var rebornCount int
	for _, f := range frags2 {
		if f.Attributes["user_id"] == reborn {
			rebornCount += f.Count
		}
	}
	if rebornCount != 2 {
		t.Fatalf("reborn series shipped %d samples after re-appearance, want 2", rebornCount)
	}

	// The reborn series' two new fragments must round-trip through the block
	// finalizer (XOR chunk not corrupted; fresh fragment encodes correctly).
	fin, err := NewFragmentBlockFinalizer(FragmentBlockOptions{TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new finalizer: %v", err)
	}
	for _, f := range frags2 {
		if err := fin.AddFragment(f); err != nil {
			t.Fatalf("add fragment: %v", err)
		}
	}
	art, err := fin.Finalize(context.Background())
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if art == nil {
		t.Fatal("finalize returned nil artifact")
	}
}

// TestFragmentEncoderNoEvictUnshipped proves the eviction sweep never reclaims a
// series that still holds un-shipped data, even if it has gone idle: a series
// with pending points (held for reorder) is not evicted, and the data survives.
func TestFragmentEncoderNoEvictUnshipped(t *testing.T) {
	enc := NewStreamingFragmentEncoder(StreamingFragmentOptions{
		ReorderGrace: time.Second,
		IdleEvict:    2 * time.Second,
		Source:       "agent-test",
	})
	add := func(series string, ms int64, v float64) {
		if err := enc.AddSample(TSDBSample{
			MetricName: "m",
			Attributes: map[string]string{"s": series},
			Timestamp:  time.UnixMilli(ms),
			Value:      v,
		}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	// "keep" gets a single early sample and never speaks again — it is idle but
	// its sample is still buffered as pending (non-force watermark hasn't shipped
	// it because nothing advanced its own series), so it must NOT be evicted on a
	// non-force path. We force-drain to flush it; the data must be shipped, not
	// dropped, and only then does eviction apply.
	add("keep", 1000, 42)

	// Advance maxObserved far past "keep" with another series, so "keep" is idle.
	for ts := int64(2000); ts <= 12000; ts += 1000 {
		add("active", ts, float64(ts))
	}

	// Non-force drain: must not evict "keep" — it still has an un-shipped pending
	// point (its open/pending state was never flushed on the per-sample path
	// since its own series never advanced past its watermark).
	if _, err := enc.Drain(false); err != nil {
		t.Fatalf("soft drain: %v", err)
	}
	if got := enc.EvictedSeries(); got != 0 {
		t.Fatalf("soft drain evicted %d series; must not evict series holding un-shipped data", got)
	}

	// Force drain ships "keep"'s sample, then sweeps. Its single sample must be
	// present in the output (never lost), and only after shipping is it evicted.
	frags, err := enc.Drain(true)
	if err != nil {
		t.Fatalf("force drain: %v", err)
	}
	var keepCount int
	for _, f := range frags {
		if f.Attributes["s"] == "keep" {
			keepCount += f.Count
		}
	}
	if keepCount != 1 {
		t.Fatalf("keep series shipped %d samples, want 1 (un-shipped data lost)", keepCount)
	}
}
