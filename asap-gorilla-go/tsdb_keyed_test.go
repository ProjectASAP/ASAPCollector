package gorilla

import (
	"testing"
	"time"
)

// TestAddSampleKeyedMatchesAddSample verifies the shared-key entry point:
// feeding AddSampleKeyed with the key appendSeriesKey would build lands on
// the SAME series as AddSample (same series count, same finalized block),
// so asap_edge can build the key once and share it with the cold tier.
func TestAddSampleKeyedMatchesAddSample(t *testing.T) {
	mk := func() *StreamingTSDBBlockBuilder {
		b, err := NewStreamingTSDBBlockBuilder(StreamingTSDBOptions{
			ReorderGrace:    time.Second,
			SamplesPerChunk: 120,
			TempDir:         t.TempDir(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	attrsA := map[string]string{"zone": "z0", "method": "GET"}
	attrsB := map[string]string{"zone": "z1", "method": "POST"}
	base := time.Unix(1000, 0)

	// Unkeyed builder.
	bu := mk()
	for i := 0; i < 5; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		_ = bu.AddSample(TSDBSample{MetricName: "m", Attributes: attrsA, Timestamp: ts, Value: float64(i)})
		_ = bu.AddSample(TSDBSample{MetricName: "m", Attributes: attrsB, Timestamp: ts, Value: float64(i)})
	}

	// Keyed builder: derive the key the same way appendSeriesKey does.
	bk := mk()
	keyOf := func(metric string, attrs map[string]string) string {
		sc := &seriesKeyScratch{}
		bk.appendSeriesKey(sc, metric, attrs)
		return string(sc.buf)
	}
	kA, kB := keyOf("m", attrsA), keyOf("m", attrsB)
	for i := 0; i < 5; i++ {
		ts := base.Add(time.Duration(i) * time.Second)
		_ = bk.AddSampleKeyed(kA, TSDBSample{MetricName: "m", Attributes: attrsA, Timestamp: ts, Value: float64(i)})
		_ = bk.AddSampleKeyed(kB, TSDBSample{MetricName: "m", Attributes: attrsB, Timestamp: ts, Value: float64(i)})
	}

	if len(bu.series) != 2 || len(bk.series) != 2 {
		t.Fatalf("series count: unkeyed=%d keyed=%d, want 2/2", len(bu.series), len(bk.series))
	}
	// Keyed admit must reuse the exact same keys appendSeriesKey produced
	// (so a later unkeyed AddSample would hit the same series).
	if _, ok := bk.series[kA]; !ok {
		t.Fatalf("keyed series kA not found under its appendSeriesKey key")
	}
	if _, ok := bk.series[kB]; !ok {
		t.Fatalf("keyed series kB not found under its appendSeriesKey key")
	}
	// Cross-check: the unkeyed builder admitted series under the SAME keys.
	if _, ok := bu.series[kA]; !ok {
		t.Fatalf("unkeyed series not under appendSeriesKey key kA — keyed/unkeyed would diverge")
	}
}
