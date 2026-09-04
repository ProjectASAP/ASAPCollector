package precompute

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkEvictionAtCap(b *testing.B) {
	for _, capacity := range []int{1_000, 10_000, 100_000} {
		b.Run(strconv.Itoa(capacity), func(b *testing.B) {
			cfg := &PrecomputeConfig{AggID: 1, SketchType: SketchTypeDDSketch,
				Mode: Tumbling, Window: WindowSpec{Size: 24 * time.Hour},
				MaxSeries: uint64(capacity), OnOverflow: OnOverflowEvictOldest}
			w := newWindowState()
			factory, observer, stats := newFakeFactory(), &fakeObserver{}, NewStats()
			for i := 0; i < capacity; i++ {
				obs := &Observation{TimestampMs: 1, Labels: []KeyValue{{Key: "k", Value: strconv.Itoa(i)}}, Value: FloatValue(1)}
				if err := w.observe(obs, cfg, factory, observer, stats); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				obs := &Observation{TimestampMs: uint64(i + 2), Labels: []KeyValue{{Key: "k", Value: "new-" + strconv.Itoa(i)}}, Value: FloatValue(1)}
				if err := w.observe(obs, cfg, factory, observer, stats); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
