package precompute

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkParallelExistingSeries(b *testing.B) {
	const series = 1024
	cfg := &PrecomputeConfig{AggID: 1, SketchType: SketchTypeDDSketch,
		Mode: Tumbling, Window: WindowSpec{Size: time.Hour}}
	w := newWindowState()
	factory, observer, stats := newFakeFactory(), &fakeObserver{}, NewStats()
	observations := make([]*Observation, series)
	keys := make([]string, series)
	for i := range observations {
		observations[i] = &Observation{TimestampMs: 1, Labels: []KeyValue{{Key: "k", Value: strconv.Itoa(i)}}, Value: FloatValue(1)}
		keys[i] = cfg.SeriesKeyFor(observations[i])
		if err := w.observeKeyed(keys[i], observations[i], cfg, factory, observer, stats); err != nil {
			b.Fatal(err)
		}
	}
	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(next.Add(1)) & (series - 1)
			if err := w.observeKeyed(keys[i], observations[i], cfg, factory, observer, stats); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
