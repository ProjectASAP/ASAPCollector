package precompute

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkDeltaRotationAtCardinality(b *testing.B) {
	for _, cardinality := range []int{1_000, 10_000, 100_000} {
		b.Run(strconv.Itoa(cardinality), func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				cfg := &PrecomputeConfig{AggID: 1, SketchType: SketchTypeDDSketch,
					Mode: Tumbling, Window: WindowSpec{Size: time.Second}, DeltaTransmission: true}
				p := New(cfg, newFakeFactory(), &fakeObserver{})
				for i := 0; i < cardinality; i++ {
					obs := &Observation{TimestampMs: 1, Labels: []KeyValue{{Key: "k", Value: strconv.Itoa(i)}}, Value: FloatValue(1)}
					if err := p.Observe(obs); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if got := len(p.Tick(1_000)); got != cardinality {
					b.Fatalf("envelopes=%d", got)
				}
			}
		})
	}
}
