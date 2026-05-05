// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package aggregate

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

// BenchmarkBuilderFilter targets the SDK-side AttributeFilter hot path
// that was identified as the source of the 4× CPU climb in the
// label-axis cost-eval sweep
// (deploy/eval-results/sdk-cost/label-axis-20260423.csv).
//
// The bench mirrors the cost-eval workload shape: a fixed number of
// attribute Sets (`cardinality`) cycled through, each one a four-key
// {zone, rack, node, pod} schema; the filter keeps either zero, two,
// or all keys. We measure ns/op, B/op, allocs/op for each
// (cardinality, filter-shape) cell.
//
// Expected before the fix: B/op grows with the Set's slice
// allocations (fresh ToSlice + newSet on every measure when the
// filter actually drops keys); with cardinality=1000 you see allocs
// roughly = 2 per call. After the cache fix the second-and-onwards
// call for any given input Set hits the sync.Map and pays only the
// hash lookup — 0 allocs once warm.
func BenchmarkBuilderFilter(b *testing.B) {
	cardinalities := []int{100, 1000}
	keepZoneRack := func(kv attribute.KeyValue) bool {
		k := string(kv.Key)
		return k == "zone" || k == "rack"
	}
	keepAll := func(attribute.KeyValue) bool { return true }
	dropAll := func(attribute.KeyValue) bool { return false }

	cases := []struct {
		name   string
		filter attribute.Filter
	}{
		{"keep-all-via-closure", keepAll},
		{"drop-all", dropAll},
		{"keep-zone-rack", keepZoneRack},
	}

	for _, card := range cardinalities {
		// Build the input attribute Sets once.
		sets := make([]attribute.Set, card)
		for i := 0; i < card; i++ {
			sets[i] = attribute.NewSet(
				attribute.String("zone", fmt.Sprintf("z%d", i%4)),
				attribute.String("rack", fmt.Sprintf("r%d", (i/4)%10)),
				attribute.String("node", fmt.Sprintf("n%d", (i/40)%25)),
				attribute.String("pod", fmt.Sprintf("p%d", (i/1000)%10)),
			)
		}

		for _, c := range cases {
			b.Run(fmt.Sprintf("card=%d/%s", card, c.name), func(b *testing.B) {
				// fakeMeasure swallows args so the bench is dominated by
				// Builder.filter's own work, not the downstream aggregator.
				var fakeMeasure fltrMeasure[float64] = func(_ context.Context, _ float64, _ attribute.Set, _ []attribute.KeyValue) {
				}
				bld := Builder[float64]{Filter: c.filter}
				m := bld.filter(fakeMeasure)
				ctx := context.Background()

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m(ctx, 1.0, sets[i%card])
				}
			})

			// Pre-fix shape: skip the cache and call attribute.(*Set).Filter
			// directly on every iteration, the way Builder.filter did before
			// the cache was introduced. Kept side-by-side with the post-fix
			// case so the speedup is visible in one bench run.
			b.Run(fmt.Sprintf("card=%d/%s/no-cache", card, c.name), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					fAttr, dropped := sets[i%card].Filter(c.filter)
					_ = fAttr
					_ = dropped
				}
			})
		}
	}
}
