// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package precompute_test

// End-to-end warm-path CPU benefit of sample_p, measured through the real
// Precompute.Observe path (series-key → window → observer → sketch.Update) for
// the two families that actually sample in production: HLL (hash-threshold) and
// CountMin (NitroSketch geometric skip). Compare the p=1.0 vs p=0.10 ns/op to
// get the PIPELINE-level saving — which is diluted vs the sketch-only upper
// bound by the non-update per-observation overhead (series-key, window lookup,
// observer dispatch, value hashing) that sampling does not remove.

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	cms "github.com/ProjectASAP/sketchlib-go/sketches/CountMinSketch"
	ddsketch "github.com/ProjectASAP/sketchlib-go/sketches/DDSketch"
	hll "github.com/ProjectASAP/sketchlib-go/sketches/HLL"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

func BenchmarkSampleP_Observe_DDSketch(b *testing.B) {
	for _, p := range []float64{1.0, 0.10} {
		pp := p
		b.Run(fmt.Sprintf("p=%.2f", pp), func(b *testing.B) {
			factory := precompute.SketchFactory(func() precompute.Sketch {
				return &benchDDSketch{sk: ddsketch.NewDDSketch(0.01).WithSampleP(pp, benchSeed)}
			})
			pc := newBenchPrecompute(b, precompute.SketchTypeDDSketch, factory, benchDDObserver{})
			rng := rand.New(rand.NewSource(benchSeed))
			obs := make([]*precompute.Observation, benchObservations)
			for i := range obs {
				obs[i] = &precompute.Observation{
					TimestampMs: 1_000,
					Metric:      "request_latency",
					Labels:      []precompute.KeyValue{{Key: "route", Value: "/api"}},
					Value:       precompute.FloatValue(rng.Float64() * 10000),
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = pc.Observe(obs[i%len(obs)])
			}
		})
	}
}

func BenchmarkSampleP_Observe_HLL(b *testing.B) {
	for _, p := range []float64{1.0, 0.10} {
		pp := p
		b.Run(fmt.Sprintf("p=%.2f", pp), func(b *testing.B) {
			factory := precompute.SketchFactory(func() precompute.Sketch {
				return &benchHLLSketch{sk: hll.NewHyperLogLog().WithSampleP(pp)}
			})
			pc := newBenchPrecompute(b, precompute.SketchTypeHLLSketch, factory, benchHLLObserver{})
			rng := rand.New(rand.NewSource(benchSeed))
			obs := make([]*precompute.Observation, benchObservations)
			for i := range obs {
				obs[i] = &precompute.Observation{
					TimestampMs: 1_000,
					Metric:      "user_id",
					Labels:      []precompute.KeyValue{{Key: "shard", Value: strconv.Itoa(i % 8)}},
					Value:       precompute.FloatValue(rng.Float64() * 1_000_000),
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = pc.Observe(obs[i%len(obs)])
			}
		})
	}
}

func BenchmarkSampleP_Observe_CMS(b *testing.B) {
	for _, p := range []float64{1.0, 0.10} {
		pp := p
		b.Run(fmt.Sprintf("p=%.2f", pp), func(b *testing.B) {
			rows, cols := 5, 1024
			factory := precompute.SketchFactory(func() precompute.Sketch {
				sk, err := cms.NewCountMinSketch(rows, cols)
				if err != nil {
					panic(err)
				}
				sk = sk.WithSampleP(pp, benchSeed)
				return &benchCMS{sk: sk}
			})
			pc := newBenchPrecompute(b, precompute.SketchTypeCountMinSketch, factory, benchCMSObserver{})
			keys := make([][]byte, 64)
			for i := range keys {
				keys[i] = []byte("flow-" + strconv.Itoa(i))
			}
			obs := make([]*precompute.Observation, benchObservations)
			for i := range obs {
				obs[i] = &precompute.Observation{
					TimestampMs: 1_000,
					Metric:      "http_requests_total",
					Labels:      []precompute.KeyValue{{Key: "service.name", Value: "web"}},
					Value:       precompute.BytesValue(keys[i%len(keys)]),
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = pc.Observe(obs[i%len(obs)])
			}
		})
	}
}
