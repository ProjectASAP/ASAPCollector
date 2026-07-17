// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"sync"

	gorilla "github.com/ProjectASAP/asap-gorilla-go"
)

// shard is one independent ingestion partition. Concurrent OTLP Export
// goroutines whose series hash to different shards proceed in parallel —
// each shard serializes only its own aggregators behind mu.
type shard struct {
	mu sync.Mutex
	// cold is the per-shard Gorilla XOR-chunk fragment encoder, fed samples
	// via AddSample. It produces compact XOR-chunk fragments (Drain) that are
	// shipped to the backend merger — the edge no longer builds TSDB blocks.
	// nil when the cold tier is disabled.
	cold *gorilla.StreamingFragmentEncoder
	// sketchAggs holds one precompute-backed aggregator per sketch-family
	// metric. Series live in a single shard, so these flush independently
	// per shard (no cross-shard merge, unlike sum).
	sketchAggs map[string]*sketchAggregator
}
