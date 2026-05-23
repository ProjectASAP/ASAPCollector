// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package asapedgeprocessor fuses the asap edge cold tier (gorilla TSDB
// archive) and the warm aggregation tier (sum + sketch families) into a
// single, key-sharded processor.
//
// Motivation (issue #46 profiling): the previous topology fanned each
// metric through a routing connector into a per-family pipeline of
// [gorillas3, <aggregator>]. That cost three things:
//
//  1. Redundant work — the cold processor and the warm aggregator each
//     re-iterated the same data points, re-converted attributes, and
//     re-built the series key. ~2× decode+key per sample.
//  2. No parallelism — every processor took a processor-wide mutex in
//     ConsumeMetrics, so concurrent OTLP Export goroutines serialized on
//     one lock; the agent could not use more than ~1 core for ingestion.
//  3. A pathological sum path — the counter's sum-by-zone aggregation ran
//     through contrib's metricstransform, whose dataPointHashKey builds a
//     grouping key per data point via json.Marshal + reflection (~17% of
//     agent CPU).
//
// asap_edge collapses all three:
//
//   - One decode + key pass. Each data point's attributes are read once
//     and the canonical series key is built once, then dispatched to both
//     the cold builder and the metric's warm aggregator.
//   - Key-hash sharding. N independent shards (shard_count, default 4),
//     each owning its own lock + cold builder + warm aggregators, so
//     concurrent Export goroutines on distinct series proceed on distinct
//     cores.
//   - An asap-native Sum aggregator (per-shard zone→sum map, merged at
//     flush) that emits the same zone-keyed delta Sum metric the backend
//     ingests today — no json.Marshal, no reflection.
//
// Topology: replaces routing connector + per-family pipelines +
// per-pipeline gorillas3 + metricstransform with a single processor:
//
//	otlp → cumulativetodelta → asap_edge → otlp/backend
package asapedgeprocessor
