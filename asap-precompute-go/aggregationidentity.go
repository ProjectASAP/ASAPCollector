// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package precompute

import "sort"

// AggregationIdentity identifies ONE collector-side aggregation instance —
// one physical CountSketch / CountMinSketch (one per AggregateBy group) —
// that a group of series feed. All series that fold into the SAME
// aggregation instance at the collector share this identity, and — for
// NitroSketch-style row sampling — share ONE GeometricSampler + ONE
// sample_p, since together they form one "update stream" in the NitroSketch
// sense.
//
// # Why two fields, and where they come from
//
// AggregateBy is what decides "how many physical sketch instances exist
// under one policy" — the collector runs ONE sketch PER GROUP (see
// asapedgeprocessor/warm_sketch.go: "AggregateBy grouping ... one sketch
// per group"). So the granularity that actually corresponds to "one
// physical sketch" is (agg_id, the group's concrete label values) — exactly
// this struct: AggID identifies the policy/materialized-view definition
// (shared across every group under it), and Filter is the concrete k/v
// values (e.g. zone=A) that pick out ONE group within that policy.
//
// Together, (AggID, Filter) plays the SAME conceptual role as the backend's
// `sid` — but we do NOT need the backend-allocated integer to make correct
// local decisions. AggregationIdentity is enough, entirely locally, to
// decide which series should share a sampler — no round-trip to the
// backend required.
//
// # AggID is NOT the same thing as the backend's `sid` — two different
// identity layers
//
//	              │ AggID (AggregationIdentity.AggID / PolicyFingerprint)         │ sid
//	──────────────┼─────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────
//	Granularity   │ one per POLICY / materialized-view DEFINITION (shared across  │ one per CONCRETE series (e.g. the single group zone=A —
//	              │ every group under it — e.g. the whole "CountSketch over       │ one of possibly many groups under one materialized view)
//	              │ metric X, group by zone" policy)                              │
//	──────────────┼─────────────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────
//	How it's      │ a pure deterministic hash (xxh64, see PolicyFingerprint) —    │ MINTED by the backend's SeriesIdResolver — an allocated
//	obtained      │ any party that knows the same policy content independently   │ counter value. Idempotent (same input always resolves to
//	              │ computes the SAME value; no coordinator round-trip needed    │ the same sid), but the actual integer must be requested
//	              │                                                              │ from the backend (resolve()) — it cannot be derived locally
//
// AggID corresponds to PolicyFingerprint (ASAPQuery-backend's
// crates/asap_types/src/policy_fingerprint.rs) — the CDM monitor identity
// and the sketch-DB's materialized-view identity are ONE system, not two;
// AggID must be computed via PolicyFingerprint (see policyfingerprint.go),
// never via an ad-hoc hash of just the metric name.
type AggregationIdentity struct {
	// AggID is this metric's PolicyFingerprint — see PolicyFingerprint /
	// PolicyFingerprintInput in policyfingerprint.go.
	AggID uint64
	// Filter is the concrete label VALUES (not just names) that pick out
	// one group within the policy — e.g. "zone=A;service=checkout" — the
	// same projection the collector's AggregateBy grouping uses to route a
	// series to its physical sketch instance. Not free-form: it must be
	// derived the same way the collector derives its own group key
	// (groupKeyBytes-equivalent), or two sides disagree on which series
	// share a sketch.
	Filter string
}

// AggregationFilter canonically encodes a series' concrete grouping label
// values into the Filter form AggregationIdentity expects — sorted by key,
// "k=v;"-joined. Mirrors asap-precompute-go's groupKeyBytes and the
// collector's AggregateBy projection, so an SDK-computed Filter agrees with
// whatever the collector would compute for the same series.
func AggregationFilter(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for i, k := range keys {
		if i > 0 {
			b = append(b, ';')
		}
		b = append(b, k...)
		b = append(b, '=')
		b = append(b, labels[k]...)
	}
	return string(b)
}
