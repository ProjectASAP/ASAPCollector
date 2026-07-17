// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package precompute

import "go.opentelemetry.io/otel/attribute"

// AggregationRouter maps a series' retained attributes to the target
// collector-side AggregationIdentity it folds into, and that aggregation's
// row fan-out d (0 for non-row sketch families, e.g. DDSketch's d=1
// whole-item case — callers treat rows<=1 as "no per-row admission, whole-
// item admit-or-skip"). ok=false means this series isn't row-sampled at
// all — the SDK should either pass it through unsampled or drop it,
// depending on a separate policy the caller applies.
type AggregationRouter func(attrs attribute.Set) (id AggregationIdentity, rows int, ok bool)

// AggregationPolicy declares the SDK's local knowledge of one collector-
// side materialized-view policy — the SAME shape as ASAPQuery-backend's
// AggregationConfig (crates/asap_types/src/aggregation_config.rs),
// expressed for a Go View declaration. A metric whose series ALL feed ONE
// collector-side sketch (the common case) declares exactly one
// AggregationPolicy; a metric that fans out to multiple sketches (e.g.
// per-zone routing to physically distinct sketches with different
// grouping) would combine several policies' routers — that composition is
// intentionally left to the caller rather than baked in here.
type AggregationPolicy struct {
	Metric string
	// AggregationType is the Rust-side PascalCase name — see
	// backendAggregationType-style mappings, e.g. "CountSketch",
	// "CountSketchWithHeap", "CountMinSketch", "DDSketch",
	// "DatasketchesKLL", "HLL". Must match whatever the control plane would
	// emit for this same policy, or AggID silently disagrees with the
	// backend's independently-computed PolicyFingerprint.
	AggregationType    string
	AggregationSubType string
	// Parameters MUST use the same keys ASAPQuery-backend's
	// sketch_params_to_json emits: CountSketch/CountMinSketch use "d"
	// (rows) and "w" (cols); CountSketch additionally carries "with_heap";
	// DDSketch uses "alpha"; KLL uses "k"; HLL uses "precision". Add
	// "item_label" when the policy has one.
	Parameters map[string]any
	// GroupingLabels is the AggregateBy label set — the SAME labels the
	// collector groups by to decide "one sketch per group". A series'
	// AggregationIdentity.Filter is this policy's GroupingLabels projected
	// onto that series' concrete attribute values.
	GroupingLabels    []string
	AggregatedLabels  []string
	RollupLabels      []string
	WindowSizeSecs    uint64
	SpatialFilter     string
	// Rows is the row fan-out d for CountSketch/CountMinSketch families
	// (should equal Parameters["d"]); leave 0 for non-row families.
	Rows int
}

// AggID computes this policy's PolicyFingerprint. Exposed separately from
// Router so callers that need the bare identity (e.g. to register a live
// grant once, up front) don't have to route a dummy attribute.Set through
// Router to get it.
func (p AggregationPolicy) AggID() uint64 {
	return PolicyFingerprint(PolicyFingerprintInput{
		Metric:             p.Metric,
		AggregationType:    p.AggregationType,
		AggregationSubType: p.AggregationSubType,
		Parameters:         p.Parameters,
		GroupingLabels:     p.GroupingLabels,
		AggregatedLabels:   p.AggregatedLabels,
		RollupLabels:       p.RollupLabels,
		WindowSizeSecs:     p.WindowSizeSecs,
		SlideIntervalSecs:  p.WindowSizeSecs, // tumbling: slide == window
		WindowType:         "tumbling",
		SpatialFilter:      p.SpatialFilter,
	})
}

// Router derives this policy's AggID ONCE (not per call) and returns an
// AggregationRouter serving every series under this policy. The returned
// router always reports ok=true — a single-policy router never declines a
// series; a multi-policy fan-out router (not provided here) is where ok=
// false would come from.
func (p AggregationPolicy) Router() AggregationRouter {
	aggID := p.AggID()
	rows := p.Rows
	grouping := p.GroupingLabels
	return func(attrs attribute.Set) (AggregationIdentity, int, bool) {
		labels := make(map[string]string, len(grouping))
		for _, k := range grouping {
			if v, ok := attrs.Value(attribute.Key(k)); ok {
				labels[k] = v.Emit()
			}
		}
		return AggregationIdentity{AggID: aggID, Filter: AggregationFilter(labels)}, rows, true
	}
}
