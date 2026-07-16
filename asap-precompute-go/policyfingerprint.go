// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package precompute

import (
	"encoding/binary"
	"encoding/json"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// PolicyFingerprintInput is the hash-relevant subset of ASAPQuery-backend's
// AggregationConfig (crates/asap_types/src/aggregation_config.rs) — the
// materialized-view *definition* a metric's policy corresponds to. Byte-for-
// byte port of PolicyFingerprint::from_config
// (crates/asap_types/src/policy_fingerprint.rs): given the SAME logical
// policy, an edge/SDK that fills this struct out from its own local config
// derives the IDENTICAL u64 the backend independently derives from the
// control plane's pushed AggregationConfig — no id allocation, no
// coordinator round-trip, no handshake. This is the CDM/sketch-DB unified
// identity space; do not compute AggID any other way (e.g. hashing only the
// metric name) — that produces a value that silently disagrees with the
// backend's PolicyFingerprint for the same policy.
type PolicyFingerprintInput struct {
	Metric string
	// AggregationType is the Rust-side PascalCase name, e.g. "CountSketch",
	// "CountSketchWithHeap", "CountMinSketch", "CountMinSketchWithHeap",
	// "DDSketch", "DatasketchesKLL", "HLL", "Sum", "Increase".
	AggregationType string
	// AggregationSubType is "" for every family the edge configures today
	// (matches control_plane/src/emit/stage_config.rs's
	// build_backend_aggregation_json, which always emits "").
	AggregationSubType string
	// Parameters are JSON-renderable values (int/float64/bool/string) — the
	// SAME keys the control plane emits in sketch_params_to_json: CMS
	// {"w","d"}, CountSketch {"w","d","with_heap"}, DDSketch {"alpha"}, KLL
	// {"k"}, HLL {"precision"}, plus "item_label" when set. w=cols, d=rows.
	Parameters map[string]any
	// GroupingLabels / AggregatedLabels / RollupLabels need not be
	// pre-sorted — PolicyFingerprint sorts them (matching the
	// KeyByLabelNames sorted-at-construction invariant the Rust side
	// relies on instead of re-sorting).
	GroupingLabels    []string
	AggregatedLabels  []string
	RollupLabels      []string
	WindowSizeSecs    uint64
	SlideIntervalSecs uint64
	// WindowType is "tumbling" or "sliding" (WindowType's
	// #[serde(rename_all = "snake_case")] form). Edge policies are always
	// tumbling with SlideIntervalSecs == WindowSizeSecs.
	WindowType string
	// SpatialFilter is the RAW (pre-normalization) filter string; this
	// function normalizes it identically to normalize_spatial_filter
	// (crates/asap_types/src/utils.rs) before hashing.
	SpatialFilter string
}

// PolicyFingerprint computes the same xxh64(seed=0) content-addressed policy
// identity as ASAPQuery-backend's PolicyFingerprint::from_config. The byte
// layout is the CROSS-LANGUAGE CONTRACT mirrored from policy_fingerprint.rs
// — don't reorder fields, don't change separator bytes; any such change
// desyncs every edge-computed AggID from the backend's independently
// computed PolicyFingerprint for the same policy.
func PolicyFingerprint(in PolicyFingerprintInput) uint64 {
	buf := make([]byte, 0, 512)

	// 1. metric name
	buf = append(buf, in.Metric...)
	buf = append(buf, 0)

	// 2. aggregation_type — Rust's manual Serialize impl calls
	// serializer.serialize_str(self.as_str()), and serde_json::to_string of
	// a string produces a QUOTED JSON string literal.
	buf = appendJSONString(buf, in.AggregationType)
	buf = append(buf, 0)

	// 3. aggregation_sub_type — hashed as raw bytes, NOT JSON-quoted (Rust
	// hashes cfg.aggregation_sub_type.as_bytes() directly).
	buf = append(buf, in.AggregationSubType...)
	buf = append(buf, 0)

	// 4. parameters — canonicalized: sorted keys, "k=<json-value>;" each.
	keys := make([]string, 0, len(in.Parameters))
	for k := range in.Parameters {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		buf = append(buf, k...)
		buf = append(buf, '=')
		buf = appendJSONValue(buf, in.Parameters[k])
		buf = append(buf, ';')
	}
	buf = append(buf, 0)

	// 5. grouping_labels — sorted, ","-joined (trailing comma after each,
	// matching the Rust loop's per-element push).
	buf = appendSortedCommaList(buf, in.GroupingLabels)
	buf = append(buf, 0)

	// 6. aggregated_labels
	buf = appendSortedCommaList(buf, in.AggregatedLabels)
	buf = append(buf, 0)

	// 7. rollup_labels
	buf = appendSortedCommaList(buf, in.RollupLabels)
	buf = append(buf, 0)

	// 8. window_size + slide_interval + window_type (cadence)
	buf = appendLE64(buf, in.WindowSizeSecs)
	buf = append(buf, 0)
	buf = appendLE64(buf, in.SlideIntervalSecs)
	buf = append(buf, 0)
	buf = appendJSONString(buf, in.WindowType)
	buf = append(buf, 0)

	// 9. spatial_filter_normalized — canonicalized predicate, NOT
	// null-terminated (last field; Rust's buf just ends here).
	buf = append(buf, normalizeSpatialFilter(in.SpatialFilter)...)

	return xxhash.Sum64(buf)
}

// appendSortedCommaList mirrors the Rust loop `for l in &labels.labels {
// buf.extend(l); buf.push(b','); }` over an ALREADY-SORTED KeyByLabelNames.
// Go's input isn't guaranteed pre-sorted, so sort defensively — a no-op if
// the caller already sorted, and correctness-preserving if not.
func appendSortedCommaList(buf []byte, labels []string) []byte {
	sorted := make([]string, len(labels))
	copy(sorted, labels)
	sort.Strings(sorted)
	for _, l := range sorted {
		buf = append(buf, l...)
		buf = append(buf, ',')
	}
	return buf
}

// appendLE64 mirrors Rust's u64::to_le_bytes().
func appendLE64(buf []byte, v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return append(buf, b[:]...)
}

// appendJSONString mirrors serde_json::to_string(&str) — a quoted JSON
// string literal with standard JSON escaping. Uses encoding/json rather
// than hand-rolled quoting so any future non-ASCII metric/enum name is
// escaped identically to serde_json's escaper.
func appendJSONString(buf []byte, s string) []byte {
	b, _ := json.Marshal(s) // Marshal on a string never errors
	return append(buf, b...)
}

// appendJSONValue mirrors serde_json::to_string(&Value) for a parameter
// value. Parameters in scope today (CMS/CountSketch: w, d int; with_heap
// bool; item_label string) round-trip byte-identically through Go's
// encoding/json for the same logical value — plain integers and bare
// true/false/quoted-strings render the same in both encoders. Floats
// (DDSketch alpha, HLL precision) are NOT yet verified byte-identical
// against serde_json's float formatter; don't route those families through
// this function until that's checked against Rust ground truth.
func appendJSONValue(buf []byte, v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return buf
	}
	return append(buf, b...)
}

// normalizeSpatialFilter mirrors normalize_spatial_filter
// (crates/asap_types/src/utils.rs): strip a leading "{"/trailing "}", split
// on ",", sort the parts, rejoin as "{a,b,c}". Empty input stays empty.
func normalizeSpatialFilter(filter string) string {
	if filter == "" {
		return ""
	}
	trimmed := strings.TrimSpace(filter)
	trimmed = strings.TrimPrefix(trimmed, "{")
	trimmed = strings.TrimSuffix(trimmed, "}")
	trimmed = strings.TrimSpace(trimmed)

	parts := strings.Split(trimmed, ",")
	sort.Strings(parts)
	return "{" + strings.Join(parts, ",") + "}"
}
