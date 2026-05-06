// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// mvp/v5: per-block postings index — `label_name=value → series_ids`.
//
// Byte-compatible with the `asap-gorilla::postings` Rust module
// (`POSTING1` magic + version + JSON body + CRC32 trailer). The
// backend `GorillaQueryEngine` reads this sidecar to skip chunks
// that don't match label predicates without scanning every chunk.

package gorillas3processor

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
)

const (
	postingsMagic   = "POSTING1"
	postingsVersion = uint8(1)
)

// postingsEntry is one row inside the on-wire postings JSON body.
type postingsEntry struct {
	LabelName  string   `json:"label_name"`
	LabelValue string   `json:"label_value"`
	SeriesIDs  []uint64 `json:"series_ids"`
}

// postingsBody is the JSON-serializable wrapper around the
// `(label_name, label_value, series_ids)` triples. Field order +
// types match the Rust `asap-gorilla::postings::PostingsBody` struct
// exactly.
type postingsBody struct {
	SchemaVersion uint8           `json:"schema_version"`
	GeneratedAtNS uint64          `json:"generated_at_ns"`
	Entries       []postingsEntry `json:"entries"`
}

// buildPostings walks the per-window series map and returns a
// `postings-v1.json` byte blob for the supplied chunks. Each chunk
// contributes one series_id per (metric, attributes) buffer that
// lives inside it. The series_id is the same 64-bit canonical-label-
// set hash already used by `IndexEntry.label_hash`, so the backend
// can join postings → index entries without a second hash table.
//
// Output bytes layout (matches the Rust crate):
//
//	[8]   magic            "POSTING1"
//	[1]   version          1
//	[4]   uint32 LE        body_json_len
//	[…]   body_json        sorted-by-(label_name,label_value) entries
//	[4]   uint32 LE        crc32_ieee(body_json)
//
// CRC is `hash/crc32.IEEE` — same polynomial as the Rust `crc32fast`
// crate (despite the file's `POSTING1` magic suggesting CRC32C; we
// favour the dependency-free IEEE poly to keep both sides identical
// and detect-corruption is the only contract). See
// `asap-gorilla/src/postings.rs` `crc32c` fn for the matching note.
func buildPostings(series map[seriesKey]*seriesBuffer, generatedAtNS int64) ([]byte, error) {
	// Step 1: bucket series_ids by (label_name, label_value).
	type labelKey struct {
		name  string
		value string
	}
	buckets := make(map[labelKey]map[uint64]struct{})
	for sk, buf := range series {
		if buf == nil || len(buf.points) == 0 {
			continue
		}
		seriesID := canonicalLabelHash(sk.metricName, buf.attributes)
		// Synthetic `__name__` posting so the backend can
		// short-circuit metric-only queries via the same path.
		add := func(n, v string) {
			lk := labelKey{name: n, value: v}
			set, ok := buckets[lk]
			if !ok {
				set = make(map[uint64]struct{})
				buckets[lk] = set
			}
			set[seriesID] = struct{}{}
		}
		add("__name__", sk.metricName)
		for k, v := range buf.attributes {
			add(k, v)
		}
	}

	// Step 2: sort keys for deterministic output (same input → same
	// bytes, same as the Rust side).
	keys := make([]labelKey, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].name != keys[j].name {
			return keys[i].name < keys[j].name
		}
		return keys[i].value < keys[j].value
	})

	entries := make([]postingsEntry, 0, len(keys))
	for _, k := range keys {
		ids := buckets[k]
		idList := make([]uint64, 0, len(ids))
		for id := range ids {
			idList = append(idList, id)
		}
		sort.Slice(idList, func(i, j int) bool { return idList[i] < idList[j] })
		entries = append(entries, postingsEntry{
			LabelName:  k.name,
			LabelValue: k.value,
			SeriesIDs:  idList,
		})
	}

	// Step 3: marshal the JSON body and frame it with magic + crc.
	body := postingsBody{
		SchemaVersion: postingsVersion,
		GeneratedAtNS: uint64(generatedAtNS),
		Entries:       entries,
	}
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("postings: marshal body: %w", err)
	}
	if len(bodyJSON) > int(^uint32(0)) {
		return nil, fmt.Errorf("postings: body too large (%d bytes)", len(bodyJSON))
	}

	out := make([]byte, 0, 8+1+4+len(bodyJSON)+4)
	out = append(out, []byte(postingsMagic)...)
	out = append(out, postingsVersion)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(bodyJSON)))
	out = append(out, lenBuf[:]...)
	out = append(out, bodyJSON...)
	crc := crc32.ChecksumIEEE(bodyJSON)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	out = append(out, crcBuf[:]...)
	return out, nil
}

// canonicalLabelHash computes a deterministic 64-bit hash of the
// (metric_name, sorted attributes) tuple. Matches the convention the
// backend uses for `IndexEntry.label_hash`. We use FNV-1a directly
// (no extra dependency) — the wire commitment is just "stable across
// agent restarts", not a particular polynomial.
func canonicalLabelHash(metricName string, attrs map[string]string) uint64 {
	const (
		fnvOffset uint64 = 14695981039346656037
		fnvPrime  uint64 = 1099511628211
	)
	h := fnvOffset
	mix := func(s string) {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= fnvPrime
		}
	}
	mix(metricName)
	mix("\x00")
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		mix(k)
		mix("=")
		mix(attrs[k])
		mix(";")
	}
	return h
}
