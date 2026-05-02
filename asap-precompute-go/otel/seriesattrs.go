package otel

import (
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// AttributesToKeyValues converts pcommon.Map into the host-neutral
// []KeyValue slice. Keys are sorted alphabetically to match today's
// per-processor seriesKey/attributesKey byte layout — the SeriesKey
// produced from this slice plus the sorted-key writeAttributesKey
// helper in matchers.go is byte-equivalent to the existing OTel
// processor's `attributesKey(...)` output.
//
// Values use pcommon.Value.AsString() so non-string types
// (Int / Double / Bool / Bytes) get the same string spelling the
// existing OTel processor uses.
func AttributesToKeyValues(attrs pcommon.Map) []precompute.KeyValue {
	if attrs.Len() == 0 {
		return nil
	}
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	out := make([]precompute.KeyValue, 0, len(keys))
	for _, k := range keys {
		v, ok := attrs.Get(k)
		if !ok {
			continue
		}
		out = append(out, precompute.KeyValue{Key: k, Value: v.AsString()})
	}
	return out
}

// KeyValuesToAttributes is the inverse — used on the encode path
// to populate output pmetric data points. Existing entries in dst
// are left in place; duplicate keys overwrite. All values are
// written as strings (since the host-neutral KeyValue carries
// strings only).
func KeyValuesToAttributes(kvs []precompute.KeyValue, dst pcommon.Map) {
	for _, kv := range kvs {
		dst.PutStr(kv.Key, kv.Value)
	}
}
