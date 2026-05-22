package gorilla

import "testing"

// TestAppendSeriesKeyPartitionsLikeLabelsFor guards the pooled-byte-key
// rewrite of getSeries: appendSeriesKey must partition (metricName, attrs)
// into series identically to the previous labelsFor(...).String() key —
// same series => same bytes, distinct series => distinct bytes — including
// labelsFor's dedup / override / empty-drop / metric-name-sanitize edges.
func TestAppendSeriesKeyPartitionsLikeLabelsFor(t *testing.T) {
	b := &StreamingTSDBBlockBuilder{
		extLabels: map[string]string{"cluster": "c1", "zone": "zEXT"},
	}
	type in struct {
		name  string
		attrs map[string]string
	}
	cases := []in{
		{"http_requests", nil},
		{"http_requests", map[string]string{}},
		{"http_requests", map[string]string{"method": "GET", "code": "200"}},
		// Same labels, different attr insertion order -> must collapse.
		{"http_requests", map[string]string{"code": "200", "method": "GET"}},
		{"http_requests", map[string]string{"method": "POST", "code": "200"}},
		{"other_metric", map[string]string{"method": "GET", "code": "200"}},
		// extLabels override an attr of the same key.
		{"http_requests", map[string]string{"zone": "zATTR"}},
		// empty-value attr is dropped (Builder.Set("") deletes) -> same as absent.
		{"http_requests", map[string]string{"method": "GET", "drop": ""}},
		{"http_requests", map[string]string{"method": "GET"}},
		// __name__ carried in attrs overrides the metric name.
		{"ignored", map[string]string{"__name__": "explicit", "method": "GET"}},
		{"explicit", map[string]string{"method": "GET"}},
	}

	myKey := make([]string, len(cases))
	refKey := make([]string, len(cases))
	for i, c := range cases {
		sc := &seriesKeyScratch{}
		b.appendSeriesKey(sc, c.name, c.attrs)
		myKey[i] = string(sc.buf)
		refKey[i] = b.labelsFor(c.name, c.attrs).String()
	}
	for i := range cases {
		for j := range cases {
			gotEq := myKey[i] == myKey[j]
			wantEq := refKey[i] == refKey[j]
			if gotEq != wantEq {
				t.Fatalf("partition mismatch i=%d j=%d: byteKeyEqual=%v labelsEqual=%v\n  ref[%d]=%q ref[%d]=%q",
					i, j, gotEq, wantEq, i, refKey[i], j, refKey[j])
			}
		}
	}
}
