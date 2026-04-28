package main

import "testing"

func TestParsePromInstruments(t *testing.T) {
	got := parsePromInstruments("counter,gauge,histogram")
	if !got.counter || !got.gauge || !got.histogram {
		t.Fatalf("parsePromInstruments missed an enabled instrument: %+v", got)
	}

	got = parsePromInstruments("unknown")
	if !got.counter || !got.gauge || got.histogram {
		t.Fatalf("parsePromInstruments fallback = %+v, want counter+gauge", got)
	}
}

func TestBuildPromLabelValuesMatchesSyntheticSchema(t *testing.T) {
	got := buildPromLabelValues(3, 2, 2, 2, 2)
	want := [][]string{
		{"z0", "r00", "n00", "pod-000"},
		{"z1", "r00", "n00", "pod-000"},
		{"z0", "r01", "n00", "pod-000"},
	}
	if len(got) != len(want) {
		t.Fatalf("len(buildPromLabelValues) = %d, want %d", len(got), len(want))
	}
	for i := range want {
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Fatalf("label[%d][%d] = %q, want %q", i, j, got[i][j], want[i][j])
			}
		}
	}
}
