package asapedgeprocessor

import (
	"testing"

	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/otlpfilter"
)

// TestWireSampleRows: family → filter fan-out mapping mirrors the wrappers'
// sampling gates (per-row for CS/CMS with configured/default dims, d=1 for
// DDSketch, no entry for never-sampled families).
func TestWireSampleRows(t *testing.T) {
	cases := []struct {
		fam  MetricFamily
		rows int
		ok   bool
	}{
		{MetricFamily{Family: FamilyCountSketch, Rows: 7, Cols: 1024}, 7, true},
		{MetricFamily{Family: FamilyCountMinSketch}, 5, true}, // csmDims default rows
		{MetricFamily{Family: FamilyDDSketch}, 1, true},
		{MetricFamily{Family: FamilySum}, 0, false},
		{MetricFamily{Family: FamilyKLL}, 0, false},
		{MetricFamily{Family: FamilyHLL}, 0, false},
	}
	for _, c := range cases {
		rows, ok := wireSampleRows(&c.fam)
		if rows != c.rows || ok != c.ok {
			t.Fatalf("%s: got (%d,%v), want (%d,%v)", c.fam.Family, rows, ok, c.rows, c.ok)
		}
	}
}

// TestGrantHookUpdatesSharedFilterState exercises the exact composition
// warm_sketch.go installs: an accepted coordinator grant on the monitor
// engine lands in otlpfilter.Default() under the input metric name with the
// family's fan-out, and a p>=1 re-grant withdraws it.
func TestGrantHookUpdatesSharedFilterState(t *testing.T) {
	const (
		metric = "http.server.duration.grantwire"
		window = uint64(60_000)
	)
	aggID := fnv64(metric)
	fam := &MetricFamily{Family: FamilyCountSketch, Rows: 6, Cols: 512}
	rows, ok := wireSampleRows(fam)
	if !ok {
		t.Fatal("CountSketch must be wire-sampleable")
	}

	eng := monitor.NewEngine("edge-test", window, nil)
	// The identical closure warm_sketch.go installs.
	eng.SetSampleGrantHook(func(a uint64, p float64) {
		if a != aggID {
			return
		}
		otlpfilter.Default().Upsert(metric, otlpfilter.SampleParams{P: p, Rows: rows})
	})
	defer otlpfilter.Default().Upsert(metric, otlpfilter.SampleParams{P: 1}) // cleanup

	// Register the monitor state, then grant p=0.25.
	eng.Observe(aggID, nil, 1.0, window)
	eng.OnGrant(monitor.Grant{AggID: aggID, WindowStartMs: window, Round: 1, LocalSlack: 5, SampleP: 0.25})

	// The wire filter must now thin this metric per (p, rows).
	got, ok2 := otlpfilter.Default().Params(metric)
	if !ok2 || got.P != 0.25 || got.Rows != rows {
		t.Fatalf("grant did not land in shared filter state: %+v ok=%v", got, ok2)
	}

	// A foreign aggID grant must not touch this metric.
	eng.Observe(aggID+1, nil, 1.0, window)
	eng.OnGrant(monitor.Grant{AggID: aggID + 1, WindowStartMs: window, Round: 1, SampleP: 0.9})
	got, _ = otlpfilter.Default().Params(metric)
	if got.P != 0.25 {
		t.Fatalf("foreign-agg grant overwrote params: %+v", got)
	}

	// Coordinator withdraws sampling (p=1) → entry removed.
	eng.OnGrant(monitor.Grant{AggID: aggID, WindowStartMs: window, Round: 2, SampleP: 1.0})
	if _, still := otlpfilter.Default().Params(metric); still {
		t.Fatal("p=1 grant must withdraw the filter entry")
	}
}
