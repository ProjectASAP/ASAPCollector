package monitor

import "testing"

// TestSampleGrantHook_FiresOnAcceptedGrant: the hook receives (aggID, SampleP)
// exactly when a grant is accepted (known monitor, fresh epoch), mirroring
// grantedSampleP storage — the OnGrant → wire-filter forwarding contract.
func TestSampleGrantHook_FiresOnAcceptedGrant(t *testing.T) {
	const (
		aggID  = uint64(42)
		window = uint64(60_000)
	)
	eng := NewEngine("edge-1", window, nil)

	var gotAgg uint64
	var gotP float64
	fired := 0
	eng.SetSampleGrantHook(func(a uint64, p float64) {
		gotAgg, gotP = a, p
		fired++
	})

	// Grant before any Observe: unknown monitor → dropped, hook silent.
	eng.OnGrant(Grant{AggID: aggID, WindowStartMs: window, Round: 1, LocalSlack: 10, SampleP: 0.25})
	if fired != 0 {
		t.Fatalf("hook fired for unknown monitor")
	}

	// Register the monitor state via the hot path, then grant.
	eng.Observe(aggID, nil, 1.0, window)
	eng.OnGrant(Grant{AggID: aggID, WindowStartMs: window, Round: 1, LocalSlack: 10, SampleP: 0.25})
	if fired != 1 || gotAgg != aggID || gotP != 0.25 {
		t.Fatalf("accepted grant: fired=%d agg=%d p=%v, want 1/%d/0.25", fired, gotAgg, gotP, aggID)
	}

	// Stale epoch → dropped, hook silent.
	eng.OnGrant(Grant{AggID: aggID, WindowStartMs: window - 60_000, Round: 2, SampleP: 0.5})
	if fired != 1 {
		t.Fatalf("hook fired for stale-epoch grant")
	}

	// Re-grant with a new p on the fresh epoch → fires again (p updates).
	eng.OnGrant(Grant{AggID: aggID, WindowStartMs: window, Round: 2, LocalSlack: 10, SampleP: 0.5})
	if fired != 2 || gotP != 0.5 {
		t.Fatalf("re-grant: fired=%d p=%v, want 2/0.5", fired, gotP)
	}
}
