package monitor

import (
	"sync"
	"testing"
)

// fakeReporter records Register/Report calls for assertions.
type fakeReporter struct {
	mu      sync.Mutex
	regs    []Registration
	reports []Report
}

func (f *fakeReporter) Register(r Registration) {
	f.mu.Lock()
	f.regs = append(f.regs, r)
	f.mu.Unlock()
}
func (f *fakeReporter) Report(r Report) {
	f.mu.Lock()
	f.reports = append(f.reports, r)
	f.mu.Unlock()
}
func (f *fakeReporter) lastReport() (Report, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reports) == 0 {
		return Report{}, false
	}
	return f.reports[len(f.reports)-1], true
}

const win = uint64(60_000)

func newTestEngine() (*Engine, *fakeReporter) {
	f := &fakeReporter{}
	return NewEngine("edge-test", win, f), f
}

// TestFirstObservationRegistersAndReports checks that the very first
// observation of an epoch both registers the monitor AND fires an immediate
// report (an early, if noisy, rate signal) — no grant is needed first:
// alerting retired, so reporting is no longer gated on a coordinator-granted
// budget.
func TestFirstObservationRegistersAndReports(t *testing.T) {
	e, f := newTestEngine()
	e.Observe(7, nil, 5, win)
	if len(f.regs) != 1 {
		t.Fatalf("expected exactly one registration, got %d", len(f.regs))
	}
	if len(f.reports) != 1 {
		t.Fatalf("expected an immediate report on the first observation, got %d", len(f.reports))
	}
	if f.regs[0].AggID != 7 || f.regs[0].EpochWindowMs != win {
		t.Fatalf("registration fields wrong: %+v", f.regs[0])
	}
}

// TestReportsOnFixedCadence checks the ReportEveryN cadence: after the
// obsCount==1 report, the next report fires only once obsCount reaches
// ReportEveryN observations — reporting no longer depends on the observed
// VALUE at all (unlike the retired slack-crossing trigger).
func TestReportsOnFixedCadence(t *testing.T) {
	e, f := newTestEngine()
	e.Observe(7, nil, 0, win) // obs #1 → report #1
	if len(f.reports) != 1 {
		t.Fatalf("expected one report after the first observation, got %d", len(f.reports))
	}
	for i := 2; i < ReportEveryN+1; i++ {
		e.Observe(7, nil, float64(i), win)
	}
	if len(f.reports) != 1 {
		t.Fatalf("expected still one report short of the cadence, got %d", len(f.reports))
	}
	e.Observe(7, nil, 999, win) // obsCount reaches ReportEveryN+1 → report #2
	if len(f.reports) != 2 {
		t.Fatalf("expected a second report once the cadence was reached, got %d", len(f.reports))
	}
	r, _ := f.lastReport()
	if r.Rate != float64(ReportEveryN+1) {
		t.Fatalf("second report Rate = %v, want %v", r.Rate, ReportEveryN+1)
	}
}

// TestGrantOnlyUpdatesSampleP checks that OnGrant no longer perturbs the
// report cadence at all — it only records SampleP (LocalSlack is vestigial,
// kept solely for SlackGrant wire compatibility).
func TestGrantOnlyUpdatesSampleP(t *testing.T) {
	e, f := newTestEngine()
	e.Observe(7, nil, 5, win) // report #1
	before := len(f.reports)
	e.OnGrant(Grant{AggID: 7, Round: 1, LocalSlack: 999, WindowStartMs: win, SampleP: 0.4})
	if len(f.reports) != before {
		t.Fatalf("OnGrant must not itself trigger a report, got %d reports", len(f.reports))
	}
	if got := e.GrantedSampleP(7); got != 0.4 {
		t.Fatalf("GrantedSampleP after grant = %v, want 0.4", got)
	}
}

// TestPollProducesImmediateReport checks that a Poll forces a report right
// away, independent of the ReportEveryN cadence.
func TestPollProducesImmediateReport(t *testing.T) {
	e, f := newTestEngine()
	e.Observe(7, nil, 0, win) // obs #1 → report #1
	if len(f.reports) != 1 {
		t.Fatalf("expected one report after the first observation, got %d", len(f.reports))
	}
	e.Observe(7, nil, 33, win) // obs #2, short of cadence → no new report
	if len(f.reports) != 1 {
		t.Fatalf("unexpected spontaneous report before the cadence, got %d", len(f.reports))
	}
	e.OnPoll(Poll{AggID: 7, Round: 1, WindowStartMs: win})
	if len(f.reports) != 2 {
		t.Fatalf("poll did not produce an immediate report, got %d", len(f.reports))
	}
	r, _ := f.lastReport()
	if r.LocalValue != 33 {
		t.Fatalf("poll report should carry current value 33, got %v", r.LocalValue)
	}
}

func TestEpochResetReRegisters(t *testing.T) {
	e, f := newTestEngine()
	e.Observe(7, nil, 0, win) // report in epoch 1
	// New epoch via explicit reset (mirrors rotateLocked).
	next := win + win
	e.EpochReset(next)
	if len(f.regs) != 1 {
		t.Fatalf("re-register should be lazy (on next Observe), regs=%d", len(f.regs))
	}
	e.Observe(7, nil, 1, next) // re-register; fresh epoch → immediate report
	if len(f.regs) != 2 {
		t.Fatalf("expected re-registration in new epoch, regs=%d", len(f.regs))
	}
	if f.regs[1].EpochWindowMs != win {
		t.Fatalf("epoch window size wrong on re-register: %+v", f.regs[1])
	}
	if len(f.reports) != 2 {
		t.Fatalf("expected a report in the new epoch too, got %d", len(f.reports))
	}
}

func TestEpochChangeViaObserveResets(t *testing.T) {
	e, _ := newTestEngine()
	e.Observe(7, nil, 0, win)
	e.OnGrant(Grant{AggID: 7, Round: 1, WindowStartMs: win, SampleP: 0.5})
	e.Observe(7, nil, 50, win)
	// Observe with a new windowStart triggers an in-line epoch reset.
	next := win + win
	e.Observe(7, nil, 3, next)
	e.mu.Lock()
	st := e.states[mapKey{7, ""}]
	e.mu.Unlock()
	if st.windowStart != next || st.obsCount != 1 || st.registered != true {
		t.Fatalf("epoch change did not reset state: %+v", st)
	}
}

// TestKeyedMonitorsAreIndependent checks that two monitors under the same
// AggID but different keys (e.g. two CMSPoint keys) get fully independent
// registration/report state.
func TestKeyedMonitorsAreIndependent(t *testing.T) {
	e, f := newTestEngine()
	ka, kb := []byte("svc=a"), []byte("svc=b")
	e.Observe(9, ka, 20, win) // report #1 for a
	e.Observe(9, kb, 5, win)  // report #1 for b
	if len(f.reports) != 2 {
		t.Fatalf("expected one report per key on first observation, got %d", len(f.reports))
	}
	if len(f.regs) != 2 {
		t.Fatalf("expected two registrations (one per key), got %d", len(f.regs))
	}
	keys := map[string]bool{}
	for _, r := range f.reports {
		keys[string(r.Key)] = true
	}
	if !keys["svc=a"] || !keys["svc=b"] {
		t.Fatalf("expected reports for both keys, got %+v", f.reports)
	}
}

// TestOnGrantStoresSampleP checks that a grant's SampleP is recorded on the
// per-(agg,key) state and is readable via GrantedSampleP for the precompute to
// apply at the next epoch boundary.
func TestOnGrantStoresSampleP(t *testing.T) {
	e, _ := newTestEngine()
	e.Observe(7, nil, 1, win) // create + register the monitor for agg 7

	// No grant yet ⇒ unsampled default.
	if got := e.GrantedSampleP(7); got != 1.0 {
		t.Fatalf("GrantedSampleP before grant = %v, want 1.0", got)
	}
	// Unknown agg ⇒ unsampled default.
	if got := e.GrantedSampleP(999); got != 1.0 {
		t.Fatalf("GrantedSampleP(unknown) = %v, want 1.0", got)
	}

	e.OnGrant(Grant{AggID: 7, Round: 1, WindowStartMs: win, SampleP: 0.3})
	if got := e.GrantedSampleP(7); got != 0.3 {
		t.Fatalf("GrantedSampleP after grant = %v, want 0.3", got)
	}

	// A grant carrying SampleP=0 means "no sampling this round"; it is recorded
	// as such and reads back as the unsampled default.
	e.OnGrant(Grant{AggID: 7, Round: 2, WindowStartMs: win, SampleP: 0})
	if got := e.GrantedSampleP(7); got != 1.0 {
		t.Fatalf("GrantedSampleP after SampleP=0 grant = %v, want 1.0 (treat-as-unset)", got)
	}
}

// TestSamplePSurvivesEpochReset checks that the granted sampling probability is
// preserved across an epoch boundary (so the new window keeps sampling at the
// last granted p) but is cleared by ForceReregister (coordinator restart).
func TestSamplePSurvivesEpochReset(t *testing.T) {
	e, _ := newTestEngine()
	e.Observe(7, nil, 1, win)
	e.OnGrant(Grant{AggID: 7, Round: 1, WindowStartMs: win, SampleP: 0.25})

	e.EpochReset(win + win) // rotate to the next epoch
	if got := e.GrantedSampleP(7); got != 0.25 {
		t.Fatalf("GrantedSampleP after EpochReset = %v, want 0.25 (sampling persists across epochs)", got)
	}
	e.ForceReregister()
	if got := e.GrantedSampleP(7); got != 1.0 {
		t.Fatalf("GrantedSampleP after ForceReregister = %v, want 1.0 (restart clears allocation)", got)
	}
}

// TestReportCarriesRate checks that a report carries the edge's observed
// per-epoch item count as Report.Rate.
func TestReportCarriesRate(t *testing.T) {
	e, f := newTestEngine()
	e.Observe(7, nil, 5, win) // obs #1 → report #1, Rate=1
	e.Observe(7, nil, 8, win) // obs #2, short of cadence → no new report
	r, ok := f.lastReport()
	if !ok {
		t.Fatalf("expected a report after the first observation")
	}
	if r.Rate != 1 {
		t.Fatalf("report Rate = %v, want 1 (observed items at report time)", r.Rate)
	}
}
