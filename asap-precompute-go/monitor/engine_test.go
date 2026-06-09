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

func TestSilentBeforeGrant(t *testing.T) {
	e, f := newTestEngine()
	// Many observations, no grant yet → must stay silent (but register once).
	for v := 1.0; v <= 100; v++ {
		e.Observe(7, nil, v, win)
	}
	if len(f.reports) != 0 {
		t.Fatalf("expected zero reports before any grant, got %d", len(f.reports))
	}
	if len(f.regs) != 1 {
		t.Fatalf("expected exactly one registration, got %d", len(f.regs))
	}
	if f.regs[0].AggID != 7 || f.regs[0].EpochWindowMs != win {
		t.Fatalf("registration fields wrong: %+v", f.regs[0])
	}
}

func TestReportsWhenSlackCrossed(t *testing.T) {
	e, f := newTestEngine()
	e.Observe(7, nil, 5, win) // register; baseline=0 (epoch start), no grant yet
	e.OnGrant(Grant{AggID: 7, Round: 1, LocalSlack: 10, WindowStartMs: win})
	// baseline stays 0; report fires when value-0 >= 10.
	e.Observe(7, nil, 8, win) // 8 < 10 → silent
	if len(f.reports) != 0 {
		t.Fatalf("reported too early: %d reports", len(f.reports))
	}
	e.Observe(7, nil, 12, win) // 12 >= 10 → report; baseline advances to 12
	e.Observe(7, nil, 18, win) // 18-12=6 < 10 → silent
	if len(f.reports) != 1 {
		t.Fatalf("expected one report after first crossing, got %d", len(f.reports))
	}
	e.Observe(7, nil, 23, win) // 23-12=11 >= 10 → second report; baseline=23
	if len(f.reports) != 2 {
		t.Fatalf("expected a second report once baseline+slack crossed again, got %d", len(f.reports))
	}
	r, _ := f.lastReport()
	if r.LocalValue != 23 || r.Round != 1 || r.Seq != 2 {
		t.Fatalf("report fields wrong: %+v", r)
	}
}

func TestShrinkingSlackLowersBar(t *testing.T) {
	e, f := newTestEngine()
	e.Observe(7, nil, 0, win)
	e.OnGrant(Grant{AggID: 7, Round: 1, LocalSlack: 10, WindowStartMs: win})
	e.Observe(7, nil, 12, win) // report #1; baseline=12
	// Coordinator re-grants a SMALLER slack (no baseline reset on the edge).
	e.OnGrant(Grant{AggID: 7, Round: 2, LocalSlack: 5, WindowStartMs: win})
	e.Observe(7, nil, 14, win) // 14-12=2 < 5 → silent
	e.Observe(7, nil, 18, win) // 18-12=6 >= 5 → report #2; baseline=18
	if len(f.reports) != 2 {
		t.Fatalf("expected 2 reports, got %d", len(f.reports))
	}
	r, _ := f.lastReport()
	if r.Round != 2 || r.LocalValue != 18 {
		t.Fatalf("second report wrong: %+v", r)
	}
}

func TestPollProducesAuthoritativeReport(t *testing.T) {
	e, f := newTestEngine()
	e.Observe(7, nil, 0, win)
	e.OnGrant(Grant{AggID: 7, Round: 1, LocalSlack: 100, WindowStartMs: win})
	e.Observe(7, nil, 33, win) // below slack → no spontaneous report
	if len(f.reports) != 0 {
		t.Fatalf("unexpected spontaneous report")
	}
	e.OnPoll(Poll{AggID: 7, Round: 1, WindowStartMs: win})
	if len(f.reports) != 1 {
		t.Fatalf("poll did not produce a report")
	}
	r, _ := f.lastReport()
	if r.LocalValue != 33 {
		t.Fatalf("poll report should carry current value 33, got %v", r.LocalValue)
	}
}

func TestEpochResetReRegisters(t *testing.T) {
	e, f := newTestEngine()
	e.Observe(7, nil, 0, win)
	e.OnGrant(Grant{AggID: 7, Round: 1, LocalSlack: 5, WindowStartMs: win})
	e.Observe(7, nil, 10, win) // report in epoch 1
	// New epoch via explicit reset (mirrors rotateLocked).
	next := win + win
	e.EpochReset(next)
	if len(f.regs) != 1 {
		t.Fatalf("re-register should be lazy (on next Observe), regs=%d", len(f.regs))
	}
	e.Observe(7, nil, 1, next) // re-register; baseline cleared; no grant yet → silent
	if len(f.regs) != 2 {
		t.Fatalf("expected re-registration in new epoch, regs=%d", len(f.regs))
	}
	if f.regs[1].EpochWindowMs != win {
		t.Fatalf("epoch window size wrong on re-register: %+v", f.regs[1])
	}
	// A grant from the OLD epoch must be ignored.
	before := len(f.reports)
	e.OnGrant(Grant{AggID: 7, Round: 1, LocalSlack: 1, WindowStartMs: win})
	e.Observe(7, nil, 100, next)
	if len(f.reports) != before {
		t.Fatalf("stale-epoch grant should not enable reporting")
	}
}

func TestEpochChangeViaObserveResets(t *testing.T) {
	e, _ := newTestEngine()
	e.Observe(7, nil, 0, win)
	e.OnGrant(Grant{AggID: 7, Round: 1, LocalSlack: 5, WindowStartMs: win})
	e.Observe(7, nil, 50, win)
	// Observe with a new windowStart triggers an in-line epoch reset.
	next := win + win
	e.Observe(7, nil, 3, next)
	e.mu.Lock()
	st := e.states[mapKey{7, ""}]
	e.mu.Unlock()
	if st.windowStart != next || st.grantedSlack != 0 || st.registered != true {
		t.Fatalf("epoch change did not reset state: %+v", st)
	}
}

func TestCMSPointKeyedMonitors(t *testing.T) {
	e, f := newTestEngine()
	ka, kb := []byte("svc=a"), []byte("svc=b")
	e.Observe(9, ka, 0, win)
	e.Observe(9, kb, 0, win)
	e.OnGrant(Grant{AggID: 9, Key: ka, Round: 1, LocalSlack: 10, WindowStartMs: win})
	e.OnGrant(Grant{AggID: 9, Key: kb, Round: 1, LocalSlack: 10, WindowStartMs: win})
	e.Observe(9, ka, 20, win) // a crosses
	e.Observe(9, kb, 5, win)  // b does not
	if len(f.reports) != 1 {
		t.Fatalf("expected one report (only key a crossed), got %d", len(f.reports))
	}
	r, _ := f.lastReport()
	if string(r.Key) != "svc=a" {
		t.Fatalf("report should be for key a, got %q", r.Key)
	}
	if len(f.regs) != 2 {
		t.Fatalf("expected two registrations (one per key), got %d", len(f.regs))
	}
}
