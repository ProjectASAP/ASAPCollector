// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"sync"
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
)

// fakeControlChannel is a deterministic in-memory ControlChannel for testing
// the poll loop without standing up an HTTP server.
type fakeControlChannel struct {
	mu     sync.Mutex
	queued []*precompute.PrecomputeConfigSet
	acked  []uint64
}

func (f *fakeControlChannel) push(set *precompute.PrecomputeConfigSet) {
	f.mu.Lock()
	f.queued = append(f.queued, set)
	f.mu.Unlock()
}

func (f *fakeControlChannel) Poll() *precompute.PrecomputeConfigSet {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.queued) == 0 {
		return nil
	}
	s := f.queued[0]
	f.queued = f.queued[1:]
	return s
}

func (f *fakeControlChannel) Ack(v uint64) {
	f.mu.Lock()
	f.acked = append(f.acked, v)
	f.mu.Unlock()
}

// TestControlPlaneAppliesConfig covers P1 #4: applyConfigSet swaps a received
// PrecomputeConfigSet into the live sketch aggregator via UpdateConfig (in
// place) and acks the version.
func TestControlPlaneAppliesConfig(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlChannel{}
	p.ctrlChan = fake

	// Build a config set targeting the live aggregator's AggID so UpdateConfig
	// picks it. A high MaxSeries proves the update is applied (the swap is
	// in-place; we only assert it doesn't panic and the version is acked).
	aggID := aggID("lat", &cfg.Metrics[0], cfg.WindowDuration, 0, 0)
	set := &precompute.PrecomputeConfigSet{
		Version: 7,
		Configs: []precompute.PrecomputeConfig{{
			AggID:      aggID,
			SketchType: precompute.SketchTypeDDSketch,
			Mode:       precompute.Tumbling,
			Window:     precompute.WindowSpec{Size: time.Hour},
			MetricName: "lat",
			MaxSeries:  500,
		}},
	}
	p.applyConfigSet(set)

	if p.ctrlLastApply.Load() != 7 {
		t.Fatalf("ctrlLastApply = %d, want 7", p.ctrlLastApply.Load())
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.acked) != 1 || fake.acked[0] != 7 {
		t.Fatalf("acked = %v, want [7]", fake.acked)
	}

	// The aggregator must still observe cleanly after the in-place swap (state
	// preserved, no rebuild).
	sa := p.shards[0].sketchAggs["lat"]
	sa.observe(map[string]string{"zone": "z0"}, 1, uint64(time.Now().UnixMilli()), false, 0, 0)
	if sa.lastObserveErr != nil {
		t.Fatalf("observe after UpdateConfig errored: %v", sa.lastObserveErr)
	}
}

// TestControlPlanePollLoop drives the goroutine end-to-end through a fake
// channel and asserts the queued set is applied + acked, then stops cleanly.
func TestControlPlanePollLoop(t *testing.T) {
	cfg := &Config{
		ShardCount:     1,
		WindowDuration: time.Hour,
		Metrics:        []MetricFamily{{Metric: "lat", Family: FamilyDDSketch, RelativeAccuracy: 0.01}},
		Cold:           ColdConfig{Enabled: false},
		ControlChannel: ControlChannelConfig{Enabled: true, PollURL: "http://unused", PollInterval: 5 * time.Millisecond},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlChannel{}
	fake.push(&precompute.PrecomputeConfigSet{
		Version: 11,
		Configs: []precompute.PrecomputeConfig{{
			AggID:      aggID("lat", &cfg.Metrics[0], cfg.WindowDuration, 0, 0),
			SketchType: precompute.SketchTypeDDSketch,
			Mode:       precompute.Tumbling,
			Window:     precompute.WindowSpec{Size: time.Hour},
			MetricName: "lat",
		}},
	})
	p.ctrlChan = fake // replace the real HTTP channel with the fake
	p.startControlPlane()

	deadline := time.After(2 * time.Second)
	for p.ctrlLastApply.Load() != 11 {
		select {
		case <-deadline:
			t.Fatal("control poll loop never applied version 11")
		default:
			time.Sleep(2 * time.Millisecond)
		}
	}
	p.stopControlPlane()
	// stopControlPlane joins the goroutine; a second call must be a no-op.
	p.stopControlPlane()
}
