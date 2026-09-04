// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"sync"
	"testing"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// fakeControlChannel is a deterministic in-memory ControlChannel for testing
// the poll loop without standing up an HTTP server.
type fakeControlChannel struct {
	mu     sync.Mutex
	queued []*precompute.PrecomputeConfigSet
	acked  []uint64
}

type blockingAckChannel struct {
	set        *precompute.PrecomputeConfigSet
	ackStarted chan struct{}
	closed     chan struct{}
	once       sync.Once
}

func (c *blockingAckChannel) Poll() *precompute.PrecomputeConfigSet {
	set := c.set
	c.set = nil
	return set
}
func (c *blockingAckChannel) Ack(uint64) {
	c.once.Do(func() { close(c.ackStarted) })
	<-c.closed
}
func (c *blockingAckChannel) Close() error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
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

func TestPhysicalGenerationAppliesExactlyAndEmitsPlanIdentity(t *testing.T) {
	cfg := &Config{
		ShardCount: 1, WindowDuration: time.Minute, DropOriginal: true,
		Metrics: []MetricFamily{{Metric: "requests", Family: FamilyHLL}},
		Cold:    ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	sink := &capMetrics{}
	p, err := newProcessor(cfg, testSettings(), sink)
	if err != nil {
		t.Fatal(err)
	}
	set, err := precompute.DecodeCollectorPlan(testCollectorPlan(t), "edge-a")
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlChannel{}
	p.ctrlChan = fake
	if err := p.applyConfigSet(set); err != nil {
		t.Fatal(err)
	}
	if len(fake.acked) != 1 || fake.acked[0] != set.Version {
		t.Fatalf("physical generation was not acknowledged exactly once: %v", fake.acked)
	}

	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("requests")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(7)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatal(err)
	}
	p.flushAll(context.Background())
	found := false
	for _, batch := range sink.got {
		rms := batch.ResourceMetrics()
		for i := 0; i < rms.Len(); i++ {
			sms := rms.At(i).ScopeMetrics()
			for j := 0; j < sms.Len(); j++ {
				metrics := sms.At(j).Metrics()
				for k := 0; k < metrics.Len(); k++ {
					metric := metrics.At(k)
					if metric.Type() != pmetric.MetricTypeHLLSketch || metric.HLLSketch().DataPoints().Len() == 0 {
						continue
					}
					attrs := metric.HLLSketch().DataPoints().At(0).Attributes()
					value, ok := attrs.Get("asap.frame.plan_id")
					found = ok && value.AsString() == "42"
				}
			}
		}
	}
	if !found {
		t.Fatal("production summary path did not attach the plan-derived frame identity")
	}
}

func TestPhysicalGenerationFailureIsNeverAcknowledged(t *testing.T) {
	cfg := &Config{
		ShardCount: 1, WindowDuration: time.Minute,
		Metrics: []MetricFamily{{Metric: "requests", Family: FamilyHLL}},
		Cold:    ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	set, err := precompute.DecodeCollectorPlan(testCollectorPlan(t), "edge-a")
	if err != nil {
		t.Fatal(err)
	}
	set.Configs[0].SketchType = precompute.SketchTypeDDSketch
	fake := &fakeControlChannel{}
	p.ctrlChan = fake
	if err := p.applyConfigSet(set); err == nil {
		t.Fatal("incompatible physical generation was accepted")
	}
	if len(fake.acked) != 0 {
		t.Fatalf("failed generation was acknowledged: %v", fake.acked)
	}
}

func TestPhysicalGenerationDrainFailureIsNeverAcknowledged(t *testing.T) {
	cfg := &Config{
		ShardCount: 1, WindowDuration: time.Minute,
		Metrics: []MetricFamily{{Metric: "requests", Family: FamilyHLL}},
		Cold:    ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &rejectMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := precompute.DecodeCollectorPlan(testCollectorPlan(t), "edge-a")
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeControlChannel{}
	p.ctrlChan = fake
	if err := p.applyConfigSet(first); err != nil {
		t.Fatal(err)
	}

	// Leave state in the first physical generation so the second cutover must
	// publish a final frame under the old identity.
	p.shards[0].sketchAggs["requests"].observe(
		map[string]string{"service": "checkout"}, 1, uint64(time.Now().UnixMilli()), false, 0, 0,
	)
	second, err := precompute.DecodeCollectorPlan(testCollectorPlan(t), "edge-a")
	if err != nil {
		t.Fatal(err)
	}
	second.Version++
	second.CollectorPlan.Envelope.PlanVersion = second.Version
	if err := p.applyConfigSet(second); err == nil {
		t.Fatal("cutover acknowledged despite rejected retired-generation frames")
	}
	if len(fake.acked) != 1 || fake.acked[0] != first.Version {
		t.Fatalf("acked = %v, want only initial version %d", fake.acked, first.Version)
	}
}

func TestPhysicalGenerationRejectsTwoMaterializationsForOneMetric(t *testing.T) {
	cfg := &Config{
		ShardCount: 1, WindowDuration: time.Minute,
		Metrics: []MetricFamily{{Metric: "requests", Family: FamilyHLL}},
		Cold:    ColdConfig{Enabled: false},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	p, err := newProcessor(cfg, testSettings(), &capMetrics{})
	if err != nil {
		t.Fatal(err)
	}
	set, err := precompute.DecodeCollectorPlan(testCollectorPlan(t), "edge-a")
	if err != nil {
		t.Fatal(err)
	}
	duplicate := set.Configs[0]
	duplicate.AggID++
	set.Configs = append(set.Configs, duplicate)
	rule := set.CollectorPlan.TransmissionRules[0]
	rule.Materialization = uint64(duplicate.AggID)
	set.CollectorPlan.TransmissionRules = append(set.CollectorPlan.TransmissionRules, rule)
	fake := &fakeControlChannel{}
	p.ctrlChan = fake
	if err := p.applyConfigSet(set); err == nil {
		t.Fatal("same-metric materializations were silently collapsed into one executor")
	}
	if len(fake.acked) != 0 {
		t.Fatalf("unsupported generation was acknowledged: %v", fake.acked)
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

func TestStopControlPlaneCancelsBlockedAckBeforeJoin(t *testing.T) {
	p := &asapEdgeProcessor{
		cfg:    &Config{ControlChannel: ControlChannelConfig{PollInterval: time.Millisecond}},
		logger: zap.NewNop(),
	}
	channel := &blockingAckChannel{
		set:        &precompute.PrecomputeConfigSet{Version: 1},
		ackStarted: make(chan struct{}), closed: make(chan struct{}),
	}
	p.ctrlChan = channel
	p.startControlPlane()
	select {
	case <-channel.ackStarted:
	case <-time.After(time.Second):
		t.Fatal("control loop never reached Ack")
	}
	done := make(chan struct{})
	go func() {
		p.stopControlPlane()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stopControlPlane deadlocked behind blocked Ack")
	}
}
