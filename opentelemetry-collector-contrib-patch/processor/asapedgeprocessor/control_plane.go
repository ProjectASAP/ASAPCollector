// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/controlchannel"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// controlChannel is the minimal surface the poll loop needs from
// controlchannel.ControlChannel. Declaring it locally lets tests inject a fake
// channel without standing up an HTTP server, and keeps the loop testable in
// isolation from the concrete HttpPollChannel.
type controlChannel interface {
	// Poll returns a non-nil set when the plan changed since the last poll;
	// nil means "no change" (or an error, which the implementation logs).
	Poll() *precompute.PrecomputeConfigSet
	// Ack confirms acceptance of a plan version.
	Ack(planVersion uint64)
}

// newControlChannel builds the concrete HTTP-poll control channel from cfg.
// Returns (nil, nil) when the control plane is disabled so callers can no-op.
func newControlChannel(cfg ControlChannelConfig, logger *zap.Logger) (controlChannel, error) {
	if !cfg.enabled() || cfg.OpAMPExtension != nil {
		return nil, nil
	}
	ch, err := controlchannel.NewHttpPollChannel(controlchannel.HttpPollConfig{
		URL:             cfg.PollURL,
		AckURL:          cfg.AckURL,
		Interval:        cfg.PollInterval,
		Timeout:         cfg.Timeout,
		BearerTokenFile: cfg.BearerTokenFile,
		// Route the channel's internal log lines through zap so they land in the
		// collector's log stream rather than the default logger.
		Logger: zapBridgeLogger(logger),
	})
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// zapBridgeLogger adapts the stdlib *log.Logger the controlchannel package
// expects onto zap (info level). A nil zap logger yields the stdlib default.
func zapBridgeLogger(logger *zap.Logger) *log.Logger {
	if logger == nil {
		return log.Default()
	}
	return log.New(&zapInfoWriter{logger: logger}, "", 0)
}

type zapInfoWriter struct{ logger *zap.Logger }

func (w *zapInfoWriter) Write(p []byte) (int, error) {
	w.logger.Info("asap_edge: control_channel", zap.ByteString("msg", p))
	return len(p), nil
}

// startControlPlane spawns the config-poll goroutine when the control channel
// is configured. The loop polls on ControlChannel's interval and applies each
// complete generation. Typed physical plans use the all-shard cutover below;
// compatibility-only config sets retain the legacy in-place update. A no-op
// when ctrlChan is nil (control plane disabled).
func (p *asapEdgeProcessor) startControlPlane() {
	if p.ctrlChan == nil {
		return
	}
	p.ctrlStarted = true
	p.ctrlStopCh = make(chan struct{})
	p.ctrlDoneCh = make(chan struct{})
	interval := p.cfg.ControlChannel.PollInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go p.controlPollLoop(interval)
}

// controlPollLoop runs until ctrlStopCh closes, polling the control channel
// each tick and applying any new config set.
func (p *asapEdgeProcessor) controlPollLoop(interval time.Duration) {
	defer close(p.ctrlDoneCh)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-p.ctrlStopCh:
			return
		case <-t.C:
			if set := p.ctrlChan.Poll(); set != nil {
				if err := p.applyConfigSet(set); err != nil {
					if rejecter, ok := p.ctrlChan.(interface{ Reject(uint64, error) }); ok {
						rejecter.Reject(set.Version, err)
					}
					p.logger.Error("asap_edge: rejected control-plane config update", zap.Error(err))
				}
			}
		}
	}
}

// applyConfigSet installs one complete config generation and acknowledges it
// only after installation succeeds. Typed CollectorPlans take the generation
// cutover barrier; compatibility-only config sets retain the historical
// lock-free Precompute.UpdateConfig path.
func (p *asapEdgeProcessor) applyConfigSet(set *precompute.PrecomputeConfigSet) error {
	if set == nil {
		return errors.New("asap_edge: nil config set")
	}
	if set.CollectorPlan != nil {
		if err := p.applyPhysicalGeneration(set); err != nil {
			return err
		}
	} else {
		// Compatibility-only legacy updates retain their historical behavior.
		for _, sh := range p.shards {
			for _, sa := range sh.sketchAggs {
				sa.pc.UpdateConfig(set)
			}
		}
	}
	p.ctrlLastApply.Store(set.Version)
	p.ctrlChan.Ack(set.Version)
	p.logger.Info("asap_edge: applied control-plane config update",
		zap.Uint64("plan_version", set.Version), zap.Int("configs", len(set.Configs)))
	return nil
}

// applyPhysicalGeneration validates the complete runtime target before taking
// every shard lock. The locked section drains the previous generation and
// installs all materializations as one ingestion/flush boundary; no partial
// generation is ever acknowledged.
func (p *asapEdgeProcessor) applyPhysicalGeneration(set *precompute.PrecomputeConfigSet) error {
	plan := set.CollectorPlan
	if plan == nil || len(set.Configs) == 0 {
		return errors.New("asap_edge: typed CollectorPlan must contain materializations")
	}
	configs := make(map[string]*precompute.PrecomputeConfig, len(set.Configs))
	for i := range set.Configs {
		cfg := &set.Configs[i]
		if cfg.MetricName == "" || configs[cfg.MetricName] != nil {
			return fmt.Errorf("asap_edge: duplicate or empty physical metric %q", cfg.MetricName)
		}
		configs[cfg.MetricName] = cfg
		if transmissionRuleFor(plan, uint64(cfg.AggID)) == nil {
			return fmt.Errorf("asap_edge: missing transmission rule for %d", cfg.AggID)
		}
	}
	if len(configs) != len(p.sketchMetrics) {
		return fmt.Errorf("asap_edge: physical generation has %d metrics; runtime has %d", len(configs), len(p.sketchMetrics))
	}
	for metric, fam := range p.sketchMetrics {
		cfg := configs[metric]
		if cfg == nil {
			return fmt.Errorf("asap_edge: physical generation omits runtime metric %q", metric)
		}
		if !runtimeShapeMatches(cfg, fam, p.cfg.WindowDuration) {
			return fmt.Errorf("asap_edge: physical materialization for %q does not match the installed executor shape", metric)
		}
	}
	for _, sh := range p.shards {
		sh.mu.Lock()
	}
	retired := pmetric.NewMetrics()
	for _, sh := range p.shards {
		for metric, sa := range sh.sketchAggs {
			cfg := configs[metric]
			// Retire the old open window before changing identity or semantics.
			// A previously plan-bound generation is emitted with its old identity;
			// bootstrap static state is discarded rather than mislabeled.
			if sa.framePlan != nil {
				sa.flush(retired)
			} else {
				_ = sa.pc.Drain()
			}
			copyCfg := *cfg
			sa.pc.UpdateConfig(&precompute.PrecomputeConfigSet{Version: set.Version, Configs: []precompute.PrecomputeConfig{copyCfg}})
			sa.pcfg = &copyCfg
			rule := transmissionRuleFor(plan, uint64(copyCfg.AggID))
			ruleCopy := *rule
			sa.framePlan = plan
			sa.frameRule = &ruleCopy
			sa.frameSequencer = precompute.FrameSequencer{}
			sa.producerEpoch = p.producerEpoch
			sa.checkpointAtMS = 0
		}
	}
	for i := len(p.shards) - 1; i >= 0; i-- {
		p.shards[i].mu.Unlock()
	}
	// Downstream publication may block; keep it outside the cutover barrier, but
	// never report APPLIED when the final frames of the retired generation were
	// rejected by the downstream pipeline.
	if err := p.forward(context.Background(), retired); err != nil {
		return fmt.Errorf("asap_edge: publish retired physical generation: %w", err)
	}
	return nil
}

func transmissionRuleFor(plan *precompute.CollectorPlan, materialization uint64) *precompute.TransmissionRule {
	for i := range plan.TransmissionRules {
		if plan.TransmissionRules[i].Materialization == materialization {
			return &plan.TransmissionRules[i]
		}
	}
	return nil
}

func runtimeShapeMatches(cfg *precompute.PrecomputeConfig, fam *MetricFamily, window time.Duration) bool {
	if cfg == nil || fam == nil || cfg.Mode != precompute.Tumbling ||
		cfg.Window.Size != window || cfg.Window.Slide != window ||
		!reflect.DeepEqual(cfg.AggregateBy, fam.AggregateBy) {
		return false
	}
	wantType := precompute.SketchTypeUnspecified
	wantAgg := precompute.AggKindSketch
	switch fam.Family {
	case FamilyDDSketch:
		wantType = precompute.SketchTypeDDSketch
		if cfg.SketchParams.Get("relative_accuracy", 0) != fam.RelativeAccuracy {
			return false
		}
	case FamilyKLL:
		wantType = precompute.SketchTypeKLLSketch
		k := fam.K
		if k < 2 {
			k = 200
		}
		if cfg.SketchParams.Get("k", 0) != float64(k) {
			return false
		}
	case FamilyHLL:
		wantType = precompute.SketchTypeHLLSketch
		if cfg.SketchParams.Get("precision", 0) != 14 || fam.HLLSparse {
			return false
		}
	case FamilyCountMinSketch:
		wantType = precompute.SketchTypeCountMinSketch
		rows, cols := csmDims(fam)
		if cfg.SketchParams.Get("rows", 0) != float64(rows) || cfg.SketchParams.Get("columns", 0) != float64(cols) {
			return false
		}
	case FamilyCountSketch:
		wantType = precompute.SketchTypeCountSketch
		rows, cols := csmDims(fam)
		if cfg.SketchParams.Get("depth", 0) != float64(rows) || cfg.SketchParams.Get("width", 0) != float64(cols) {
			return false
		}
	case FamilySum:
		wantAgg = precompute.AggKindSum
	default:
		return false
	}
	return cfg.SketchType == wantType && cfg.AggKind == wantAgg
}

// stopControlPlane signals the poll loop to exit and waits (bounded by the
// loop's own tick granularity) for it to drain. No-op when not started, and
// idempotent (a second call returns immediately) so it is safe to call from
// both Shutdown and a test.
func (p *asapEdgeProcessor) stopControlPlane() {
	if !p.ctrlStarted {
		return
	}
	p.ctrlStarted = false
	close(p.ctrlStopCh)
	if c, ok := p.ctrlChan.(interface{ Close() error }); ok {
		_ = c.Close()
	}
	<-p.ctrlDoneCh
}
