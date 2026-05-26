// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"log"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/controlchannel"
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
	if !cfg.enabled() {
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
// is configured. The loop polls on ControlChannel's interval and, on a received
// PrecomputeConfigSet, applies it to every live sketch aggregator's Precompute
// via UpdateConfig — IN PLACE: no sketch/cold state is rebuilt (design §8/R5).
// A no-op when ctrlChan is nil (control plane disabled).
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
				p.applyConfigSet(set)
			}
		}
	}
}

// applyConfigSet swaps the new PrecomputeConfigSet into every live sketch
// aggregator across all shards via Precompute.UpdateConfig (in place — no state
// rebuild) and acks the plan version. UpdateConfig is concurrency-safe with the
// observe hot path (the runtime stores the config pointer atomically), so it
// does NOT take the shard lock; that keeps a slow control update from stalling
// ingestion.
func (p *asapEdgeProcessor) applyConfigSet(set *precompute.PrecomputeConfigSet) {
	if set == nil {
		return
	}
	for _, sh := range p.shards {
		for _, sa := range sh.sketchAggs {
			sa.pc.UpdateConfig(set)
		}
	}
	p.ctrlLastApply.Store(set.Version)
	p.ctrlChan.Ack(set.Version)
	p.logger.Info("asap_edge: applied control-plane config update",
		zap.Uint64("plan_version", set.Version), zap.Int("configs", len(set.Configs)))
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
	<-p.ctrlDoneCh
	if c, ok := p.ctrlChan.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}
