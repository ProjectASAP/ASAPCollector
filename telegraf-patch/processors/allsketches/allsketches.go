// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package allsketches

import (
	_ "embed"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/telegraf"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	telegrafcodec "github.com/ProjectASAP/asap-precompute-go/telegraf"
)

//go:embed sample.conf
var sampleConfig string

// AllSketches is the unified Telegraf StreamingProcessor plugin per
// docs/design-asap-telegraf-integration.md §3. One plugin instance
// owns one Precompute (one sketch type) and one window-flush ticker;
// multi-sketch deployments declare multiple [[processors.allsketches]]
// blocks with different sketch_type values.
//
// Lifecycle: Start spawns the flush ticker goroutine; Add observes via
// the Phase-B codec; Stop drains pending windows and exits the
// goroutine cleanly. Output metrics are emitted via acc.AddMetric on
// each tick — input metrics are consumed (this is a "consume" plugin
// per the design doc, not a "transform" plugin).
type AllSketches struct {
	// Sketch type and window cadence — see sample.conf for spellings.
	SketchType string `toml:"sketch_type"`
	WindowSize string `toml:"window_size"`

	// Field names on input/output metrics.
	ValueField       string `toml:"value_field"`
	EnvelopeField    string `toml:"envelope_field"`
	OutputMetricName string `toml:"output_metric_name"`

	// Sketch-specific tuning knobs. Only the keys for the configured
	// sketch_type are read; others are ignored.
	Alpha     float64 `toml:"alpha"`     // ddsketch
	K         int     `toml:"k"`         // kll heap size
	Seed      int64   `toml:"seed"`      // kll determinism (0 = time-based)
	Precision int     `toml:"precision"` // hll
	Width     int     `toml:"width"`     // countsketch / cms
	Depth     int     `toml:"depth"`     // countsketch / cms

	// Delta transmission knobs.
	DeltaTransmission bool   `toml:"delta_transmission"`
	DeltaThreshold    uint64 `toml:"delta_threshold"`

	// Series-key shape.
	OmitResourceAttrs bool `toml:"omit_resource_attrs"`
	GlobalAggregation bool `toml:"global_aggregation"`
	EmitWindowStats   bool `toml:"emit_window_stats"`

	Log telegraf.Logger `toml:"-"`

	// Internal state — not exposed via TOML.
	pc      precompute.Precompute
	adapter *telegrafcodec.Adapter
	acc     telegraf.Accumulator
	window  time.Duration

	ticker  *time.Ticker
	done    chan struct{}
	wg      sync.WaitGroup
	started atomic.Bool
}

// SampleConfig returns the embedded canonical sample [[processors.allsketches]]
// block. Telegraf's PluginDescriber surface uses this for `--list-processors`.
func (*AllSketches) SampleConfig() string { return sampleConfig }

// Start validates config, builds the Precompute + codec adapter, and
// spawns the window-flush ticker goroutine. The accumulator passed in
// is stashed on the receiver so the ticker goroutine can call
// acc.AddMetric on each flush.
func (a *AllSketches) Start(acc telegraf.Accumulator) error {
	st, err := resolveSketchType(a.SketchType)
	if err != nil {
		return err
	}
	if a.WindowSize == "" {
		return fmt.Errorf("allsketches: window_size is required")
	}
	window, err := time.ParseDuration(a.WindowSize)
	if err != nil {
		return fmt.Errorf("allsketches: parse window_size %q: %w", a.WindowSize, err)
	}
	if window <= 0 {
		return fmt.Errorf("allsketches: window_size must be > 0 (got %s)", window)
	}

	factory, observer, err := sketchFactory(a.SketchType, a)
	if err != nil {
		return err
	}

	pcfg := a.toPrecomputeConfig(window, st)
	a.pc = precompute.New(pcfg, factory, observer)
	a.adapter = telegrafcodec.NewAdapter(a.toAdapterConfig())
	a.acc = acc
	a.window = window

	a.done = make(chan struct{})
	a.ticker = time.NewTicker(window)
	a.wg.Add(1)
	go a.tickLoop()
	a.started.Store(true)
	return nil
}

// Add is called for every inbound metric. Decode → observe; the input
// is consumed (no acc.AddMetric here) because the plugin emits sketch
// envelopes on the flush ticker, not in-line per metric.
func (a *AllSketches) Add(m telegraf.Metric, _ telegraf.Accumulator) error {
	if a.adapter == nil || a.pc == nil {
		// Plugin hasn't started; drop quietly. This shouldn't happen
		// in normal Telegraf flow but keeps Add() panic-free if a
		// reload races with a stale Add call.
		return nil
	}
	obs, err := a.adapter.Decode(m)
	if err != nil {
		if a.Log != nil {
			a.Log.Debugf("allsketches: decode dropped metric %q: %v", m.Name(), err)
		}
		return nil
	}
	if err := a.pc.Observe(obs); err != nil {
		if a.Log != nil {
			a.Log.Debugf("allsketches: observe dropped metric %q: %v", m.Name(), err)
		}
	}
	return nil
}

// Stop signals the ticker goroutine to exit, performs a final drain,
// and waits for the goroutine to return. Telegraf calls Stop exactly
// once per plugin instance and serializes Stop after all Add calls,
// so no extra synchronization is needed.
func (a *AllSketches) Stop() {
	if !a.started.CompareAndSwap(true, false) {
		// Stop without a successful Start — no goroutine to wind down.
		return
	}
	if a.ticker != nil {
		a.ticker.Stop()
	}
	close(a.done)
	a.wg.Wait()
	// Final drain after the ticker goroutine has exited. Pass a wall
	// clock that is unambiguously past the active window's exclusive
	// upper bound (now + 2*window) so the runtime's tumbling rotate
	// fires regardless of how long the plugin actually ran — without
	// this, Stop'ing before the window naturally closes would silently
	// drop in-flight observations.
	a.flush(time.Now().Add(2 * a.window))
}

// tickLoop owns the flush cadence. On each tick it calls flush(now);
// on done it returns and the deferred wg.Done lets Stop unblock.
func (a *AllSketches) tickLoop() {
	defer a.wg.Done()
	for {
		select {
		case <-a.done:
			return
		case t := <-a.ticker.C:
			a.flush(t)
		}
	}
}

// flush rotates the active window via Precompute.Tick, encodes the
// resulting envelopes through the codec, and hands the resulting
// telegraf.Metrics to the stashed accumulator. Errors during encode
// are logged but never bubble up — the ticker goroutine must keep
// running so subsequent windows still emit.
func (a *AllSketches) flush(now time.Time) {
	if a.pc == nil || a.adapter == nil || a.acc == nil {
		return
	}
	envs := a.pc.Tick(uint64(now.UnixMilli()))
	if len(envs) == 0 {
		return
	}
	out, err := a.adapter.Encode(envs)
	if err != nil {
		if a.Log != nil {
			a.Log.Errorf("allsketches: encode failed: %v", err)
		}
		return
	}
	for _, m := range out {
		a.acc.AddMetric(m)
	}
}
