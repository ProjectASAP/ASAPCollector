package harness

import (
	"fmt"

	"github.com/influxdata/telegraf"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	tgpre "github.com/ProjectASAP/asap-precompute-go/telegraf"
)

// RunTelegraf drives all five Precompute instances against the
// []telegraf.Metric input. Returns the per-sketch envelopes the
// runtime emits at Drain time. Mirrors RunOTel's structure so the
// caller's per-sketch envelope comparison is structurally identical.
//
// Note: the Telegraf codec emits ONE Observation per metric (it has
// no "data points" sub-array), so the OTel-equivalent expansion is
// done at input-construction time in BuildTelegrafInput rather than
// here.
func RunTelegraf(input []telegraf.Metric, cfg RuntimeConfig) (map[string][]*precompute.SketchEnvelope, error) {
	out := make(map[string][]*precompute.SketchEnvelope, 5)
	// nil AdapterConfig — Decode then defaults to ValueField="value",
	// which matches BuildTelegrafInput's field name. Going through
	// NewAdapter here keeps the call shape parallel to RunOTel and
	// makes any future per-test config knob trivial to thread.
	adapter := tgpre.NewAdapter(nil)

	for _, sd := range allSketchDescriptors(cfg) {
		envs, err := runTelegrafOne(input, adapter, sd, cfg)
		if err != nil {
			return nil, fmt.Errorf("RunTelegraf %s: %w", sd.metric, err)
		}
		out[sd.metric] = envs
	}
	return out, nil
}

// runTelegrafOne configures one Precompute, drives the Telegraf input
// through the Telegraf adapter, and Drains. Returns the closed-window
// envelopes.
func runTelegrafOne(
	input []telegraf.Metric,
	adapter *tgpre.Adapter,
	sd sketchDescriptor,
	cfg RuntimeConfig,
) ([]*precompute.SketchEnvelope, error) {
	pp := precompute.New(sd.precomputeConfig(cfg), sd.factory(cfg), sd.observer)

	for i, m := range input {
		obs, err := adapter.Decode(m)
		if err != nil {
			return nil, fmt.Errorf("Decode[%d]: %w", i, err)
		}
		// CMS observation kind hand-off — same shape as RunOTel: the
		// codec emits KindFloat for scalar fields, but the harness's
		// cmsObserver hashes a flowKey built from the observation's
		// labels. Replace the value in-place; obs is owned by this
		// invocation only so the rewrite never bleeds across samples.
		if sd.sketchType == precompute.SketchTypeCountMinSketch &&
			obs.Metric == sd.metric {
			obs.Value = precompute.BytesValue(
				[]byte(precompute.AttributesKey(obs.Labels, nil)),
			)
		}
		if err := pp.Observe(obs); err != nil {
			return nil, fmt.Errorf("Observe[%d]: %w", i, err)
		}
	}
	return pp.Drain(), nil
}
