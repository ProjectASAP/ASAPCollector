package kllprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

// createDefaultConfig builds the KLL processor's default Config.
//
// NOTE on delta-encoded transmission: KLL has no delta variant. KLL
// uses a randomised compaction step to keep its sample buffer
// bounded, which means two KLL sketches with the same input history
// are not bit-identical and the sketches are not additively
// mergeable in the linear sense the other four families
// (DDSketch / HLL / CountSketch / Count-Min) are. Delta transmission
// (sparse "what changed since the last flush" diff) is therefore not
// defined for KLL — `Config.Validate` rejects `delta_transmission:
// true` with an explicit error rather than silently falling back.
//
// The MVP demo's cross-family delta-by-default policy
// (DDSketch / HLL / CountSketch / Count-Min default to
// `DeltaTransmission: true`) intentionally omits KLL: KLL's wire
// payload is always the full sketch state, and downstream tooling
// budgets bandwidth for KLL accordingly.
//
// See `Implementation.tex` ("KLL has no delta variant and matches
// its full cost") and the KLL bandwidth discussion in
// `docs/mvp-demo-runbook.md` §"Verifying the verdict".
func createDefaultConfig() component.Config {
	return &Config{
		Mode:                 ModeBatch,
		WindowDuration:       60 * time.Second,
		K:                    256,
		Quantiles:            []float64{0.5, 0.99},
		TransmitSketch:       false,
		WriteSeen:            false,
		DropOriginal:         true,
		ReadAsInt:            false,
		MetricSuffix:         "",
		EnableSelfMonitoring: true,
		// DeltaTransmission deliberately left at the zero value (false):
		// KLL has no delta variant (see the doc comment above).
	}
}

func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("KLL"), createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelAlpha),
	)
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	oCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsed is not of type *kllprocessor.Config")
	}
	if err := oCfg.Validate(); err != nil {
		return nil, err
	}
	_ = ctx
	proc := newProcessor(oCfg, set.Logger, next)
	if oCfg.EnableSelfMonitoring {
		proc.enableSelfMonitoring(set.TelemetrySettings, set.ID.String())
	}
	return proc, nil
}
