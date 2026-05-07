package hllprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

func createDefaultConfig() component.Config {
	return &Config{
		Mode:                 ModeBatch,
		WindowDuration:       60 * time.Second,
		TransmitSketch:       false,
		DropOriginal:         true,
		MetricSuffix:         "",
		EnableSelfMonitoring: true,
		// Delta-encoded transmission is the operational default for the
		// MVP demo: HLL's per-window wire footprint is dominated by the
		// fixed-size register array, so emitting only registers that
		// increased since the last flush (instead of the full state) is
		// the difference between losing and winning bandwidth versus raw
		// at 10 Hz × 60 s = 600 samples/window.
		// Note: DeltaTransmission requires TransmitSketch=true; this
		// default is harmless when the operator leaves TransmitSketch=false
		// (the encoder only consults DeltaTransmission on the sketch path).
		DeltaTransmission: true,
	}
}

func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("HLL"), createDefaultConfig,
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
		return nil, fmt.Errorf("configuration parsed is not of type *hllprocessor.Config")
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
