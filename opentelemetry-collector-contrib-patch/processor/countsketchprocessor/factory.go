package countsketchprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

var typeStr = component.MustNewType("countsketch")

func NewFactory() processor.Factory {
	return processor.NewFactory(
		typeStr,
		createDefaultConfig,

		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		AggregateBy:          []string{},
		Epsilon:              0.01,
		Delta:                0.99,
		WindowDuration:       5 * time.Second,
		TransmitSketch:       false,
		EnableSelfMonitoring: true,
		// DropOriginal=true is the MVP-bandwidth default: the sketch
		// summary REPLACES the raw on the outbound pmetric stream. The
		// raw remains available via the gorillas3processor archive write
		// that runs UPSTREAM of this processor in the agent pipeline
		// (see ① bandwidth FAIL fix). Operators that want the legacy
		// "raw passthrough + sketch graft" shape must set
		// drop_original: false explicitly in YAML.
		DropOriginal: true,
		// Delta-encoded transmission is the operational default for the
		// MVP demo. CountSketch's per-window wire footprint is dominated
		// by the d×w cell matrix; emitting only cells that changed since
		// the last flush (instead of the full matrix) shifts the
		// bandwidth verdict from a ~133× loss (full-state at 1 Hz × 60 s)
		// to ~13× (delta at 10 Hz × 60 s = 600 samples/window) — still
		// loses at this scale knee, but by an order of magnitude less.
		// DeltaThreshold defaults to 1.0 in Validate().
		// Note: DeltaTransmission requires TransmitSketch=true; this
		// default is harmless when the operator leaves TransmitSketch=false
		// (the encoder only consults DeltaTransmission on the sketch path).
		DeltaTransmission: true,
	}
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	proc := newProcessor(set.Logger, cfg.(*Config), next)
	if cfg.(*Config).EnableSelfMonitoring {
		proc.enableSelfMonitoring(set.TelemetrySettings, set.ID.String())
	}

	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		next,
		proc.processMetrics,
		processorhelper.WithStart(proc.Start),
		processorhelper.WithShutdown(proc.Shutdown),
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}
