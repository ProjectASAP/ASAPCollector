// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package countminsketchprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
)

// NewFactory creates a new CountMin processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("countmin"),
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		MetricName:           "countmin_sketch",
		Rows:                 5,
		Columns:              1024, // Power of two required by new lib
		EnableSelfMonitoring: true,
		TransmitSketch:       true,
		// DropOriginal=true is the MVP-bandwidth default: the sketch
		// summary REPLACES the raw on the outbound pmetric stream. The
		// raw remains available via the gorillas3processor archive write
		// that runs UPSTREAM of this processor in the agent pipeline
		// (see ① bandwidth FAIL fix). Operators that want the legacy
		// "raw passthrough + sketch graft" shape must set
		// drop_original: false explicitly in YAML.
		DropOriginal:         true,
		GroupBy:              []string{},
		WindowDuration:       10 * time.Second,
		// Delta-encoded transmission is the operational default for the
		// MVP demo: Count-Min's per-window wire footprint is dominated
		// by the d×w cell matrix, so emitting only cells that changed
		// since the last flush (instead of the full matrix) keeps the
		// bandwidth verdict on the right side of break-even at the
		// 10 Hz × 60 s = 600 samples/window operating point.
		// DeltaThreshold defaults to 1.0 in Validate().
		DeltaTransmission: true,
	}
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	oCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("configuration parsed is not of type *countminsketchprocessor.Config")
	}

	if err := oCfg.Validate(); err != nil {
		return nil, err
	}

	// Pass 'next' to the constructor manually as requested
	proc := newProcessor(oCfg, next, set.Logger)
	if oCfg.EnableSelfMonitoring {
		proc.enableSelfMonitoring(set.TelemetrySettings, set.ID.String())
	}

	return processorhelper.NewMetrics(
		ctx,
		set,
		cfg,
		next,
		proc.ConsumeMetrics,
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(proc.Start),
		processorhelper.WithShutdown(proc.Shutdown),
	)
}
