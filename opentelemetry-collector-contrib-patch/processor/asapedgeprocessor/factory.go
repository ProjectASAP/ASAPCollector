// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

// NewFactory creates the asap_edge processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		Type,
		createDefaultConfig,
		processor.WithMetrics(createMetricsProcessor, MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ShardCount:     12,
		WindowDuration: 60 * time.Second,
		DropOriginal:   true,
		// Bound the per-shard sketch/sum series maps out of the box so a
		// cardinality explosion can't grow them without limit (0 => unlimited;
		// Validate applies this same default).
		MaxSeries: 100000,
		// DeltaTransmission defaults OFF (conservative: full state every window).
		DeltaTransmission: false,
		// ControlChannel is disabled by default (zero value); set poll_url to
		// enable the control-plane config-poll loop.
		Cold: ColdConfig{
			Enabled:       true,
			BlockDuration: 60 * time.Second,
			ReorderGrace:  2 * time.Second,
		},
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
		return nil, fmt.Errorf("configuration is not *asapedgeprocessor.Config")
	}
	if err := oCfg.Validate(); err != nil {
		return nil, err
	}
	_ = ctx // lifecycle handled via Start/Shutdown
	// The processor implements processor.Metrics directly (Capabilities /
	// Start / Shutdown / ConsumeMetrics), mirroring ddsketchprocessor — no
	// processorhelper wrapper.
	return newProcessor(oCfg, set, next)
}
