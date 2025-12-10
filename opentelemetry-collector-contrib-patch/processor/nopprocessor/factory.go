package nopprocessor

import (
    "context"
    "go.opentelemetry.io/collector/component"
    "go.opentelemetry.io/collector/consumer"
    "go.opentelemetry.io/collector/processor"
)

var typeStr = component.MustNewType("nop")

func NewFactory() processor.Factory {
    return processor.NewFactory(
        typeStr,
        func() component.Config { return &Config{} },
        processor.WithTraces(createTracesProcessor, component.StabilityLevelDevelopment),
    )
}

func createTracesProcessor(ctx context.Context, set processor.Settings, cfg component.Config, next consumer.Traces) (processor.Traces, error) {
    return &nopProcessor{next: next}, nil
}