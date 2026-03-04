module go.opentelemetry.io/otel/exporters/stdout/stdoutlog

go 1.24.0

// Contains broken dependency on go.opentelemetry.io/otel/sdk/log/logtest.
retract v0.12.0

replace go.opentelemetry.io/otel/sdk/log => ../../../sdk/log

replace go.opentelemetry.io/otel/sdk/log/logtest => ../../../sdk/log/logtest

replace go.opentelemetry.io/otel/log => ../../../log

replace go.opentelemetry.io/otel => ../../..

replace go.opentelemetry.io/otel/trace => ../../../trace

replace go.opentelemetry.io/otel/sdk => ../../../sdk

replace go.opentelemetry.io/otel/metric => ../../../metric

replace go.opentelemetry.io/otel/sdk/metric => ../../../sdk/metric
