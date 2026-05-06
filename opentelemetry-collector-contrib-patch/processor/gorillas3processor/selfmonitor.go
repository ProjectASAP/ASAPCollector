// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package gorillas3processor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/selfmonitor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// gorillaS3MeterName scopes the processor-private counters
// (chunks_written, s3_put_failures) — distinct from selfmonitor's
// generic processor meter.
const gorillaS3MeterName = "github.com/ProjectASAP/opentelemetry-collector-contrib/processor/gorillas3processor"

// monitor bundles the upstream selfmonitor.Monitor with a small set of
// gorillas3-specific counters.
type monitor struct {
	mon                *selfmonitor.Monitor
	chunksWritten      metric.Int64Counter
	s3PutFailures      metric.Int64Counter
	chunkBytesWritten  metric.Int64Counter
	chunkPointsWritten metric.Int64Counter
	addOpt             metric.AddOption
}

func newMonitor(settings component.TelemetrySettings, processorID string, activeSeriesFn selfmonitor.ActiveSeriesFunc, logger *zap.Logger) *monitor {
	m := &monitor{}
	upstream, err := selfmonitor.New(settings, processorID, Type.String(), activeSeriesFn)
	if err != nil {
		if logger != nil {
			logger.Warn("gorillas3processor: failed to initialize selfmonitor", zap.Error(err))
		}
	} else {
		m.mon = upstream
	}

	if settings.MeterProvider == nil {
		return m
	}
	meter := settings.MeterProvider.Meter(gorillaS3MeterName)

	if c, err := meter.Int64Counter(
		"gorillas3_chunks_written_total",
		metric.WithDescription("Number of GORILLA1 chunks successfully written to S3."),
		metric.WithUnit("{chunk}"),
	); err == nil {
		m.chunksWritten = c
	} else if logger != nil {
		logger.Warn("gorillas3processor: chunks_written counter init failed", zap.Error(err))
	}
	if c, err := meter.Int64Counter(
		"gorillas3_s3_put_failures_total",
		metric.WithDescription("Number of S3 PutObject failures (after all retries)."),
		metric.WithUnit("{failure}"),
	); err == nil {
		m.s3PutFailures = c
	} else if logger != nil {
		logger.Warn("gorillas3processor: put_failures counter init failed", zap.Error(err))
	}
	if c, err := meter.Int64Counter(
		"gorillas3_chunk_bytes_written_total",
		metric.WithDescription("Total bytes of GORILLA1 chunk payload written to S3."),
		metric.WithUnit("By"),
	); err == nil {
		m.chunkBytesWritten = c
	} else if logger != nil {
		logger.Warn("gorillas3processor: chunk_bytes counter init failed", zap.Error(err))
	}
	if c, err := meter.Int64Counter(
		"gorillas3_chunk_points_written_total",
		metric.WithDescription("Total number of data points encoded into successfully written GORILLA1 chunks."),
		metric.WithUnit("{datapoint}"),
	); err == nil {
		m.chunkPointsWritten = c
	} else if logger != nil {
		logger.Warn("gorillas3processor: chunk_points counter init failed", zap.Error(err))
	}

	m.addOpt = metric.WithAttributeSet(attribute.NewSet(
		attribute.String("processor.id", processorID),
		attribute.String("processor.type", Type.String()),
	))
	return m
}

func (m *monitor) recordInput(ctx context.Context, md pmetric.Metrics) {
	if m == nil || m.mon == nil {
		return
	}
	m.mon.RecordInput(ctx, md)
}

func (m *monitor) recordOutput(ctx context.Context, md pmetric.Metrics) {
	if m == nil || m.mon == nil {
		return
	}
	m.mon.RecordOutput(ctx, md)
}

func (m *monitor) chunkWritten(ctx context.Context, sizeBytes int, points int) {
	if m == nil {
		return
	}
	if m.chunksWritten != nil {
		m.chunksWritten.Add(ctx, 1, m.addOpt)
	}
	if m.chunkBytesWritten != nil {
		m.chunkBytesWritten.Add(ctx, int64(sizeBytes), m.addOpt)
	}
	if m.chunkPointsWritten != nil {
		m.chunkPointsWritten.Add(ctx, int64(points), m.addOpt)
	}
}

func (m *monitor) putFailure(ctx context.Context) {
	if m == nil || m.s3PutFailures == nil {
		return
	}
	m.s3PutFailures.Add(ctx, 1, m.addOpt)
}

func (m *monitor) shutdown() {
	if m == nil || m.mon == nil {
		return
	}
	m.mon.Shutdown()
}
