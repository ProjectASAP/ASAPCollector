// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfexporter

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

type serfExporter struct {
	cfg    *Config
	logger *zap.Logger
	meter  metric.Meter

	series     map[seriesKey]*seriesBuffer
	mu         sync.Mutex
	blockStart time.Time
	blockEnd   time.Time

	ticker     *time.Ticker
	done       chan struct{}
	httpClient *http.Client

	bytesSentCounter metric.Int64Counter
}

type seriesBuffer struct {
	attributes map[string]string
	points     []point
}

func newExporter(cfg *Config, set exporter.Settings) *serfExporter {
	return &serfExporter{
		cfg:        cfg,
		logger:     set.Logger,
		meter:      set.TelemetrySettings.MeterProvider.Meter("serfhttp"),
		series:     make(map[seriesKey]*seriesBuffer),
		done:       make(chan struct{}),
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (e *serfExporter) Start(ctx context.Context, host component.Host) error {
	var err error
	e.bytesSentCounter, err = e.meter.Int64Counter(
		"serf_exporter_bytes_sent_total",
		metric.WithDescription("Total bytes sent via compressed SERF1 HTTP POST"),
		metric.WithUnit("By"),
	)
	if err != nil {
		e.logger.Warn("serfhttp exporter: failed to create bytes counter", zap.Error(err))
		// fall back to noop counter
		e.bytesSentCounter, _ = noop.NewMeterProvider().Meter("serfhttp").
			Int64Counter("serf_exporter_bytes_sent_total")
	}

	e.logger.Info("Starting Serf HTTP exporter",
		zap.String("endpoint", e.cfg.Endpoint),
		zap.String("compression", e.cfg.Compression),
		zap.Duration("window_interval", e.cfg.WindowInterval),
		zap.Float64("max_diff", e.cfg.MaxDiff),
	)

	e.ticker = time.NewTicker(e.cfg.WindowInterval)
	go func() {
		for {
			select {
			case <-e.ticker.C:
				e.flushWindow()
			case <-e.done:
				return
			}
		}
	}()
	return nil
}

func (e *serfExporter) Shutdown(ctx context.Context) error {
	if e.ticker != nil {
		e.ticker.Stop()
	}
	close(e.done)
	e.flushWindow()
	return nil
}

func (e *serfExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *serfExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				e.ingestMetric(metrics.At(k))
			}
		}
	}
	return nil
}

func (e *serfExporter) ingestMetric(m pmetric.Metric) {
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			e.addPoint(m.Name(), dps.At(i))
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			e.addPoint(m.Name(), dps.At(i))
		}
	}
}

func (e *serfExporter) addPoint(metricName string, dp pmetric.NumberDataPoint) {
	var val float64
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		val = dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		val = float64(dp.IntValue())
	default:
		return
	}

	attrsKey := encodeAttributesAsKey(dp.Attributes())
	sk := seriesKey{metricName: metricName, attributesKey: attrsKey}
	ts := dp.Timestamp().AsTime()

	e.mu.Lock()
	defer e.mu.Unlock()

	buf, exists := e.series[sk]
	if !exists {
		buf = &seriesBuffer{
			attributes: attributesToMap(dp.Attributes()),
			points:     make([]point, 0, 128),
		}
		e.series[sk] = buf
	}

	buf.points = append(buf.points, point{ts: ts.UnixNano(), v: val})

	if e.blockStart.IsZero() || ts.Before(e.blockStart) {
		e.blockStart = ts
	}
	if ts.After(e.blockEnd) {
		e.blockEnd = ts
	}
}

func (e *serfExporter) flushWindow() {
	e.mu.Lock()
	if len(e.series) == 0 {
		e.mu.Unlock()
		return
	}
	snapshot := e.series
	e.series = make(map[seriesKey]*seriesBuffer)
	e.blockStart = time.Time{}
	e.blockEnd = time.Time{}
	e.mu.Unlock()

	objects, err := buildObjects(snapshot, e.cfg.MaxObjectBytes, e.cfg.Compression, e.cfg.MaxDiff, e.cfg.AdjustDigit)
	if err != nil {
		e.logger.Error("serfhttp exporter: build objects failed", zap.Error(err))
		return
	}

	for _, obj := range objects {
		if err := e.postObject(obj.data); err != nil {
			e.logger.Error("serfhttp exporter: POST failed",
				zap.String("endpoint", e.cfg.Endpoint),
				zap.Error(err),
			)
		} else {
			var ratio float64
			if obj.rawBytes > 0 {
				ratio = float64(len(obj.data)) / float64(obj.rawBytes)
			}
			e.logger.Info("serfhttp exporter: POST complete",
				zap.Int("series", obj.seriesCount),
				zap.Int("points", obj.points),
				zap.Int("compressed_bytes", len(obj.data)),
				zap.Int64("raw_bytes", obj.rawBytes),
				zap.Float64("compression_ratio", ratio),
			)
			e.bytesSentCounter.Add(context.Background(), int64(len(obj.data)))
		}
	}
}

func (e *serfExporter) postObject(data []byte) error {
	req, err := http.NewRequest(http.MethodPost, e.cfg.Endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

func encodeAttributesAsKey(attrs pcommon.Map) string {
	keys := make([]string, 0, attrs.Len())
	attrs.Range(func(k string, _ pcommon.Value) bool {
		keys = append(keys, k)
		return true
	})
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		v, _ := attrs.Get(k)
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(v.AsString())
		sb.WriteString(";")
	}
	return sb.String()
}

func attributesToMap(attrs pcommon.Map) map[string]string {
	m := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}
