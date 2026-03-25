// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfprocessor

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// seriesBuffer accumulates points for a single series within a window.
type seriesBuffer struct {
	attributes map[string]string
	points     []point
}

type serfProcessor struct {
	cfg    *Config
	logger *zap.Logger

	nextConsumer consumer.Metrics

	series     map[seriesKey]*seriesBuffer
	mu         sync.Mutex
	blockStart time.Time
	blockEnd   time.Time

	ticker   *time.Ticker
	done     chan struct{}
	s3Client *s3.S3
}

func newProcessor(cfg *Config, next consumer.Metrics, logger *zap.Logger) *serfProcessor {
	return &serfProcessor{
		cfg:          cfg,
		logger:       logger,
		nextConsumer: next,
		series:       make(map[seriesKey]*seriesBuffer),
		done:         make(chan struct{}),
	}
}

// Start initialises the S3 client and starts the background flush goroutine.
func (p *serfProcessor) Start(ctx context.Context, host component.Host) error {
	if p.cfg.S3.Bucket != "" {
		sess, err := session.NewSession(&aws.Config{
			Region: aws.String(p.cfg.S3.Region),
		})
		if err != nil {
			return fmt.Errorf("serf: create AWS session: %w", err)
		}
		p.s3Client = s3.New(sess)
	}

	if p.cfg.LocalDir != "" {
		if err := os.MkdirAll(p.cfg.LocalDir, 0o755); err != nil {
			return fmt.Errorf("serf: create local_dir: %w", err)
		}
	}

	p.logger.Info("Starting Serf processor",
		zap.String("compression", p.cfg.Compression),
		zap.Duration("window_interval", p.cfg.WindowInterval),
		zap.Float64("max_diff", p.cfg.MaxDiff),
		zap.Int64("adjust_digit", p.cfg.AdjustDigit),
		zap.String("s3_bucket", p.cfg.S3.Bucket),
		zap.String("local_dir", p.cfg.LocalDir),
	)

	p.ticker = time.NewTicker(p.cfg.WindowInterval)
	go func() {
		for {
			select {
			case <-p.ticker.C:
				p.flushWindow()
			case <-p.done:
				return
			}
		}
	}()

	return nil
}

// Shutdown stops the ticker, closes the done channel, and flushes remaining data.
func (p *serfProcessor) Shutdown(ctx context.Context) error {
	if p.ticker != nil {
		p.ticker.Stop()
	}
	close(p.done)
	p.flushWindow()
	return nil
}

func (p *serfProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// ConsumeMetrics ingests incoming metrics, buffering Gauge and Sum data points.
func (p *serfProcessor) ConsumeMetrics(
	ctx context.Context,
	md pmetric.Metrics,
) (pmetric.Metrics, error) {
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				p.ingestMetric(metrics.At(k))
			}
		}
	}

	if p.cfg.DropOriginal {
		return pmetric.NewMetrics(), nil
	}
	return md, nil
}

func (p *serfProcessor) ingestMetric(metric pmetric.Metric) {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dps := metric.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			p.addPoint(metric.Name(), dps.At(i))
		}
	case pmetric.MetricTypeSum:
		dps := metric.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			p.addPoint(metric.Name(), dps.At(i))
		}
	}
}

func (p *serfProcessor) addPoint(metricName string, dp pmetric.NumberDataPoint) {
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

	p.mu.Lock()
	defer p.mu.Unlock()

	buf, exists := p.series[sk]
	if !exists {
		buf = &seriesBuffer{
			attributes: attributesToMap(dp.Attributes()),
			points:     make([]point, 0, 128),
		}
		p.series[sk] = buf
	}

	buf.points = append(buf.points, point{ts: ts.UnixNano(), v: val})

	if p.blockStart.IsZero() || ts.Before(p.blockStart) {
		p.blockStart = ts
	}
	if ts.After(p.blockEnd) {
		p.blockEnd = ts
	}
}

// flushWindow snapshots the current series buffers, compresses them using
// Serf XOR encoding, and writes the resulting objects to S3 and/or local disk.
func (p *serfProcessor) flushWindow() {
	p.mu.Lock()
	if len(p.series) == 0 {
		p.mu.Unlock()
		return
	}
	snapshot := p.series
	p.series = make(map[seriesKey]*seriesBuffer)
	blockStart := p.blockStart
	blockEnd := p.blockEnd
	p.blockStart = time.Time{}
	p.blockEnd = time.Time{}
	p.mu.Unlock()

	objects, err := buildObjects(snapshot, p.cfg.MaxObjectBytes, p.cfg.Compression, p.cfg.MaxDiff, p.cfg.AdjustDigit)
	if err != nil {
		p.logger.Error("serf: build objects failed", zap.Error(err))
		return
	}

	for idx, obj := range objects {
		var ratio float64
		if obj.rawBytes > 0 {
			ratio = float64(len(obj.data)) / float64(obj.rawBytes)
		}

		key := buildObjectKey(p.cfg.S3.Prefix, p.cfg.S3.ObjectName, blockEnd, idx)

		if p.s3Client != nil {
			if err := p.uploadWithRetry(key, obj.data); err != nil {
				p.logger.Error("serf: S3 upload failed",
					zap.String("key", key),
					zap.Error(err),
				)
			} else {
				p.logger.Info("serf: S3 upload complete",
					zap.String("key", key),
					zap.Int("series", obj.seriesCount),
					zap.Int("points", obj.points),
					zap.Int("compressed_bytes", len(obj.data)),
					zap.Int64("raw_bytes", obj.rawBytes),
					zap.Float64("compression_ratio", ratio),
					zap.Time("block_start", blockStart),
					zap.Time("block_end", blockEnd),
				)
			}
		}

		if p.cfg.LocalDir != "" {
			localPath, err := writeLocalFile(p.cfg.LocalDir, key, obj.data)
			if err != nil {
				p.logger.Error("serf: local write failed", zap.Error(err))
			} else {
				p.logger.Info("serf: local write complete",
					zap.String("path", localPath),
					zap.Int("series", obj.seriesCount),
					zap.Int("points", obj.points),
					zap.Int("compressed_bytes", len(obj.data)),
					zap.Int64("raw_bytes", obj.rawBytes),
					zap.Float64("compression_ratio", ratio),
				)
			}
		}
	}
}

// encodeAttributesAsKey produces a deterministic string from OTel attributes.
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

// attributesToMap converts OTel attributes to a simple string map for JSON metadata.
func attributesToMap(attrs pcommon.Map) map[string]string {
	m := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsString()
		return true
	})
	return m
}
