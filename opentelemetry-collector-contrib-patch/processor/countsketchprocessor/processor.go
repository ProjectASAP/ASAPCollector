package countsketchprocessor

import (
	"context"
	"time"

	"github.com/froot-netsys/promsketch"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	// "go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

type countSketchProcessor struct {
	logger       *zap.Logger
	next         consumer.Metrics
	
	rowSketch    *promsketch.CountSketch // Tracks Metric Names (Rows)
	colSketch    *promsketch.CountSketch // Tracks Host Names (Columns)
	
	windowTicker *time.Ticker
	doneCh       chan struct{}
}

func newProcessor(logger *zap.Logger, cfg *Config, next consumer.Metrics) (*countSketchProcessor) {
	duration := cfg.WindowSize

	// Epsilon = 0.01 (1% error), Delta = 0.99 (99% confidence)
	rowS, _ := promsketch.NewCountSketchWithEstimates(cfg.Epsilon, cfg.Delta)
	colS, _ := promsketch.NewCountSketchWithEstimates(cfg.Epsilon, cfg.Delta)

	return &countSketchProcessor{
		logger:       logger,
		next:         next,
		rowSketch:    rowS,
		colSketch:    colS,
		windowTicker: time.NewTicker(duration),
		doneCh:       make(chan struct{}),
	}
}

func (p *countSketchProcessor) Start(ctx context.Context, host component.Host) error {
	p.logger.Info("Starting Count Sketch Window Loop...")
	go p.startWindowLoop()
	return nil
}

func (p *countSketchProcessor) Shutdown(ctx context.Context) error {
	close(p.doneCh)

	p.rowSketch.FreeCountSketch()
	p.colSketch.FreeCountSketch()
	return nil
}

func (p *countSketchProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	rm := md.ResourceMetrics()
	
	for i := 0; i < rm.Len(); i++ {
		resourceMetric := rm.At(i)
		
		// Extract Hostname
		hostKey := "unknown-host"
		if v, ok := resourceMetric.Resource().Attributes().Get("host.name"); ok {
			hostKey = v.Str()
		} 

		// p.logger.Info("Batch Size", zap.Int("Metrics Count", metrics.Len()))

		sms := resourceMetric.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				
				// ROW KEY: Metric Name
				rowKey := metric.Name()
				
				// Sum all data points
				var value float64
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dps := metric.Gauge().DataPoints()

					p.logger.Info("DataPoints Count", 
						zap.Int("DPs inside this metric", dps.Len()), // Will print 10
					)

					for l := 0; l < dps.Len(); l++ {
						value += getVal(dps.At(l))

						p.logger.Info("         >>> DataPoint Value <<<", 
                            zap.Any("value", getVal(dps.At(l))),
						)
					}

				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						value += getVal(dps.At(l))
					}

				case pmetric.MetricTypeHistogram:
					dps := metric.Histogram().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						value += float64(dps.At(l).Count())
					}
				}

				p.rowSketch.UpdateString(rowKey, value)
				p.colSketch.UpdateString(hostKey, value)
			}
		}
	}

	return md, p.next.ConsumeMetrics(ctx, md)
}

func getVal(dp pmetric.NumberDataPoint) float64 {
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		return float64(dp.IntValue())
	}
	
	return dp.DoubleValue()
}

func (p *countSketchProcessor) startWindowLoop() {
	for {
		select {
		case <-p.doneCh:
			return

		case <-p.windowTicker.C:
			p.flushSketches()
		}
	}
}

func (p *countSketchProcessor) flushSketches() {
	hostVal := p.colSketch.EstimateStringCount("host-A")

	p.logger.Info("Flushing Count Sketches...", 
		zap.Int64("Estimated_Host-A_Count", hostVal),
	)

	p.rowSketch.FreeCountSketch()
	p.colSketch.FreeCountSketch()

	p.rowSketch, _ = promsketch.NewCountSketchWithEstimates(0.01, 0.99)
	p.colSketch, _ = promsketch.NewCountSketchWithEstimates(0.01, 0.99)
}