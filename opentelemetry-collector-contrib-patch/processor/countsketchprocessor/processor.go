package countsketchprocessor

import (
	"context"
	"sync"
	"time"

	"github.com/froot-netsys/promsketch"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type countSketchProcessor struct {
	logger *zap.Logger
	next   consumer.Metrics

	config *Config

	// To prevent race conditions between 
	// processMetrics (Write) and flushSketches (Reset)
	mutex sync.Mutex

	rowSketch *promsketch.CountSketch // Tracks Metric Names
	colSketch *promsketch.CountSketch // Tracks Host Names

	windowTicker *time.Ticker
	doneCh       chan struct{}
}

func newProcessor(logger *zap.Logger, cfg *Config, next consumer.Metrics) *countSketchProcessor {
	rowS, _ := promsketch.NewCountSketchWithEstimates(cfg.Epsilon, cfg.Delta)
	colS, _ := promsketch.NewCountSketchWithEstimates(cfg.Epsilon, cfg.Delta)

	return &countSketchProcessor{
		logger:       logger,
		next:         next,
		config:       cfg,
		rowSketch:    rowS,
		colSketch:    colS,
		windowTicker: time.NewTicker(cfg.WindowSize),
		doneCh:       make(chan struct{}),
	}
}

func (p *countSketchProcessor) Start(ctx context.Context, host component.Host) error {
	p.logger.Info("Starting Count Sketch Processor",
		zap.Float64("epsilon", p.config.Epsilon),
		zap.Float64("delta", p.config.Delta),
		zap.Duration("window", p.config.WindowSize),
	)
	
	go p.startWindowLoop()
	return nil
}

func (p *countSketchProcessor) Shutdown(ctx context.Context) error {
	close(p.doneCh)
	return nil
}

func (p *countSketchProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	rm := md.ResourceMetrics()
	for i := 0; i < rm.Len(); i++ {
		resourceMetric := rm.At(i)

		hostKey := "unknown-host"
		// TODO: Might need to change the key based on the input
		if v, ok := resourceMetric.Resource().Attributes().Get("host.name"); ok {
			hostKey = v.Str()
		}

		sms := resourceMetric.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()

			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				rowKey := metric.Name()

				var value float64

				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					dps := metric.Gauge().DataPoints()
					value = sumPoints(dps)
				
				case pmetric.MetricTypeSum:
					dps := metric.Sum().DataPoints()
					value = sumPoints(dps)
					
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

func sumPoints(dps pmetric.NumberDataPointSlice) float64 {
	var total float64
	for i := 0; i < dps.Len(); i++ {
		dp := dps.At(i)
		if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
			total += float64(dp.IntValue())
		} else {
			total += dp.DoubleValue()
		}
	}
	return total
}

func (p *countSketchProcessor) startWindowLoop() {
	defer func() {
		p.mutex.Lock()
		p.rowSketch.FreeCountSketch()
		p.colSketch.FreeCountSketch()
		p.mutex.Unlock()
	}()

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
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// hostVal := p.colSketch.EstimateStringCount("host-A")

	// p.logger.Info("WINDOW FLUSH",
	// 	zap.Int64("Estimated_Host-A_Sum", hostVal),
	// 	zap.Float64("Using_Config_Epsilon", p.config.Epsilon),
	// )

	p.rowSketch.FreeCountSketch()
	p.colSketch.FreeCountSketch()

	var err error
	p.rowSketch, err = promsketch.NewCountSketchWithEstimates(p.config.Epsilon, p.config.Delta)
	if err != nil {
		p.logger.Error("Failed to reset row sketch", zap.Error(err))
	}
	
	p.colSketch, err = promsketch.NewCountSketchWithEstimates(p.config.Epsilon, p.config.Delta)
	if err != nil {
		p.logger.Error("Failed to reset col sketch", zap.Error(err))
	}
}