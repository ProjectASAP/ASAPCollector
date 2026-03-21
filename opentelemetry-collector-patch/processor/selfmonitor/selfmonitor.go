// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package selfmonitor // import "go.opentelemetry.io/collector/processor/selfmonitor"

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

const meterName = "go.opentelemetry.io/collector/processor/selfmonitor"

type ActiveSeriesFunc func() int64

type Monitor struct {
	attrs     attribute.Set
	addOpt    metric.AddOption
	obsOpt    metric.ObserveOption
	marshaler pmetric.ProtoMarshaler

	inputBytes   metric.Int64Counter
	outputBytes  metric.Int64Counter
	inputPoints  metric.Int64Counter
	outputPoints metric.Int64Counter

	inputBandwidth   metric.Float64ObservableGauge
	outputBandwidth  metric.Float64ObservableGauge
	inputThroughput  metric.Float64ObservableGauge
	outputThroughput metric.Float64ObservableGauge
	cpuUserTime      metric.Float64ObservableCounter
	cpuSysTime       metric.Float64ObservableCounter
	rssMemory        metric.Int64ObservableGauge
	heapAlloc        metric.Int64ObservableGauge
	heapSys          metric.Int64ObservableGauge
	goroutines       metric.Int64ObservableGauge
	activeSeries     metric.Int64ObservableGauge

	reg metric.Registration

	totalInputBytes   atomic.Int64
	totalOutputBytes  atomic.Int64
	totalInputPoints  atomic.Int64
	totalOutputPoints atomic.Int64

	rateMu           sync.Mutex
	lastSample       time.Time
	lastInputBytes   int64
	lastOutputBytes  int64
	lastInputPoints  int64
	lastOutputPoints int64

	activeSeriesFn ActiveSeriesFunc
}

func New(settings component.TelemetrySettings, processorID, processorType string, activeSeriesFn ActiveSeriesFunc) (*Monitor, error) {
	meter := settings.MeterProvider.Meter(meterName)
	attrs := attribute.NewSet(
		attribute.String("processor.id", processorID),
		attribute.String("processor.type", processorType),
	)

	m := &Monitor{
		attrs:          attrs,
		addOpt:         metric.WithAttributeSet(attrs),
		obsOpt:         metric.WithAttributeSet(attrs),
		activeSeriesFn: activeSeriesFn,
	}

	var err error
	if m.inputBytes, err = meter.Int64Counter(
		"otelcol_datacollector_processor_input_bytes",
		metric.WithDescription("Estimated protobuf bytes received by the processor."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if m.outputBytes, err = meter.Int64Counter(
		"otelcol_datacollector_processor_output_bytes",
		metric.WithDescription("Estimated protobuf bytes emitted by the processor."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if m.inputPoints, err = meter.Int64Counter(
		"otelcol_datacollector_processor_input_metric_points",
		metric.WithDescription("Metric data points received by the processor."),
		metric.WithUnit("{datapoint}"),
	); err != nil {
		return nil, err
	}
	if m.outputPoints, err = meter.Int64Counter(
		"otelcol_datacollector_processor_output_metric_points",
		metric.WithDescription("Metric data points emitted by the processor."),
		metric.WithUnit("{datapoint}"),
	); err != nil {
		return nil, err
	}
	if m.inputBandwidth, err = meter.Float64ObservableGauge(
		"otelcol_datacollector_processor_input_bandwidth",
		metric.WithDescription("Estimated processor input bandwidth."),
		metric.WithUnit("By/s"),
	); err != nil {
		return nil, err
	}
	if m.outputBandwidth, err = meter.Float64ObservableGauge(
		"otelcol_datacollector_processor_output_bandwidth",
		metric.WithDescription("Estimated processor output bandwidth."),
		metric.WithUnit("By/s"),
	); err != nil {
		return nil, err
	}
	if m.inputThroughput, err = meter.Float64ObservableGauge(
		"otelcol_datacollector_processor_input_throughput",
		metric.WithDescription("Estimated processor input throughput."),
		metric.WithUnit("{datapoint}/s"),
	); err != nil {
		return nil, err
	}
	if m.outputThroughput, err = meter.Float64ObservableGauge(
		"otelcol_datacollector_processor_output_throughput",
		metric.WithDescription("Estimated processor output throughput."),
		metric.WithUnit("{datapoint}/s"),
	); err != nil {
		return nil, err
	}
	if m.cpuUserTime, err = meter.Float64ObservableCounter(
		"otelcol_datacollector_processor_process_cpu_user_time",
		metric.WithDescription("Process user CPU time observed by the processor."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.cpuSysTime, err = meter.Float64ObservableCounter(
		"otelcol_datacollector_processor_process_cpu_system_time",
		metric.WithDescription("Process system CPU time observed by the processor."),
		metric.WithUnit("s"),
	); err != nil {
		return nil, err
	}
	if m.rssMemory, err = meter.Int64ObservableGauge(
		"otelcol_datacollector_processor_process_resident_memory",
		metric.WithDescription("Process resident memory observed by the processor."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if m.heapAlloc, err = meter.Int64ObservableGauge(
		"otelcol_datacollector_processor_heap_alloc",
		metric.WithDescription("Current heap allocation observed by the processor."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if m.heapSys, err = meter.Int64ObservableGauge(
		"otelcol_datacollector_processor_heap_sys",
		metric.WithDescription("Heap memory obtained from the system as observed by the processor."),
		metric.WithUnit("By"),
	); err != nil {
		return nil, err
	}
	if m.goroutines, err = meter.Int64ObservableGauge(
		"otelcol_datacollector_processor_goroutines",
		metric.WithDescription("Current goroutine count observed by the processor."),
		metric.WithUnit("{goroutine}"),
	); err != nil {
		return nil, err
	}
	if m.activeSeries, err = meter.Int64ObservableGauge(
		"otelcol_datacollector_processor_active_series",
		metric.WithDescription("Active processor series or partitions currently retained in memory."),
		metric.WithUnit("{series}"),
	); err != nil {
		return nil, err
	}

	m.reg, err = meter.RegisterCallback(m.observe,
		m.inputBandwidth,
		m.outputBandwidth,
		m.inputThroughput,
		m.outputThroughput,
		m.cpuUserTime,
		m.cpuSysTime,
		m.rssMemory,
		m.heapAlloc,
		m.heapSys,
		m.goroutines,
		m.activeSeries,
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Monitor) Shutdown() {
	if m == nil || m.reg == nil {
		return
	}
	m.reg.Unregister()
}

func (m *Monitor) RecordInput(ctx context.Context, md pmetric.Metrics) {
	m.record(ctx, md, true)
}

func (m *Monitor) RecordOutput(ctx context.Context, md pmetric.Metrics) {
	m.record(ctx, md, false)
}

func (m *Monitor) record(ctx context.Context, md pmetric.Metrics, input bool) {
	if m == nil {
		return
	}
	size := int64(m.marshaler.MetricsSize(md))
	points := countMetricPoints(md)
	if ctx == nil {
		ctx = context.Background()
	}

	if input {
		m.inputBytes.Add(ctx, size, m.addOpt)
		m.inputPoints.Add(ctx, points, m.addOpt)
		m.totalInputBytes.Add(size)
		m.totalInputPoints.Add(points)
		return
	}

	m.outputBytes.Add(ctx, size, m.addOpt)
	m.outputPoints.Add(ctx, points, m.addOpt)
	m.totalOutputBytes.Add(size)
	m.totalOutputPoints.Add(points)
}

func (m *Monitor) observe(_ context.Context, obs metric.Observer) error {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	inputBytes := m.totalInputBytes.Load()
	outputBytes := m.totalOutputBytes.Load()
	inputPoints := m.totalInputPoints.Load()
	outputPoints := m.totalOutputPoints.Load()
	inputBps, outputBps, inputPps, outputPps := m.sampleRates(inputBytes, outputBytes, inputPoints, outputPoints)
	userCPU, sysCPU := processCPUTime()
	activeSeries := int64(0)
	if m.activeSeriesFn != nil {
		activeSeries = m.activeSeriesFn()
	}

	obs.ObserveFloat64(m.inputBandwidth, inputBps, m.obsOpt)
	obs.ObserveFloat64(m.outputBandwidth, outputBps, m.obsOpt)
	obs.ObserveFloat64(m.inputThroughput, inputPps, m.obsOpt)
	obs.ObserveFloat64(m.outputThroughput, outputPps, m.obsOpt)
	obs.ObserveFloat64(m.cpuUserTime, userCPU, m.obsOpt)
	obs.ObserveFloat64(m.cpuSysTime, sysCPU, m.obsOpt)
	obs.ObserveInt64(m.rssMemory, processRSSBytes(), m.obsOpt)
	obs.ObserveInt64(m.heapAlloc, int64(ms.HeapAlloc), m.obsOpt)
	obs.ObserveInt64(m.heapSys, int64(ms.HeapSys), m.obsOpt)
	obs.ObserveInt64(m.goroutines, int64(runtime.NumGoroutine()), m.obsOpt)
	obs.ObserveInt64(m.activeSeries, activeSeries, m.obsOpt)
	return nil
}

func (m *Monitor) sampleRates(inputBytes, outputBytes, inputPoints, outputPoints int64) (float64, float64, float64, float64) {
	now := time.Now()

	m.rateMu.Lock()
	defer m.rateMu.Unlock()

	if m.lastSample.IsZero() {
		m.lastSample = now
		m.lastInputBytes = inputBytes
		m.lastOutputBytes = outputBytes
		m.lastInputPoints = inputPoints
		m.lastOutputPoints = outputPoints
		return 0, 0, 0, 0
	}

	elapsed := now.Sub(m.lastSample).Seconds()
	if elapsed <= 0 {
		return 0, 0, 0, 0
	}

	inputBps := float64(inputBytes-m.lastInputBytes) / elapsed
	outputBps := float64(outputBytes-m.lastOutputBytes) / elapsed
	inputPps := float64(inputPoints-m.lastInputPoints) / elapsed
	outputPps := float64(outputPoints-m.lastOutputPoints) / elapsed
	m.lastSample = now
	m.lastInputBytes = inputBytes
	m.lastOutputBytes = outputBytes
	m.lastInputPoints = inputPoints
	m.lastOutputPoints = outputPoints
	return inputBps, outputBps, inputPps, outputPps
}

func processCPUTime() (float64, float64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	user := float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	sys := float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	return user, sys
}

func processRSSBytes() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return value * 1024
	}
	return 0
}

func countMetricPoints(md pmetric.Metrics) int64 {
	var total int64
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			metrics := sms.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					total += int64(metric.Gauge().DataPoints().Len())
				case pmetric.MetricTypeSum:
					total += int64(metric.Sum().DataPoints().Len())
				case pmetric.MetricTypeHistogram:
					total += int64(metric.Histogram().DataPoints().Len())
				case pmetric.MetricTypeExponentialHistogram:
					total += int64(metric.ExponentialHistogram().DataPoints().Len())
				case pmetric.MetricTypeSummary:
					total += int64(metric.Summary().DataPoints().Len())
				case pmetric.MetricTypeDDSketch:
					total += int64(metric.DDSketch().DataPoints().Len())
				case pmetric.MetricTypeKLLSketch:
					total += int64(metric.KLLSketch().DataPoints().Len())
				case pmetric.MetricTypeCountSketch:
					total += int64(metric.CountSketch().DataPoints().Len())
				case pmetric.MetricTypeCountMinSketch:
					total += int64(metric.CountMinSketch().DataPoints().Len())
				case pmetric.MetricTypeHLLSketch:
					total += int64(metric.HLLSketch().DataPoints().Len())
				}
			}
		}
	}
	return total
}
