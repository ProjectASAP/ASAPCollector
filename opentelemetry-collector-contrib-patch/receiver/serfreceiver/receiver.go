// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package serfreceiver

import (
	"context"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

type serfReceiver struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	meter        metric.Meter

	server               *http.Server
	bytesRecvCounter     metric.Int64Counter
	pointsDecodedCounter metric.Int64Counter
}

func newReceiver(cfg *Config, set receiver.Settings, next consumer.Metrics) *serfReceiver {
	return &serfReceiver{
		cfg:          cfg,
		logger:       set.Logger,
		nextConsumer: next,
		meter:        set.TelemetrySettings.MeterProvider.Meter("serfhttp"),
	}
}

func (r *serfReceiver) Start(ctx context.Context, host component.Host) error {
	var err error
	r.bytesRecvCounter, err = r.meter.Int64Counter(
		"serf_receiver_bytes_received_total",
		metric.WithDescription("Total compressed bytes received by the Serf HTTP receiver"),
		metric.WithUnit("By"),
	)
	if err != nil {
		r.logger.Warn("serfhttp receiver: failed to create bytes counter", zap.Error(err))
		r.bytesRecvCounter, _ = noop.NewMeterProvider().Meter("serfhttp").
			Int64Counter("serf_receiver_bytes_received_total")
	}

	r.pointsDecodedCounter, err = r.meter.Int64Counter(
		"serf_receiver_decoded_points_total",
		metric.WithDescription("Total metric data points decoded by the Serf HTTP receiver"),
	)
	if err != nil {
		r.logger.Warn("serfhttp receiver: failed to create points counter", zap.Error(err))
		r.pointsDecodedCounter, _ = noop.NewMeterProvider().Meter("serfhttp").
			Int64Counter("serf_receiver_decoded_points_total")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/serf", r.handleSERF1)

	r.server = &http.Server{
		Addr:    r.cfg.Endpoint,
		Handler: mux,
	}

	r.logger.Info("Starting Serf HTTP receiver",
		zap.String("endpoint", r.cfg.Endpoint),
		zap.String("compression", r.cfg.Compression),
	)

	go func() {
		if err := r.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			r.logger.Error("serfhttp receiver: server error", zap.Error(err))
		}
	}()
	return nil
}

func (r *serfReceiver) Shutdown(ctx context.Context) error {
	if r.server != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return r.server.Shutdown(shutdownCtx)
	}
	return nil
}

func (r *serfReceiver) handleSERF1(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	data, err := io.ReadAll(req.Body)
	if err != nil {
		r.logger.Error("serfhttp receiver: read body failed", zap.Error(err))
		http.Error(w, "read error", http.StatusInternalServerError)
		return
	}
	defer req.Body.Close()

	r.bytesRecvCounter.Add(req.Context(), int64(len(data)))

	series, err := decodeSERF1(data, r.cfg.Compression, r.cfg.MaxDiff)
	if err != nil {
		r.logger.Error("serfhttp receiver: decode failed", zap.Error(err))
		http.Error(w, "decode error", http.StatusBadRequest)
		return
	}

	md := buildMetrics(series)
	totalPoints := md.DataPointCount()
	r.pointsDecodedCounter.Add(req.Context(), int64(totalPoints))

	if err := r.nextConsumer.ConsumeMetrics(req.Context(), md); err != nil {
		r.logger.Error("serfhttp receiver: downstream consume failed", zap.Error(err))
		http.Error(w, "consume error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// buildMetrics converts decoded series into a pmetric.Metrics object.
// Each series becomes a Gauge metric with its original name and attributes.
func buildMetrics(series []decodedSeries) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("serfhttp")

	for _, s := range series {
		if len(s.points) == 0 {
			continue
		}
		m := sm.Metrics().AppendEmpty()
		m.SetName(s.metricName)
		gauge := m.SetEmptyGauge()

		for _, pt := range s.points {
			dp := gauge.DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.Timestamp(pt.ts))
			dp.SetDoubleValue(pt.v)
			for k, v := range s.attributes {
				dp.Attributes().PutStr(k, v)
			}
		}
	}
	return md
}
