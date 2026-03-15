// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

// MetricType specifies the type of data in a Metric.
type MetricType int32

const (
	// MetricTypeEmpty means that metric type is unset.
	MetricTypeEmpty MetricType = iota
	MetricTypeGauge
	MetricTypeSum
	MetricTypeHistogram
	MetricTypeExponentialHistogram
	MetricTypeDDSketch
	MetricTypeSummary
	MetricTypeKLLSketch
	MetricTypeCountSketch
	MetricTypeCountMinSketch
	MetricTypeHLLSketch
)

// String returns the string representation of the MetricType.
func (mdt MetricType) String() string {
	switch mdt {
	case MetricTypeEmpty:
		return "Empty"
	case MetricTypeGauge:
		return "Gauge"
	case MetricTypeSum:
		return "Sum"
	case MetricTypeHistogram:
		return "Histogram"
	case MetricTypeExponentialHistogram:
		return "ExponentialHistogram"
	case MetricTypeDDSketch:
		return "DDSketch"
	case MetricTypeSummary:
		return "Summary"
	case MetricTypeKLLSketch:
		return "KLLSketch"
	case MetricTypeCountSketch:
		return "CountSketch"
	case MetricTypeCountMinSketch:
		return "CountMinSketch"
	case MetricTypeHLLSketch:
		return "HLLSketch"
	}
	return ""
}
