// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

// DDSketchDataPointSumType specifies the type of sum stored in DDSketchDataPoint.
type DDSketchDataPointSumType int32

const (
	// DDSketchDataPointSumTypeEmpty means the sum is not set.
	DDSketchDataPointSumTypeEmpty DDSketchDataPointSumType = iota
	// DDSketchDataPointSumTypeDouble indicates the sum is stored as a double.
	DDSketchDataPointSumTypeDouble
	// DDSketchDataPointSumTypeInt indicates the sum is stored as an int.
	DDSketchDataPointSumTypeInt
)

// String returns the string representation of the DDSketchDataPointSumType.
func (t DDSketchDataPointSumType) String() string {
	switch t {
	case DDSketchDataPointSumTypeEmpty:
		return "Empty"
	case DDSketchDataPointSumTypeDouble:
		return "Double"
	case DDSketchDataPointSumTypeInt:
		return "Int"
	}
	return ""
}
