// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

// DDSketchDataPointMaxType specifies the type of maximum stored in DDSketchDataPoint.
type DDSketchDataPointMaxType int32

const (
	// DDSketchDataPointMaxTypeEmpty means the maximum is not set.
	DDSketchDataPointMaxTypeEmpty DDSketchDataPointMaxType = iota
	// DDSketchDataPointMaxTypeDouble indicates the maximum is stored as a double.
	DDSketchDataPointMaxTypeDouble
	// DDSketchDataPointMaxTypeInt indicates the maximum is stored as an int.
	DDSketchDataPointMaxTypeInt
)

// String returns the string representation of the DDSketchDataPointMaxType.
func (t DDSketchDataPointMaxType) String() string {
	switch t {
	case DDSketchDataPointMaxTypeEmpty:
		return "Empty"
	case DDSketchDataPointMaxTypeDouble:
		return "Double"
	case DDSketchDataPointMaxTypeInt:
		return "Int"
	}
	return ""
}
