// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pmetric // import "go.opentelemetry.io/collector/pdata/pmetric"

// DDSketchDataPointMinType specifies the type of minimum stored in DDSketchDataPoint.
type DDSketchDataPointMinType int32

const (
	// DDSketchDataPointMinTypeEmpty means the minimum is not set.
	DDSketchDataPointMinTypeEmpty DDSketchDataPointMinType = iota
	// DDSketchDataPointMinTypeDouble indicates the minimum is stored as a double.
	DDSketchDataPointMinTypeDouble
	// DDSketchDataPointMinTypeInt indicates the minimum is stored as an int.
	DDSketchDataPointMinTypeInt
)

// String returns the string representation of the DDSketchDataPointMinType.
func (t DDSketchDataPointMinType) String() string {
	switch t {
	case DDSketchDataPointMinTypeEmpty:
		return "Empty"
	case DDSketchDataPointMinTypeDouble:
		return "Double"
	case DDSketchDataPointMinTypeInt:
		return "Int"
	}
	return ""
}
