// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metric // import "go.opentelemetry.io/otel/sdk/metric"

import (
	"errors"
	"fmt"
	"slices"
)

// errAgg is wrapped by misconfigured aggregations.
var errAgg = errors.New("aggregation")

// Aggregation is the aggregation used to summarize recorded measurements.
type Aggregation interface {
	// copy returns a deep copy of the Aggregation.
	copy() Aggregation

	// err returns an error for any misconfigured Aggregation.
	err() error
}

// AggregationDrop is an Aggregation that drops all recorded data.
type AggregationDrop struct{} // AggregationDrop has no parameters.

var _ Aggregation = AggregationDrop{}

// copy returns a deep copy of d.
func (d AggregationDrop) copy() Aggregation { return d }

// err returns an error for any misconfiguration. A drop aggregation has no
// parameters and cannot be misconfigured, therefore this always returns nil.
func (AggregationDrop) err() error { return nil }

// AggregationDefault is an Aggregation that uses the default instrument kind selection
// mapping to select another Aggregation. A metric reader can be configured to
// make an aggregation selection based on instrument kind that differs from
// the default. This Aggregation ensures the default is used.
//
// See the [DefaultAggregationSelector] for information about the default
// instrument kind selection mapping.
type AggregationDefault struct{} // AggregationDefault has no parameters.

var _ Aggregation = AggregationDefault{}

// copy returns a deep copy of d.
func (d AggregationDefault) copy() Aggregation { return d }

// err returns an error for any misconfiguration. A default aggregation has no
// parameters and cannot be misconfigured, therefore this always returns nil.
func (AggregationDefault) err() error { return nil }

// AggregationSum is an Aggregation that summarizes a set of measurements as their
// arithmetic sum.
type AggregationSum struct{} // AggregationSum has no parameters.

var _ Aggregation = AggregationSum{}

// copy returns a deep copy of s.
func (s AggregationSum) copy() Aggregation { return s }

// err returns an error for any misconfiguration. A sum aggregation has no
// parameters and cannot be misconfigured, therefore this always returns nil.
func (AggregationSum) err() error { return nil }

// AggregationLastValue is an Aggregation that summarizes a set of measurements as the
// last one made.
type AggregationLastValue struct{} // AggregationLastValue has no parameters.

var _ Aggregation = AggregationLastValue{}

// copy returns a deep copy of l.
func (l AggregationLastValue) copy() Aggregation { return l }

// err returns an error for any misconfiguration. A last-value aggregation has
// no parameters and cannot be misconfigured, therefore this always returns
// nil.
func (AggregationLastValue) err() error { return nil }

// AggregationExplicitBucketHistogram is an Aggregation that summarizes a set of
// measurements as an histogram with explicitly defined buckets.
type AggregationExplicitBucketHistogram struct {
	// Boundaries are the increasing bucket boundary values. Boundary values
	// define bucket upper bounds. Buckets are exclusive of their lower
	// boundary and inclusive of their upper bound (except at positive
	// infinity). A measurement is defined to fall into the greatest-numbered
	// bucket with a boundary that is greater than or equal to the
	// measurement. As an example, boundaries defined as:
	//
	// []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 1000}
	//
	// Will define these buckets:
	//
	// (-∞, 0], (0, 5.0], (5.0, 10.0], (10.0, 25.0], (25.0, 50.0],
	// (50.0, 75.0], (75.0, 100.0], (100.0, 250.0], (250.0, 500.0],
	// (500.0, 1000.0], (1000.0, +∞)
	Boundaries []float64
	// NoMinMax indicates whether to not record the min and max of the
	// distribution. By default, these extrema are recorded.
	//
	// Recording these extrema for cumulative data is expected to have little
	// value, they will represent the entire life of the instrument instead of
	// just the current collection cycle. It is recommended to set this to true
	// for that type of data to avoid computing the low-value extrema.
	NoMinMax bool
}

var _ Aggregation = AggregationExplicitBucketHistogram{}

// errHist is returned by misconfigured ExplicitBucketHistograms.
var errHist = fmt.Errorf("%w: explicit bucket histogram", errAgg)

// err returns an error for any misconfiguration.
func (h AggregationExplicitBucketHistogram) err() error {
	if len(h.Boundaries) <= 1 {
		return nil
	}

	// Check boundaries are monotonic.
	i := h.Boundaries[0]
	for _, j := range h.Boundaries[1:] {
		if i >= j {
			return fmt.Errorf("%w: non-monotonic boundaries: %v", errHist, h.Boundaries)
		}
		i = j
	}

	return nil
}

// copy returns a deep copy of h.
func (h AggregationExplicitBucketHistogram) copy() Aggregation {
	return AggregationExplicitBucketHistogram{
		Boundaries: slices.Clone(h.Boundaries),
		NoMinMax:   h.NoMinMax,
	}
}

// AggregationBase2ExponentialHistogram is an Aggregation that summarizes a set of
// measurements as an histogram with bucket widths that grow exponentially.
type AggregationBase2ExponentialHistogram struct {
	// MaxSize is the maximum number of buckets to use for the histogram.
	MaxSize int32
	// MaxScale is the maximum resolution scale to use for the histogram.
	//
	// MaxScale has a maximum value of 20. Using a value of 20 means the
	// maximum number of buckets that can fit within the range of a
	// signed 32-bit integer index could be used.
	//
	// MaxScale has a minimum value of -10. Using a value of -10 means only
	// two buckets will be used.
	MaxScale int32

	// NoMinMax indicates whether to not record the min and max of the
	// distribution. By default, these extrema are recorded.
	//
	// Recording these extrema for cumulative data is expected to have little
	// value, they will represent the entire life of the instrument instead of
	// just the current collection cycle. It is recommended to set this to true
	// for that type of data to avoid computing the low-value extrema.
	NoMinMax bool
}

var _ Aggregation = AggregationBase2ExponentialHistogram{}

// copy returns a deep copy of the Aggregation.
func (e AggregationBase2ExponentialHistogram) copy() Aggregation {
	return e
}

const (
	expoMaxScale = 20
	expoMinScale = -10
)

// errExpoHist is returned by misconfigured Base2ExponentialBucketHistograms.
var errExpoHist = fmt.Errorf("%w: exponential histogram", errAgg)

// err returns an error for any misconfigured Aggregation.
func (e AggregationBase2ExponentialHistogram) err() error {
	if e.MaxScale > expoMaxScale {
		return fmt.Errorf("%w: max size %d is greater than maximum scale %d", errExpoHist, e.MaxSize, expoMaxScale)
	}
	if e.MaxSize <= 0 {
		return fmt.Errorf("%w: max size %d is less than or equal to zero", errExpoHist, e.MaxSize)
	}
	return nil
}

// AggregationDDSketch summarizes recorded measurements as a DDSketch.
type AggregationDDSketch struct {
	// RelativeAccuracy controls the target relative accuracy. When zero, a
	// default value is used.
	RelativeAccuracy float64
	// NoMinMax indicates whether to not record minima and maxima.
	NoMinMax bool
}

var _ Aggregation = AggregationDDSketch{}

var errDDSketch = fmt.Errorf("%w: ddsketch", errAgg)

func (a AggregationDDSketch) copy() Aggregation { return a }

func (a AggregationDDSketch) err() error {
	if a.RelativeAccuracy == 0 {
		return nil
	}
	if a.RelativeAccuracy <= 0 || a.RelativeAccuracy >= 1 {
		return fmt.Errorf("%w: relative accuracy %v must be in (0,1)", errDDSketch, a.RelativeAccuracy)
	}
	return nil
}

// AggregationKLLSketch summarizes recorded measurements as a KLL sketch.
type AggregationKLLSketch struct {
	// K controls the sketch compaction parameter. When zero, a default value is
	// used.
	K int
}

var _ Aggregation = AggregationKLLSketch{}

var errKLLSketch = fmt.Errorf("%w: kll sketch", errAgg)

func (a AggregationKLLSketch) copy() Aggregation { return a }

func (a AggregationKLLSketch) err() error {
	if a.K < 0 {
		return fmt.Errorf("%w: k %d must be greater than or equal to zero", errKLLSketch, a.K)
	}
	return nil
}

// AggregationCountSketch summarizes recorded measurements as a CountSketch.
type AggregationCountSketch struct {
	// Rows is the number of hash functions used by the sketch. When zero, a
	// default value is used.
	Rows int
	// Cols is the number of buckets per row. When zero, a default value is used.
	Cols int
	// Epsilon is the configured accuracy parameter to report alongside the
	// serialized sketch. When zero, it is omitted from validation.
	Epsilon float64
	// Delta is the configured failure probability parameter to report alongside
	// the serialized sketch. When zero, it is omitted from validation.
	Delta float64
	// Dimension describes the sketched dimension.
	Dimension string
	// DeltaTransmission enables sparse delta encoding for cumulative exports.
	// When true, only cells that changed since the last export are transmitted.
	// Has no effect for delta-temporality exports (those reset every interval).
	DeltaTransmission bool
	// DeltaThreshold is the minimum absolute cell change required to include a
	// cell in a delta payload. Defaults to 1.0 when DeltaTransmission is true.
	DeltaThreshold float64
}

var _ Aggregation = AggregationCountSketch{}

var errCountSketch = fmt.Errorf("%w: count sketch", errAgg)

func (a AggregationCountSketch) copy() Aggregation { return a }

func (a AggregationCountSketch) err() error {
	if a.Rows < 0 {
		return fmt.Errorf("%w: rows %d must be greater than or equal to zero", errCountSketch, a.Rows)
	}
	if a.Cols < 0 {
		return fmt.Errorf("%w: cols %d must be greater than or equal to zero", errCountSketch, a.Cols)
	}
	if a.Epsilon != 0 && (a.Epsilon <= 0 || a.Epsilon >= 1) {
		return fmt.Errorf("%w: epsilon %v must be in (0,1)", errCountSketch, a.Epsilon)
	}
	if a.Delta != 0 && (a.Delta <= 0 || a.Delta >= 1) {
		return fmt.Errorf("%w: delta %v must be in (0,1)", errCountSketch, a.Delta)
	}
	return nil
}

// AggregationCountMinSketch summarizes recorded measurements as a Count-Min Sketch.
type AggregationCountMinSketch struct {
	// Rows is the number of hash functions used by the sketch. When zero, a
	// default value is used.
	Rows int
	// Cols is the number of buckets per row. When zero, a default value is used.
	Cols int
	// DeltaTransmission enables sparse delta encoding for cumulative exports.
	// When true, only cells that changed since the last export are transmitted.
	// Has no effect for delta-temporality exports (those reset every interval).
	DeltaTransmission bool
	// DeltaThreshold is the minimum absolute cell change required to include a
	// cell in a delta payload. Defaults to 1.0 when DeltaTransmission is true.
	DeltaThreshold float64
}

var _ Aggregation = AggregationCountMinSketch{}

var errCountMinSketch = fmt.Errorf("%w: count-min sketch", errAgg)

func (a AggregationCountMinSketch) copy() Aggregation { return a }

func (a AggregationCountMinSketch) err() error {
	if a.Rows < 0 {
		return fmt.Errorf("%w: rows %d must be greater than or equal to zero", errCountMinSketch, a.Rows)
	}
	if a.Cols < 0 {
		return fmt.Errorf("%w: cols %d must be greater than or equal to zero", errCountMinSketch, a.Cols)
	}
	return nil
}

// AggregationHLLSketch summarizes recorded measurements as a HyperLogLog sketch.
type AggregationHLLSketch struct {
	// DeltaTransmission enables sparse delta encoding for cumulative exports.
	// When true, only registers that increased since the last export are transmitted
	// (HLL uses max semantics so registers never decrease).
	// Has no effect for delta-temporality exports (those reset every interval).
	DeltaTransmission bool
}

var _ Aggregation = AggregationHLLSketch{}

func (a AggregationHLLSketch) copy() Aggregation { return a }

func (AggregationHLLSketch) err() error { return nil }
