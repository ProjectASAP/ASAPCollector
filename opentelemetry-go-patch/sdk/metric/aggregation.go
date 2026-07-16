// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metric // import "go.opentelemetry.io/otel/sdk/metric"

import (
	"errors"
	"fmt"
	"slices"

	precompute "github.com/ProjectASAP/asap-precompute-go"
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
	// DeltaTransmission enables sparse delta encoding in the cumulative export
	// path: only buckets that changed by at least DeltaThreshold counts since
	// the previous snapshot are included in the payload.
	DeltaTransmission bool
	// DeltaThreshold is the minimum absolute bucket count change required to
	// include a bucket in a delta payload. Defaults to 1 when DeltaTransmission
	// is true and DeltaThreshold is 0.
	DeltaThreshold uint64
	// SampleP is the geometric admission rate applied at this SDK aggregator
	// (whole-item, d=1). Values <=0 or >=1 disable sampling; 0 < SampleP < 1
	// enables NitroSketch skip-sampling (raw counts stored, wire stamps p,
	// consumer rescales ×1/p).
	SampleP float64
}

var _ Aggregation = AggregationDDSketch{}

var errDDSketch = fmt.Errorf("%w: ddsketch", errAgg)

func (a AggregationDDSketch) copy() Aggregation { return a }

func (a AggregationDDSketch) err() error {
	if a.SampleP < 0 || a.SampleP > 1 {
		return fmt.Errorf("%w: sample_p %v must be in [0,1]", errDDSketch, a.SampleP)
	}
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
	// SampleP is the per-row geometric admission rate (NitroSketch skip-sampling)
	// applied at this SDK aggregator. Values <=0 or >=1 disable sampling (every
	// update touches all rows); 0 < SampleP < 1 admits each row with probability
	// SampleP and applies the 1/SampleP inverse-probability weight. Hosting the
	// admission here is the "sampling at the SDK" location of the GOS design.
	SampleP float64
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
	if a.SampleP < 0 || a.SampleP > 1 {
		return fmt.Errorf("%w: sample_p %v must be in [0,1]", errCountSketch, a.SampleP)
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
	// SampleP is the per-row geometric admission rate applied at this SDK
	// aggregator. Values <=0 or >=1 disable sampling; 0 < SampleP < 1 admits each
	// row with probability SampleP and applies the 1/SampleP weight in-place.
	SampleP float64
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
	if a.SampleP < 0 || a.SampleP > 1 {
		return fmt.Errorf("%w: sample_p %v must be in [0,1]", errCountMinSketch, a.SampleP)
	}
	return nil
}

// AggregationRowSampledSketch summarizes recorded measurements by deciding,
// PER RAW OCCURRENCE, row admission into a downstream (collector-side)
// row/col sketch — NitroSketch-style geometric skip-sampling run at the SDK,
// before the occurrence is ever serialized. An occurrence that admits no
// row is discarded here and never leaves the process; an occurrence that
// admits at least one row is exported individually (never pre-merged with
// any other occurrence — merging would destroy the per-occurrence key the
// collector needs to pick that occurrence's column). See
// metricdata.RowSampledSketch and precompute.AggregationRouter for the
// full design rationale.
type AggregationRowSampledSketch struct {
	// Router maps a series' retained attributes to the target collector-
	// side aggregation instance (precompute.AggregationIdentity) it folds
	// into and that target's row fan-out. Required — a policy with no
	// router cannot route anything.
	//
	// The common case (one metric feeds one collector-side sketch) is
	// precompute.AggregationPolicy{...}.Router(); a metric that fans out
	// to multiple physically distinct sketches supplies a custom router.
	Router precompute.AggregationRouter

	// CoordinatorURL is the data-plane MonitorService gRPC endpoint this
	// policy's live sample-rate grant is read from — the SAME protocol
	// otel-app/sample_controller.go and the edge collector's continuous
	// monitor already use, dialed DIRECTLY (bypassing the collector).
	// Empty disables live grants; BootstrapSampleP is then permanent.
	CoordinatorURL string
	// EdgeID identifies this process to the coordinator (Registration.EdgeID).
	EdgeID string
	// WindowSizeSecs is the CDM epoch length — should match the collector's
	// warm-tier window (the coordinator's slack-countdown protocol is a
	// per-epoch round).
	WindowSizeSecs uint64
	// BootstrapSampleP is used before the first live grant arrives (and
	// permanently if CoordinatorURL is empty). 1.0 (default) admits every
	// row of every occurrence — byte-equivalent to unsampled passthrough.
	BootstrapSampleP float64
}

var _ Aggregation = AggregationRowSampledSketch{}

var errRowSampledSketch = fmt.Errorf("%w: row-sampled sketch", errAgg)

func (a AggregationRowSampledSketch) copy() Aggregation { return a }

func (a AggregationRowSampledSketch) err() error {
	if a.Router == nil {
		return fmt.Errorf("%w: router is required", errRowSampledSketch)
	}
	if a.BootstrapSampleP < 0 || a.BootstrapSampleP > 1 {
		return fmt.Errorf("%w: bootstrap_sample_p %v must be in [0,1]", errRowSampledSketch, a.BootstrapSampleP)
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

// AggregationRawBuffer emits every recorded measurement as its own
// data point. Unlike the sketch aggregators it does not reduce a
// stream of Add/Record calls into one summary per attribute set;
// the SDK "aggregator" here is just a bounded buffer that preserves
// (timestamp, attrs, value) tuples until the next collect.
//
// This is the SDK-side "raw-buffer" encoding on the paper's
// three-axis framework — see
// docs/sdk-aggregation-three-axis-design.md. It is the
// experimental baseline used to measure what the SDK decision
// point costs when it chooses not to aggregate.
//
// Semantics:
//   - On each Add/Record, append (now(), attrs, value) to the
//     per-attribute buffer.
//   - On Collect, emit every buffered tuple as a separate
//     metricdata.DataPoint[N] inside a Gauge[N]. Buffer is then
//     cleared regardless of the requested temporality (cumulative
//     with raw-buffer would mean re-emitting history forever, which
//     is not a meaningful choice — the delta-temporality-like reset
//     behaviour is what experiments want).
//   - Overflow: when a single attribute's buffer exceeds
//     MaxEventsPerSeries, new measurements on that attribute are
//     silently dropped. Drop accounting is per-attribute (in-memory
//     only for v1 — exposing the drop count as a side-channel
//     counter is a follow-up noted in PROGRESS.md).
type AggregationRawBuffer struct {
	// MaxEventsPerSeries caps the per-attribute buffer length to
	// prevent unbounded memory growth when the exporter stalls.
	// When zero, a default of 10000 is used.
	MaxEventsPerSeries int
}

var _ Aggregation = AggregationRawBuffer{}

var errRawBuffer = fmt.Errorf("%w: raw buffer", errAgg)

func (a AggregationRawBuffer) copy() Aggregation { return a }

func (a AggregationRawBuffer) err() error {
	if a.MaxEventsPerSeries < 0 {
		return fmt.Errorf("%w: max events per series %d must be non-negative", errRawBuffer, a.MaxEventsPerSeries)
	}
	return nil
}
