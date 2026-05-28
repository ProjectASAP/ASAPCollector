// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pdata // import "go.opentelemetry.io/collector/internal/cmd/pdatagen/internal/pdata"
import (
	"go.opentelemetry.io/collector/internal/cmd/pdatagen/internal/proto"
)

var pmetric = &Package{
	info: &PackageInfo{
		name: "pmetric",
		path: "pmetric",
		imports: []string{
			`"encoding/binary"`,
			`"fmt"`,
			`"iter"`,
			`"math"`,
			`"sort"`,
			`"sync"`,
			``,
			`"go.opentelemetry.io/collector/pdata/internal"`,
			`"go.opentelemetry.io/collector/pdata/internal/json"`,
			`"go.opentelemetry.io/collector/pdata/internal/proto"`,
			`"go.opentelemetry.io/collector/pdata/pcommon"`,
		},
		testImports: []string{
			`"strconv"`,
			`"testing"`,
			`"unsafe"`,
			``,
			`"github.com/stretchr/testify/assert"`,
			`"github.com/stretchr/testify/require"`,
			`"google.golang.org/protobuf/proto"`,
			`gootlpcollectormetrics "go.opentelemetry.io/proto/slim/otlp/collector/metrics/v1"`,
			`gootlpmetrics "go.opentelemetry.io/proto/slim/otlp/metrics/v1"`,
			``,
			`"go.opentelemetry.io/collector/pdata/internal"`,
			`"go.opentelemetry.io/collector/pdata/internal/json"`,
			`"go.opentelemetry.io/collector/pdata/pcommon"`,
		},
	},
	structs: []baseStruct{
		metrics,
		metricsData,
		resourceMetricsSlice,
		resourceMetrics,
		scopeMetricsSlice,
		scopeMetrics,
		metricSlice,
		metric,
		gauge,
		sum,
		histogram,
		exponentialHistogram,
		ddsketch,
		kllsketch,
		hllsketch,
		countsketch,
		countminsketch,
		sumAgg,
		summary,
		numberDataPointSlice,
		numberDataPoint,
		histogramDataPointSlice,
		histogramDataPoint,
		exponentialHistogramDataPointSlice,
		exponentialHistogramDataPoint,
		ddsketchDataPointSlice,
		ddsketchDataPoint,
		kllsketchDataPointSlice,
		kllsketchDataPoint,
		hllsketchDataPointSlice,
		hllsketchDataPoint,
		countsketchDataPointSlice,
		countsketchDataPoint,
		countminsketchDataPointSlice,
		countminsketchDataPoint,
		sumAggDataPointSlice,
		sumAggDataPoint,
		bucketsValues,
		summaryDataPointSlice,
		summaryDataPoint,
		quantileValuesSlice,
		quantileValues,
		exemplarSlice,
		exemplar,
	},
	enums: []*proto.Enum{
		aggregationTemporalityEnum,
		ddsketchEncodingEnum,
		kllsketchEncodingEnum,
		hllsketchEncodingEnum,
		countsketchEncodingEnum,
		countminsketchEncodingEnum,
		sumAggEncodingEnum,
	},
}

var metrics = &messageStruct{
	structName:    "Metrics",
	description:   "// Metrics is the top-level struct that is propagated through the metrics pipeline.\n// Use NewMetrics to create new instance, zero-initialized instance is not valid for use.",
	protoName:     "ExportMetricsServiceRequest",
	upstreamProto: "gootlpcollectormetrics.ExportMetricsServiceRequest",
	fields: []Field{
		&SliceField{
			fieldName:   "ResourceMetrics",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: resourceMetricsSlice,
		},
	},
	hasWrapper: true,
}

var metricsData = &messageStruct{
	structName:    "MetricsData",
	description:   "// MetricsData represents the metrics data that can be stored in a persistent storage,\n// OR can be embedded by other protocols that transfer OTLP metrics data but do not\n// implement the OTLP protocol..",
	protoName:     "MetricsData",
	upstreamProto: "gootlpmetrics.MetricsData",
	fields: []Field{
		&SliceField{
			fieldName:   "ResourceMetrics",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: resourceMetricsSlice,
		},
	},
	hasOnlyInternal: true,
}

var resourceMetricsSlice = &messageSlice{
	structName:      "ResourceMetricsSlice",
	elementNullable: true,
	element:         resourceMetrics,
}

var resourceMetrics = &messageStruct{
	structName:    "ResourceMetrics",
	description:   "// ResourceMetrics is a collection of metrics from a Resource.",
	protoName:     "ResourceMetrics",
	upstreamProto: "gootlpmetrics.ResourceMetrics",
	fields: []Field{
		&MessageField{
			fieldName:     "Resource",
			protoID:       1,
			returnMessage: resource,
		},
		&SliceField{
			fieldName:   "ScopeMetrics",
			protoID:     2,
			protoType:   proto.TypeMessage,
			returnSlice: scopeMetricsSlice,
		},
		&PrimitiveField{
			fieldName: "SchemaUrl",
			protoID:   3,
			protoType: proto.TypeString,
		},
		&SliceField{
			fieldName:   "DeprecatedScopeMetrics",
			protoType:   proto.TypeMessage,
			protoID:     1000,
			returnSlice: scopeMetricsSlice,
			// Hide accessors for this field because it is a HACK:
			// Workaround for istio 1.15 / envoy 1.23.1 mistakenly emitting deprecated field.
			hideAccessors: true,
		},
	},
}

var scopeMetricsSlice = &messageSlice{
	structName:      "ScopeMetricsSlice",
	elementNullable: true,
	element:         scopeMetrics,
}

var scopeMetrics = &messageStruct{
	structName:    "ScopeMetrics",
	description:   "// ScopeMetrics is a collection of metrics from a LibraryInstrumentation.",
	protoName:     "ScopeMetrics",
	upstreamProto: "gootlpmetrics.ScopeMetrics",
	fields: []Field{
		&MessageField{
			fieldName:     "Scope",
			protoID:       1,
			returnMessage: scope,
		},
		&SliceField{
			fieldName:   "Metrics",
			protoID:     2,
			protoType:   proto.TypeMessage,
			returnSlice: metricSlice,
		},
		&PrimitiveField{
			fieldName: "SchemaUrl",
			protoID:   3,
			protoType: proto.TypeString,
		},
	},
}

var metricSlice = &messageSlice{
	structName:      "MetricSlice",
	elementNullable: true,
	element:         metric,
}

var metric = &messageStruct{
	structName: "Metric",
	description: "// Metric represents one metric as a collection of datapoints.\n" +
		"// See Metric definition in OTLP: https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto",
	protoName:     "Metric",
	upstreamProto: "gootlpmetrics.Metric",
	fields: []Field{
		&PrimitiveField{
			fieldName: "Name",
			protoID:   1,
			protoType: proto.TypeString,
		},
		&PrimitiveField{
			fieldName: "Description",
			protoID:   2,
			protoType: proto.TypeString,
		},
		&PrimitiveField{
			fieldName: "Unit",
			protoID:   3,
			protoType: proto.TypeString,
		},
		&OneOfField{
			typeName:                   "MetricType",
			originFieldName:            "Data",
			testValueIdx:               1, // Sum
			omitOriginFieldNameInNames: true,
			values: []oneOfValue{
				&OneOfMessageValue{
					fieldName:     "Gauge",
					protoID:       5,
					returnMessage: gauge,
				},
				&OneOfMessageValue{
					fieldName:     "Sum",
					protoID:       7,
					returnMessage: sum,
				},
				&OneOfMessageValue{
					fieldName:     "Histogram",
					protoID:       9,
					returnMessage: histogram,
				},
				&OneOfMessageValue{
					fieldName:     "ExponentialHistogram",
					protoID:       10,
					returnMessage: exponentialHistogram,
				},
				&OneOfMessageValue{
					fieldName:     "DDSketch",
					protoID:       13,
					returnMessage: ddsketch,
				},
				&OneOfMessageValue{
					fieldName:     "Summary",
					protoID:       11,
					returnMessage: summary,
				},
				&OneOfMessageValue{
					fieldName:     "KLLSketch",
					protoID:       14,
					returnMessage: kllsketch,
				},
				&OneOfMessageValue{
					fieldName:     "CountSketch",
					protoID:       15,
					returnMessage: countsketch,
				},
				&OneOfMessageValue{
					fieldName:     "CountMinSketch",
					protoID:       16,
					returnMessage: countminsketch,
				},
				&OneOfMessageValue{
					fieldName:     "HLLSketch",
					protoID:       17,
					returnMessage: hllsketch,
				},
				&OneOfMessageValue{
					fieldName:     "SumAgg",
					protoID:       18,
					returnMessage: sumAgg,
				},
			},
		},
		&SliceField{
			fieldName:   "Metadata",
			protoID:     12,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
	},
}

var gauge = &messageStruct{
	structName:    "Gauge",
	description:   "// Gauge represents the type of a numeric metric that always exports the \"current value\" for every data point.",
	protoName:     "Gauge",
	upstreamProto: "gootlpmetrics.Gauge",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: numberDataPointSlice,
		},
	},
}

var sum = &messageStruct{
	structName:    "Sum",
	description:   "// Sum represents the type of a numeric metric that is calculated as a sum of all reported measurements over a time interval.",
	protoName:     "Sum",
	upstreamProto: "gootlpmetrics.Sum",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: numberDataPointSlice,
		},
		&TypedField{
			fieldName:  "AggregationTemporality",
			protoID:    2,
			returnType: aggregationTemporalityType,
		},
		&PrimitiveField{
			fieldName: "IsMonotonic",
			protoID:   3,
			protoType: proto.TypeBool,
		},
	},
}

var histogram = &messageStruct{
	structName:    "Histogram",
	description:   "// Histogram represents the type of a metric that is calculated by aggregating as a Histogram of all reported measurements over a time interval.",
	protoName:     "Histogram",
	upstreamProto: "gootlpmetrics.Histogram",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: histogramDataPointSlice,
		},
		&TypedField{
			fieldName:  "AggregationTemporality",
			protoID:    2,
			returnType: aggregationTemporalityType,
		},
	},
}

var exponentialHistogram = &messageStruct{
	structName: "ExponentialHistogram",
	description: `// ExponentialHistogram represents the type of a metric that is calculated by aggregating
	// as a ExponentialHistogram of all reported double measurements over a time interval.`,
	protoName:     "ExponentialHistogram",
	upstreamProto: "gootlpmetrics.ExponentialHistogram",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: exponentialHistogramDataPointSlice,
		},
		&TypedField{
			fieldName:  "AggregationTemporality",
			protoID:    2,
			returnType: aggregationTemporalityType,
		},
	},
}

var ddsketch = &messageStruct{
	structName:    "DDSketch",
	description:   "// DDSketch represents the type of a metric encoded with the DataDog DDSketch data structure.\n// The DDSketch payload holds an approximation of the underlying value distribution and optional extrema/sum tracking, similar to the Sum and Histogram aggregations.",
	protoName:     "DDSketch",
	upstreamProto: "gootlpmetrics.DDSketch",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: ddsketchDataPointSlice,
		},
		&TypedField{
			fieldName:  "AggregationTemporality",
			protoID:    2,
			returnType: aggregationTemporalityType,
		},
		&PrimitiveField{
			fieldName: "RelativeAccuracy",
			protoID:   3,
			protoType: proto.TypeDouble,
		},
	},
}

var sumAgg = &messageStruct{
	structName:    "SumAgg",
	description:   "// SumAgg represents a first-class scalar Sum aggregate (AggregationKind = Sum), carried as a portable {sum,count} envelope in the Sketch bytes. It is NOT a sketch; it rides the modified-OTLP metric data oneof alongside the sketch families so the backend can decode it via the same envelope path (into the ExactAgg(Sum) accumulator).",
	protoName:     "SumAgg",
	upstreamProto: "gootlpmetrics.SumAgg",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: sumAggDataPointSlice,
		},
		&TypedField{
			fieldName:  "AggregationTemporality",
			protoID:    2,
			returnType: aggregationTemporalityType,
		},
	},
}

var kllsketch = &messageStruct{
	structName:    "KLLSketch",
	description:   "// KLLSketch represents the type of a metric encoded using the KLL quantile sketch algorithm.",
	protoName:     "KLLSketch",
	upstreamProto: "gootlpmetrics.KLLSketch",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: kllsketchDataPointSlice,
		},
		&TypedField{
			fieldName:  "AggregationTemporality",
			protoID:    2,
			returnType: aggregationTemporalityType,
		},
		&PrimitiveField{
			fieldName: "K",
			protoID:   3,
			protoType: proto.TypeUint32,
		},
	},
}

var hllsketch = &messageStruct{
	structName:    "HLLSketch",
	description:   "// HLLSketch represents the type of a metric encoded using HyperLogLog cardinality estimation.",
	protoName:     "HLLSketch",
	upstreamProto: "gootlpmetrics.HLLSketch",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: hllsketchDataPointSlice,
		},
		&TypedField{
			fieldName:  "AggregationTemporality",
			protoID:    2,
			returnType: aggregationTemporalityType,
		},
		&PrimitiveField{
			fieldName: "Precision",
			protoID:   3,
			protoType: proto.TypeUint32,
		},
	},
}

var countsketch = &messageStruct{
	structName:    "CountSketch",
	description:   "// CountSketch represents the type of a metric encoded using the CountSketch frequency estimation algorithm.",
	protoName:     "CountSketch",
	upstreamProto: "gootlpmetrics.CountSketch",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: countsketchDataPointSlice,
		},
		&TypedField{
			fieldName:  "AggregationTemporality",
			protoID:    2,
			returnType: aggregationTemporalityType,
		},
		&PrimitiveField{
			fieldName: "Rows",
			protoID:   3,
			protoType: proto.TypeInt32,
		},
		&PrimitiveField{
			fieldName: "Cols",
			protoID:   4,
			protoType: proto.TypeInt32,
		},
	},
}

var countminsketch = &messageStruct{
	structName:    "CountMinSketch",
	description:   "// CountMinSketch represents the type of a metric encoded using the Count-Min Sketch frequency estimation algorithm.",
	protoName:     "CountMinSketch",
	upstreamProto: "gootlpmetrics.CountMinSketch",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: countminsketchDataPointSlice,
		},
		&TypedField{
			fieldName:  "AggregationTemporality",
			protoID:    2,
			returnType: aggregationTemporalityType,
		},
		&PrimitiveField{
			fieldName: "Rows",
			protoID:   3,
			protoType: proto.TypeInt32,
		},
		&PrimitiveField{
			fieldName: "Cols",
			protoID:   4,
			protoType: proto.TypeInt32,
		},
	},
}

var summary = &messageStruct{
	structName:    "Summary",
	description:   "// Summary represents the type of a metric that is calculated by aggregating as a Summary of all reported double measurements over a time interval.",
	protoName:     "Summary",
	upstreamProto: "gootlpmetrics.Summary",
	fields: []Field{
		&SliceField{
			fieldName:   "DataPoints",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: summaryDataPointSlice,
		},
	},
}

var numberDataPointSlice = &messageSlice{
	structName:      "NumberDataPointSlice",
	elementNullable: true,
	element:         numberDataPoint,
}

var numberDataPoint = &messageStruct{
	structName:    "NumberDataPoint",
	description:   "// NumberDataPoint is a single data point in a timeseries that describes the time-varying value of a number metric.",
	protoName:     "NumberDataPoint",
	upstreamProto: "gootlpmetrics.NumberDataPoint",
	fields: []Field{
		&SliceField{
			fieldName:   "Attributes",
			protoID:     7,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&PrimitiveField{
			fieldName: "SeriesID",
			protoID:   9,
			protoType: proto.TypeUint64,
		},
		&TypedField{
			fieldName:       "StartTimestamp",
			originFieldName: "StartTimeUnixNano",
			protoID:         2,
			returnType:      timestampType,
		},
		&TypedField{
			fieldName:       "Timestamp",
			originFieldName: "TimeUnixNano",
			protoID:         3,
			returnType:      timestampType,
		},
		&OneOfField{
			typeName:        "NumberDataPointValueType",
			originFieldName: "Value",
			testValueIdx:    0, // Double
			values: []oneOfValue{
				&OneOfPrimitiveValue{
					fieldName:       "Double",
					protoID:         4,
					originFieldName: "AsDouble",
					protoType:       proto.TypeDouble,
				},
				&OneOfPrimitiveValue{
					fieldName:       "Int",
					protoID:         6,
					originFieldName: "AsInt",
					protoType:       proto.TypeSFixed64,
				},
			},
		},
		&SliceField{
			fieldName:   "Exemplars",
			protoID:     5,
			protoType:   proto.TypeMessage,
			returnSlice: exemplarSlice,
		},
		&TypedField{
			fieldName: "Flags",
			protoID:   8,
			returnType: &TypedType{
				structName: "DataPointFlags",
				protoType:  proto.TypeUint32,
				defaultVal: "0",
				testVal:    "1",
			},
		},
	},
}

var histogramDataPointSlice = &messageSlice{
	structName:      "HistogramDataPointSlice",
	elementNullable: true,
	element:         histogramDataPoint,
}

var histogramDataPoint = &messageStruct{
	structName:    "HistogramDataPoint",
	description:   "// HistogramDataPoint is a single data point in a timeseries that describes the time-varying values of a Histogram of values.",
	protoName:     "HistogramDataPoint",
	upstreamProto: "gootlpmetrics.HistogramDataPoint",
	fields: []Field{
		&SliceField{
			fieldName:   "Attributes",
			protoID:     9,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&PrimitiveField{
			fieldName: "SeriesID",
			protoID:   13,
			protoType: proto.TypeUint64,
		},
		&TypedField{
			fieldName:       "StartTimestamp",
			originFieldName: "StartTimeUnixNano",
			protoID:         2,
			returnType:      timestampType,
		},
		&TypedField{
			fieldName:       "Timestamp",
			originFieldName: "TimeUnixNano",
			protoID:         3,
			returnType:      timestampType,
		},
		&PrimitiveField{
			fieldName: "Count",
			protoID:   4,
			protoType: proto.TypeFixed64,
		},
		&OptionalPrimitiveField{
			fieldName: "Sum",
			protoID:   5,
			protoType: proto.TypeDouble,
		},
		&SliceField{
			fieldName:   "BucketCounts",
			protoID:     6,
			protoType:   proto.TypeFixed64,
			returnSlice: uInt64Slice,
		},
		&SliceField{
			fieldName:   "ExplicitBounds",
			protoID:     7,
			protoType:   proto.TypeDouble,
			returnSlice: float64Slice,
		},
		&SliceField{
			fieldName:   "Exemplars",
			protoID:     8,
			protoType:   proto.TypeMessage,
			returnSlice: exemplarSlice,
		},
		&TypedField{
			fieldName: "Flags",
			protoID:   10,
			returnType: &TypedType{
				structName: "DataPointFlags",
				protoType:  proto.TypeUint32,
				defaultVal: "0",
				testVal:    "1",
			},
		},
		&OptionalPrimitiveField{
			fieldName: "Min",
			protoID:   11,
			protoType: proto.TypeDouble,
		},
		&OptionalPrimitiveField{
			fieldName: "Max",
			protoID:   12,
			protoType: proto.TypeDouble,
		},
	},
}

var exponentialHistogramDataPointSlice = &messageSlice{
	structName:      "ExponentialHistogramDataPointSlice",
	elementNullable: true,
	element:         exponentialHistogramDataPoint,
}

var exponentialHistogramDataPoint = &messageStruct{
	structName: "ExponentialHistogramDataPoint",
	description: `// ExponentialHistogramDataPoint is a single data point in a timeseries that describes the
	// time-varying values of a ExponentialHistogram of double values. A ExponentialHistogram contains
	// summary statistics for a population of values, it may optionally contain the
	// distribution of those values across a set of buckets.`,
	protoName:     "ExponentialHistogramDataPoint",
	upstreamProto: "gootlpmetrics.ExponentialHistogramDataPoint",
	fields: []Field{
		&SliceField{
			fieldName:   "Attributes",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&PrimitiveField{
			fieldName: "SeriesID",
			protoID:   15,
			protoType: proto.TypeUint64,
		},
		&TypedField{
			fieldName:       "StartTimestamp",
			protoID:         2,
			originFieldName: "StartTimeUnixNano",
			returnType:      timestampType,
		},
		&TypedField{
			fieldName:       "Timestamp",
			protoID:         3,
			originFieldName: "TimeUnixNano",
			returnType:      timestampType,
		},
		&PrimitiveField{
			fieldName: "Count",
			protoID:   4,
			protoType: proto.TypeFixed64,
		},
		&OptionalPrimitiveField{
			fieldName: "Sum",
			protoID:   5,
			protoType: proto.TypeDouble,
		},
		&PrimitiveField{
			fieldName: "Scale",
			protoID:   6,
			protoType: proto.TypeSInt32,
		},
		&PrimitiveField{
			fieldName: "ZeroCount",
			protoID:   7,
			protoType: proto.TypeFixed64,
		},
		&MessageField{
			fieldName:     "Positive",
			protoID:       8,
			returnMessage: bucketsValues,
		},
		&MessageField{
			fieldName:     "Negative",
			protoID:       9,
			returnMessage: bucketsValues,
		},
		&TypedField{
			fieldName: "Flags",
			protoID:   10,
			returnType: &TypedType{
				structName: "DataPointFlags",
				protoType:  proto.TypeUint32,
				defaultVal: "0",
				testVal:    "1",
			},
		},
		&SliceField{
			fieldName:   "Exemplars",
			protoID:     11,
			protoType:   proto.TypeMessage,
			returnSlice: exemplarSlice,
		},
		&OptionalPrimitiveField{
			fieldName: "Min",
			protoID:   12,
			protoType: proto.TypeDouble,
		},
		&OptionalPrimitiveField{
			fieldName: "Max",
			protoID:   13,
			protoType: proto.TypeDouble,
		},
		&PrimitiveField{
			fieldName: "ZeroThreshold",
			protoID:   14,
			protoType: proto.TypeDouble,
		},
	},
}

var bucketsValues = &messageStruct{
	structName:    "ExponentialHistogramDataPointBuckets",
	description:   "// ExponentialHistogramDataPointBuckets are a set of bucket counts, encoded in a contiguous array of counts.",
	protoName:     "ExponentialHistogramDataPointBuckets",
	upstreamProto: "gootlpmetrics.ExponentialHistogramDataPoint_Buckets",
	fields: []Field{
		&PrimitiveField{
			fieldName: "Offset",
			protoID:   1,
			protoType: proto.TypeSInt32,
		},
		&SliceField{
			fieldName:   "BucketCounts",
			protoID:     2,
			protoType:   proto.TypeUint64,
			returnSlice: uInt64Slice,
		},
	},
}

var ddsketchDataPointSlice = &messageSlice{
	structName:      "DDSketchDataPointSlice",
	elementNullable: true,
	element:         ddsketchDataPoint,
}

var ddsketchDataPoint = &messageStruct{
	structName:    "DDSketchDataPoint",
	description:   "// DDSketchDataPoint is a single data point that encodes a distribution using the DDSketch format.",
	protoName:     "DDSketchDataPoint",
	upstreamProto: "gootlpmetrics.DDSketchDataPoint",
	fields: []Field{
		&SliceField{
			fieldName:   "Attributes",
			protoID:     9,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&PrimitiveField{
			fieldName: "SeriesID",
			protoID:   16,
			protoType: proto.TypeUint64,
		},
		&TypedField{
			fieldName:       "StartTimestamp",
			originFieldName: "StartTimeUnixNano",
			protoID:         2,
			returnType:      timestampType,
		},
		&TypedField{
			fieldName:       "Timestamp",
			originFieldName: "TimeUnixNano",
			protoID:         3,
			returnType:      timestampType,
		},
		&PrimitiveField{
			fieldName: "Sketch",
			protoID:   8,
			protoType: proto.TypeBytes,
		},
		&TypedField{
			fieldName:  "Encoding",
			protoID:    10,
			returnType: ddsketchEncodingType,
		},
		&SliceField{
			fieldName:   "Exemplars",
			protoID:     11,
			protoType:   proto.TypeMessage,
			returnSlice: exemplarSlice,
		},
		&TypedField{
			fieldName: "Flags",
			protoID:   15,
			returnType: &TypedType{
				structName: "DataPointFlags",
				protoType:  proto.TypeUint32,
				defaultVal: "0",
				testVal:    "1",
			},
		},
	},
}

var sumAggDataPointSlice = &messageSlice{
	structName:      "SumAggDataPointSlice",
	elementNullable: true,
	element:         sumAggDataPoint,
}

var sumAggDataPoint = &messageStruct{
	structName:    "SumAggDataPoint",
	description:   "// SumAggDataPoint is a single data point carrying a scalar Sum aggregate as a portable {sum,count} envelope in the Sketch bytes.",
	protoName:     "SumAggDataPoint",
	upstreamProto: "gootlpmetrics.SumAggDataPoint",
	fields: []Field{
		&SliceField{
			fieldName:   "Attributes",
			protoID:     9,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&PrimitiveField{
			fieldName: "SeriesID",
			protoID:   16,
			protoType: proto.TypeUint64,
		},
		&TypedField{
			fieldName:       "StartTimestamp",
			originFieldName: "StartTimeUnixNano",
			protoID:         2,
			returnType:      timestampType,
		},
		&TypedField{
			fieldName:       "Timestamp",
			originFieldName: "TimeUnixNano",
			protoID:         3,
			returnType:      timestampType,
		},
		&PrimitiveField{
			fieldName: "Sketch",
			protoID:   8,
			protoType: proto.TypeBytes,
		},
		&TypedField{
			fieldName:  "Encoding",
			protoID:    10,
			returnType: sumAggEncodingType,
		},
		&SliceField{
			fieldName:   "Exemplars",
			protoID:     11,
			protoType:   proto.TypeMessage,
			returnSlice: exemplarSlice,
		},
		&TypedField{
			fieldName: "Flags",
			protoID:   15,
			returnType: &TypedType{
				structName: "DataPointFlags",
				protoType:  proto.TypeUint32,
				defaultVal: "0",
				testVal:    "1",
			},
		},
	},
}

var kllsketchDataPointSlice = &messageSlice{
	structName:      "KLLSketchDataPointSlice",
	elementNullable: true,
	element:         kllsketchDataPoint,
}

var kllsketchDataPoint = &messageStruct{
	structName:    "KLLSketchDataPoint",
	description:   "// KLLSketchDataPoint is a single data point that encodes a distribution using the KLL sketch format.",
	protoName:     "KLLSketchDataPoint",
	upstreamProto: "gootlpmetrics.KLLSketchDataPoint",
	fields: []Field{
		&SliceField{
			fieldName:   "Attributes",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&TypedField{
			fieldName:       "StartTimestamp",
			originFieldName: "StartTimeUnixNano",
			protoID:         2,
			returnType:      timestampType,
		},
		&TypedField{
			fieldName:       "Timestamp",
			originFieldName: "TimeUnixNano",
			protoID:         3,
			returnType:      timestampType,
		},
		&PrimitiveField{
			fieldName: "Sketch",
			protoID:   8,
			protoType: proto.TypeBytes,
		},
		&TypedField{
			fieldName:  "Encoding",
			protoID:    9,
			returnType: kllsketchEncodingType,
		},
		&TypedField{
			fieldName: "Flags",
			protoID:   10,
			returnType: &TypedType{
				structName: "DataPointFlags",
				protoType:  proto.TypeUint32,
				defaultVal: "0",
				testVal:    "1",
			},
		},
		&PrimitiveField{
			fieldName: "SeriesID",
			protoID:   11,
			protoType: proto.TypeUint64,
		},
	},
}

var hllsketchDataPointSlice = &messageSlice{
	structName:      "HLLSketchDataPointSlice",
	elementNullable: true,
	element:         hllsketchDataPoint,
}

var hllsketchDataPoint = &messageStruct{
	structName:    "HLLSketchDataPoint",
	description:   "// HLLSketchDataPoint is a single data point that encodes cardinality estimations using HyperLogLog.",
	protoName:     "HLLSketchDataPoint",
	upstreamProto: "gootlpmetrics.HLLSketchDataPoint",
	fields: []Field{
		&SliceField{
			fieldName:   "Attributes",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&TypedField{
			fieldName:       "StartTimestamp",
			originFieldName: "StartTimeUnixNano",
			protoID:         2,
			returnType:      timestampType,
		},
		&TypedField{
			fieldName:       "Timestamp",
			originFieldName: "TimeUnixNano",
			protoID:         3,
			returnType:      timestampType,
		},
		&PrimitiveField{
			fieldName: "Sketch",
			protoID:   6,
			protoType: proto.TypeBytes,
		},
		&TypedField{
			fieldName:  "Encoding",
			protoID:    7,
			returnType: hllsketchEncodingType,
		},
		&TypedField{
			fieldName: "Flags",
			protoID:   9,
			returnType: &TypedType{
				structName: "DataPointFlags",
				protoType:  proto.TypeUint32,
				defaultVal: "0",
				testVal:    "1",
			},
		},
		&PrimitiveField{
			fieldName: "SeriesID",
			protoID:   10,
			protoType: proto.TypeUint64,
		},
	},
}

var countsketchDataPointSlice = &messageSlice{
	structName:      "CountSketchDataPointSlice",
	elementNullable: true,
	element:         countsketchDataPoint,
}

var countsketchDataPoint = &messageStruct{
	structName:    "CountSketchDataPoint",
	description:   "// CountSketchDataPoint is a single data point that encodes frequency estimations using CountSketch.",
	protoName:     "CountSketchDataPoint",
	upstreamProto: "gootlpmetrics.CountSketchDataPoint",
	fields: []Field{
		&SliceField{
			fieldName:   "Attributes",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&TypedField{
			fieldName:       "StartTimestamp",
			originFieldName: "StartTimeUnixNano",
			protoID:         2,
			returnType:      timestampType,
		},
		&TypedField{
			fieldName:       "Timestamp",
			originFieldName: "TimeUnixNano",
			protoID:         3,
			returnType:      timestampType,
		},
		&PrimitiveField{
			fieldName: "Sketch",
			protoID:   4,
			protoType: proto.TypeBytes,
		},
		&TypedField{
			fieldName:  "Encoding",
			protoID:    5,
			returnType: countsketchEncodingType,
		},
		&TypedField{
			fieldName: "Flags",
			protoID:   9,
			returnType: &TypedType{
				structName: "DataPointFlags",
				protoType:  proto.TypeUint32,
				defaultVal: "0",
				testVal:    "1",
			},
		},
		&PrimitiveField{
			fieldName: "SeriesID",
			protoID:   10,
			protoType: proto.TypeUint64,
		},
	},
}

var countminsketchDataPointSlice = &messageSlice{
	structName:      "CountMinSketchDataPointSlice",
	elementNullable: true,
	element:         countminsketchDataPoint,
}

var countminsketchDataPoint = &messageStruct{
	structName:    "CountMinSketchDataPoint",
	description:   "// CountMinSketchDataPoint is a single data point that encodes frequency estimations using Count-Min Sketch.",
	protoName:     "CountMinSketchDataPoint",
	upstreamProto: "gootlpmetrics.CountMinSketchDataPoint",
	fields: []Field{
		&SliceField{
			fieldName:   "Attributes",
			protoID:     1,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&TypedField{
			fieldName:       "StartTimestamp",
			originFieldName: "StartTimeUnixNano",
			protoID:         2,
			returnType:      timestampType,
		},
		&TypedField{
			fieldName:       "Timestamp",
			originFieldName: "TimeUnixNano",
			protoID:         3,
			returnType:      timestampType,
		},
		&PrimitiveField{
			fieldName: "Sketch",
			protoID:   5,
			protoType: proto.TypeBytes,
		},
		&TypedField{
			fieldName:  "Encoding",
			protoID:    6,
			returnType: countminsketchEncodingType,
		},
		&TypedField{
			fieldName: "Flags",
			protoID:   9,
			returnType: &TypedType{
				structName: "DataPointFlags",
				protoType:  proto.TypeUint32,
				defaultVal: "0",
				testVal:    "1",
			},
		},
		&PrimitiveField{
			fieldName: "SeriesID",
			protoID:   10,
			protoType: proto.TypeUint64,
		},
	},
}

var summaryDataPointSlice = &messageSlice{
	structName:      "SummaryDataPointSlice",
	elementNullable: true,
	element:         summaryDataPoint,
}

var summaryDataPoint = &messageStruct{
	structName:    "SummaryDataPoint",
	description:   "// SummaryDataPoint is a single data point in a timeseries that describes the time-varying values of a Summary of double values.",
	protoName:     "SummaryDataPoint",
	upstreamProto: "gootlpmetrics.SummaryDataPoint",
	fields: []Field{
		&SliceField{
			fieldName:   "Attributes",
			protoID:     7,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&PrimitiveField{
			fieldName: "SeriesID",
			protoID:   9,
			protoType: proto.TypeUint64,
		},
		&TypedField{
			fieldName:       "StartTimestamp",
			originFieldName: "StartTimeUnixNano",
			protoID:         2,
			returnType:      timestampType,
		},
		&TypedField{
			fieldName:       "Timestamp",
			originFieldName: "TimeUnixNano",
			protoID:         3,
			returnType:      timestampType,
		},
		&PrimitiveField{
			fieldName: "Count",
			protoID:   4,
			protoType: proto.TypeFixed64,
		},
		&PrimitiveField{
			fieldName: "Sum",
			protoID:   5,
			protoType: proto.TypeDouble,
		},
		&SliceField{
			fieldName:   "QuantileValues",
			protoID:     6,
			protoType:   proto.TypeMessage,
			returnSlice: quantileValuesSlice,
		},
		&TypedField{
			fieldName: "Flags",
			protoID:   8,
			returnType: &TypedType{
				structName: "DataPointFlags",
				protoType:  proto.TypeUint32,
				defaultVal: "0",
				testVal:    "1",
			},
		},
	},
}

var quantileValuesSlice = &messageSlice{
	structName:      "SummaryDataPointValueAtQuantileSlice",
	elementNullable: true,
	element:         quantileValues,
}

var quantileValues = &messageStruct{
	structName:    "SummaryDataPointValueAtQuantile",
	description:   "// SummaryDataPointValueAtQuantile is a quantile value within a Summary data point.",
	protoName:     "SummaryDataPointValueAtQuantile",
	upstreamProto: "gootlpmetrics.SummaryDataPoint_ValueAtQuantile",
	fields: []Field{
		&PrimitiveField{
			fieldName: "Quantile",
			protoID:   1,
			protoType: proto.TypeDouble,
		},
		&PrimitiveField{
			fieldName: "Value",
			protoID:   2,
			protoType: proto.TypeDouble,
		},
	},
}

var exemplarSlice = &messageSlice{
	structName:      "ExemplarSlice",
	elementNullable: false,
	element:         exemplar,
}

var exemplar = &messageStruct{
	structName: "Exemplar",
	description: "// Exemplar is a sample input double measurement.\n//\n" +
		"// Exemplars also hold information about the environment when the measurement was recorded,\n" +
		"// for example the span and trace ID of the active span when the exemplar was recorded.",
	protoName:     "Exemplar",
	upstreamProto: "gootlpmetrics.Exemplar",
	fields: []Field{
		&SliceField{
			fieldName:   "FilteredAttributes",
			protoID:     7,
			protoType:   proto.TypeMessage,
			returnSlice: mapStruct,
		},
		&TypedField{
			fieldName:       "Timestamp",
			originFieldName: "TimeUnixNano",
			protoID:         2,
			returnType:      timestampType,
		},
		&OneOfField{
			typeName:        "ExemplarValueType",
			originFieldName: "Value",
			testValueIdx:    1, // Int
			values: []oneOfValue{
				&OneOfPrimitiveValue{
					fieldName:       "Double",
					originFieldName: "AsDouble",
					protoID:         3,
					protoType:       proto.TypeDouble,
				},
				&OneOfPrimitiveValue{
					fieldName:       "Int",
					originFieldName: "AsInt",
					protoID:         6,
					protoType:       proto.TypeSFixed64,
				},
			},
		},
		&TypedField{
			fieldName:       "TraceID",
			originFieldName: "TraceId",
			protoID:         5,
			returnType:      traceIDType,
		},
		&TypedField{
			fieldName:       "SpanID",
			originFieldName: "SpanId",
			protoID:         4,
			returnType:      spanIDType,
		},
	},
}

var aggregationTemporalityType = &TypedType{
	structName:  "AggregationTemporality",
	protoType:   proto.TypeEnum,
	messageName: "AggregationTemporality",
	defaultVal:  "AggregationTemporality(0)",
	testVal:     "AggregationTemporality(1)",
}

var aggregationTemporalityEnum = &proto.Enum{
	Name:        "AggregationTemporality",
	Description: "// AggregationTemporality defines how a metric aggregator reports aggregated values.\n// It describes how those values relate to the time interval over which they are aggregated.",
	Fields: []*proto.EnumField{
		{Name: "AGGREGATION_TEMPORALITY_UNSPECIFIED", Value: 0},
		{Name: "AGGREGATION_TEMPORALITY_DELTA", Value: 1},
		{Name: "AGGREGATION_TEMPORALITY_CUMULATIVE", Value: 2},
	},
}

var ddsketchEncodingType = &TypedType{
	structName:  "DDSketchEncoding",
	protoType:   proto.TypeEnum,
	messageName: "DDSketchEncoding",
	defaultVal:  "DDSketchEncoding(0)",
	testVal:     "DDSketchEncoding(1)",
}

var ddsketchEncodingEnum = &proto.Enum{
	Name:        "DDSketchEncoding",
	Description: "// DDSketchEncoding identifies how the DDSketch payload bytes are encoded.",
	Fields: []*proto.EnumField{
		{Name: "DDSKETCH_ENCODING_UNSPECIFIED", Value: 0},
		{Name: "DDSKETCH_ENCODING_PROTO", Value: 1},
		{Name: "DDSKETCH_ENCODING_PROTO_DELTA", Value: 2},
		{Name: "DDSKETCH_ENCODING_MSGPACK", Value: 3},
		{Name: "DDSKETCH_ENCODING_MSGPACK_DELTA", Value: 4},
	},
}

var sumAggEncodingType = &TypedType{
	structName:  "SumAggEncoding",
	protoType:   proto.TypeEnum,
	messageName: "SumAggEncoding",
	defaultVal:  "SumAggEncoding(0)",
	testVal:     "SumAggEncoding(1)",
}

var sumAggEncodingEnum = &proto.Enum{
	Name:        "SumAggEncoding",
	Description: "// SumAggEncoding identifies how the SumAgg payload bytes are encoded.",
	Fields: []*proto.EnumField{
		{Name: "SUM_AGG_ENCODING_UNSPECIFIED", Value: 0},
		{Name: "SUM_AGG_ENCODING_PROTO", Value: 1},
		{Name: "SUM_AGG_ENCODING_PROTO_DELTA", Value: 2},
		{Name: "SUM_AGG_ENCODING_MSGPACK", Value: 3},
		{Name: "SUM_AGG_ENCODING_MSGPACK_DELTA", Value: 4},
	},
}

var kllsketchEncodingType = &TypedType{
	structName:  "KLLSketchEncoding",
	protoType:   proto.TypeEnum,
	messageName: "KLLSketchEncoding",
	defaultVal:  "KLLSketchEncoding(0)",
	testVal:     "KLLSketchEncoding(1)",
}

var kllsketchEncodingEnum = &proto.Enum{
	Name:        "KLLSketchEncoding",
	Description: "// KLLSketchEncoding identifies how the KLL sketch payload bytes are encoded.",
	Fields: []*proto.EnumField{
		{Name: "KLL_SKETCH_ENCODING_UNSPECIFIED", Value: 0},
		{Name: "KLL_SKETCH_ENCODING_PROTO", Value: 1},
		{Name: "KLL_SKETCH_ENCODING_MSGPACK", Value: 3},
		{Name: "KLL_SKETCH_ENCODING_MSGPACK_DELTA", Value: 4},
	},
}

var hllsketchEncodingType = &TypedType{
	structName:  "HLLSketchEncoding",
	protoType:   proto.TypeEnum,
	messageName: "HLLSketchEncoding",
	defaultVal:  "HLLSketchEncoding(0)",
	testVal:     "HLLSketchEncoding(1)",
}

var hllsketchEncodingEnum = &proto.Enum{
	Name:        "HLLSketchEncoding",
	Description: "// HLLSketchEncoding identifies how the HLL sketch payload bytes are encoded.",
	Fields: []*proto.EnumField{
		{Name: "HLL_SKETCH_ENCODING_UNSPECIFIED", Value: 0},
		{Name: "HLL_SKETCH_ENCODING_PROTO", Value: 1},
		{Name: "HLL_SKETCH_ENCODING_DELTA", Value: 2},
		{Name: "HLL_SKETCH_ENCODING_MSGPACK", Value: 3},
		{Name: "HLL_SKETCH_ENCODING_MSGPACK_DELTA", Value: 4},
	},
}

var countsketchEncodingType = &TypedType{
	structName:  "CountSketchEncoding",
	protoType:   proto.TypeEnum,
	messageName: "CountSketchEncoding",
	defaultVal:  "CountSketchEncoding(0)",
	testVal:     "CountSketchEncoding(1)",
}

var countsketchEncodingEnum = &proto.Enum{
	Name:        "CountSketchEncoding",
	Description: "// CountSketchEncoding identifies how the CountSketch payload bytes are encoded.",
	Fields: []*proto.EnumField{
		{Name: "COUNT_SKETCH_ENCODING_UNSPECIFIED", Value: 0},
		{Name: "COUNT_SKETCH_ENCODING_PROTO", Value: 1},
		{Name: "COUNT_SKETCH_ENCODING_DELTA", Value: 2},
		{Name: "COUNT_SKETCH_ENCODING_MSGPACK", Value: 3},
		{Name: "COUNT_SKETCH_ENCODING_MSGPACK_DELTA", Value: 4},
	},
}

var countminsketchEncodingType = &TypedType{
	structName:  "CountMinSketchEncoding",
	protoType:   proto.TypeEnum,
	messageName: "CountMinSketchEncoding",
	defaultVal:  "CountMinSketchEncoding(0)",
	testVal:     "CountMinSketchEncoding(1)",
}

var countminsketchEncodingEnum = &proto.Enum{
	Name:        "CountMinSketchEncoding",
	Description: "// CountMinSketchEncoding identifies how the CountMinSketch payload bytes are encoded.",
	Fields: []*proto.EnumField{
		{Name: "COUNT_MIN_SKETCH_ENCODING_UNSPECIFIED", Value: 0},
		{Name: "COUNT_MIN_SKETCH_ENCODING_PROTO", Value: 1},
		{Name: "COUNT_MIN_SKETCH_ENCODING_DELTA", Value: 2},
		{Name: "COUNT_MIN_SKETCH_ENCODING_MSGPACK", Value: 3},
		{Name: "COUNT_MIN_SKETCH_ENCODING_MSGPACK_DELTA", Value: 4},
	},
}
