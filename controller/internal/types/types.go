// Package types defines the core domain types for the DataCollector controller.
// These mirror the proto definitions but are used internally without the
// protobuf runtime dependency.
package types

import "time"

// ── Enumerations ──────────────────────────────────────────────────────────────

type AggType int

const (
	AggTypeQuantile    AggType = iota + 1
	AggTypeCardinality
	AggTypeFrequency
)

func (a AggType) String() string {
	switch a {
	case AggTypeQuantile:
		return "quantile"
	case AggTypeCardinality:
		return "cardinality"
	case AggTypeFrequency:
		return "frequency"
	default:
		return "unknown"
	}
}

type SketchType int

const (
	SketchTypeDDSketch       SketchType = iota + 1
	SketchTypeKLL
	SketchTypeHLL
	SketchTypeCountSketch
	SketchTypeCountMinSketch
)

func (s SketchType) String() string {
	switch s {
	case SketchTypeDDSketch:
		return "ddsketch"
	case SketchTypeKLL:
		return "kll"
	case SketchTypeHLL:
		return "hll"
	case SketchTypeCountSketch:
		return "countsketch"
	case SketchTypeCountMinSketch:
		return "countminsketch"
	default:
		return "unknown"
	}
}

type OutputMode int

const (
	OutputModeRaw    OutputMode = iota + 1
	OutputModeSketch OutputMode = iota
)

type ProcessorMode int

const (
	ProcessorModeBatch  ProcessorMode = iota + 1
	ProcessorModeWindow
)

func (p ProcessorMode) String() string {
	switch p {
	case ProcessorModeBatch:
		return "batch"
	case ProcessorModeWindow:
		return "window"
	default:
		return "unknown"
	}
}

// ── Core types ────────────────────────────────────────────────────────────────

// QueryWorkload describes what queries need from a given metric.
type QueryWorkload struct {
	MetricName    string
	LabelFilters  map[string]string
	GroupByLabels []string
	Aggregations  []AggType
	TimeWindow    time.Duration
	RepeatEvery   time.Duration
	AccuracySLA   float64 // relative error, e.g. 0.01 = 1%
	LatencySLA    time.Duration
}

// SketchParams holds type-specific sketch configuration.
type SketchParams struct {
	RelativeAccuracy float64
	K                int     // KLL
	Precision        int     // HLL (log2 of registers)
	Rows             int     // CountSketch / CountMinSketch
	Cols             int
	Quantiles        []float64
}

// AgentCollectorConfig is the configuration pushed to agent OTel collectors.
type AgentCollectorConfig struct {
	OutputMode     OutputMode
	SketchType     SketchType
	SketchParams   SketchParams
	AggregateBy    []string
	LabelMatchers  []string
	WindowDuration time.Duration
	Mode           ProcessorMode
	TransmitSketch bool
	DropOriginal   bool
}

// GatewayCollectorConfig configures the gateway collector (passthrough by default).
type GatewayCollectorConfig struct {
	Passthrough bool
}

// BackendCollectorConfig configures the backend collector that merges sketches.
type BackendCollectorConfig struct {
	MergeSketchType SketchType
	GroupBy         []string
}

// PrecomputeJob is a materialization job registered with ASAPQuery.
type PrecomputeJob struct {
	QueryExpr    string
	Granularity  time.Duration
	SketchSource string
	StorePath    string
}

// CollectionPlan is the full output of the planner for a given QueryWorkload.
type CollectionPlan struct {
	AgentConfig   AgentCollectorConfig
	GatewayConfig GatewayCollectorConfig
	BackendConfig BackendCollectorConfig
	Precompute    []PrecomputeJob
	ValidUntil    time.Time
}
