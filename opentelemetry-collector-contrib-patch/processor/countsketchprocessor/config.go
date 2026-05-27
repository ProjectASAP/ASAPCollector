package countsketchprocessor

import (
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/collector/component"
)

// InputMode controls when the processor flushes its CountSketch output.
// - "batch": per-batch summary flush (no background window ticker)
// - "window": tumbling window flush driven by WindowDuration.
type InputMode string

const (
	ModeBatch  InputMode = "batch"
	ModeWindow InputMode = "window"
)

// LabelMatcher specifies an exact label key=value filter.
// A data point matches only if the named label exists and its string value equals Value.
type LabelMatcher struct {
	Key   string `mapstructure:"key"`
	Value string `mapstructure:"value"`
}

type Config struct {
	// Mode controls when this processor flushes CountSketch output.
	//   - "batch": per-batch aggregation/flush
	//   - "window": accumulate across a tumbling time window before flushing
	Mode InputMode `mapstructure:"mode"`

	// Epsilon: The acceptable error rate (e.g., 0.01 for 1% error).
	// Lower epsilon = Larger sketch = More memory.
	Epsilon float64 `mapstructure:"epsilon"`

	// Delta: The probability of failure (e.g., 0.05 for 95% confidence).
	// Lower delta = More hash functions = More CPU.
	Delta float64 `mapstructure:"delta"`

	// WindowDuration is the time duration for each sketch window (e.g. "10s", "1m").
	// Used only when Mode = "window".
	WindowDuration time.Duration `mapstructure:"window_duration"`

	// TransmitSketch enables sketch-payload emission (proto-serialized CountSketch).
	// When false, only metric-form summaries are emitted.
	TransmitSketch bool `mapstructure:"transmit_sketch"`

	// DropOriginal controls whether to drop original metrics and only emit sketches.
	// When true, original metrics are not forwarded, only sketch outputs are emitted.
	DropOriginal bool `mapstructure:"drop_original"`

	// EnableSelfMonitoring controls whether processor self-monitoring metrics are emitted.
	EnableSelfMonitoring bool `mapstructure:"enable_self_monitoring"`

	// AggregateBy lists label keys to group by for cross-series (matrix) aggregation.
	// All data points sharing the same values for these labels are merged into one sketch.
	// The output data point carries only these labels.
	// Empty (default): one global sketch (all series merged).
	AggregateBy []string `mapstructure:"aggregate_by"`

	// LabelMatchers filters which data points to include before aggregation.
	// A data point is included only if ALL matchers are satisfied (exact match).
	// Empty (default) = include all data points.
	LabelMatchers []LabelMatcher `mapstructure:"label_matchers"`

	// DeltaTransmission enables sparse delta encoding: only cells that changed
	// by at least DeltaThreshold since the last snapshot are transmitted.
	// Requires TransmitSketch=true; has no effect in batch mode.
	DeltaTransmission bool `mapstructure:"delta_transmission"`

	// DeltaThreshold is the minimum absolute cell change required to include a
	// cell in the delta payload. Defaults to 1.0 when DeltaTransmission=true.
	DeltaThreshold float64 `mapstructure:"delta_threshold"`

	// Encoding selects the wire format for the `CountSketchDataPoint.Sketch`
	// bytes. See `SketchEncoding` for supported values. Defaults to "proto".
	Encoding SketchEncoding `mapstructure:"encoding"`

	// MetricName is the metric name stamped on flushed CountSketch
	// envelopes. Optional; when empty the legacy fixed name
	// "countsketch_partition" is used (back-compat). Set it to the input
	// metric this sketch represents (e.g. "top_endpoint_qps") so the
	// emitted sketch lands under the same name the backend's
	// streaming-config aggregation and PromQL queries key by — otherwise
	// the sketch registers under "countsketch_partition" and a query for
	// the real metric finds no sid. Mirrors the CMS processor's
	// `metric_name` field (which is required there; here it is optional).
	MetricName string `mapstructure:"metric_name"`

	// ItemLabel names the data-point label whose VALUE is the
	// heavy-hitter "item" the CountSketch counts/ranks (e.g.
	// "endpoint" for top_endpoint_qps). Each observation is keyed in
	// the sketch by `dpAttrs[ItemLabel]` (falling back to the resource
	// attrs, then to the metric name) so distinct items get distinct
	// cells and TopK can rank them. Optional; when empty the legacy
	// behavior is preserved: every observation is keyed by the metric
	// NAME, collapsing all items into one cell (so a top-k over the
	// metric ranks a single key). Set it to the workload's item
	// dimension (the otel-app emits `endpoint` on top_endpoint_qps) so
	// the heavy-hitter dimension is retained. Back-compat: leaving it
	// empty keeps byte-parity with the prior metric-name-keyed output.
	ItemLabel string `mapstructure:"item_label"`

	// EmitHeap selects the heap-bearing CountSketch wire variant: the
	// emitted sketch carries a bounded top-k min-heap of heavy-hitter
	// items alongside the count matrix, serialized as the MessagePack
	// `{sketch, topk_heap, heap_size}` payload the ASAPQuery backend
	// detects as `CountSketchWithHeap` (Capability::FrequencyTopk) — so
	// `topk(metric)` queries route to this sketch instead of returning
	// "No result". Implies msgpack encoding (heap-bearing payloads have
	// no proto/delta wire form). Pair with ItemLabel so the heap ranks
	// the real item dimension (e.g. endpoint), not the metric name.
	// Default false → legacy plain CountSketch (proto, FrequencyEstimate).
	EmitHeap bool `mapstructure:"emit_heap"`

	// HeapSize bounds the transmitted top-k heap when EmitHeap=true.
	// Defaults to 100 (sketchlib-go's CountSketch TOPK_SIZE) when <=0.
	HeapSize int `mapstructure:"heap_size"`
}

// SketchEncoding selects the wire format for the serialized sketch bytes
// carried in `CountSketchDataPoint.Sketch`. The corresponding
// `CountSketchDataPoint.Encoding` enum value is written alongside so the
// downstream consumer knows how to decode.
//
//   - "proto" (default) — sketchlib-go `SerializeProtoBytes`
//     (sketchlib `CountSketchState` proto). Tag =
//     `CountSketchEncodingProto` or `CountSketchEncodingDelta` depending
//     on DeltaTransmission.
//   - "msgpack"           — sketchlib-go `SerializeMsgpack`. Tag =
//     `CountSketchEncodingMsgpack`. Delta transmission is currently
//     proto-only; `encoding = msgpack` + `delta_transmission = true`
//     still falls back to proto deltas per window until sketchlib-go
//     grows a msgpack delta path.
type SketchEncoding string

const (
	EncodingProto   SketchEncoding = "proto"
	EncodingMsgpack SketchEncoding = "msgpack"
)

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	// Default to batch mode.
	switch c.Mode {
	case "":
		c.Mode = ModeBatch
	case ModeBatch, ModeWindow:
	default:
		return fmt.Errorf("invalid mode %q, must be %q or %q", c.Mode, ModeBatch, ModeWindow)
	}

	if c.Epsilon <= 0 || c.Epsilon >= 1 {
		return fmt.Errorf("epsilon must be between 0 and 1 (exclusive), got %f", c.Epsilon)
	}

	if c.Delta <= 0 || c.Delta >= 1 {
		return fmt.Errorf("delta must be between 0 and 1 (exclusive), got %f", c.Delta)
	}

	// WindowDuration validation applies only in window mode.
	if c.Mode == ModeWindow {
		if c.WindowDuration <= 0 {
			return fmt.Errorf("window_duration must be positive: %s", c.WindowDuration)
		}
		// Prevents users from setting minute values like "1ms".
		if c.WindowDuration < 1*time.Second {
			return fmt.Errorf("window_duration is too small: %s (minimum is 1s)", c.WindowDuration)
		}
	}

	// Sort AggregateBy so buildPartitionKey always produces a consistent ordering.
	sort.Strings(c.AggregateBy)

	if c.DeltaTransmission {
		if c.DeltaThreshold <= 0 {
			c.DeltaThreshold = 1.0
		}
	}

	switch c.Encoding {
	case "":
		c.Encoding = EncodingProto
	case EncodingProto, EncodingMsgpack:
	default:
		return fmt.Errorf(
			"invalid encoding %q, must be %q or %q",
			c.Encoding, EncodingProto, EncodingMsgpack)
	}

	// The heap-bearing variant uses the MessagePack wire form (the backend
	// reads the full frame via CountMinSketchWithHeap::from_msgpack and the
	// delta frame via its data_plane rmp_serde decode + matrix-delta apply),
	// so force msgpack encoding. emit_heap + delta_transmission now coexist:
	// window 1 ships a full heap frame (MSGPACK), each later window ships a
	// sparse matrix delta + full heap (MSGPACK_DELTA), reconstructed under
	// the per-window-reset model (delta-baseline-contract.md §3).
	if c.EmitHeap {
		c.Encoding = EncodingMsgpack
		if c.HeapSize <= 0 {
			c.HeapSize = 100
		}
		if !c.TransmitSketch {
			return fmt.Errorf("emit_heap requires transmit_sketch=true")
		}
	}

	return nil
}
