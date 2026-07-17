// Copyright ProjectASAP Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"sync/atomic"
	"time"

	precompute "github.com/ProjectASAP/asap-precompute-go"
	"github.com/ProjectASAP/asap-precompute-go/monitor"
	"github.com/ProjectASAP/asap-precompute-go/monitor/grpcclient"
	oteladapter "github.com/ProjectASAP/asap-precompute-go/otel"
	"github.com/ProjectASAP/asap-precompute-go/sketches"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// sketchAggregator wraps a precompute.Precompute for one sketch-family metric.
// Samples are fed via ObserveKeyed (the precompute-format key built once
// upstream from the shared decode); flush ticks the window and encodes the
// resulting envelopes back to pmetric for forwarding. Mirrors the standalone
// sketch processors (ddsketch etc.), generalized + keyed.
type sketchAggregator struct {
	pc   precompute.Precompute
	pcfg *precompute.PrecomputeConfig
	enc  *oteladapter.AdapterConfig
	// factory is the per-window sketch constructor handed to precompute.New
	// (it bakes in the per-family params + the warm-sketch sample_p). Retained
	// so the built sampling probability is observable (e.g. in tests) without
	// reaching into precompute internals.
	factory precompute.SketchFactory
	// obsKind selects how observe() shapes each ObservationValue for the wired
	// family's observer:
	//   obsKindFloat     — Kind=KindFloat, Float=value (DDSketch / KLL / HLL).
	//   obsKindBytesHash — Kind=KindBytes, Bytes=attrKey: CMS hashes the encoded
	//                      attribute set to count series frequency (its observer
	//                      requires KindBytes; a KindFloat is rejected).
	//   obsKindKeyedFreq — Kind=KindFloat, Float=1, Bytes=attrKey: CountSketch's
	//                      observer UpdateString(key, weight)s the attribute-set
	//                      key (NOT the metric name) with weight 1, so it counts
	//                      the SAME subject CMS does — per-attribute-set frequency
	//                      — instead of degenerately counting one key (the metric
	//                      name) weighted by the sample value (see B6).
	obsKind observeKind
	// itemLabel is the data-point attribute whose value is the CountSketch
	// heap key, used only by obsKindKeyedItem (the heap-bearing CountSketch
	// emit_heap path). Empty => every sample keys by the metric name (the
	// observer's DefaultKey), the degenerate single-key case.
	itemLabel string
	// weightMode selects how the obsKindKeyedItem (emit_heap) observe path
	// weights each datapoint into the heap key: topkWeightValue (default) adds
	// the datapoint VALUE (Σ value per item → top-k by total), topkWeightCount
	// adds +1 (occurrence frequency → heavy-hitter top-k). Ignored by every
	// non-heap observe path. Mirrors the backend's TopkWeight (PR #372).
	weightMode topkWeight
	logger    *zap.Logger
	// lastObserveErr is the most recent ObserveKeyed result (nil when the last
	// sample recorded cleanly). The observe error used to be discarded, which
	// hid exactly the CMS KindBytes mismatch above; it is now retained (and
	// logged once) so a value-kind regression is visible instead of silent.
	lastObserveErr   error
	loggedObserveErr bool
	// loggedEncodeErr latches the one-time flush-encode-failure log so repeated
	// failures don't spam the log; the encodeDropCount counter stays accurate.
	loggedEncodeErr bool
	// droppedSamples counts samples ObserveKeyed rejected. The log is latched
	// (loggedObserveErr), so without this counter later drops would be invisible;
	// it keeps every drop observable even after the one-time log fires.
	droppedSamples atomic.Uint64
	// procDropCount, when non-nil, is the processor-wide sketch-drop counter the
	// aggregator also bumps so all aggregators' drops roll up to one number.
	procDropCount *atomic.Uint64
	// encodeDropCount counts flush envelopes that failed to encode to pmetric
	// (oteladapter.Encode error). Without it an Encode failure dropped the
	// window's envelopes silently (P0-2); the counter keeps the loss
	// observable. Rolled up to procEncodeDropCount when set.
	encodeDropCount atomic.Uint64
	// procEncodeDropCount, when non-nil, is the processor-wide encode-drop
	// counter the aggregator also bumps so all aggregators' encode failures
	// roll up to one number.
	procEncodeDropCount *atomic.Uint64

	// monitorClient is the CDM gRPC transport for this aggregator's continuous
	// monitor, when Threshold.Enabled. nil otherwise. Held so Shutdown can stop
	// the background stream goroutine.
	monitorClient *grpcclient.Client

	// --- per-shard observe() scratch (P1-3) ---
	// One sketchAggregator exists per (shard, metric) and the shard lock
	// serializes every observe() call into it, so these scratch buffers can be
	// reused across samples without their own lock. They cut the documented
	// per-sample allocations on the hot path: the KeyValue slice, the
	// attribute-key bytes, and the obs struct.
	//
	// kvScratch is reused as the obs.Labels slice each sample (re-filled from
	// the attr map). Its backing array is reused; only a label-count growth
	// reallocates. obs.Labels is consumed synchronously by ObserveKeyed (the
	// window copies what it retains for a NEW series), so reuse is safe.
	kvScratch []precompute.KeyValue
	// attrKeyScratch is reused for the []byte form of AttributesKey (the CMS /
	// CountSketch keyed paths), avoiding a fresh []byte per sample.
	attrKeyScratch []byte
	// obsScratch is the reused Observation struct so observe() doesn't heap a
	// fresh one each sample.
	obsScratch precompute.Observation
}

// observeKind selects how observe() shapes each ObservationValue for the wired
// family's observer (see sketchAggregator.obsKind).
type observeKind uint8

const (
	// obsKindFloat: numeric value via KindFloat (DDSketch / KLL / HLL).
	obsKindFloat observeKind = iota
	// obsKindBytesHash: attribute-set key via KindBytes (CountMinSketch).
	obsKindBytesHash
	// obsKindKeyedFreq: attribute-set key + weight 1 via KindFloat (CountSketch).
	obsKindKeyedFreq
	// obsKindKeyedItem: heap-bearing CountSketch (emit_heap). Like
	// obsKindKeyedFreq (KindFloat, weight 1) but the key is the configured
	// item_label's VALUE (the heavy-hitter dimension, e.g. endpoint) rather
	// than the full attribute-set frequency key — so the producer's
	// Space-Saving tracker feeds distinct items into the top-k heap and the
	// heap ranks the real item dimension. When the item_label attribute is
	// absent on a data point (or item_label is unset), the key is left empty
	// so the observer falls back to its DefaultKey (the metric name).
	obsKindKeyedItem
	// obsKindItemHLL: HLL keyed by the configured item_label's VALUE. The HLL
	// hashes that label value (KindBytes) so the sketch measures the
	// CARDINALITY of the item_label dimension (e.g. distinct user_ids) within
	// each grouping bucket, instead of UpdateValue-ing the numeric sample. The
	// item_label is projected OUT of the series key + output labels by observe()
	// so there is ONE HLL per group, not one cardinality-1 HLL per item value.
	// Only used when item_label is set; an unset item_label keeps the numeric
	// KindFloat HLL path (obsKindFloat), byte-unchanged.
	obsKindItemHLL
	// obsKindItemCMS: CountMinSketch keyed by the configured item_label's VALUE.
	// The CMS hashes that label value (KindBytes) so frequency is counted PER
	// item_label value (e.g. per endpoint) within each grouping bucket, instead
	// of hashing the full attribute-set key (which made each {…,endpoint} tuple
	// its own series). The item_label is projected OUT of the series key +
	// output labels by observe() so there is ONE CMS per group. Only used when
	// item_label is set; an unset item_label keeps the attribute-set KindBytes
	// path (obsKindBytesHash), byte-unchanged.
	obsKindItemCMS
)

// topkWeight selects what quantity the heap-bearing CountSketch (emit_heap)
// accumulates per heap key, i.e. how the agent-built top-k heap (Mode 1, the
// modified-OTLP path) is RANKED. It mirrors the backend's TopkWeight::Value
// (default) / Count enum (PR #372) so the agent-side modified-sketch path and
// the raw-input precompute path agree on the default value-weighted semantics.
type topkWeight uint8

const (
	// topkWeightValue (DEFAULT): each datapoint adds its VALUE to the key's
	// running total, so the CountSketch matrix estimate (and therefore the
	// wire heap built from it) ranks items by Σ value — "top-k <item> by total
	// <metric>".
	topkWeightValue topkWeight = iota
	// topkWeightCount: each datapoint adds +1 (occurrence frequency), the
	// textbook heavy-hitter / frequency top-k. Opt-in.
	topkWeightCount
)

// parseTopkWeight maps the (already config_validate-normalised) weight_mode
// string to the enum. Validation collapses every accepted alias to "value" or
// "count" and rejects anything else, so the empty/default and the "value"
// strings both yield value-weighting and an unrecognised value (only reachable
// if a caller bypassed Validate) defaults to value-weighting too.
func parseTopkWeight(mode string) topkWeight {
	if mode == "count" {
		return topkWeightCount
	}
	return topkWeightValue
}

// backendAggregationType maps this edge's family config to ASAPQuery-backend's
// AggregationType PascalCase name (crates/promql_utilities/src/query_logics/
// enums.rs::AggregationType::as_str), mirroring the control plane's
// sketch_kind_to_backend_type (control_plane/src/emit/stage_config.rs). Must
// stay byte-identical to that mapping — it feeds PolicyFingerprint (see
// aggID below), and a wrong string here silently desyncs this edge's AggID
// from the backend's independently-computed one for the same policy.
func backendAggregationType(fam *MetricFamily) string {
	switch fam.Family {
	case FamilyDDSketch:
		return "DDSketch"
	case FamilyKLL:
		return "DatasketchesKLL"
	case FamilyHLL:
		return "HLL"
	case FamilyCountSketch:
		if fam.EmitHeap {
			return "CountSketchWithHeap"
		}
		return "CountSketch"
	case FamilyCountMinSketch:
		// This edge never wires a heap-bearing CMS path (see
		// newSketchAggregator's FamilyCountMinSketch case) — always the
		// plain form.
		return "CountMinSketch"
	case FamilySum:
		return "Sum"
	default:
		return string(fam.Family)
	}
}

// backendAggregationParameters mirrors sketch_params_to_json
// (control_plane/src/emit/stage_config.rs): the SAME parameter key names
// the control plane emits, so this edge's PolicyFingerprint hash input
// matches the backend's byte-for-byte. w=cols, d=rows (sketchlib/backend
// convention). Only CountSketch's JSON carries "with_heap" — CMS's does
// not, even for a heap-bearing CMS (distinguished via aggregation_type
// alone there); this edge never emits heap-bearing CMS regardless.
func backendAggregationParameters(fam *MetricFamily, rows, cols int) map[string]any {
	params := map[string]any{}
	switch fam.Family {
	case FamilyCountSketch:
		params["d"] = rows
		params["w"] = cols
		params["with_heap"] = fam.EmitHeap
	case FamilyCountMinSketch:
		params["d"] = rows
		params["w"] = cols
	case FamilyDDSketch:
		params["alpha"] = fam.RelativeAccuracy
	case FamilyKLL:
		k := fam.K
		if k < 2 {
			k = 200
		}
		params["k"] = k
	case FamilyHLL:
		// No configurable precision surfaced on this edge today (fixed
		// register width); nothing to add.
	}
	if fam.ItemLabel != "" {
		params["item_label"] = fam.ItemLabel
	}
	return params
}

// aggID computes this metric's AggID as ASAPQuery-backend's
// PolicyFingerprint (crates/asap_types/src/policy_fingerprint.rs) — the
// SAME content-addressed xxh64 hash the backend independently derives from
// its own AggregationConfig for this metric's policy. CDM (this monitor
// identity) and the sketch-DB's materialized-view identity are ONE system;
// computing AggID any other way (e.g. hashing only the metric name, as this
// used to) silently desyncs the two. window is the tumbling window
// duration; this edge always runs tumbling windows, so slide_interval ==
// window_size and window_type == "tumbling". aggregated_labels /
// rollup_labels are always empty — the control plane never sets them for
// warm-tier sketch policies today.
func aggID(metric string, fam *MetricFamily, window time.Duration, rows, cols int) precompute.AggId {
	fp := precompute.PolicyFingerprint(precompute.PolicyFingerprintInput{
		Metric:             metric,
		AggregationType:    backendAggregationType(fam),
		AggregationSubType: "",
		Parameters:         backendAggregationParameters(fam, rows, cols),
		GroupingLabels:     fam.AggregateBy,
		WindowSizeSecs:     uint64(window.Seconds()),
		SlideIntervalSecs:  uint64(window.Seconds()),
		WindowType:         "tumbling",
		SpatialFilter:      fam.SpatialFilter,
	})
	return precompute.AggId(fp)
}

// sketchOpts bundles the cross-cutting runtime settings (window length, the
// series-cardinality cap, delta-transmission, and late-data grace) the
// processor resolves once from config and threads into every per-shard sketch
// aggregator. Keeping them in one struct avoids growing newSketchAggregator's
// positional signature each time a PrecomputeConfig knob is plumbed.
type sketchOpts struct {
	window time.Duration
	// maxSeries caps the per-shard precompute series map (0 => unlimited).
	maxSeries uint64
	// delta enables PROTO_DELTA transmission for delta-capable families.
	delta bool
	// deltaThreshold caps the delta size. 0 is a valid value, NOT a sentinel:
	// it means "include every non-zero bucket in the delta", which is the
	// correct/cheapest setting under the empty-base per-window-reset (PWR)
	// contract (delta-baseline-contract.md §3) — the base is always an empty
	// sketch, so a 0 threshold never spuriously forces a full frame. A
	// non-zero value caps the delta size (full state emitted once the delta
	// reaches threshold * full-state size). No runtime default is substituted
	// for 0 (P1-4: the old "0 => runtime default" comment was misleading).
	deltaThreshold uint64
	// allowedLateness is the WARM window's own late-data grace (P1-1),
	// decoupled from Cold.ReorderGrace. precompute drops samples whose event
	// timestamp is older than activeStart-allowedLateness. The processor
	// threads Config.WarmAllowedLateness here (default = WindowDuration), so a
	// sample that actually falls within the (WindowDuration-wide) warm window
	// is admitted — unlike the old coupling to the ~2s cold reorder grace,
	// which dropped most processing-delayed-but-in-window samples.
	allowedLateness time.Duration
	// edgeID is this collector instance's stable identity, reported to the CDM
	// coordinator at registration. Empty disables monitor registration.
	edgeID string
	// subWindowInterval / subWindowEpsilon drive the threshold-driven sub-window
	// delta producer (PrecomputeConfig.SubWindowInterval/SubWindowEpsilon). 0
	// interval disables it; epsilon 0 = fixed mode.
	subWindowInterval time.Duration
	subWindowEpsilon  float64
	// gosDeltaEpsilon / gosSites configure the GOS isotropic insert-time delta
	// gate (PrecomputeConfig.GosDeltaEpsilon/GosSites). gosDeltaEpsilon 0
	// disables it (fixed DeltaThreshold path). Supported by the plain
	// CountSketch (non-heap), CountMinSketch, DDSketch, Sum, KLL, and HLL
	// factories today (HLL reinterprets gosDeltaEpsilon as τ).
	gosDeltaEpsilon float64
	gosSites        uint32
}

// parseFunctional maps the YAML functional name to the monitor enum. Unknown /
// empty defaults to Sum (the most common additive monitor).
func parseFunctional(s string) monitor.Functional {
	switch s {
	case "cms_point":
		return monitor.FunctionalCMSPoint
	case "linear_buckets":
		return monitor.FunctionalLinearBuckets
	default:
		return monitor.FunctionalSum
	}
}

// closeMonitor stops the CDM transport's background stream goroutine, if any.
func (s *sketchAggregator) closeMonitor() {
	if s.monitorClient != nil {
		s.monitorClient.Close()
		s.monitorClient = nil
	}
}

// newSketchAggregator builds the aggregator for fam, or (nil,false) if the
// family isn't wired yet. DDSketch (observes the number value) is wired;
// KLL/HLL/CS/CMS follow with their per-family params + observe-subject.
func newSketchAggregator(metric string, fam *MetricFamily, opts sketchOpts, logger *zap.Logger) (*sketchAggregator, bool) {
	if logger == nil {
		logger = zap.NewNop()
	}
	window := opts.window
	var (
		st       precompute.SketchType
		factory  precompute.SketchFactory
		observer precompute.SketchObserver
		// encoding is the per-family wire encoding stamped on PrecomputeConfig.
		// Default PROTO_FULL keeps every existing family byte-identical; only the
		// heap-bearing CountSketch (emit_heap) overrides it to MSGPACK so the
		// runtime tags its full/delta frames MSGPACK / MSGPACK_DELTA (precompute
		// serializeSeries) and the otel adapter maps those to the heap-bearing
		// pmetric encodings the backend promotes to CountSketchWithHeap.
		encoding = precompute.EncodingProtoFull
		// aggKind is the umbrella AggregationKind stamped on the config/envelope.
		// Defaults to Sketch; the FamilySum case flips it to Sum so the otel
		// adapter emits a first-class SumAgg envelope instead of a sketch metric.
		aggKind = precompute.AggKindSketch
		// obsKindOverride, when non-zero-meaningful, replaces observeKindFor for
		// this family. Used by the heap CountSketch path so each sample keys the
		// sketch by the configured item_label value (the heavy-hitter dimension)
		// rather than the attribute-set frequency key the non-heap path uses.
		obsKindOverride *observeKind
		// itemLabel is the dp-attribute whose value is the heap key (heap mode).
		itemLabel string
		// globalAgg / omitResource collapse the series grouping for the
		// heap-bearing CountSketch so every heavy-hitter item lands in ONE sketch
		// (per AggregateBy group) whose top-k heap ranks them together. Without
		// this the default per-attribute-set series grouping puts each distinct
		// item_label value in its own series → its own single-item heap, so the
		// heap could never rank items against each other. Mirrors the standalone
		// countsketchprocessor's config_translate (GlobalAggregation when
		// AggregateBy is empty; OmitResourceAttrs always). Off for every other
		// family / the non-heap CountSketch (byte-unchanged).
		globalAgg    bool
		omitResource bool
	)
	// sampleP is the warm-sketch sampling probability (1.0 = disabled). It is
	// applied via sketchlib-go's WithSampleP to the families whose geometric
	// skip actually avoids work: DDSketch (value-independent skip avoids the
	// bucket-index mapping + store increment) and CountMinSketch (skip avoids
	// the d×w cell update). WithSampleP(1.0) is an exact no-op, so the default
	// path stays byte-identical to the pre-sampling build. HLL is deliberately
	// NEVER sampled: its hash must be computed regardless (it is both the
	// admission threshold AND the register index), so sampling buys ~0 CPU while
	// degrading cardinality accuracy — it is forced to no-sampling here. KLL /
	// CountSketch have no sampling support and ignore it.
	sampleP := fam.SampleP
	if sampleP <= 0 {
		sampleP = 1.0
	}
	switch fam.Family {
	case FamilyDDSketch:
		alpha := fam.RelativeAccuracy
		st = precompute.SketchTypeDDSketch
		gosEpsilon, gosSites := opts.gosDeltaEpsilon, opts.gosSites
		factory = func() precompute.Sketch {
			w := sketches.NewDDSketchWrapper(alpha).WithSampleP(sampleP)
			// Prime GOS mode at series creation (not just at the next flush's
			// applyGosMode call) so inserts before this brand-new series' first
			// flush are already insert-time gated — same convention as the
			// plain CountSketch factory. precompute.applyGosMode re-stamps this
			// on every already-live series at flush time, so a control-plane
			// change to gos_delta_epsilon still takes effect; this priming only
			// matters for the gap between series birth and that series' first
			// flush.
			if gosEpsilon > 0 {
				w.SetGosMode(gosEpsilon, gosSites)
			}
			return w
		}
		observer = sketches.DDSketchObserver{}
	case FamilyKLL:
		k := fam.K
		if k < 2 {
			k = 200
		}
		st = precompute.SketchTypeKLLSketch
		gosEpsilon := opts.gosDeltaEpsilon
		factory = func() precompute.Sketch {
			w := sketches.NewKLLWrapper(k, nil)
			// Prime GOS mode at series creation (not just at the next flush's
			// applyGosMode call) so inserts before this brand-new series' first
			// flush are already insert-time gated — mirrors the CountSketch
			// factory priming below. precompute.applyGosMode re-stamps this on
			// every already-live series at flush time, so a control-plane
			// change to gos_delta_epsilon still takes effect.
			if gosEpsilon > 0 {
				w.SetGosMode(gosEpsilon)
			}
			return w
		}
		observer = sketches.KLLObserver{}
	case FamilyHLL:
		// HLL is never sampled: the hash is needed for both the admission
		// threshold and the register index, so sampling saves ~0 CPU while
		// degrading cardinality accuracy. Force no-sampling regardless of
		// fam.SampleP.
		st = precompute.SketchTypeHLLSketch
		// HLLSparse selects the in-memory sparse base (NewHLLWrapperSparse) so
		// low-cardinality warm series avoid the dense ~16KB/series register
		// array; default false keeps the dense base and is byte-identical on
		// the wire (the sparse base serializes to the same proto). The choice
		// is also surfaced as the documented HLL "sparse" SketchParam below so
		// the emitted PrecomputeConfig reflects which base is in use. The
		// constructor is the source of truth for the base selection; the
		// SketchParam is for config introspection only.
		//
		// GOS mode: gos_delta_epsilon is REINTERPRETED as τ for HLL (a count of
		// "doublings"; see PrecomputeConfig.GosDeltaEpsilon /
		// HLLWrapper.SetGosMode). Prime it at series creation — exactly like the
		// CountSketch factory below — so inserts before a brand-new series' first
		// flush are already register-change gated; precompute.applyGosMode
		// re-stamps it at each flush so a control-plane change still takes
		// effect. Scoped to the DENSE base only: the SPARSE base has no
		// per-register GOS path (its inserts route through sparseInsert, not the
		// dense register-change accessors), so config_validate rejects
		// gos_delta_epsilon together with hll_sparse (see config_validate.go).
		hllGosTau, hllGosSites := opts.gosDeltaEpsilon, opts.gosSites
		if fam.HLLSparse {
			factory = func() precompute.Sketch { return sketches.NewHLLWrapperSparse() }
		} else {
			factory = func() precompute.Sketch {
				w := sketches.NewHLLWrapper()
				if hllGosTau > 0 {
					w.SetGosMode(hllGosTau, hllGosSites)
				}
				return w
			}
		}
		observer = sketches.HLLObserver{}
		// item_label support: when set, hash the item_label's VALUE (the
		// high-cardinality inner dimension, e.g. user_id) so the HLL counts
		// DISTINCT label values per group rather than UpdateValue-ing the
		// numeric sample — and project the item_label out of the series key so
		// there is ONE HLL per group instead of one cardinality-1 HLL per value.
		if fam.ItemLabel != "" {
			k := obsKindItemHLL
			obsKindOverride = &k
			itemLabel = fam.ItemLabel
		}
	case FamilyCountSketch:
		rows, cols := csmDims(fam)
		st = precompute.SketchTypeCountSketch
		// The observer's DefaultKey is the metric name: it is the fallback key
		// when a sample carries no key in ObservationValue.Bytes (the heap path
		// when item_label is absent on a data point, mirroring the standalone
		// shim's sketchKey metric-name fallback).
		observer = sketches.CountSketchObserver{DefaultKey: metric}
		if fam.EmitHeap {
			// Heap-bearing variant: build the wrapper whose Snapshot() emits the
			// `{sketch, topk_heap, heap_size}` MSGPACK frame and whose
			// ComputeDeltaAgainst emits the DELTA-HEAP frame. The top-k heap is
			// fed by UpdateString (the keyed-observe path), so the observe path
			// keys each sample by the item_label value (obsKindKeyedItem) — the
			// heavy-hitter dimension the heap ranks. Encoding MSGPACK so the
			// runtime tags the emitted frames MSGPACK / MSGPACK_DELTA.
			heapSize := fam.HeapSize
			// P0-1(b): validate the dimensions ONCE up front. The wrapper
			// constructor returns (nil, err) when rows*ceil(log2(cols)) exceeds
			// the 64-bit row-hash budget; discarding that error left a
			// nil-backed wrapper that panicked on the first sample. If
			// construction fails (e.g. config_validate was bypassed), log and
			// skip wiring this family rather than returning a nil-wrapping
			// sketch.
			if _, err := sketches.NewCountSketchWithHeapWrapper(rows, cols, heapSize); err != nil {
				logger.Error("asap_edge: skipping countsketch (emit_heap) family: invalid dimensions",
					zap.String("metric", metric), zap.Int("rows", rows), zap.Int("cols", cols), zap.Error(err))
				return nil, false
			}
			factory = func() precompute.Sketch {
				w, err := sketches.NewCountSketchWithHeapWrapper(rows, cols, heapSize)
				if err != nil {
					// Unreachable in practice (validated above); return nil so
					// the runtime's nil guards skip rather than panic.
					return nil
				}
				return w
			}
			encoding = precompute.EncodingMsgpack
			k := obsKindKeyedItem
			obsKindOverride = &k
			itemLabel = fam.ItemLabel
			// Collapse series grouping so all items aggregate into one heap per
			// group: empty AggregateBy => one global sketch (the heavy-hitter
			// dimension is the item, never a series key); a set AggregateBy =>
			// one sketch per group (SeriesKey projects out the item_label).
			// Resource attrs never split the heap (mirrors the standalone shim).
			globalAgg = len(fam.AggregateBy) == 0
			omitResource = true
		} else {
			// P0-1(b): same up-front validation for the plain CountSketch.
			if _, err := sketches.NewCountSketchWrapper(rows, cols); err != nil {
				logger.Error("asap_edge: skipping countsketch family: invalid dimensions",
					zap.String("metric", metric), zap.Int("rows", rows), zap.Int("cols", cols), zap.Error(err))
				return nil, false
			}
			gosEpsilon, gosSites := opts.gosDeltaEpsilon, opts.gosSites
			factory = func() precompute.Sketch {
				w, err := sketches.NewCountSketchWrapper(rows, cols)
				if err != nil {
					return nil
				}
				// Prime GOS mode at series creation (not just at the next flush's
				// applyGosMode call) so inserts before this brand-new series' first
				// flush are already insert-time gated. precompute.applyGosMode
				// re-stamps this on every already-live series at flush time, so a
				// control-plane change to gos_delta_epsilon still takes effect —
				// this priming only matters for the gap between series birth and
				// that series' first flush.
				if gosEpsilon > 0 {
					w.SetGosMode(gosEpsilon, gosSites)
				}
				return w
			}
		}
	case FamilyCountMinSketch:
		rows, cols := csmDims(fam)
		st = precompute.SketchTypeCountMinSketch
		gosEpsilon, gosSites := opts.gosDeltaEpsilon, opts.gosSites
		factory = func() precompute.Sketch {
			w := sketches.NewCMSWrapper(rows, cols, false).WithSampleP(sampleP)
			// Prime GOS mode at series creation (not just at the next flush's
			// applyGosMode call) so inserts before this brand-new series' first
			// flush are already insert-time gated. precompute.applyGosMode
			// re-stamps this on every already-live series at flush time, so a
			// control-plane change to gos_delta_epsilon still takes effect —
			// this priming only matters for the gap between series birth and
			// that series' first flush. Mirrors the plain CountSketch factory.
			if gosEpsilon > 0 {
				w.SetGosMode(gosEpsilon, gosSites)
			}
			return w
		}
		observer = sketches.CMSObserver{}
		// item_label support: when set, key frequency by the item_label's VALUE
		// (the inner dimension, e.g. endpoint) so the CMS counts per-endpoint
		// frequency within each group — and project the item_label out of the
		// series key so there is ONE CMS per group instead of one per
		// {…,endpoint} tuple. Unset keeps the full-attribute-set frequency key.
		if fam.ItemLabel != "" {
			k := obsKindItemCMS
			obsKindOverride = &k
			itemLabel = fam.ItemLabel
		}
	case FamilySum:
		// Sum is a first-class AggregationType, NOT a sketch: build a
		// precompute-backed SumWrapper so it flushes a SumAgg envelope
		// (AggKind=Sum) through the same windowed runtime as the sketch
		// families. Per-shard partials are additive; the backend sums the
		// per-window SumAgg deltas for the same sid (ExactAgg(Sum)).
		st = precompute.SketchTypeUnspecified
		aggKind = precompute.AggKindSum
		gosEpsilon, gosSites := opts.gosDeltaEpsilon, opts.gosSites
		factory = func() precompute.Sketch {
			w := sketches.NewSumWrapper()
			// Prime GOS mode at series creation (not just at the next
			// flush's applyGosMode call) so inserts before this brand-new
			// series' first flush are already insert-time gated — mirrors
			// the CountSketch factory above. precompute.applyGosMode
			// re-stamps this on every already-live series at flush time, so
			// a control-plane change to gos_delta_epsilon still takes
			// effect; this priming only matters for the gap between series
			// birth and that series' first flush.
			if gosEpsilon > 0 {
				w.SetGosMode(gosEpsilon, gosSites)
			}
			return w
		}
		observer = sketches.SumObserver{}
	default:
		return nil, false
	}
	fmRows, fmCols := csmDims(fam)
	pcfg := &precompute.PrecomputeConfig{
		AggID:      aggID(metric, fam, window, fmRows, fmCols),
		SketchType: st,
		AggKind:    aggKind,
		Mode:       precompute.Tumbling,
		// Scope is the per-series vs whole-stream aggregation scope chosen by the
		// control plane (config `mode:`). Empty ⇒ ModePerSeries (today's
		// behavior). WholeStream collapses every matching datapoint into one
		// sketch per metric and emits one envelope per window. It composes with
		// the heap path's GlobalAggregation below: precompute's effectiveScope()
		// treats EITHER signal as whole-stream, so an emit_heap CountSketch is
		// whole-stream over its item dimension whether the operator set
		// `mode: whole_stream` or relied on the legacy collapse.
		Scope:          fam.scope(),
		Window:         precompute.WindowSpec{Size: window, AllowedLateness: opts.allowedLateness},
		AggregateBy:    fam.AggregateBy,
		TransmitSketch: true,
		// Encoding follows the family: PROTO_FULL for every family except the
		// heap-bearing CountSketch (emit_heap), which sets MSGPACK so the runtime
		// tags its frames MSGPACK / MSGPACK_DELTA and the backend promotes the
		// sid to CountSketchWithHeap (FrequencyTopk).
		Encoding:   encoding,
		MetricName: metric,
		// Heap-bearing CountSketch only: collapse series grouping so heavy-hitter
		// items aggregate into one sketch/heap per group (see globalAgg above).
		GlobalAggregation: globalAgg,
		OmitResourceAttrs: omitResource,
		Temporality:       int32(pmetric.AggregationTemporalityDelta),
		// Bound the per-shard series map so a cardinality explosion cannot grow
		// it without limit; a new series past the cap is dropped (counted via
		// Stats().DroppedOverflow).
		MaxSeries:  opts.maxSeries,
		OnOverflow: precompute.OnOverflowDrop,
		// Delta transmission: when enabled the runtime emits PROTO_DELTA frames
		// after the first PROTO_FULL snapshot. Only set for delta-capable
		// families (KLL cannot delta) — opts.delta is already gated on family by
		// config.effectiveDelta.
		DeltaTransmission: opts.delta,
		// DeltaThreshold: 0 is a real, correct value under the empty-base PWR
		// contract — NOT an unfilled sentinel (P1-4). Each window's delta is
		// computed against an EMPTY base, so a 0 threshold simply includes
		// every non-zero bucket and never spuriously promotes to a full frame;
		// the ComputeDeltaAgainst clamp still caps a delta that grows larger
		// than the equivalent full snapshot. A non-zero value caps the delta
		// at threshold * full-state size. No runtime default is substituted.
		DeltaThreshold: opts.deltaThreshold,
		// Threshold-driven sub-window producer: the flush loop's check ticker
		// fires EmitSubWindow every SubWindowInterval; SubWindowEpsilon gates
		// per-series emission by divergence (0 = fixed mode). No-op without
		// DeltaTransmission.
		SubWindowInterval: opts.subWindowInterval,
		SubWindowEpsilon:  opts.subWindowEpsilon,
		// GosDeltaEpsilon/GosSites configure the isotropic GOS insert-time
		// delta gate (CountSketch non-heap, CountMinSketch, DDSketch, Sum,
		// KLL, and HLL today). 0 leaves the fixed DeltaThreshold path
		// unchanged.
		GosDeltaEpsilon: opts.gosDeltaEpsilon,
		GosSites:        opts.gosSites,
	}
	// Surface the HLLSparse typed flag as the documented HLL "sparse"
	// SketchParams key (1 = sparse base; absent/0 = dense default) so config
	// introspection and the engine see one consistent representation. The base
	// is actually selected by the factory constructor above; this only mirrors
	// that choice onto the emitted PrecomputeConfig.
	if fam.HLLSparse {
		pcfg.SketchParams = precompute.SketchParams{"sparse": 1}
	}
	obsKind := observeKindFor(fam.Family)
	if obsKindOverride != nil {
		obsKind = *obsKindOverride
	}
	// Sketch families suffix the output metric ("_ddsketch" etc.); Sum is a
	// first-class aggregate and keeps the metric's OWN name (no suffix) so a
	// `sum(metric)` query resolves to the same name the backend registers.
	metricSuffix := "_" + string(fam.Family)
	if fam.Family == FamilySum {
		metricSuffix = ""
	}

	// Continuous monitoring (CDM Discipline B): if a threshold is configured,
	// fold the monitor spec into the config BEFORE precompute.New (which copies
	// it), then attach the engine + gRPC transport after construction.
	if fam.Threshold != nil && fam.Threshold.Enabled {
		pcfg.Monitor = monitor.Spec{
			Enabled:        true,
			Functional:     parseFunctional(fam.Threshold.Functional),
			Key:            []byte(fam.Threshold.Key),
			Coeffs:         fam.Threshold.Coeffs,
			CoordinatorURL: fam.Threshold.CoordinatorURL,
			Tau:            fam.Threshold.Tau,
			Epsilon:        fam.Threshold.Epsilon,
		}
	}
	pc := precompute.New(pcfg, factory, observer)
	var monClient *grpcclient.Client
	if pcfg.Monitor.Enabled {
		if err := pcfg.Monitor.Validate(); err != nil {
			// A non-monotone / malformed spec would void the countdown's
			// correctness — disable monitoring for this metric and log loudly.
			logger.Warn("disabling continuous monitor: invalid threshold spec",
				zap.String("metric", metric), zap.Error(err))
		} else if opts.edgeID == "" {
			logger.Warn("disabling continuous monitor: empty edge_id",
				zap.String("metric", metric))
		} else {
			windowMs := uint64(window / time.Millisecond)
			eng := monitor.NewEngine(opts.edgeID, windowMs, nil)
			monClient = grpcclient.New(pcfg.Monitor.CoordinatorURL, eng)
			eng.SetReporter(monClient)
			pc.SetMonitorEngine(eng)
			logger.Info("continuous monitor enabled",
				zap.String("metric", metric),
				zap.String("functional", fam.Threshold.Functional),
				zap.String("coordinator", pcfg.Monitor.CoordinatorURL))
		}
	}

	return &sketchAggregator{
		pc:            pc,
		pcfg:          pcfg,
		enc:           &oteladapter.AdapterConfig{MetricSuffix: metricSuffix, DropOriginal: true},
		factory:       factory,
		obsKind:       obsKind,
		itemLabel:     itemLabel,
		weightMode:    parseTopkWeight(fam.WeightMode),
		logger:        logger,
		monitorClient: monClient,
	}, true
}

// observeKindFor maps a sketch family to its observe shaping. CMS hashes the
// attribute-set key (KindBytes); CountSketch counts the attribute-set key with
// weight 1 (KindFloat + Bytes) so it counts the same subject as CMS rather than
// the degenerate metric-name single key (B6); every other family observes the
// numeric value (KindFloat).
func observeKindFor(f FamilyKind) observeKind {
	switch f {
	case FamilyCountMinSketch:
		return obsKindBytesHash
	case FamilyCountSketch:
		return obsKindKeyedFreq
	default:
		return obsKindFloat
	}
}

// csmDims returns the CountSketch/CountMinSketch matrix dimensions, with
// defaults (5 x 2048) mirroring the standalone processors.
func csmDims(fam *MetricFamily) (rows, cols int) {
	rows, cols = fam.Rows, fam.Cols
	if rows < 1 {
		rows = 5
	}
	if cols < 2 {
		cols = 2048
	}
	return rows, cols
}

func kvFromMap(am map[string]string) []precompute.KeyValue {
	out := make([]precompute.KeyValue, 0, len(am))
	for k, v := range am {
		out = append(out, precompute.KeyValue{Key: k, Value: v})
	}
	return out
}

// kvFromMapExcept is kvFromMap but omits the single attribute named `drop`. The
// HLL / CMS item_label paths use it to PROJECT the high-cardinality item_label
// (e.g. user_id / endpoint) out of the observation labels, so the item_label
// lands in neither the series key (one sketch per group, not per item value)
// nor the emitted output labels — while its VALUE is still fed into the sketch
// as the cardinality / frequency subject (see observe). When `drop` is empty
// this is exactly kvFromMap.
func kvFromMapExcept(am map[string]string, drop string) []precompute.KeyValue {
	if drop == "" {
		return kvFromMap(am)
	}
	out := make([]precompute.KeyValue, 0, len(am))
	for k, v := range am {
		if k == drop {
			continue
		}
		out = append(out, precompute.KeyValue{Key: k, Value: v})
	}
	return out
}

// fillKVScratch refills s.kvScratch from the attribute map, omitting the
// single `drop` key when non-empty. Reuses the backing array across samples
// (P1-3): only a label-count growth past the current capacity reallocates.
// Returns the filled slice (an alias of s.kvScratch).
func (s *sketchAggregator) fillKVScratch(am map[string]string, drop string) []precompute.KeyValue {
	out := s.kvScratch[:0]
	for k, v := range am {
		if drop != "" && k == drop {
			continue
		}
		out = append(out, precompute.KeyValue{Key: k, Value: v})
	}
	s.kvScratch = out
	return out
}

// attrKeyBytes returns the AttributesKey of kv as a []byte, reusing
// s.attrKeyScratch to avoid the per-sample []byte(string) allocation (P1-3).
// The returned slice aliases the scratch and is only valid until the next
// observe() call; the precompute observer copies what it needs synchronously.
func (s *sketchAggregator) attrKeyBytes(kv []precompute.KeyValue) []byte {
	key := precompute.AttributesKey(kv, nil)
	s.attrKeyScratch = append(s.attrKeyScratch[:0], key...)
	return s.attrKeyScratch
}

// observe feeds one sample. The precompute key is built once here from the
// shared decoded attrs and passed via ObserveKeyed (no internal re-key).
//
// Per-sample allocation note (P1-3): the KeyValue slice, the attribute-key
// bytes, and the Observation struct are reused across samples via per-shard
// scratch on the aggregator (the shard lock serializes observe()), so the hot
// path no longer allocates them each sample. The SeriesKeyFor key string is
// still allocated per sample — the keyed precompute entry point takes a
// string and there is no exported zero-alloc keyed path — so that one
// allocation remains.
//
// rowSampled/admittedRows/sampleP carry an SDK-side pre-decided row-admission
// bitmask (NitroSketch-style skip sampling, see AggregationRowSampledSketch
// in the SDK). When rowSampled is true, the ObservationValue built below is
// tagged so the CMS/CountSketch observer applies the bitmask verbatim via
// Sketch.ApplyAdmittedOccurrence instead of the plain insert path — see the
// precompute.ObservationValue.RowSampled doc for why this must not be
// re-derived collector-side. Every non-row-sampled caller passes
// rowSampled=false (admittedRows/sampleP ignored).
func (s *sketchAggregator) observe(am map[string]string, val float64, tsMs uint64, rowSampled bool, admittedRows uint64, sampleP float64) {
	// For the HLL / CMS item_label paths the item_label attribute is the sketch
	// subject (its value is hashed below), so it must NOT appear in the series
	// key or the emitted labels — project it out of the observation labels here.
	// Every other path keeps the full attribute set (drop == "" => all labels).
	dropLabel := ""
	if s.obsKind == obsKindItemHLL || s.obsKind == obsKindItemCMS {
		dropLabel = s.itemLabel
	}
	kv := s.fillKVScratch(am, dropLabel)
	obs := &s.obsScratch
	*obs = precompute.Observation{
		TimestampMs: tsMs,
		Labels:      kv,
		Value:       precompute.FloatValue(val),
	}
	switch s.obsKind {
	case obsKindBytesHash:
		// CountMinSketch's observer consumes KindBytes: it hashes the encoded
		// attribute key (matching the standalone countminsketchprocessor's
		// AttributesKey(labels, nil)) to count series cardinality, not the numeric
		// value. AggregateBy grouping is applied separately by SeriesKeyFor below,
		// so the inserted key is the full attribute set (nil), identical to the
		// standalone shim.
		obs.Value = precompute.BytesValue(s.attrKeyBytes(kv))
	case obsKindKeyedFreq:
		// CountSketch's observer is UpdateString(key, weight) where key defaults
		// to DefaultKey (the metric NAME) when Bytes is empty — the degenerate
		// single-key case (B6). To count the SAME subject CMS does (per
		// attribute-set frequency), supply the encoded attribute set as the key
		// (Bytes) and weight 1 (Float), so each sample increments its own
		// attribute set's frequency by one.
		obs.Value = precompute.ObservationValue{
			Kind:  precompute.KindFloat,
			Float: 1,
			Bytes: s.attrKeyBytes(kv),
		}
	case obsKindKeyedItem:
		// Heap-bearing CountSketch (emit_heap): key each sample by the configured
		// item_label's VALUE (the heavy-hitter dimension) so the producer's
		// Space-Saving tracker feeds distinct items into the top-k heap and the
		// heap ranks the real item dimension. The asap_edge observe path receives
		// the data-point attribute set (am); when item_label is unset or absent on
		// this sample, Bytes is left empty so the CountSketchObserver falls back to
		// its DefaultKey (the metric name) — the degenerate single-key case,
		// mirroring the standalone shim's sketchKey metric-name fallback.
		key := ""
		if s.itemLabel != "" {
			key = am[s.itemLabel]
		}
		// Reuse attrKeyScratch for the item key bytes (P1-3); aliases the
		// scratch, consumed synchronously by the observer.
		s.attrKeyScratch = append(s.attrKeyScratch[:0], key...)
		// weight is what the CountSketch matrix + Space-Saving tracker (and so
		// the wire heap built from the matrix estimate) accumulate per item:
		//   * value-weighted (DEFAULT): the datapoint VALUE, so the heap ranks
		//     items by Sum value (top-k <item> by total <metric>) -- matching the
		//     backend reducer's value-sum ranking and PR #372's raw-input path.
		//   * count-weighted (opt-in): +1 per event (occurrence frequency).
		// The SS tracker's Update(key, w) and CountSketch UpdateString(key, w)
		// both already honour an arbitrary weight, so no sketchlib / wire / proto
		// / backend-decode change is needed: a value-built heap round-trips
		// through the same MSGPACK frame and the backend ranks by value end-to-end.
		weight := val
		if s.weightMode == topkWeightCount {
			weight = 1
		}
		obs.Value = precompute.ObservationValue{
			Kind:  precompute.KindFloat,
			Float: weight,
			Bytes: s.attrKeyScratch,
		}
	case obsKindItemHLL:
		// HLL cardinality of the item_label dimension: hash the item_label's
		// VALUE (KindBytes) so the register set counts DISTINCT label values
		// (e.g. distinct user_ids) within the group. The item_label was already
		// projected out of obs.Labels above, so the series key/labels carry only
		// the grouping dimensions. An absent item_label value yields empty Bytes,
		// which the HLL observer treats as a no-op (no element added).
		s.attrKeyScratch = append(s.attrKeyScratch[:0], am[s.itemLabel]...)
		obs.Value = precompute.BytesValue(s.attrKeyScratch)
	case obsKindItemCMS:
		// CMS frequency keyed by the item_label dimension: hash the item_label's
		// VALUE (KindBytes) so frequency is counted PER label value (e.g. per
		// endpoint) within the group, instead of per full-attribute-set tuple.
		// The item_label was projected out of obs.Labels above.
		s.attrKeyScratch = append(s.attrKeyScratch[:0], am[s.itemLabel]...)
		obs.Value = precompute.BytesValue(s.attrKeyScratch)
	}
	if rowSampled {
		switch s.obsKind {
		case obsKindBytesHash, obsKindKeyedFreq, obsKindKeyedItem:
			obs.Value.RowSampled = true
			obs.Value.AdmittedRows = admittedRows
			obs.Value.SampleP = sampleP
		default:
			// obsKindFloat/obsKindItemHLL/obsKindItemCMS have no *AtRows
			// sketchlib primitive (DDSketch/KLL/HLL aren't row-replicated
			// matrices) — this can only happen if the SDK's AggregationRouter
			// and this collector's AggID disagreed about which family a
			// PolicyFingerprint targets. Drop rather than silently misapply
			// an unrelated observer.
			s.droppedSamples.Add(1)
			if s.procDropCount != nil {
				s.procDropCount.Add(1)
			}
			return
		}
	}
	if err := s.pc.ObserveKeyed(s.pcfg.SeriesKeyFor(obs), obs); err != nil {
		s.lastObserveErr = err
		// Always count the drop so it stays observable; the log is latched to
		// avoid spam but the counter is not (fix B8).
		s.droppedSamples.Add(1)
		if s.procDropCount != nil {
			s.procDropCount.Add(1)
		}
		if !s.loggedObserveErr {
			s.loggedObserveErr = true
			s.logger.Warn("asap_edge: sketch observe dropped sample (further drops counted, not logged)",
				zap.String("metric", s.pcfg.MetricName), zap.Error(err))
		}
		return
	}
	s.lastObserveErr = nil
}

// flush force-rotates the window (Drain, regardless of wall-clock — the
// asap_edge flush tick IS the window boundary) and appends the encoded
// sketch envelopes to dst.
func (s *sketchAggregator) flush(dst pmetric.Metrics) {
	envs := s.pc.Drain()
	if len(envs) == 0 {
		return
	}
	out, err := oteladapter.Encode(envs, s.enc)
	if err != nil {
		// P0-2: an Encode failure dropped the whole window's envelopes
		// silently. Count it (per-aggregator + rolled up) and log once so the
		// loss is observable instead of vanishing.
		s.encodeDropCount.Add(uint64(len(envs)))
		if s.procEncodeDropCount != nil {
			s.procEncodeDropCount.Add(uint64(len(envs)))
		}
		if !s.loggedEncodeErr {
			s.loggedEncodeErr = true
			s.logger.Warn("asap_edge: sketch flush encode failed (further encode drops counted, not logged)",
				zap.String("metric", s.pcfg.MetricName), zap.Int("dropped_envelopes", len(envs)), zap.Error(err))
		}
		return
	}
	if out.ResourceMetrics().Len() == 0 {
		return
	}
	out.ResourceMetrics().MoveAndAppendTo(dst.ResourceMetrics())
}

// subWindowEnabled reports whether this aggregator runs the threshold-driven
// sub-window producer: EITHER a positive SubWindowInterval (the legacy
// periodic-tick path) OR GOS insert-time detection active (which drives
// EmitSubWindow via wakeSubWindow instead of a ticker, so it needs no
// interval configured at all) — in both cases AND delta transmission on (the
// runtime no-ops EmitSubWindow without delta — there is no in-window base).
func (s *sketchAggregator) subWindowEnabled() bool {
	if !s.pcfg.DeltaTransmission {
		return false
	}
	return s.pcfg.SubWindowInterval > 0 || s.pcfg.GosDeltaEpsilon > 0
}

// emitSubWindow fires an INCREMENTAL sub-window delta emit (no rotate) for this
// aggregator's diverged series and appends the encoded pmetric to dst.
func (s *sketchAggregator) emitSubWindow(dst pmetric.Metrics, nowMs uint64) {
	if !s.subWindowEnabled() {
		return
	}
	envs := s.pc.EmitSubWindow(nowMs)
	if len(envs) == 0 {
		return
	}
	out, err := oteladapter.Encode(envs, s.enc)
	if err != nil || out.ResourceMetrics().Len() == 0 {
		return
	}
	out.ResourceMetrics().MoveAndAppendTo(dst.ResourceMetrics())
}
