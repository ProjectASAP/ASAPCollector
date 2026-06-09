// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package asapedgeprocessor

import (
	"time"

	"go.opentelemetry.io/collector/component"
)

// FamilyKind selects the warm aggregator applied to a metric.
type FamilyKind string

const (
	// FamilySum groups by the metric's AggregateBy labels and sums the
	// (delta) values, emitting a Sum metric — the asap-native replacement
	// for contrib metricstransform's aggregate_labels/sum.
	FamilySum FamilyKind = "sum"
	// FamilyDDSketch / KLL produce quantile sketches.
	FamilyDDSketch FamilyKind = "ddsketch"
	FamilyKLL      FamilyKind = "kll"
	// FamilyHLL produces a cardinality sketch.
	FamilyHLL FamilyKind = "hll"
	// FamilyCountSketch / CountMinSketch produce frequency sketches.
	FamilyCountSketch    FamilyKind = "countsketch"
	FamilyCountMinSketch FamilyKind = "countminsketch"
)

// Tier selects which storage tiers a metric flows into.
type Tier string

const (
	// TierWarm builds the warm sketch/aggregation ONLY; the metric's raw
	// samples are NOT added to the cold gorilla fragment stream. Use for
	// high-cardinality sketch-only metrics whose per-series cold archive would
	// balloon the gorilla encoder.
	TierWarm Tier = "warm"
	// TierBoth builds the warm sketch/agg AND the cold gorilla archive. This is
	// the default (today's behavior) when tier is omitted.
	TierBoth Tier = "both"
	// TierCold cold-archives the raw series ONLY; no warm sketch/agg is built.
	TierCold Tier = "cold"
)

// ColdFormat selects the cold archive wire format the edge ships.
type ColdFormat string

const (
	// ColdFormatFragment ships the gorilla-XOR ASAPFRG1 fragment batch to
	// ShipEndpoint. This is the default (empty maps here) and today's behavior.
	ColdFormatFragment ColdFormat = "fragment"
	// ColdFormatIntchunk ships a lossless intchunk coldpart.Part to
	// ColdPartEndpoint (the merger's /ingest/coldpart). Opt-in.
	ColdFormatIntchunk ColdFormat = "intchunk"
)

// MetricFamily configures the warm aggregation for one metric name. A
// metric not listed here is cold-archived only (no warm aggregation).
type MetricFamily struct {
	// Metric is the metric name this entry matches (OTLP Metric.Name).
	Metric string `mapstructure:"metric"`
	// Family selects the warm aggregator.
	Family FamilyKind `mapstructure:"family"`
	// AggregateBy is the grouping label set. For Sum this is the
	// collapse key (e.g. [zone] => one summed series per zone). For the
	// sketches, empty means per-series (group by full attribute set).
	AggregateBy []string `mapstructure:"aggregate_by"`

	// Tier selects which storage tiers this metric flows into: "warm" (build
	// the warm sketch/agg only, skip the cold gorilla archive), "cold"
	// (cold-archive the raw series only, no warm sketch/agg), or "both" (warm
	// AND cold — the default when omitted, today's behavior). Marking a
	// high-cardinality sketch-only metric "warm" keeps the cold tier from
	// archiving it per-series (which balloons the gorilla encoder).
	Tier Tier `mapstructure:"tier"`

	// RelativeAccuracy is the DDSketch alpha (0,1). Default 0.01.
	RelativeAccuracy float64 `mapstructure:"relative_accuracy"`
	// K is the KLL sketch parameter (>=2). Default 200.
	K int `mapstructure:"k"`
	// Rows / Cols size the CountSketch / CountMinSketch matrix. Defaults
	// 5 x 2048 (mirror the standalone processors' typical dimensions).
	Rows int `mapstructure:"rows"`
	Cols int `mapstructure:"cols"`
	// HLL takes no sizing knob (fixed precision in the wrapper).

	// SampleP is the per-metric warm-sketch sampling probability in (0,1].
	// The control plane sets it from a metric's workload spec (only when
	// sample_p < 1). 0/unset is treated as 1.0 — sampling disabled — so the
	// emitted wire bytes are byte-identical to the pre-sampling build. A value
	// <1 thins updates to the sampling-aware families (HLL / CountMinSketch)
	// to ~p; sketchlib-go stamps p on the SketchEnvelope so the backend rescales
	// by 1/p at query time. Families without sampling support (DDSketch / KLL /
	// CountSketch) ignore it. Mirrors the standalone hll/countminsketch
	// processors' sample_p (this is the fused asap_edge equivalent).
	SampleP float64 `mapstructure:"sample_p"`

	// MaxSeries caps the per-shard sketch series cardinality for this metric
	// (plumbed into precompute.PrecomputeConfig.MaxSeries). 0 means "inherit the
	// top-level Config.MaxSeries default"; a negative-equivalent is rejected at
	// validation. A new series beyond the cap is dropped (precompute's
	// OnOverflowDrop) so a cardinality explosion can't grow the sketch series
	// map without bound. Set it to a very large value to effectively disable the
	// cap for one metric while keeping the global default.
	MaxSeries int `mapstructure:"max_series"`

	// DeltaTransmission, when true, makes the delta-capable sketch families
	// (DDSketch / CountSketch / HLL / CountMinSketch) emit PROTO_DELTA frames
	// against the previous window's cached snapshot instead of a full
	// PROTO_FULL state every window — large bandwidth savings for slowly
	// changing sketches. The first window per series still ships PROTO_FULL
	// (no prior snapshot). Defaults to the top-level Config.DeltaTransmission
	// when unset here. KLL is not delta-capable and ignores this.
	DeltaTransmission *bool `mapstructure:"delta_transmission"`
	// DeltaThreshold caps the delta size: when the computed delta is at least
	// DeltaThreshold * full-state size, the full state is emitted instead. 0
	// inherits the runtime default (always prefer delta). Unit is
	// sketch-specific (bucket counts / cells); see PrecomputeConfig.DeltaThreshold.
	DeltaThreshold uint64 `mapstructure:"delta_threshold"`

	// EmitHeap selects the heap-bearing CountSketch wire variant for a
	// `family: countsketch` metric: the emitted sketch carries a bounded
	// top-k min-heap of heavy-hitter items alongside the count matrix,
	// serialized as the MessagePack `{sketch, topk_heap, heap_size}` payload
	// the ASAPQuery backend detects as `CountSketchWithHeap`
	// (Capability::FrequencyTopk) — so a warm `topk(metric)` query routes to
	// this sketch instead of returning "No result". Implies msgpack encoding
	// (heap-bearing payloads have no proto wire form): the first window per
	// series ships a full MSGPACK heap frame, each later window a MSGPACK_DELTA
	// frame (sparse matrix delta + full heap) when delta_transmission is on.
	// Pair with ItemLabel so the heap ranks the real item dimension (e.g.
	// endpoint), not the metric name. Only valid on `family: countsketch`;
	// rejected at validation for any other family. Default false → plain
	// proto CountSketch (FrequencyEstimate), byte-unchanged from before.
	// Mirrors the standalone countsketchprocessor's emit_heap.
	EmitHeap bool `mapstructure:"emit_heap"`
	// HeapSize bounds the transmitted top-k heap when EmitHeap is true.
	// Defaults to 100 (sketchlib-go's CountSketch TOPK_SIZE) when <=0.
	// Ignored when EmitHeap is false.
	HeapSize int `mapstructure:"heap_size"`
	// ItemLabel names the data-point attribute whose VALUE is the inner
	// high-cardinality dimension a sketch counts/ranks over (e.g. "endpoint"
	// for top_endpoint_qps, "user_id" for unique_users_per_min). Consulted by:
	//
	//   * CountSketch + EmitHeap — the heap-bearing top-k path: each
	//     observation is keyed by `dpAttrs[ItemLabel]` so distinct items get
	//     distinct cells and the top-k heap ranks them (mirrors the standalone
	//     countsketchprocessor's item_label).
	//   * HLL — the cardinality of the ItemLabel dimension: the HLL hashes
	//     `dpAttrs[ItemLabel]` so it counts DISTINCT label values per group
	//     (e.g. distinct user_ids per zone) instead of one cardinality-1 HLL
	//     per value.
	//   * CountMinSketch — frequency keyed by the ItemLabel value: the CMS
	//     hashes `dpAttrs[ItemLabel]` so it estimates per-value frequency
	//     within each group instead of per full-attribute-set tuple.
	//
	// In ALL three the ItemLabel attribute is PROJECTED OUT of the series key
	// and the emitted output labels, so there is ONE sketch per grouping bucket
	// (the remaining attributes / AggregateBy) rather than one sketch per item
	// value. When empty: CountSketch+heap keys by the metric NAME (degenerate
	// single-key), while HLL/CMS keep their pre-item_label keying (HLL hashes
	// the numeric sample; CMS hashes the full attribute-set key), byte-unchanged.
	// The non-heap plain CountSketch keeps its attribute-set frequency keying
	// regardless (B6).
	ItemLabel string `mapstructure:"item_label"`

	// Threshold, when set and Enabled, turns on continuous intra-window
	// monitoring (CDM Discipline B) for this metric: the edge runs the
	// slack-countdown protocol against the coordinator and reports only when its
	// local additive value climbs past the granted slack. Optional; nil/disabled
	// adds nothing beyond a per-observation nil-check. Only additive (monotone)
	// functionals are supported.
	Threshold *ThresholdConfig `mapstructure:"threshold"`
}

// ThresholdConfig configures continuous-monitoring (CDM) for one metric family.
// τ is authoritative at the coordinator; the edge copy here is advisory.
type ThresholdConfig struct {
	// Enabled gates the monitor.
	Enabled bool `mapstructure:"enabled"`
	// Functional selects the additive readout to threshold: "sum" (default),
	// "cms_point", or "linear_buckets".
	Functional string `mapstructure:"functional"`
	// Key is the CMS point-frequency key x (functional=cms_point).
	Key string `mapstructure:"key"`
	// Coeffs are the linear-functional coefficients (functional=linear_buckets);
	// must be non-negative (monotone) or the edge rejects the spec.
	Coeffs []float64 `mapstructure:"coeffs"`
	// CoordinatorURL is the data-plane MonitorService gRPC endpoint
	// (e.g. "data-plane:4319").
	CoordinatorURL string `mapstructure:"coordinator_url"`
	// Tau / Epsilon are advisory at the edge (authoritative copies live at the
	// coordinator's streaming-config `monitors:` entry for this agg_id).
	Tau     float64 `mapstructure:"tau"`
	Epsilon float64 `mapstructure:"epsilon"`
}

// ColdConfig configures the per-shard Gorilla cold archive. Each shard
// encodes samples into compact Gorilla-XOR chunk fragments and, on flush,
// SHIPS the batch (shared ASAPFRG1 binary frame, gzipped) to a downstream
// merger/backend ingest endpoint, which builds the TSDB block + index and cuts
// the 1h window block to S3. The edge does no index build / S3 PUT. Disabled
// when Enabled is false (warm-only edge aggregation).
type ColdConfig struct {
	// Enabled turns the per-shard cold archive on. Default true.
	Enabled bool `mapstructure:"enabled"`

	// ShipEndpoint is the merger/backend /ingest URL the per-window XOR-chunk
	// fragment batch is POSTed to (e.g. http://merger:9099/ingest, or the
	// backend gorilla storage-engine ingest once the merger moves
	// server-side). This is the primary delivery path — it keeps the index
	// build + S3 PUTs off the edge. Empty => drain-only (no shipping).
	ShipEndpoint string `mapstructure:"ship_endpoint"`

	// Format selects the cold archive wire format. Default (empty or
	// ColdFormatFragment) is the gorilla-XOR ASAPFRG1 fragment batch POSTed to
	// ShipEndpoint — today's behavior, unchanged. ColdFormatIntchunk turns on
	// the parallel intchunk cold-part path: the same drained samples are
	// re-encoded as a lossless intchunk coldpart.Part and POSTed to
	// ColdPartEndpoint (the merger's /ingest/coldpart). This is opt-in; an
	// operator typically wires it from an env var, e.g.
	// `cold.format: ${env:ASAP_COLD_FORMAT}` with ASAP_COLD_FORMAT=intchunk.
	Format ColdFormat `mapstructure:"format"`
	// ColdPartEndpoint is the merger's POST /ingest/coldpart URL the serialized
	// intchunk cold part is shipped to when Format is intchunk (e.g.
	// http://merger:9099/ingest/coldpart). Required when Format is intchunk;
	// ignored for the fragment format.
	ColdPartEndpoint string `mapstructure:"coldpart_endpoint"`

	// BlockDuration is the per-emit (head) block tumbling window. Default =
	// WindowDuration. The merger/backend cuts these into the larger (e.g.
	// 1h) window block downstream.
	BlockDuration time.Duration `mapstructure:"block_duration"`
	// ReorderGrace is the per-series out-of-order lateness bound.
	ReorderGrace time.Duration `mapstructure:"reorder_grace"`
	// ExternalLabels are stamped onto every cold-archived series.
	ExternalLabels map[string]string `mapstructure:"external_labels"`

	// --- durable async ship (decouples flush from the network) ---
	// The flush goroutine hands each drained fragment batch to a background
	// ship worker over a buffered channel, so a slow/failing merger never
	// blocks the next window flush. On ship failure the encoded batch is
	// written to SpoolDir and re-shipped by a periodic retry loop (durable
	// across restarts), bounded by SpoolMaxBytes (oldest dropped when over).

	// SpoolDir is the directory the worker persists failed (gzipped ASAPFRG1)
	// batches to. Empty => default <os.TempDir()>/asap-edge-spool. The spool
	// is only used when ShipEndpoint is set (build-only mode never spools).
	SpoolDir string `mapstructure:"spool_dir"`
	// SpoolMaxBytes bounds the total spool size. When a new batch would exceed
	// it, the oldest spooled batches are dropped (logged as a permanent
	// cold-archive hole) so disk can't grow unbounded. Default 256 MiB.
	// <=0 => unbounded (not recommended).
	SpoolMaxBytes int64 `mapstructure:"spool_max_bytes"`
	// ShipQueueDepth is the buffered ship-channel depth between flushAll and
	// the worker. When full, flushAll spools the batch directly (still
	// non-blocking). Default 64.
	ShipQueueDepth int `mapstructure:"ship_queue_depth"`
	// SpoolRetryInterval is how often the worker re-ships spooled batches.
	// Default 30s.
	SpoolRetryInterval time.Duration `mapstructure:"spool_retry_interval"`

	// --- legacy S3-direct fallback (used only when ShipEndpoint is empty) ---
	// Endpoint/TSDBBucket/etc. write blocks straight to S3/MinIO from the
	// edge. Retained for back-compat; the shipping path above is preferred.
	Endpoint        string `mapstructure:"endpoint"`
	TSDBBucket      string `mapstructure:"tsdb_bucket"`
	Tenant          string `mapstructure:"tenant"`
	Region          string `mapstructure:"region"`
	AccessKeyID     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	UseSSL          bool   `mapstructure:"use_ssl"`
}

// Config is the asap_edge processor configuration.
type Config struct {
	// ShardCount is the number of key-hash ingestion shards. Each shard
	// owns its own lock + cold builder + warm aggregators, so concurrent
	// OTLP Export goroutines on distinct series ingest in parallel.
	//
	// It ALSO sets the flush staggering granularity: the flush loop ticks
	// every WindowDuration/ShardCount and flushes exactly one (phase-shifted)
	// shard per tick (see flushLoop), so a window's seal/serialize/ship work
	// splits into ShardCount small bursts instead of one. A higher count
	// flattens the per-tick CPU spike and the memory sawtooth (peak work scales
	// ~1/ShardCount) WITHOUT changing per-shard semantics — each shard still
	// flushes once per WindowDuration and the backend's per-group delta totals
	// are unchanged. Default 12 (a value that meaningfully smooths the curve
	// under the dense raw-buffer workload while staying cheap). Must be >= 1.
	ShardCount int `mapstructure:"shard_count"`

	// EdgeID is this collector instance's stable identity, reported to the CDM
	// monitor coordinator at registration (see MetricFamily.Threshold). Empty
	// disables continuous monitoring even if a metric configures a threshold.
	// Defaults to the OS hostname in the factory when left blank.
	EdgeID string `mapstructure:"edge_id"`

	// WindowDuration is the warm-tier (sum/sketch) flush cadence.
	WindowDuration time.Duration `mapstructure:"window_duration"`

	// WarmAllowedLateness is the warm-window late-data grace: a sample whose
	// event timestamp is older than the active window's aligned start by more
	// than this is dropped as late (precompute ErrLateData). It is the WARM
	// tier's own knob, DECOUPLED from cold.reorder_grace.
	//
	// Why a separate knob (P1-1): the warm window is WindowDuration wide
	// (default 60s), but cold.reorder_grace defaults to ~2s. Threading the
	// 2s cold grace into the 60s warm window dropped any sample whose
	// event-time was >2s older than the window's aligned start — i.e. most
	// processing-delayed-but-in-window samples — even though they belong in
	// the 60s window. Defaulting this to WindowDuration (set in Validate when
	// unset) makes the warm window accept anything that actually falls within
	// it. Set a smaller value to tighten warm lateness independently of cold.
	WarmAllowedLateness time.Duration `mapstructure:"warm_allowed_lateness"`

	// Metrics maps metric names to their warm aggregation family.
	Metrics []MetricFamily `mapstructure:"metrics"`

	// Cold configures the per-shard cold archive tier.
	Cold ColdConfig `mapstructure:"cold"`

	// ControlChannel optionally configures the control-plane config-poll loop.
	// DISABLED by default (zero value / unset): existing deployments are
	// unaffected. When Enabled with a PollURL, Start() spawns a goroutine that
	// polls the controller and applies received PrecomputeConfigSet updates to
	// the live sketch aggregators via Precompute.UpdateConfig — in place, with
	// NO sketch/cold state rebuild (design §8/R5).
	ControlChannel ControlChannelConfig `mapstructure:"control_channel"`

	// MaxSeries is the default per-shard sketch series-cardinality cap applied
	// to any metric that does not set its own metrics[].max_series. Bounds the
	// precompute series map so a cardinality explosion can't grow it without
	// bound. Default 100000; 0 means unlimited (not recommended).
	MaxSeries int `mapstructure:"max_series"`

	// DeltaTransmission is the default delta-transmission setting for
	// delta-capable sketch families when a metric does not set its own
	// metrics[].delta_transmission. Defaults to false (full state every
	// window — the conservative choice that matches existing behavior).
	DeltaTransmission bool `mapstructure:"delta_transmission"`

	// DropOriginal drops the raw metric from the outbound stream after
	// cold-archiving + warm-aggregating it (the asap default — the warm
	// envelopes + cold blocks carry the data downstream). Default true.
	DropOriginal bool `mapstructure:"drop_original"`
}

// ControlChannelConfig configures the optional control-plane config-poll loop
// (design §8). When Enabled is false (the default) the processor never imports
// or runs the poller, so an unconfigured deployment is byte-for-byte
// unaffected.
type ControlChannelConfig struct {
	// Enabled turns the config-poll loop on. Default false (disabled). A
	// non-empty PollURL with Enabled unset is also treated as enabled (so a
	// minimal `control_channel: {poll_url: ...}` works).
	Enabled bool `mapstructure:"enabled"`
	// PollURL is the controller GET endpoint returning a JSON-encoded
	// precompute.PrecomputeConfigSet (HttpPollChannel wire format). Required
	// when enabled.
	PollURL string `mapstructure:"poll_url"`
	// AckURL is the optional POST endpoint that receives plan-version acks.
	// Empty => acks are no-ops.
	AckURL string `mapstructure:"ack_url"`
	// PollInterval is how often the loop polls PollURL. Default 30s.
	PollInterval time.Duration `mapstructure:"poll_interval"`
	// Timeout is the per-request HTTP timeout. Default 10s.
	Timeout time.Duration `mapstructure:"timeout"`
	// BearerTokenFile, when set, is read on every request for an
	// Authorization: Bearer header (rotated without restart).
	BearerTokenFile string `mapstructure:"bearer_token_file"`
}

// enabled reports whether the control-plane poll loop should run: explicitly
// Enabled, or a PollURL given (convenience).
func (c ControlChannelConfig) enabled() bool {
	return c.Enabled || c.PollURL != ""
}

var _ component.Config = (*Config)(nil)
