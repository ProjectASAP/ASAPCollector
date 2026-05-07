//! Phase B (MVP v6) — turn a typed L5 [`StageConfig`] map (produced by
//! [`crate::stage_split::ThreeStageEmitter`]) into the **wire bytes** the
//! three executors actually consume:
//!
//! - [`emit_edge_yaml`] → OTel-collector YAML for the edge agent (OTLP
//!   receiver → per-sketch processor(s) → OTLP exporter to gateway).
//! - [`emit_gateway_yaml`] → OTel-collector YAML for the gateway
//!   aggregator (OTLP receiver → per-family `*merge` processor(s) → OTLP
//!   exporter to backend).
//! - [`emit_backend_config_json`] → JSON document matching the
//!   ASAPQuery-backend `POST /api/v1/streaming-config` API surface — same
//!   shape that [`crate::config::asapquery_backend::generate_streaming_config_yaml`]
//!   builds today, just from the typed [`BackendStageConfig`] instead of
//!   a `CollectionPlan`.
//! - [`emit_backend_storage_routing`] → JSON document matching the
//!   ASAPQuery-backend `POST /api/v1/storage_routing` API surface —
//!   per-metric query-shape → engine routing table (Phase α). Sources
//!   the per-metric sketch families from the typed [`BackendStageConfig`]
//!   inputs and turns them into `(metric, [target])` rows the backend's
//!   HTTP query handler consults via `BackendStorageRouting::lookup_with_shape`.
//!
//! These four functions are deliberately **stage-shaped**, not
//! plan-shaped: the typed L5 emitter has already split the SketchExpr
//! across edge / gateway / backend, so each function only sees the slice
//! that's relevant to its executor. The legacy emitters in
//! [`crate::config::agent`] / [`crate::config::backend`] still operate
//! on the legacy `AgentCollectorConfig` / `BackendCollectorConfig` —
//! Phase C will gate-flip the demo overlay onto these typed emitters.
//!
//! All three are pure transformations: no I/O, no env lookup. The
//! `opamp_endpoint` parameter is the controller's WebSocket URL the
//! emitted YAML's `extensions.opamp` block must point at; the caller
//! threads it through from `AppState::opamp_endpoint`.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value as JsonValue};
use serde_yaml::{Mapping, Value};
use std::collections::HashMap;

use crate::sketch_algebra::params::{SketchKind, SketchParams};
use crate::sketch_algebra::sketch_expr::EstimateOp;
use crate::stage_split::emitter::{
    BackendAggregation, BackendReadout, BackendStageConfig, EdgeSketchProcessor, EdgeStageConfig,
    ExportTarget, GatewayMergeProcessor, GatewayStageConfig,
};
use crate::stage_split::stage_id::StageId;

// ── YAML structural types ─────────────────────────────────────────────────────
//
// These mirror the structural types in `config::agent`. We keep a
// private copy here rather than re-exporting because the L5 typed path
// has slightly different shape constraints (e.g. no `series_id_ttl` on
// the receiver block — that's a wire-layer concern Phase G+ owns).

#[derive(Serialize)]
struct CollectorYaml {
    extensions: HashMap<String, Value>,
    receivers: HashMap<String, Value>,
    processors: HashMap<String, Value>,
    exporters: HashMap<String, Value>,
    service: ServiceSection,
}

#[derive(Serialize)]
struct ServiceSection {
    extensions: Vec<String>,
    pipelines: HashMap<String, Pipeline>,
}

#[derive(Serialize)]
struct Pipeline {
    receivers: Vec<String>,
    processors: Vec<String>,
    exporters: Vec<String>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Build the OTel-collector YAML for an edge agent from the typed L5
/// [`EdgeStageConfig`] payload.
///
/// The `opamp_endpoint` is embedded under `extensions.opamp.server.ws.endpoint`
/// so the agent can receive runtime config updates without restart.
///
/// The emitter does NOT resolve `ExportTarget::Stage(_)` to a concrete
/// network address; Phase C plumbs a `DeploymentConstraints` resolver
/// that maps the symbolic stage role to e.g. `gateway:4317`. Until
/// then, we emit a documented placeholder (`gateway:4317`) so the YAML
/// is syntactically valid and round-trips through Otel's loader for
/// integration tests.
pub fn emit_edge_yaml(cfg: &EdgeStageConfig, opamp_endpoint: &str) -> Result<String> {
    // ── Receivers ─────────────────────────────────────────────────────────────
    // Edge agents accept OTLP gRPC on 4317 + HTTP on 4318. Phase B does
    // not yet plumb an alternate port through `EdgeStageConfig`; if/when
    // that field is added, swap the literal here for a `cfg.otlp_port`
    // read.
    let otlp_receiver: Value = serde_yaml::from_str(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:4317\"\n  http:\n    endpoint: \"0.0.0.0:4318\"\n",
    )
    .context("parse static OTLP receiver block")?;

    // ── Processors ────────────────────────────────────────────────────────────
    // One processor per `EdgeSketchProcessor`. Names come straight from
    // `EdgeSketchProcessor::processor_name` (already resolved by
    // `emitter::edge_processor_name`) and the param block is built from
    // the typed `SketchParams` payload.
    let mut processors: HashMap<String, Value> = HashMap::new();
    let mut pipeline_processors: Vec<String> = Vec::new();
    for sp in &cfg.sketch_processors {
        let block = build_edge_processor_block(sp, cfg.window_secs, &cfg.label_filters);
        // Use the processor_name verbatim as the YAML key — matches the
        // factory `Type` strings the patched OTel-contrib build registers
        // (see `opentelemetry-collector-contrib-patch/processor/*processor/factory.go`).
        processors.insert(sp.processor_name.clone(), block);
        pipeline_processors.push(sp.processor_name.clone());
    }

    // ── Exporters ─────────────────────────────────────────────────────────────
    // Edge always exports to the gateway. ExportTarget gets resolved to
    // a concrete endpoint here (Phase B): symbolic stages map to
    // documented hostnames the demo overlay (Phase C) will provision.
    let (exporter_key, exporter_val) = build_otlp_exporter("gateway", &cfg.exporter_target);

    // ── OpAMP extension ───────────────────────────────────────────────────────
    let opamp_ext: Value = serde_yaml::from_str(&format!(
        "server:\n  ws:\n    endpoint: \"{opamp_endpoint}\"\n"
    ))
    .context("parse opamp extension block")?;

    // ── Top-level YAML ────────────────────────────────────────────────────────
    let doc = CollectorYaml {
        extensions: [("opamp".to_string(), opamp_ext)].into(),
        receivers: [("otlp".to_string(), otlp_receiver)].into(),
        processors,
        exporters: [(exporter_key.clone(), exporter_val)].into(),
        service: ServiceSection {
            extensions: vec!["opamp".into()],
            pipelines: [(
                "metrics".to_string(),
                Pipeline {
                    receivers: vec!["otlp".into()],
                    processors: pipeline_processors,
                    exporters: vec![exporter_key],
                },
            )]
            .into(),
        },
    };

    serde_yaml::to_string(&doc).context("serialize edge stage config")
}

/// Build the OTel-collector YAML for a gateway aggregator from the
/// typed L5 [`GatewayStageConfig`] payload.
///
/// The gateway runs one `<sketch_kind>merge` processor per
/// `GatewayMergeProcessor` entry — these are the patched merge
/// processors in `opentelemetry-collector-contrib-patch/processor/`.
pub fn emit_gateway_yaml(cfg: &GatewayStageConfig, opamp_endpoint: &str) -> Result<String> {
    // Receiver — port from cfg, both gRPC + HTTP.
    let port = cfg.otlp_receiver_port;
    let otlp_receiver: Value = serde_yaml::from_str(&format!(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:{port}\"\n    max_recv_msg_size_mib: 64\n  http:\n    endpoint: \"0.0.0.0:{}\"\n",
        port + 1,
    ))
    .context("parse gateway OTLP receiver block")?;

    // Processors — one merge processor per merge entry. Naming
    // convention matches the patched contrib build:
    //   * SketchKind::DDSketch    → `ddsketchmerge`
    //   * SketchKind::Kll         → `kllmerge`
    //   * SketchKind::Hll         → `hllmerge`
    //   * SketchKind::Cms         → `countminmerge`
    //   * SketchKind::CountSketch → `countsketchmerge`
    //
    // We honour `GatewayMergeProcessor::processor_name` if non-empty
    // (the typed emitter today populates it as `"sketchmergeprocessor"`
    // — a placeholder until Phase C flips factory names per-family),
    // otherwise we derive the family-specific name from `sketch_kind`.
    let mut processors: HashMap<String, Value> = HashMap::new();
    let mut pipeline_processors: Vec<String> = Vec::new();
    for mp in &cfg.merge_processors {
        let key = gateway_merge_processor_name(mp);
        let block = build_gateway_merge_block(mp);
        processors.insert(key.clone(), block);
        pipeline_processors.push(key);
    }

    // Exporter — backend OTLP.
    let (exporter_key, exporter_val) = build_otlp_exporter("backend", &cfg.exporter_target);

    let opamp_ext: Value = serde_yaml::from_str(&format!(
        "server:\n  ws:\n    endpoint: \"{opamp_endpoint}\"\n"
    ))
    .context("parse opamp extension block")?;

    let doc = CollectorYaml {
        extensions: [("opamp".to_string(), opamp_ext)].into(),
        receivers: [("otlp".to_string(), otlp_receiver)].into(),
        processors,
        exporters: [(exporter_key.clone(), exporter_val)].into(),
        service: ServiceSection {
            extensions: vec!["opamp".into()],
            pipelines: [(
                "metrics".to_string(),
                Pipeline {
                    receivers: vec!["otlp".into()],
                    processors: pipeline_processors,
                    exporters: vec![exporter_key],
                },
            )]
            .into(),
        },
    };

    serde_yaml::to_string(&doc).context("serialize gateway stage config")
}

/// Build the JSON document the ASAPQuery-backend's
/// `POST /api/v1/streaming-config` endpoint accepts, sourced from the
/// typed L5 [`BackendStageConfig`].
///
/// Output shape mirrors the YAML shape produced by
/// [`crate::config::asapquery_backend::generate_streaming_config_yaml`]:
/// a top-level `aggregations` array of
/// `{ aggregationId, aggregationType, metric, parameters, ... }` rows.
/// We additionally surface a parallel `readouts` array so the backend's
/// query engine can prepare per-readout dispatch entries up-front (the
/// existing YAML form has no readouts list because the legacy planner
/// materialises one aggregation per metric and infers readouts from the
/// PromQL query at execution time; Phase B's typed `BackendStageConfig`
/// carries the readouts explicitly, so we ship them too — backends that
/// don't recognise the field will ignore it without erroring).
pub fn emit_backend_config_json(cfg: &BackendStageConfig) -> Result<JsonValue> {
    let aggregations: Vec<JsonValue> = cfg
        .aggregations
        .iter()
        .map(build_backend_aggregation_json)
        .collect();

    let readouts: Vec<JsonValue> = cfg.readouts.iter().map(build_backend_readout_json).collect();

    Ok(json!({
        "aggregations": aggregations,
        "readouts": readouts,
    }))
}

/// Phase α (MVP): build the JSON document the ASAPQuery-backend's
/// `POST /api/v1/storage_routing` endpoint accepts, sourced from the
/// typed L5 [`BackendStageConfig`] payloads emitted by [`crate::planner::stage_split`].
///
/// `metric_plans` is the list of `(metric_name, &BackendStageConfig)`
/// pairs the controller has produced this planning cycle — one entry
/// per workload that ran through the typed L5 path. Each entry yields
/// one `metrics:` row in the emitted JSON. `default_engine` is the
/// fallback for any metric the backend's HTTP handler observes that the
/// controller did not plan for.
///
/// ## Schema
///
/// Output mirrors the existing `deploy/configs/backend-storage-routing.yaml`
/// schema (the `routes:` form), serialised as JSON:
///
/// ```json
/// {
///   "default_engine": "sketch_warm_tier",
///   "metrics": [
///     { "name": "http_requests_total",
///       "targets": [
///         { "engine": "thanos_archive",
///           "applies_to_query_shape": ["count", "topk", "rate_post_hoc",
///                                      "histogram_quantile", "delta", "absent"] },
///         { "engine": "sketch_warm_tier" }
///       ]
///     }
///   ]
/// }
/// ```
///
/// ## Classification rules (Phase α)
///
/// For each `(metric, BackendStageConfig)` we derive a target list by
/// inspecting the L4 sketch families landed at the backend:
///
/// * **DDSketch / KLL** present → warm-tier serves `quantile` shape;
///   warm-tier is the default for everything the archive doesn't claim.
/// * **HLL** present → warm-tier serves `count` shape (cardinality
///   readout). NOTE: with HLL planned, `count` does NOT route to archive
///   — the warm-tier sketch is lossier-but-cheaper than archive scan and
///   the controller already chose to spend the bandwidth on it.
/// * **Count-Sketch** present → warm-tier serves `topk` shape (the
///   sketch's whole purpose).
/// * **CountMinSketch** present → warm-tier serves `point_count` /
///   `count` shape (the CMS's `Estimate` readout).
///
/// The `thanos_archive` target is always added with the **archive-eligible
/// shape list** — those PromQL shapes that no warm-tier sketch can
/// answer at all (`histogram_quantile`, `delta`, `deriv`, `absent`,
/// post-hoc / un-planned ranges). When a sketch-eligible shape is also
/// in the archive's claim list (e.g. `count` when no HLL was planned)
/// it is added so the archive picks it up as a fallback.
///
/// Phase α is conservative: we always emit BOTH a warm-tier default
/// slot AND a thanos archive slot for every planned metric, so v7
/// dual-routing semantics are preserved by construction. Future phases
/// (β / γ) may prune the archive slot for metrics the cost model
/// prices out of cold storage.
pub fn emit_backend_storage_routing(
    metric_plans: &[(String, &BackendStageConfig)],
) -> Result<JsonValue> {
    let mut metrics_json: Vec<JsonValue> = Vec::with_capacity(metric_plans.len());
    for (metric_name, backend_cfg) in metric_plans {
        metrics_json.push(build_routing_entry(metric_name, backend_cfg));
    }
    Ok(json!({
        "default_engine": "sketch_warm_tier",
        "metrics": metrics_json,
    }))
}

// ── Internals ─────────────────────────────────────────────────────────────────

/// Build the JSON `metrics:` entry for one (metric, BackendStageConfig)
/// pair — picks per-shape targets from the L4 sketch families the plan
/// landed at the backend.
///
/// Returns a JSON object of shape:
/// ```text
/// { "name": <metric>, "targets": [<target>, ...] }
/// ```
/// where each `<target>` is either `{ "engine": <engine>, "applies_to_query_shape": [...] }`
/// or `{ "engine": <engine> }` for the default slot.
fn build_routing_entry(metric_name: &str, cfg: &BackendStageConfig) -> JsonValue {
    let kinds: Vec<SketchKind> = cfg
        .aggregations
        .iter()
        .map(|a| a.sketch_kind.clone())
        .collect();

    // Sketch-eligible shapes — the warm tier serves these natively
    // because we planned a sketch for them.
    let mut warm_shapes: Vec<&'static str> = Vec::new();
    let has_quantile_sketch = kinds
        .iter()
        .any(|k| matches!(k, SketchKind::DDSketch | SketchKind::Kll));
    if has_quantile_sketch {
        warm_shapes.push("quantile");
        warm_shapes.push("quantile_over_time");
    }
    let has_hll = kinds.iter().any(|k| matches!(k, SketchKind::Hll));
    if has_hll {
        warm_shapes.push("count");
    }
    let has_count_sketch = kinds.iter().any(|k| matches!(k, SketchKind::CountSketch));
    if has_count_sketch {
        warm_shapes.push("topk");
    }
    let has_cms = kinds.iter().any(|k| matches!(k, SketchKind::Cms));
    if has_cms {
        // CMS's `Estimate` readout serves point-count / count queries.
        // If HLL also planned, `count` is already in the list — push
        // only when not already there (keep order stable).
        if !warm_shapes.contains(&"count") {
            warm_shapes.push("count");
        }
    }
    // Sketch-planned `rate / sum / avg / min / max` over the planned
    // ranges — every sketch family the planner emits also tracks the
    // range aggregation needed to answer these from the warm tier
    // (the gateway merge processor produces a windowed accumulator).
    if !kinds.is_empty() {
        warm_shapes.push("rate");
        warm_shapes.push("sum");
        warm_shapes.push("avg");
        warm_shapes.push("min");
        warm_shapes.push("max");
    }

    // Archive-eligible shapes — Thanos / cold archive answers these
    // because no warm-tier sketch can.
    //
    // Classification rule (surprised-me bullet for the report): `topk`
    // and `count` route to archive only when NO matching sketch was
    // planned. With Count-Sketch the warm tier answers `topk` via the
    // CountSketch's heap-augmented Estimate; with HLL the warm tier
    // answers `count` via the cardinality estimate. Pruning the
    // archive's claim list is what makes Phase α a planner-driven
    // routing table rather than a static "everything goes to archive"
    // failover.
    let mut archive_shapes: Vec<&'static str> = Vec::new();
    archive_shapes.push("histogram_quantile");
    archive_shapes.push("delta");
    archive_shapes.push("deriv");
    archive_shapes.push("absent");
    archive_shapes.push("rate_post_hoc");
    if !has_count_sketch {
        archive_shapes.push("topk");
    }
    if !has_hll && !has_cms {
        archive_shapes.push("count");
    }

    // Emit the warm-tier default slot first (no filter — catches every
    // shape the archive doesn't claim), then the archive slot with the
    // explicit-shape claim list. Ordering matches the existing
    // `deploy/configs/backend-storage-routing.yaml` convention. The
    // backend's `lookup_with_shape` is two-pass: explicit-shape match
    // wins (so `count` / `topk` / etc. land on archive when listed
    // there), default slot otherwise (so `quantile` / `sum` / etc.
    // land on warm).
    //
    // We do NOT attach `applies_to_query_shape` to the warm slot —
    // attaching it would turn warm into a shape-specific target and
    // any unanticipated shape (e.g. `LastOverTime` on a metric where
    // the operator added a probe after planning) would fall through
    // to the archive's first-target fallback, which is the wrong
    // failure mode. Warm = default; archive = the specific shapes
    // archive serves better.
    let mut targets: Vec<JsonValue> = Vec::new();
    targets.push(json!({
        "engine": "sketch_warm_tier",
    }));
    if !archive_shapes.is_empty() {
        targets.push(json!({
            "engine": "thanos_archive",
            "applies_to_query_shape": archive_shapes,
        }));
    }

    // The warm-shape list is informational — surface it on a side
    // field for operators / tests to spot-check what the controller
    // decided the warm tier serves natively. The backend ignores
    // unknown fields (`#[serde(default)]` on the parser side).
    let mut entry = json!({
        "name": metric_name,
        "targets": targets,
    });
    if !warm_shapes.is_empty() {
        entry["warm_tier_native_shapes"] = json!(warm_shapes);
    }
    entry
}

/// Resolve an `ExportTarget` to a concrete `endpoint:port` string. Phase
/// B uses documented placeholder hostnames (`gateway:4317`,
/// `backend:4317`) for symbolic stages — Phase C plumbs a real
/// `DeploymentConstraints::executors()` resolver.
fn resolve_export_endpoint(default_host: &str, target: &ExportTarget) -> String {
    match target {
        ExportTarget::Endpoint(s) => s.clone(),
        ExportTarget::Stage(StageId::Edge) => "edge:4317".to_string(),
        ExportTarget::Stage(StageId::Gateway) => format!("{default_host}:4317"),
        ExportTarget::Stage(StageId::Backend) => format!("{default_host}:4317"),
    }
}

/// Build the `(component_id, yaml)` pair for an OTLP exporter pointed
/// at the supplied symbolic / concrete target. `default_host` is the
/// host portion used when the target is a symbolic stage role.
fn build_otlp_exporter(default_host: &str, target: &ExportTarget) -> (String, Value) {
    let endpoint = resolve_export_endpoint(default_host, target);
    let yaml = format!(
        "endpoint: \"{endpoint}\"\ntls:\n  insecure: true\ncompression: none\n",
    );
    (
        "otlp/backend".to_string(),
        serde_yaml::from_str(&yaml).expect("inline OTLP exporter yaml is valid"),
    )
}

/// Build the per-edge-processor parameter block. Mirrors the param
/// surface of `crate::config::agent::build_processor_block` but reads
/// from the typed `EdgeSketchProcessor` + ambient `EdgeStageConfig`
/// fields rather than the legacy `AgentCollectorConfig`.
fn build_edge_processor_block(
    sp: &EdgeSketchProcessor,
    window_secs: Option<u64>,
    label_filters: &[(String, String)],
) -> Value {
    let mut m = Mapping::new();

    // Mode — `window` whenever a window landed on edge, else `batch`.
    if let Some(w) = window_secs {
        m.insert("mode".into(), Value::String("window".to_string()));
        m.insert(
            "window_duration".into(),
            Value::String(format!("{w}s")),
        );
    } else {
        m.insert("mode".into(), Value::String("batch".to_string()));
    }
    m.insert("transmit_sketch".into(), Value::Bool(true));
    m.insert("enable_self_monitoring".into(), Value::Bool(true));

    // Label matchers — same `[{key, value}]` shape the legacy agent
    // emitter uses (Go processor expects `[]LabelMatcher{Key, Value}`).
    if !label_filters.is_empty() {
        let matchers: Vec<Value> = label_filters
            .iter()
            .map(|(k, v)| {
                let mut e = Mapping::new();
                e.insert("key".into(), Value::String(k.clone()));
                e.insert("value".into(), Value::String(v.clone()));
                Value::Mapping(e)
            })
            .collect();
        m.insert("label_matchers".into(), Value::Sequence(matchers));
    }

    // Aggregation ID — Phase B threads the typed
    // `EdgeSketchProcessor::aggregation_id` through so the backend's
    // OtlpReceiver can route the typed sketch state by id. This is a
    // new field Phase C will register on each processor's `Config`
    // struct in `opentelemetry-collector-contrib-patch/processor/`.
    m.insert(
        "aggregation_id".into(),
        Value::String(sp.aggregation_id.clone()),
    );

    // Family-specific params.
    match &sp.sketch_params {
        SketchParams::Kll(p) => {
            m.insert("k".into(), Value::Number((p.k as u64).into()));
            m.insert("encoding".into(), Value::String("msgpack".into()));
        }
        SketchParams::DDSketch(p) => {
            m.insert(
                "relative_accuracy".into(),
                Value::Number(p.alpha.into()),
            );
        }
        SketchParams::Hll(_p) => {
            // hllprocessor takes no precision knob in its Config (the
            // patched build hard-codes p=14); nothing further to set.
            m.insert("encoding".into(), Value::String("msgpack".into()));
        }
        SketchParams::Cms(p) => {
            m.insert("rows".into(), Value::Number((p.d as u64).into()));
            m.insert("columns".into(), Value::Number((p.w as u64).into()));
        }
        SketchParams::CountSketch(p) => {
            // Translate (w, d) to the legacy (epsilon, delta) surface
            // that the patched countsketch processor's Config accepts —
            // matches `crate::sketch_algebra::params::SketchParams::to_legacy`.
            let epsilon = std::f64::consts::E / (p.w as f64);
            let delta = 2f64.powi(-(p.d as i32));
            m.insert("epsilon".into(), Value::Number(epsilon.into()));
            m.insert("delta".into(), Value::Number(delta.into()));
        }
    }

    // Sketch-kind tag — defensive belt-and-braces for downstream
    // consumers that key on the kind string rather than the variant
    // tag of the params block.
    m.insert(
        "sketch_kind".into(),
        Value::String(sketch_kind_tag(&sp.sketch_kind).to_string()),
    );

    Value::Mapping(m)
}

/// Compute the gateway-side merge processor name for a `GatewayMergeProcessor`.
///
/// Today the typed emitter populates every entry's `processor_name`
/// with the placeholder `"sketchmergeprocessor"`; the patched contrib
/// build instead has per-family merge processors:
/// `kllmerge`, `ddsketchmerge`, `hllmerge`, `countminmerge`,
/// `countsketchmerge`. We map the kind to the family-specific name
/// here so the emitted YAML round-trips through the patched build.
fn gateway_merge_processor_name(mp: &GatewayMergeProcessor) -> String {
    match mp.sketch_kind {
        SketchKind::Kll => "kllmerge".to_string(),
        SketchKind::DDSketch => "ddsketchmerge".to_string(),
        SketchKind::Hll => "hllmerge".to_string(),
        SketchKind::Cms => "countminmerge".to_string(),
        SketchKind::CountSketch => "countsketchmerge".to_string(),
    }
}

/// Build the per-merge-processor parameter block for the gateway YAML.
fn build_gateway_merge_block(mp: &GatewayMergeProcessor) -> Value {
    let mut m = Mapping::new();
    m.insert("mode".into(), Value::String("merge".to_string()));
    m.insert(
        "aggregation_id".into(),
        Value::String(mp.aggregation_id.clone()),
    );
    m.insert(
        "sketch_kind".into(),
        Value::String(sketch_kind_tag(&mp.sketch_kind).to_string()),
    );
    Value::Mapping(m)
}

/// Build one aggregation row in the backend streaming-config JSON.
fn build_backend_aggregation_json(agg: &BackendAggregation) -> JsonValue {
    let parameters = sketch_params_to_json(&agg.sketch_params);
    json!({
        "aggregationId": agg.aggregation_id,
        "aggregationType": sketch_kind_to_backend_type(&agg.sketch_kind),
        "parameters": parameters,
    })
}

/// Build one readout row in the backend streaming-config JSON.
fn build_backend_readout_json(r: &BackendReadout) -> JsonValue {
    match &r.op {
        EstimateOp::Quantile { q } => json!({
            "aggregationId": r.aggregation_id,
            "op": "quantile",
            "q": q,
        }),
        EstimateOp::Cardinality => json!({
            "aggregationId": r.aggregation_id,
            "op": "cardinality",
        }),
        EstimateOp::PointCount { key } => json!({
            "aggregationId": r.aggregation_id,
            "op": "point_count",
            "key": key,
        }),
        EstimateOp::TopK { k } => json!({
            "aggregationId": r.aggregation_id,
            "op": "topk",
            "k": k,
        }),
    }
}

/// Map a `SketchKind` to the backend's `AggregationType::Display` string
/// — the same mapping
/// [`crate::config::asapquery_backend::map_sketch_type_to_agg_type`] uses
/// (the strings must match `AggregationType::FromStr` in the backend's
/// `promql_utilities::query_logics::enums`).
fn sketch_kind_to_backend_type(kind: &SketchKind) -> &'static str {
    match kind {
        SketchKind::DDSketch => "DDSketch",
        SketchKind::Kll => "DatasketchesKLL",
        SketchKind::Hll => "HLL",
        SketchKind::CountSketch => "CountSketch",
        SketchKind::Cms => "CountMinSketch",
    }
}

/// Stable lowercase tag for a `SketchKind` — used as a passthrough
/// `sketch_kind` field in YAML so downstream consumers can dispatch
/// without round-tripping through serde.
fn sketch_kind_tag(kind: &SketchKind) -> &'static str {
    match kind {
        SketchKind::Kll => "kll",
        SketchKind::DDSketch => "ddsketch",
        SketchKind::Hll => "hll",
        SketchKind::Cms => "cms",
        SketchKind::CountSketch => "count_sketch",
    }
}

/// Serialize a `SketchParams` payload to a flat JSON object the backend
/// can read directly without round-tripping through the controller's
/// internally-tagged enum form.
fn sketch_params_to_json(p: &SketchParams) -> JsonValue {
    match p {
        SketchParams::Kll(p) => json!({ "k": p.k }),
        SketchParams::DDSketch(p) => json!({ "alpha": p.alpha }),
        SketchParams::Hll(p) => json!({ "precision": p.precision }),
        SketchParams::Cms(p) => json!({ "w": p.w, "d": p.d }),
        SketchParams::CountSketch(p) => {
            json!({ "w": p.w, "d": p.d, "with_heap": p.with_heap })
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sketch_algebra::params::{
        CmsParams, CountSketchParams, DDSketchParams, HllParams, KllParams,
    };

    fn ddsketch_edge_cfg() -> EdgeStageConfig {
        EdgeStageConfig {
            source_metric: Some("http_request_duration_seconds".to_string()),
            label_filters: vec![("service".to_string(), "api".to_string())],
            window_secs: Some(60),
            sketch_processors: vec![EdgeSketchProcessor {
                processor_name: "ddsketchprocessor".to_string(),
                sketch_kind: SketchKind::DDSketch,
                sketch_params: SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
                aggregation_id: "agg0".to_string(),
            }],
            exporter_target: ExportTarget::Stage(StageId::Gateway),
        }
    }

    #[test]
    fn edge_yaml_contains_processor_and_pipeline_refs() {
        let yaml = emit_edge_yaml(&ddsketch_edge_cfg(), "ws://ctrl:4320/v1/opamp")
            .expect("emit_edge_yaml ok");

        // Receiver block.
        assert!(yaml.contains("receivers:"), "missing receivers section\n{yaml}");
        assert!(yaml.contains("otlp:"), "missing otlp receiver key\n{yaml}");
        assert!(yaml.contains("4317"), "missing gRPC port\n{yaml}");

        // Processor key + pipeline ref.
        assert!(
            yaml.contains("ddsketchprocessor:"),
            "missing ddsketchprocessor key\n{yaml}"
        );
        assert!(
            yaml.contains("- ddsketchprocessor"),
            "pipeline must reference ddsketchprocessor\n{yaml}"
        );

        // Window + label filter + aggregation id surfaced.
        assert!(yaml.contains("window_duration: 60s"), "missing window_duration\n{yaml}");
        assert!(yaml.contains("relative_accuracy"), "missing alpha\n{yaml}");
        assert!(yaml.contains("aggregation_id: agg0"), "missing aggregation id\n{yaml}");
        assert!(yaml.contains("key: service"), "missing label matcher key\n{yaml}");
        assert!(yaml.contains("value: api"), "missing label matcher value\n{yaml}");

        // Exporter — gateway.
        assert!(yaml.contains("otlp/backend:"), "missing exporter\n{yaml}");
        assert!(yaml.contains("gateway:4317"), "exporter should target gateway\n{yaml}");

        // OpAMP extension carries the controller endpoint.
        assert!(
            yaml.contains("ws://ctrl:4320/v1/opamp"),
            "missing opamp endpoint\n{yaml}"
        );
    }

    #[test]
    fn edge_yaml_kll_uses_k_param() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.sketch_processors[0] = EdgeSketchProcessor {
            processor_name: "kllprocessor".to_string(),
            sketch_kind: SketchKind::Kll,
            sketch_params: SketchParams::Kll(KllParams { k: 200 }),
            aggregation_id: "agg7".to_string(),
        };
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        assert!(yaml.contains("kllprocessor:"), "{yaml}");
        assert!(yaml.contains("k: 200"), "{yaml}");
        assert!(yaml.contains("aggregation_id: agg7"), "{yaml}");
        assert!(!yaml.contains("relative_accuracy"), "KLL must not carry alpha\n{yaml}");
    }

    #[test]
    fn edge_yaml_batch_mode_when_no_window() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.window_secs = None;
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        assert!(yaml.contains("mode: batch"), "{yaml}");
        assert!(!yaml.contains("window_duration"), "batch mode must not have window_duration\n{yaml}");
    }

    fn ddsketch_gateway_cfg() -> GatewayStageConfig {
        GatewayStageConfig {
            otlp_receiver_port: 4317,
            merge_processors: vec![GatewayMergeProcessor {
                processor_name: "sketchmergeprocessor".to_string(),
                sketch_kind: SketchKind::DDSketch,
                aggregation_id: "agg0".to_string(),
            }],
            exporter_target: ExportTarget::Stage(StageId::Backend),
        }
    }

    #[test]
    fn gateway_yaml_uses_family_specific_merge_name() {
        let yaml = emit_gateway_yaml(&ddsketch_gateway_cfg(), "ws://ctrl:4320/v1/opamp")
            .expect("emit_gateway_yaml ok");

        // Family-specific merge name (NOT the placeholder).
        assert!(yaml.contains("ddsketchmerge:"), "{yaml}");
        assert!(yaml.contains("- ddsketchmerge"), "{yaml}");
        assert!(!yaml.contains("sketchmergeprocessor"), "placeholder must be replaced\n{yaml}");

        // Receiver bound to declared port.
        assert!(yaml.contains("0.0.0.0:4317"), "{yaml}");

        // Aggregation id threaded through.
        assert!(yaml.contains("aggregation_id: agg0"), "{yaml}");

        // Exporter targets backend.
        assert!(yaml.contains("backend:4317"), "{yaml}");

        // OpAMP endpoint embedded.
        assert!(yaml.contains("ws://ctrl:4320/v1/opamp"), "{yaml}");
    }

    #[test]
    fn gateway_yaml_emits_one_processor_per_merge_entry() {
        let cfg = GatewayStageConfig {
            otlp_receiver_port: 4317,
            merge_processors: vec![
                GatewayMergeProcessor {
                    processor_name: "x".into(),
                    sketch_kind: SketchKind::Kll,
                    aggregation_id: "agg0".into(),
                },
                GatewayMergeProcessor {
                    processor_name: "x".into(),
                    sketch_kind: SketchKind::Hll,
                    aggregation_id: "agg1".into(),
                },
            ],
            exporter_target: ExportTarget::Stage(StageId::Backend),
        };
        let yaml = emit_gateway_yaml(&cfg, "ws://c/").expect("emit ok");
        assert!(yaml.contains("kllmerge:"), "{yaml}");
        assert!(yaml.contains("hllmerge:"), "{yaml}");
        assert!(yaml.contains("- kllmerge"), "pipeline missing kll merge\n{yaml}");
        assert!(yaml.contains("- hllmerge"), "pipeline missing hll merge\n{yaml}");
    }

    #[test]
    fn backend_json_round_trips_aggregations_and_readouts() {
        let cfg = BackendStageConfig {
            aggregations: vec![
                BackendAggregation {
                    aggregation_id: "agg0".into(),
                    sketch_kind: SketchKind::DDSketch,
                    sketch_params: SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
                },
                BackendAggregation {
                    aggregation_id: "agg1".into(),
                    sketch_kind: SketchKind::Hll,
                    sketch_params: SketchParams::Hll(HllParams { precision: 14 }),
                },
            ],
            readouts: vec![
                BackendReadout {
                    aggregation_id: "agg0".into(),
                    op: EstimateOp::Quantile { q: 0.99 },
                },
                BackendReadout {
                    aggregation_id: "agg1".into(),
                    op: EstimateOp::Cardinality,
                },
            ],
        };
        let v = emit_backend_config_json(&cfg).expect("emit ok");

        let aggs = v["aggregations"].as_array().expect("aggregations array");
        assert_eq!(aggs.len(), 2, "{v}");
        assert_eq!(aggs[0]["aggregationId"], "agg0");
        assert_eq!(aggs[0]["aggregationType"], "DDSketch");
        assert_eq!(aggs[0]["parameters"]["alpha"], 0.01);
        assert_eq!(aggs[1]["aggregationType"], "HLL");
        assert_eq!(aggs[1]["parameters"]["precision"], 14);

        let reads = v["readouts"].as_array().expect("readouts array");
        assert_eq!(reads.len(), 2, "{v}");
        assert_eq!(reads[0]["op"], "quantile");
        assert_eq!(reads[0]["q"], 0.99);
        assert_eq!(reads[1]["op"], "cardinality");
    }

    #[test]
    fn backend_json_handles_topk_and_pointcount_readouts() {
        let cfg = BackendStageConfig {
            aggregations: vec![
                BackendAggregation {
                    aggregation_id: "agg0".into(),
                    sketch_kind: SketchKind::CountSketch,
                    sketch_params: SketchParams::CountSketch(CountSketchParams {
                        w: 2048,
                        d: 5,
                        with_heap: true,
                    }),
                },
                BackendAggregation {
                    aggregation_id: "agg1".into(),
                    sketch_kind: SketchKind::Cms,
                    sketch_params: SketchParams::Cms(CmsParams { w: 4096, d: 4 }),
                },
            ],
            readouts: vec![
                BackendReadout {
                    aggregation_id: "agg0".into(),
                    op: EstimateOp::TopK { k: 10 },
                },
                BackendReadout {
                    aggregation_id: "agg1".into(),
                    op: EstimateOp::PointCount {
                        key: "user_42".into(),
                    },
                },
            ],
        };
        let v = emit_backend_config_json(&cfg).expect("emit ok");
        let reads = v["readouts"].as_array().unwrap();
        assert_eq!(reads[0]["op"], "topk");
        assert_eq!(reads[0]["k"], 10);
        assert_eq!(reads[1]["op"], "point_count");
        assert_eq!(reads[1]["key"], "user_42");

        let aggs = v["aggregations"].as_array().unwrap();
        assert_eq!(aggs[0]["aggregationType"], "CountSketch");
        assert_eq!(aggs[0]["parameters"]["with_heap"], true);
        assert_eq!(aggs[1]["aggregationType"], "CountMinSketch");
        assert_eq!(aggs[1]["parameters"]["w"], 4096);
    }

    #[test]
    fn export_target_endpoint_is_passed_through_verbatim() {
        let mut cfg = ddsketch_edge_cfg();
        cfg.exporter_target = ExportTarget::Endpoint("custom-gw:5317".into());
        let yaml = emit_edge_yaml(&cfg, "ws://c/").expect("emit ok");
        assert!(yaml.contains("custom-gw:5317"), "{yaml}");
    }

    // ── Phase α: BackendStorageRouting emitter tests ──────────────────────

    /// Helper: build a single-aggregation BackendStageConfig of the
    /// requested kind. `aggregation_id` is hard-coded — the routing
    /// emitter doesn't care about it.
    fn backend_cfg_with_kind(kind: SketchKind) -> BackendStageConfig {
        let params = match kind {
            SketchKind::DDSketch => SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
            SketchKind::Kll => SketchParams::Kll(KllParams { k: 200 }),
            SketchKind::Hll => SketchParams::Hll(HllParams { precision: 14 }),
            SketchKind::Cms => SketchParams::Cms(CmsParams { w: 4096, d: 4 }),
            SketchKind::CountSketch => SketchParams::CountSketch(CountSketchParams {
                w: 2048,
                d: 5,
                with_heap: true,
            }),
        };
        BackendStageConfig {
            aggregations: vec![BackendAggregation {
                aggregation_id: "agg0".into(),
                sketch_kind: kind.clone(),
                sketch_params: params,
            }],
            readouts: vec![BackendReadout {
                aggregation_id: "agg0".into(),
                op: match kind {
                    SketchKind::DDSketch | SketchKind::Kll => EstimateOp::Quantile { q: 0.99 },
                    SketchKind::Hll => EstimateOp::Cardinality,
                    SketchKind::CountSketch => EstimateOp::TopK { k: 10 },
                    SketchKind::Cms => EstimateOp::PointCount {
                        key: "user_42".into(),
                    },
                },
            }],
        }
    }

    #[test]
    fn storage_routing_emits_default_engine_and_metrics_array() {
        let ddsketch = backend_cfg_with_kind(SketchKind::DDSketch);
        let plans: Vec<(String, &BackendStageConfig)> =
            vec![("http_request_duration_seconds".to_string(), &ddsketch)];
        let v = emit_backend_storage_routing(&plans).expect("emit ok");
        assert_eq!(v["default_engine"], "sketch_warm_tier");
        let metrics = v["metrics"].as_array().expect("metrics array");
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0]["name"], "http_request_duration_seconds");
    }

    #[test]
    fn storage_routing_ddsketch_warm_serves_quantile_archive_serves_others() {
        let ddsketch = backend_cfg_with_kind(SketchKind::DDSketch);
        let v = emit_backend_storage_routing(&[("latency".into(), &ddsketch)]).expect("emit ok");
        let metric = &v["metrics"][0];
        let targets = metric["targets"].as_array().expect("targets array");

        // Default slot — warm tier, no filter.
        assert_eq!(targets[0]["engine"], "sketch_warm_tier");
        assert!(
            targets[0].get("applies_to_query_shape").is_none(),
            "warm slot must be the default (no filter); got {targets:?}"
        );

        // Archive slot — must carry the predictable archive shapes.
        assert_eq!(targets[1]["engine"], "thanos_archive");
        let archive_shapes: Vec<String> = targets[1]["applies_to_query_shape"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        assert!(archive_shapes.contains(&"histogram_quantile".to_string()));
        assert!(archive_shapes.contains(&"delta".to_string()));
        assert!(archive_shapes.contains(&"absent".to_string()));
        assert!(archive_shapes.contains(&"rate_post_hoc".to_string()));
        // DDSketch planned → `topk` and `count` not warm-tier-eligible
        // (only quantile is). Both stay in archive's claim list.
        assert!(archive_shapes.contains(&"topk".to_string()));
        assert!(archive_shapes.contains(&"count".to_string()));

        // Warm-tier native shapes surfaced for spot-check.
        let warm_native: Vec<String> = metric["warm_tier_native_shapes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        assert!(warm_native.contains(&"quantile".to_string()));
        assert!(warm_native.contains(&"quantile_over_time".to_string()));
    }

    #[test]
    fn storage_routing_count_sketch_pulls_topk_off_archive() {
        let cs = backend_cfg_with_kind(SketchKind::CountSketch);
        let v = emit_backend_storage_routing(&[("requests".into(), &cs)]).expect("emit ok");
        let archive_shapes: Vec<String> = v["metrics"][0]["targets"][1]["applies_to_query_shape"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        // Count-Sketch planned → warm tier serves `topk`, archive
        // claim list must NOT include topk.
        assert!(
            !archive_shapes.contains(&"topk".to_string()),
            "Count-Sketch planned ⇒ topk must drop off the archive list; got {archive_shapes:?}"
        );
        // `count` still routes to archive (no HLL / CMS).
        assert!(archive_shapes.contains(&"count".to_string()));
    }

    #[test]
    fn storage_routing_hll_pulls_count_off_archive() {
        let hll = backend_cfg_with_kind(SketchKind::Hll);
        let v = emit_backend_storage_routing(&[("active_users".into(), &hll)]).expect("emit ok");
        let archive_shapes: Vec<String> = v["metrics"][0]["targets"][1]["applies_to_query_shape"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect();
        // HLL planned → warm tier serves `count` (cardinality);
        // archive claim list must NOT include count. `topk` still
        // routes to archive (no Count-Sketch).
        assert!(
            !archive_shapes.contains(&"count".to_string()),
            "HLL planned ⇒ count must drop off the archive list; got {archive_shapes:?}"
        );
        assert!(archive_shapes.contains(&"topk".to_string()));
    }

    #[test]
    fn storage_routing_three_metric_snapshot_stable() {
        // Snapshot test: three metrics with three different sketch
        // families. The serialized form must be deterministic across
        // runs (HashMap iteration order can drift, but our impl
        // stages everything through a Vec so order matches input
        // order).
        let ddsketch = backend_cfg_with_kind(SketchKind::DDSketch);
        let hll = backend_cfg_with_kind(SketchKind::Hll);
        let cs = backend_cfg_with_kind(SketchKind::CountSketch);
        let plans: Vec<(String, &BackendStageConfig)> = vec![
            ("http_requests_total".into(), &cs),
            ("active_users".into(), &hll),
            ("request_latency_seconds".into(), &ddsketch),
        ];
        let v = emit_backend_storage_routing(&plans).expect("emit ok");
        let s = serde_json::to_string_pretty(&v).expect("ser");

        // Pretty-print the snapshot for easy regression diffing.
        let expected = r#"{
  "default_engine": "sketch_warm_tier",
  "metrics": [
    {
      "name": "http_requests_total",
      "targets": [
        {
          "engine": "sketch_warm_tier"
        },
        {
          "applies_to_query_shape": [
            "histogram_quantile",
            "delta",
            "deriv",
            "absent",
            "rate_post_hoc",
            "count"
          ],
          "engine": "thanos_archive"
        }
      ],
      "warm_tier_native_shapes": [
        "topk",
        "rate",
        "sum",
        "avg",
        "min",
        "max"
      ]
    },
    {
      "name": "active_users",
      "targets": [
        {
          "engine": "sketch_warm_tier"
        },
        {
          "applies_to_query_shape": [
            "histogram_quantile",
            "delta",
            "deriv",
            "absent",
            "rate_post_hoc",
            "topk"
          ],
          "engine": "thanos_archive"
        }
      ],
      "warm_tier_native_shapes": [
        "count",
        "rate",
        "sum",
        "avg",
        "min",
        "max"
      ]
    },
    {
      "name": "request_latency_seconds",
      "targets": [
        {
          "engine": "sketch_warm_tier"
        },
        {
          "applies_to_query_shape": [
            "histogram_quantile",
            "delta",
            "deriv",
            "absent",
            "rate_post_hoc",
            "topk",
            "count"
          ],
          "engine": "thanos_archive"
        }
      ],
      "warm_tier_native_shapes": [
        "quantile",
        "quantile_over_time",
        "rate",
        "sum",
        "avg",
        "min",
        "max"
      ]
    }
  ]
}"#;
        assert_eq!(s, expected, "snapshot mismatch:\n{s}");
    }

    #[test]
    fn storage_routing_empty_input_emits_empty_metrics_array() {
        let v = emit_backend_storage_routing(&[]).expect("emit ok");
        assert_eq!(v["default_engine"], "sketch_warm_tier");
        assert_eq!(v["metrics"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn storage_routing_empty_aggregations_still_emits_archive_default() {
        // A plan with no aggregations (degenerate; should not happen
        // in practice but we don't want to panic). The metric still
        // lands in the table as archive-only — no warm-tier-native
        // shapes, no warm-tier annotation field.
        let cfg = BackendStageConfig {
            aggregations: vec![],
            readouts: vec![],
        };
        let v = emit_backend_storage_routing(&[("orphan".into(), &cfg)]).expect("emit ok");
        let metric = &v["metrics"][0];
        assert_eq!(metric["name"], "orphan");
        // No warm_tier_native_shapes side field.
        assert!(metric.get("warm_tier_native_shapes").is_none());
        // Targets: warm-tier default + archive default-shape list.
        let targets = metric["targets"].as_array().unwrap();
        assert_eq!(targets[0]["engine"], "sketch_warm_tier");
        assert_eq!(targets[1]["engine"], "thanos_archive");
    }

    // ── Phase β: emit_backend_config_json snapshot for new pattern coverage ──
    //
    // The new archive-only L3 intents (HistogramQuantile, Absent, Delta, …)
    // bind to `SketchExpr::Logical` rather than producing a `BackendAggregation`,
    // so they correctly stay OUT of the warm-tier StreamingConfig the
    // backend's SimpleEngine receives. Phase α wires the archive routing
    // entry separately. This snapshot pins that contract.

    /// Snapshot: an empty `BackendStageConfig` produces the canonical
    /// `{"aggregations": [], "readouts": []}` shape — what the backend
    /// receives when every intent in the workload is archive-only.
    #[test]
    fn phase_b_empty_warm_tier_snapshot_for_all_archive_only_workload() {
        let cfg = BackendStageConfig {
            aggregations: vec![],
            readouts: vec![],
        };
        let v = emit_backend_config_json(&cfg).expect("emit ok");
        let s = serde_json::to_string(&v).unwrap();
        assert_eq!(s, r#"{"aggregations":[],"readouts":[]}"#);
    }

    /// Snapshot: every Phase β warm-tier-bound intent (KLL/DDSketch
    /// quantile, HLL cardinality, CMS frequency, CountSketch topk) maps to
    /// a stable `aggregationType` string the backend's `AggregationType::
    /// FromStr` recognises. This is the contract the L4 → L5 → backend
    /// pipeline relies on; pinning it here so a sketch-kind rename can't
    /// silently break the backend.
    #[test]
    fn phase_b_backend_agg_type_strings_for_every_sketch_kind() {
        let cases = vec![
            (SketchKind::Kll, "DatasketchesKLL"),
            (SketchKind::DDSketch, "DDSketch"),
            (SketchKind::Hll, "HLL"),
            (SketchKind::Cms, "CountMinSketch"),
            (SketchKind::CountSketch, "CountSketch"),
        ];
        for (kind, expected) in cases {
            assert_eq!(
                sketch_kind_to_backend_type(&kind),
                expected,
                "sketch_kind_to_backend_type({kind:?}) drift — backend FromStr will reject"
            );
        }
    }

    /// Snapshot: aggregations + readouts together exhibit the
    /// id-aliasing the backend uses to wire readouts back to their
    /// producing aggregation. Pins the sort order + key names. Phase β
    /// uses this as the wire-format anchor for the wider intent set —
    /// the JSON shape is intent-orthogonal, so adding new intents to L3
    /// can't drift this off so long as they bind through SketchKind /
    /// SketchParams.
    #[test]
    fn phase_b_backend_json_aggregation_readout_alias_snapshot() {
        let cfg = BackendStageConfig {
            aggregations: vec![BackendAggregation {
                aggregation_id: "phase_b_agg0".into(),
                sketch_kind: SketchKind::Kll,
                sketch_params: SketchParams::Kll(KllParams { k: 200 }),
            }],
            readouts: vec![BackendReadout {
                aggregation_id: "phase_b_agg0".into(),
                op: EstimateOp::Quantile { q: 0.99 },
            }],
        };
        let v = emit_backend_config_json(&cfg).expect("emit ok");
        // The id surfaces on both the agg and the readout, with the same
        // key name — the backend looks the readout up by `aggregationId`.
        assert_eq!(v["aggregations"][0]["aggregationId"], "phase_b_agg0");
        assert_eq!(v["readouts"][0]["aggregationId"], "phase_b_agg0");
        assert_eq!(v["aggregations"][0]["aggregationType"], "DatasketchesKLL");
        assert_eq!(v["aggregations"][0]["parameters"]["k"], 200);
        assert_eq!(v["readouts"][0]["op"], "quantile");
        assert_eq!(v["readouts"][0]["q"], 0.99);
    }
}
