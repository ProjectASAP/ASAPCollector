pub mod agent;
pub mod asapquery_backend;
pub mod backend;
pub mod precompute;
pub mod stage_config;
pub mod stage_config_otap;
pub mod stage_config_telegraf;
pub mod workloads;

pub use agent::generate_agent_config;
pub use asapquery_backend::generate_streaming_config_yaml;
pub use backend::{generate_backend_config, generate_backend_config_staged};
pub use precompute::{should_precompute, build_precompute_jobs, PrecomputeClient};
pub use stage_config::{
    emit_backend_config_json, emit_backend_storage_routing,
    emit_backend_storage_routing_for_tenant, emit_backend_storage_routing_with_prometheus,
    emit_backend_storage_routing_with_prometheus_for_tenant, emit_edge_yaml, emit_gateway_yaml,
    DEFAULT_TENANT,
};
pub use stage_config_otap::emit_otap_dag_yaml;
pub use stage_config_telegraf::emit_telegraf_toml;
pub use workloads::WorkloadRegistry;

use crate::stage_split::emitter::EdgeStageConfig;
use anyhow::Result;

/// Phase ε.1.5 — which edge runtime an agent identifies as.
///
/// Today every agent the controller has built for runs the OTel-collector
/// (`AsapOtel`); Phase ε.1.5 adds the two new runtime variants the
/// per-runtime emitters target. The runtime is reported by the agent on
/// OpAMP `on_connect` (header `X-Agent-Runtime`); when absent (legacy
/// agents) the controller defaults to `AsapOtel` so the existing
/// behaviour is preserved.
///
/// Phase ε.1.5 commits the enum + emit-dispatch function. Threading the
/// runtime through OpAMP `on_connect` and into the typed L5 emit path
/// is a follow-up — the emitters can be exercised in isolation today
/// (the Phase ε.1.5 test suite does exactly that).
///
/// Naming history: the variants were originally `Sketchcollector` /
/// `Sketchotap` / `Sketchtelegraf`; the rename to `AsapOtel` /
/// `AsapOtap` / `AsapTelegraf` (PR `refactor/rename-edge-runtimes-...`)
/// drops the v0 `sketch*` prefix in favour of the symmetric `asap-*`
/// namespace. `from_header` accepts both forms during transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRuntime {
    /// Default — OTel-collector contrib build (existing behaviour).
    AsapOtel,
    /// otap-dataflow Rust runtime.
    AsapOtap,
    /// Telegraf runtime.
    AsapTelegraf,
}

impl Default for AgentRuntime {
    fn default() -> Self {
        AgentRuntime::AsapOtel
    }
}

impl AgentRuntime {
    /// Parse an `X-Agent-Runtime` header value. Recognises
    /// `asap-otel` / `asap-otap` / `asap-telegraf`
    /// (case-insensitive); any other value (including the empty string)
    /// defaults to `AsapOtel` so legacy agents keep working.
    ///
    /// Backwards compatibility (transitional, deprecated):
    /// the v0 strings `sketchcollector` / `sketchotap` / `sketchtelegraf`
    /// are still accepted and parse to the corresponding new variant so
    /// agents pinned to an older image keep working through the rollout.
    /// TODO(remove-after-2026-Q3): drop the v0 aliases once all deployed
    /// agents have rebuilt against the new image tags.
    pub fn from_header(value: &str) -> Self {
        match value.trim().to_lowercase().as_str() {
            "asap-otap" | "otap" => AgentRuntime::AsapOtap,
            "asap-telegraf" | "telegraf" => AgentRuntime::AsapTelegraf,
            "asap-otel" => AgentRuntime::AsapOtel,
            // Deprecated v0 aliases — accepted for transition only.
            "sketchotap" => AgentRuntime::AsapOtap,
            "sketchtelegraf" => AgentRuntime::AsapTelegraf,
            "sketchcollector" => AgentRuntime::AsapOtel,
            _ => AgentRuntime::AsapOtel,
        }
    }
}

/// Phase ε.1.5 — dispatch the edge emit by agent runtime. Mirrors
/// `emit_edge_yaml`'s `(cfg, opamp_endpoint) -> String` shape; the OTAP
/// and Telegraf emitters take an additional optional Prometheus URL
/// override which we pass through `prometheus_url`.
///
/// `prometheus_url` is the Mode-3 destination override:
///   * `AsapOtel` → ignored (the OTel-collector emitter already
///     reads `${ASAP_PROMETHEUS_OTLP_URL}` at runtime);
///   * `AsapOtap`      → OTLP HTTP URL passed to `emit_otap_dag_yaml`;
///   * `AsapTelegraf`  → remote-write URL passed to `emit_telegraf_toml`.
pub fn emit_for_runtime(
    runtime: AgentRuntime,
    cfg: &EdgeStageConfig,
    opamp_endpoint: &str,
    prometheus_url: Option<&str>,
) -> Result<String> {
    match runtime {
        AgentRuntime::AsapOtel => emit_edge_yaml(cfg, opamp_endpoint),
        AgentRuntime::AsapOtap => emit_otap_dag_yaml(cfg, opamp_endpoint, prometheus_url),
        AgentRuntime::AsapTelegraf => emit_telegraf_toml(cfg, prometheus_url),
    }
}

/// 10s flush window for the freshness probes — see the comment in
/// `emit_bootstrap_typed` (and the original PR #333) for the rationale.
/// Smallest window that produces well-formed Prometheus-TSDB blocks
/// while keeping criterion ⑥'s warm-tier p50 ≤ 30s budget.
pub const FRESHNESS_PROBE_WINDOW_SECS: u64 = 10;

/// 60s window for non-probe workload-registry metrics added to the
/// archive tier so the accuracy reducer's archive engine has ground
/// truth for every replay row.
pub const WORKLOAD_ARCHIVE_WINDOW_SECS: u64 = 60;

/// The two freshness probes — bootstrap/replan demo plumbing for
/// criterion ⑥. Not user metrics. The replay client polls them via
/// `last_over_time(http_freshness_probe_warm[10s])`; without
/// warm-passthrough routing the DDSketch processor renames them to
/// `_quantile`, and without `gorillas3` archive write the warm engine
/// has nothing to look at.
pub const FRESHNESS_PROBE_METRICS: &[&str] = &[
    "http_freshness_probe_warm",
    "http_freshness_probe_archive",
];

/// Bootstrap/replan-scope plumbing: extend an Edge stage config with
/// the freshness-probe metrics (`http_freshness_probe_warm` /
/// `http_freshness_probe_archive`) AND the workload-registry archive
/// metrics so the agent's `gorillas3` processor writes them into the
/// Gorilla-S3 / Thanos archive — required for criterion ⑥
/// (freshness probe routing) and criterion ④ (archive ground truth).
///
/// Mutates `edge_cfg` in place. Idempotent — metrics already present
/// in `archive_tier_metrics` / `warm_passthrough_metrics` are not
/// duplicated.
///
/// Originally inlined in `main::emit_bootstrap_typed`; lifted here so
/// the typed-replan path in `replan::Replanner` can apply the same
/// extension without depending on private state in `main.rs`.
///
/// ## Scope note
///
/// The live planner stays free to plan per-metric without these
/// defaults bleeding into its output — the helper is only invoked
/// from the bootstrap GET path and the OpAMP-on-connect / replan
/// push paths, both of which are demo-scope contracts.
pub fn extend_edge_with_demo_plumbing(
    edge_cfg: &mut EdgeStageConfig,
    workload_registry_metrics: impl IntoIterator<Item = String>,
) {
    use crate::stage_split::emitter::ArchiveTierMetric;

    // 1. Freshness probes → archive tier with the tight 10s window.
    for m in FRESHNESS_PROBE_METRICS.iter() {
        if !edge_cfg.archive_tier_metrics.iter().any(|a| a.metric == *m) {
            edge_cfg.archive_tier_metrics.push(ArchiveTierMetric {
                metric: (*m).to_string(),
                window_secs: Some(FRESHNESS_PROBE_WINDOW_SECS),
            });
        }
    }

    // 2. Freshness probes → warm-passthrough so the DDSketch processor
    //    doesn't rename them to `_quantile`.
    for m in FRESHNESS_PROBE_METRICS.iter() {
        if !edge_cfg.warm_passthrough_metrics.iter().any(|s| s == m) {
            edge_cfg.warm_passthrough_metrics.push((*m).to_string());
        }
    }

    // 3. All non-probe workload-registry metrics → archive tier (60s).
    let mut seen: std::collections::HashSet<String> = edge_cfg
        .archive_tier_metrics
        .iter()
        .map(|a| a.metric.clone())
        .collect();
    for metric in workload_registry_metrics {
        if seen.insert(metric.clone()) {
            edge_cfg.archive_tier_metrics.push(ArchiveTierMetric {
                metric,
                window_secs: Some(WORKLOAD_ARCHIVE_WINDOW_SECS),
            });
        }
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;

    #[test]
    fn agent_runtime_from_header_recognises_three_values() {
        assert_eq!(AgentRuntime::from_header("asap-otel"), AgentRuntime::AsapOtel);
        assert_eq!(AgentRuntime::from_header("asap-otap"), AgentRuntime::AsapOtap);
        assert_eq!(AgentRuntime::from_header("asap-telegraf"), AgentRuntime::AsapTelegraf);
    }

    #[test]
    fn agent_runtime_from_header_short_aliases() {
        assert_eq!(AgentRuntime::from_header("otap"), AgentRuntime::AsapOtap);
        assert_eq!(AgentRuntime::from_header("telegraf"), AgentRuntime::AsapTelegraf);
    }

    #[test]
    fn agent_runtime_from_header_default_is_asap_otel() {
        assert_eq!(AgentRuntime::from_header(""), AgentRuntime::AsapOtel);
        assert_eq!(AgentRuntime::from_header("garbage"), AgentRuntime::AsapOtel);
    }

    #[test]
    fn agent_runtime_from_header_accepts_legacy_v0_strings() {
        // Legacy strings the controller emitted/accepted before the
        // sketchcol/sketchotap/sketchtelegraf → asap-otel/asap-otap/asap-telegraf
        // rename. Kept for transitional backwards compatibility so an
        // agent pinned to an older image keeps getting a typed plan.
        // TODO(remove-after-2026-Q3): drop these aliases.
        assert_eq!(AgentRuntime::from_header("sketchcollector"), AgentRuntime::AsapOtel);
        assert_eq!(AgentRuntime::from_header("Sketchcollector"), AgentRuntime::AsapOtel);
        assert_eq!(AgentRuntime::from_header("sketchotap"), AgentRuntime::AsapOtap);
        assert_eq!(AgentRuntime::from_header("sketchtelegraf"), AgentRuntime::AsapTelegraf);
    }

    #[test]
    fn emit_for_runtime_default_matches_emit_edge_yaml() {
        use crate::sketch_algebra::params::{DDSketchParams, SketchParams};
        use crate::stage_split::emitter::{EdgeSketchProcessor, ExportTarget};
        use crate::stage_split::stage_id::StageId;
        use crate::sketch_algebra::params::SketchKind;

        let cfg = EdgeStageConfig {
            source_metric: Some("m".to_string()),
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: vec![EdgeSketchProcessor {
                processor_name: "ddsketchprocessor".to_string(),
                sketch_kind: SketchKind::DDSketch,
                sketch_params: SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
                aggregation_id: "agg0".to_string(),
            }],
            exporter_target: ExportTarget::Stage(StageId::Gateway),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family: std::collections::HashMap::new(),
        };

        let collector = emit_for_runtime(
            AgentRuntime::AsapOtel, &cfg, "ws://ctrl/v1/opamp", None,
        ).expect("collector emit ok");
        let direct = emit_edge_yaml(&cfg, "ws://ctrl/v1/opamp").expect("direct emit ok");
        assert_eq!(collector, direct, "AsapOtel dispatch must equal emit_edge_yaml");
    }

    #[test]
    fn emit_for_runtime_otap_yields_dag_yaml() {
        use crate::stage_split::emitter::ExportTarget;
        use crate::stage_split::stage_id::StageId;

        let cfg = EdgeStageConfig {
            source_metric: Some("m".to_string()),
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Stage(StageId::Gateway),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family: std::collections::HashMap::new(),
        };
        let yaml = emit_for_runtime(
            AgentRuntime::AsapOtap, &cfg, "ws://ctrl/v1/opamp", None,
        ).expect("otap emit ok");
        // OTAP-specific token.
        assert!(yaml.contains("otel_dataflow/v1"), "expected OTAP DAG version\n{yaml}");
    }

    #[test]
    fn emit_for_runtime_telegraf_yields_toml() {
        use crate::stage_split::emitter::ExportTarget;
        use crate::stage_split::stage_id::StageId;

        let cfg = EdgeStageConfig {
            source_metric: Some("m".to_string()),
            label_filters: Vec::new(),
            window_secs: Some(60),
            sketch_processors: Vec::new(),
            exporter_target: ExportTarget::Stage(StageId::Gateway),
            prometheus_archive_metrics: Vec::new(),
            archive_tier_metrics: Vec::new(),
            warm_passthrough_metrics: Vec::new(),
            metric_to_family: std::collections::HashMap::new(),
        };
        let toml = emit_for_runtime(
            AgentRuntime::AsapTelegraf, &cfg, "ws://ctrl/v1/opamp", None,
        ).expect("telegraf emit ok");
        // Telegraf-specific token.
        assert!(toml.contains("[[inputs.opentelemetry]]"), "expected Telegraf TOML header\n{toml}");
    }
}
