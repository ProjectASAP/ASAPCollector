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
    emit_backend_storage_routing_with_prometheus, emit_edge_yaml, emit_gateway_yaml,
};
pub use stage_config_otap::emit_otap_dag_yaml;
pub use stage_config_telegraf::emit_telegraf_toml;
pub use workloads::WorkloadRegistry;

use crate::stage_split::emitter::EdgeStageConfig;
use anyhow::Result;

/// Phase ε.1.5 — which edge runtime an agent identifies as.
///
/// Today every agent the controller has built for runs the OTel-collector
/// (`Sketchcollector`); Phase ε.1.5 adds the two new runtime variants the
/// per-runtime emitters target. The runtime is reported by the agent on
/// OpAMP `on_connect` (header `X-Agent-Runtime`); when absent (legacy
/// agents) the controller defaults to `Sketchcollector` so the existing
/// behaviour is preserved.
///
/// Phase ε.1.5 commits the enum + emit-dispatch function. Threading the
/// runtime through OpAMP `on_connect` and into the typed L5 emit path
/// is a follow-up — the emitters can be exercised in isolation today
/// (the Phase ε.1.5 test suite does exactly that).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentRuntime {
    /// Default — OTel-collector contrib build (existing behaviour).
    Sketchcollector,
    /// otap-dataflow Rust runtime.
    Sketchotap,
    /// Telegraf runtime.
    Sketchtelegraf,
}

impl Default for AgentRuntime {
    fn default() -> Self {
        AgentRuntime::Sketchcollector
    }
}

impl AgentRuntime {
    /// Parse an `X-Agent-Runtime` header value. Recognises
    /// `sketchcollector` / `sketchotap` / `sketchtelegraf`
    /// (case-insensitive); any other value (including the empty string)
    /// defaults to `Sketchcollector` so legacy agents keep working.
    pub fn from_header(value: &str) -> Self {
        match value.trim().to_lowercase().as_str() {
            "sketchotap" | "otap" => AgentRuntime::Sketchotap,
            "sketchtelegraf" | "telegraf" => AgentRuntime::Sketchtelegraf,
            _ => AgentRuntime::Sketchcollector,
        }
    }
}

/// Phase ε.1.5 — dispatch the edge emit by agent runtime. Mirrors
/// `emit_edge_yaml`'s `(cfg, opamp_endpoint) -> String` shape; the OTAP
/// and Telegraf emitters take an additional optional Prometheus URL
/// override which we pass through `prometheus_url`.
///
/// `prometheus_url` is the Mode-3 destination override:
///   * `Sketchcollector` → ignored (the OTel-collector emitter already
///     reads `${ASAP_PROMETHEUS_OTLP_URL}` at runtime);
///   * `Sketchotap`      → OTLP HTTP URL passed to `emit_otap_dag_yaml`;
///   * `Sketchtelegraf`  → remote-write URL passed to `emit_telegraf_toml`.
pub fn emit_for_runtime(
    runtime: AgentRuntime,
    cfg: &EdgeStageConfig,
    opamp_endpoint: &str,
    prometheus_url: Option<&str>,
) -> Result<String> {
    match runtime {
        AgentRuntime::Sketchcollector => emit_edge_yaml(cfg, opamp_endpoint),
        AgentRuntime::Sketchotap => emit_otap_dag_yaml(cfg, opamp_endpoint, prometheus_url),
        AgentRuntime::Sketchtelegraf => emit_telegraf_toml(cfg, prometheus_url),
    }
}

#[cfg(test)]
mod runtime_tests {
    use super::*;

    #[test]
    fn agent_runtime_from_header_recognises_three_values() {
        assert_eq!(AgentRuntime::from_header("sketchcollector"), AgentRuntime::Sketchcollector);
        assert_eq!(AgentRuntime::from_header("sketchotap"), AgentRuntime::Sketchotap);
        assert_eq!(AgentRuntime::from_header("sketchtelegraf"), AgentRuntime::Sketchtelegraf);
    }

    #[test]
    fn agent_runtime_from_header_short_aliases() {
        assert_eq!(AgentRuntime::from_header("otap"), AgentRuntime::Sketchotap);
        assert_eq!(AgentRuntime::from_header("telegraf"), AgentRuntime::Sketchtelegraf);
    }

    #[test]
    fn agent_runtime_from_header_default_is_sketchcollector() {
        assert_eq!(AgentRuntime::from_header(""), AgentRuntime::Sketchcollector);
        assert_eq!(AgentRuntime::from_header("garbage"), AgentRuntime::Sketchcollector);
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
        };

        let collector = emit_for_runtime(
            AgentRuntime::Sketchcollector, &cfg, "ws://ctrl/v1/opamp", None,
        ).expect("collector emit ok");
        let direct = emit_edge_yaml(&cfg, "ws://ctrl/v1/opamp").expect("direct emit ok");
        assert_eq!(collector, direct, "Sketchcollector dispatch must equal emit_edge_yaml");
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
        };
        let yaml = emit_for_runtime(
            AgentRuntime::Sketchotap, &cfg, "ws://ctrl/v1/opamp", None,
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
        };
        let toml = emit_for_runtime(
            AgentRuntime::Sketchtelegraf, &cfg, "ws://ctrl/v1/opamp", None,
        ).expect("telegraf emit ok");
        // Telegraf-specific token.
        assert!(toml.contains("[[inputs.opentelemetry]]"), "expected Telegraf TOML header\n{toml}");
    }
}
