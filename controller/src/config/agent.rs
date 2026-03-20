use std::collections::HashMap;
use anyhow::Context;
use serde::Serialize;
use serde_yaml::{Mapping, Value};

use crate::analyzer::format_duration;
use crate::types::*;

// ── YAML structural types ─────────────────────────────────────────────────────

#[derive(Serialize)]
struct CollectorYaml {
    receivers:  HashMap<String, Value>,
    processors: HashMap<String, Value>,
    exporters:  HashMap<String, Value>,
    service:    ServiceSection,
}

#[derive(Serialize)]
struct ServiceSection {
    pipelines: HashMap<String, Pipeline>,
}

#[derive(Serialize)]
struct Pipeline {
    receivers:  Vec<String>,
    processors: Vec<String>,
    exporters:  Vec<String>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Generates an OTel collector YAML string for an agent collector from a plan.
///
/// The `opamp_endpoint` parameter is accepted for API compatibility but the
/// generated config intentionally omits the opamp extension: the controller
/// currently speaks JSON over WebSocket while the opamp-go client used by the
/// collector expects binary protobuf, so including it would only produce
/// repeated "bad handshake" errors in the collector log. Configs are delivered
/// via the HTTP config provider instead (`--config=http://...`).
pub fn generate_agent_config(cfg: &AgentCollectorConfig, _opamp_endpoint: &str) -> anyhow::Result<String> {
    let processor_key = cfg.sketch_type.to_string();
    let processor_val = build_processor_block(cfg);

    // Standard OTLP receiver (gRPC + HTTP).
    let otlp_receiver: Value = serde_yaml::from_str(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:4317\"\n  http:\n    endpoint: \"0.0.0.0:4318\"\n",
    ).unwrap();

    // Prometheus exporter so downstream scrapers can observe the pipeline.
    let prom_exporter: Value = serde_yaml::from_str(
        "endpoint: \"0.0.0.0:8889\"\n",
    ).unwrap();

    let doc = CollectorYaml {
        receivers:  [("otlp".to_string(), otlp_receiver)].into(),
        processors: [(processor_key.clone(), processor_val)].into(),
        exporters:  [("prometheus".to_string(), prom_exporter)].into(),
        service: ServiceSection {
            pipelines: [(
                "metrics".to_string(),
                Pipeline {
                    receivers:  vec!["otlp".into()],
                    processors: vec![processor_key],
                    exporters:  vec!["prometheus".into()],
                },
            )].into(),
        },
    };

    serde_yaml::to_string(&doc).context("serialize agent config")
}

fn build_processor_block(cfg: &AgentCollectorConfig) -> Value {
    let mut m = Mapping::new();

    m.insert("mode".into(),            Value::String(cfg.mode.to_string()));
    m.insert("transmit_sketch".into(), Value::Bool(cfg.transmit_sketch));

    if cfg.mode == ProcessorMode::Window {
        if let Some(wd) = cfg.window_duration {
            m.insert("window_duration".into(), Value::String(format_duration(wd)));
        }
    }

    if !cfg.aggregate_by.is_empty() {
        m.insert("aggregate_by".into(), seq_of_strings(&cfg.aggregate_by));
    }
    if !cfg.label_matchers.is_empty() {
        m.insert("label_matchers".into(), seq_of_strings(&cfg.label_matchers));
    }

    // Sketch-type-specific params.
    let p = &cfg.sketch_params;
    match &cfg.sketch_type {
        SketchType::DDSketch => {
            m.insert("relative_accuracy".into(), Value::Number(p.relative_accuracy.into()));
            if !p.quantiles.is_empty() {
                m.insert("quantiles".into(),
                    Value::Sequence(p.quantiles.iter().map(|q| Value::Number((*q).into())).collect()));
            }
        }
        SketchType::KLL => {
            m.insert("k".into(), Value::Number((p.k as u64).into()));
            if !p.quantiles.is_empty() {
                m.insert("quantiles".into(),
                    Value::Sequence(p.quantiles.iter().map(|q| Value::Number((*q).into())).collect()));
            }
        }
        SketchType::HLL => {
            m.insert("precision".into(), Value::Number((p.precision as u64).into()));
        }
        SketchType::CountSketch | SketchType::CountMinSketch => {
            m.insert("rows".into(), Value::Number((p.rows as u64).into()));
            m.insert("cols".into(), Value::Number((p.cols as u64).into()));
        }
    }

    Value::Mapping(m)
}

fn seq_of_strings(v: &[String]) -> Value {
    Value::Sequence(v.iter().map(|s| Value::String(s.clone())).collect())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn ddsketch_cfg() -> AgentCollectorConfig {
        AgentCollectorConfig {
            output_mode:     OutputMode::Sketch,
            sketch_type:     SketchType::DDSketch,
            sketch_params:   SketchParams {
                relative_accuracy: 0.01,
                quantiles: vec![0.5, 0.9, 0.99],
                ..Default::default()
            },
            aggregate_by:    vec!["host.name".into(), "service".into()],
            label_matchers:  vec!["env=prod".into()],
            window_duration: Some(Duration::from_secs(300)),
            mode:            ProcessorMode::Window,
            transmit_sketch: true,
            drop_original:   true,
        }
    }

    #[test]
    fn contains_processor_key() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("ddsketch:"), "YAML should contain 'ddsketch:'\n{yaml}");
    }

    #[test]
    fn omits_opamp_extension() {
        // The opamp extension is intentionally absent: the controller speaks JSON
        // but the opamp-go client expects protobuf, causing bad-handshake errors.
        // Config delivery uses the HTTP config provider instead.
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(!yaml.contains("opamp"), "YAML must not include the opamp extension\n{yaml}");
    }

    #[test]
    fn contains_window_duration() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("5m"), "YAML should contain window_duration\n{yaml}");
    }

    #[test]
    fn batch_mode_omits_window_duration() {
        let mut cfg = ddsketch_cfg();
        cfg.mode = ProcessorMode::Batch;
        cfg.window_duration = None;
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(!yaml.contains("window_duration"), "batch mode should not have window_duration\n{yaml}");
    }

    #[test]
    fn contains_aggregate_by() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("host.name"), "YAML should contain aggregate_by labels\n{yaml}");
    }

    #[test]
    fn hll_processor() {
        let cfg = AgentCollectorConfig {
            sketch_type:   SketchType::HLL,
            sketch_params: SketchParams { precision: 14, ..Default::default() },
            mode:          ProcessorMode::Batch,
            window_duration: None,
            output_mode:   OutputMode::Sketch,
            aggregate_by:  vec![],
            label_matchers: vec![],
            transmit_sketch: true,
            drop_original: true,
        };
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("hll:"),      "YAML should contain 'hll:'\n{yaml}");
        assert!(yaml.contains("precision"), "YAML should contain 'precision'\n{yaml}");
    }

    #[test]
    fn countminsketch_processor() {
        let cfg = AgentCollectorConfig {
            sketch_type:   SketchType::CountMinSketch,
            sketch_params: SketchParams { rows: 5, cols: 2048, ..Default::default() },
            mode:          ProcessorMode::Batch,
            window_duration: None,
            output_mode:   OutputMode::Sketch,
            aggregate_by:  vec![],
            label_matchers: vec![],
            transmit_sketch: true,
            drop_original: true,
        };
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("countminsketch:"), "YAML should contain processor key\n{yaml}");
    }

    #[test]
    fn contains_otlp_receiver() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("receivers:"),  "YAML should have receivers section\n{yaml}");
        assert!(yaml.contains("otlp:"),       "YAML should have otlp receiver\n{yaml}");
        assert!(yaml.contains("4317"),        "YAML should have gRPC port\n{yaml}");
        assert!(yaml.contains("4318"),        "YAML should have HTTP port\n{yaml}");
    }

    #[test]
    fn contains_prometheus_exporter() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("exporters:"),   "YAML should have exporters section\n{yaml}");
        assert!(yaml.contains("prometheus:"),  "YAML should have prometheus exporter\n{yaml}");
        assert!(yaml.contains("8889"),         "YAML should have prometheus port\n{yaml}");
    }

    #[test]
    fn pipeline_has_receivers_and_exporters() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        // Ensure the pipeline block references both receiver and exporter keys.
        assert!(yaml.contains("- otlp"),       "pipeline receivers should list otlp\n{yaml}");
        assert!(yaml.contains("- prometheus"), "pipeline exporters should list prometheus\n{yaml}");
    }
}
