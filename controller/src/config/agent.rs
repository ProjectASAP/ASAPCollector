use std::collections::HashMap;
use anyhow::Context;
use serde::Serialize;
use serde_yaml::{Mapping, Value};

use crate::analyzer::format_duration;
use crate::types::*;

// ── YAML structural types ─────────────────────────────────────────────────────

#[derive(Serialize)]
struct CollectorYaml {
    extensions: Extensions,
    processors: HashMap<String, Value>,
    service:    ServiceSection,
}

#[derive(Serialize)]
struct Extensions { opamp: OpampExtension }

#[derive(Serialize)]
struct OpampExtension { server: OpampServer }

#[derive(Serialize)]
struct OpampServer { ws: WsConfig }

#[derive(Serialize)]
struct WsConfig { endpoint: String }

#[derive(Serialize)]
struct ServiceSection {
    extensions: Vec<String>,
    pipelines:  HashMap<String, Pipeline>,
}

#[derive(Serialize)]
struct Pipeline { processors: Vec<String> }

// ── Public API ────────────────────────────────────────────────────────────────

/// Generates an OTel collector YAML string for an agent collector from a plan.
pub fn generate_agent_config(cfg: &AgentCollectorConfig, opamp_endpoint: &str) -> anyhow::Result<String> {
    let processor_key = cfg.sketch_type.to_string();
    let processor_val = build_processor_block(cfg);

    let doc = CollectorYaml {
        extensions: Extensions {
            opamp: OpampExtension {
                server: OpampServer {
                    ws: WsConfig { endpoint: opamp_endpoint.to_string() },
                },
            },
        },
        processors: [(processor_key.clone(), processor_val)].into(),
        service: ServiceSection {
            extensions: vec!["opamp".into()],
            pipelines: [(
                "metrics".to_string(),
                Pipeline { processors: vec![processor_key] },
            )].into(),
        },
    };

    serde_yaml::to_string(&doc).context("serialize agent config")
}

fn build_processor_block(cfg: &AgentCollectorConfig) -> Value {
    let mut m = Mapping::new();

    m.insert("mode".into(),            Value::String(cfg.mode.to_string()));
    m.insert("transmit_sketch".into(), Value::Bool(cfg.transmit_sketch));
    m.insert("drop_original".into(),   Value::Bool(cfg.drop_original));

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
    fn contains_opamp_endpoint() {
        let ep = "ws://my-controller:4320/v1/opamp";
        let yaml = generate_agent_config(&ddsketch_cfg(), ep).unwrap();
        assert!(yaml.contains(ep), "YAML should contain endpoint\n{yaml}");
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
}
