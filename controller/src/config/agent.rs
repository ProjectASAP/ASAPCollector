use anyhow::Context;
use serde::Serialize;
use serde_yaml::{Mapping, Value};
use std::collections::HashMap;

use crate::analyzer::format_duration;
use crate::types::*;

/// Processor map key and pipeline entry must match the OpenTelemetry **component type**
/// string from each factory (`MustNewType` in
/// `opentelemetry-collector-contrib-patch/processor/*/factory.go`). This is not always
/// the same as `SketchType`'s `Display` (e.g. HLL vs `hll`, KLL vs `kll`).
fn collector_processor_component_id(st: &SketchType) -> &'static str {
    match st {
        SketchType::DDSketch => "ddsketch",
        SketchType::KLL => "KLL",
        SketchType::HLL => "HLL",
        SketchType::CountSketch => "countsketch",
        SketchType::CountMinSketch => "countmin",
    }
}

// ── YAML structural types ─────────────────────────────────────────────────────

#[derive(Serialize)]
struct CollectorYaml {
    receivers: HashMap<String, Value>,
    processors: HashMap<String, Value>,
    exporters: HashMap<String, Value>,
    service: ServiceSection,
}

#[derive(Serialize)]
struct ServiceSection {
    pipelines: HashMap<String, Pipeline>,
}

#[derive(Serialize)]
struct Pipeline {
    receivers: Vec<String>,
    processors: Vec<String>,
    exporters: Vec<String>,
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
pub fn generate_agent_config(
    cfg: &AgentCollectorConfig,
    _opamp_endpoint: &str,
) -> anyhow::Result<String> {
    let processor_key = collector_processor_component_id(&cfg.sketch_type).to_string();
    let processor_val = build_processor_block(cfg);

    // Standard OTLP receiver (gRPC + HTTP).
    let otlp_receiver: Value = serde_yaml::from_str(
        "protocols:\n  grpc:\n    endpoint: \"0.0.0.0:4317\"\n  http:\n    endpoint: \"0.0.0.0:4318\"\n",
    ).unwrap();

    // Prometheus exporter so downstream scrapers can observe the pipeline.
    let prom_exporter: Value = serde_yaml::from_str("endpoint: \"0.0.0.0:8889\"\n").unwrap();

    let mut exporters: HashMap<String, Value> =
        [("prometheus".to_string(), prom_exporter)].into();
    let mut pipeline_exporters = vec!["prometheus".to_string()];

    // Optional file exporter: write one OTLP-JSON line per window flush.
    // Enabled when `file_output_path` is set in the agent config (benchmark use).
    if let Some(ref path) = cfg.file_output_path {
        let file_exporter: Value = serde_yaml::from_str(
            &format!("path: {path:?}\n"),
        ).unwrap();
        exporters.insert("file".to_string(), file_exporter);
        pipeline_exporters.push("file".to_string());
    }

    let doc = CollectorYaml {
        receivers: [("otlp".to_string(), otlp_receiver)].into(),
        processors: [(processor_key.clone(), processor_val)].into(),
        exporters,
        service: ServiceSection {
            pipelines: [(
                "metrics".to_string(),
                Pipeline {
                    receivers: vec!["otlp".into()],
                    processors: vec![processor_key],
                    exporters: pipeline_exporters,
                },
            )]
            .into(),
        },
    };

    serde_yaml::to_string(&doc).context("serialize agent config")
}

fn build_processor_block(cfg: &AgentCollectorConfig) -> Value {
    let mut m = Mapping::new();

    m.insert("mode".into(), Value::String(cfg.mode.to_string()));
    m.insert(
        "enable_self_monitoring".into(),
        Value::Bool(cfg.enable_self_monitoring),
    );
    m.insert("transmit_sketch".into(), Value::Bool(cfg.transmit_sketch));
    m.insert("drop_original".into(), Value::Bool(cfg.drop_original));

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

    // Delta transmission: only emit fields each processor's Config actually defines.
    // KLL rejects delta_transmission at Validate(); HLL has no delta_threshold key.
    if cfg.delta_transmission && cfg.sketch_type != SketchType::KLL {
        m.insert("delta_transmission".into(), Value::Bool(true));
        if matches!(
            cfg.sketch_type,
            SketchType::DDSketch | SketchType::CountSketch | SketchType::CountMinSketch
        ) {
            m.insert(
                "delta_threshold".into(),
                Value::Number(cfg.delta_threshold.into()),
            );
        }
    }

    // Sketch-type-specific params.
    let p = &cfg.sketch_params;
    match &cfg.sketch_type {
        SketchType::DDSketch => {
            m.insert(
                "relative_accuracy".into(),
                Value::Number(p.relative_accuracy.into()),
            );
            if !p.quantiles.is_empty() {
                m.insert(
                    "quantiles".into(),
                    Value::Sequence(
                        p.quantiles
                            .iter()
                            .map(|q| Value::Number((*q).into()))
                            .collect(),
                    ),
                );
            }
        }
        SketchType::KLL => {
            m.insert("k".into(), Value::Number((p.k as u64).into()));
            if !p.quantiles.is_empty() {
                m.insert(
                    "quantiles".into(),
                    Value::Sequence(
                        p.quantiles
                            .iter()
                            .map(|q| Value::Number((*q).into()))
                            .collect(),
                    ),
                );
            }
        }
        SketchType::HLL => {
            // hllprocessor uses a fixed HLL precision in code; Config has no precision field.
        }
        SketchType::CountSketch => {
            // countsketchprocessor requires epsilon and delta (not rows/cols).
            m.insert("epsilon".into(), Value::Number(p.epsilon.into()));
            m.insert("delta".into(), Value::Number(p.delta.into()));
        }
        SketchType::CountMinSketch => {
            // countminsketchprocessor requires metric_name, rows, and columns.
            m.insert(
                "metric_name".into(),
                Value::String(p.metric_name.clone()),
            );
            m.insert("rows".into(), Value::Number((p.rows as u64).into()));
            m.insert("columns".into(), Value::Number((p.cols as u64).into()));
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
            output_mode: OutputMode::Sketch,
            sketch_type: SketchType::DDSketch,
            sketch_params: SketchParams {
                relative_accuracy: 0.01,
                quantiles: vec![0.5, 0.9, 0.99],
                ..Default::default()
            },
            aggregate_by: vec!["host.name".into(), "service".into()],
            label_matchers: vec!["env=prod".into()],
            window_duration: Some(Duration::from_secs(300)),
            mode: ProcessorMode::Window,
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            file_output_path: None,
        }
    }

    #[test]
    fn contains_processor_key() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("ddsketch:"),
            "YAML should contain 'ddsketch:'\n{yaml}"
        );
        assert!(
            yaml.contains("enable_self_monitoring: true"),
            "YAML should carry enable_self_monitoring\n{yaml}"
        );
    }

    #[test]
    fn omits_opamp_extension() {
        // The opamp extension is intentionally absent: the controller speaks JSON
        // but the opamp-go client expects protobuf, causing bad-handshake errors.
        // Config delivery uses the HTTP config provider instead.
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            !yaml.contains("opamp"),
            "YAML must not include the opamp extension\n{yaml}"
        );
    }

    #[test]
    fn contains_window_duration() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("5m"),
            "YAML should contain window_duration\n{yaml}"
        );
    }

    #[test]
    fn batch_mode_omits_window_duration() {
        let mut cfg = ddsketch_cfg();
        cfg.mode = ProcessorMode::Batch;
        cfg.window_duration = None;
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            !yaml.contains("window_duration"),
            "batch mode should not have window_duration\n{yaml}"
        );
    }

    #[test]
    fn contains_aggregate_by() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("host.name"),
            "YAML should contain aggregate_by labels\n{yaml}"
        );
    }

    #[test]
    fn hll_processor() {
        let cfg = AgentCollectorConfig {
            sketch_type: SketchType::HLL,
            sketch_params: SketchParams {
                precision: 14,
                ..Default::default()
            },
            mode: ProcessorMode::Batch,
            window_duration: None,
            output_mode: OutputMode::Sketch,
            aggregate_by: vec![],
            label_matchers: vec![],
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            file_output_path: None,
        };
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("HLL:"), "YAML should contain HLL processor key\n{yaml}");
        assert!(
            yaml.contains("- HLL"),
            "pipeline should reference HLL processor\n{yaml}"
        );
        assert!(
            !yaml.contains("precision"),
            "HLL processor YAML must not set precision (not in Config)\n{yaml}"
        );
    }

    #[test]
    fn countminsketch_processor() {
        let cfg = AgentCollectorConfig {
            sketch_type: SketchType::CountMinSketch,
            sketch_params: SketchParams {
                rows: 5,
                cols: 2048,
                ..Default::default()
            },
            mode: ProcessorMode::Batch,
            window_duration: None,
            output_mode: OutputMode::Sketch,
            aggregate_by: vec![],
            label_matchers: vec![],
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            file_output_path: None,
        };
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("countmin:"),
            "YAML should use countmin component id (factory type)\n{yaml}"
        );
    }

    #[test]
    fn contains_otlp_receiver() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("receivers:"),
            "YAML should have receivers section\n{yaml}"
        );
        assert!(
            yaml.contains("otlp:"),
            "YAML should have otlp receiver\n{yaml}"
        );
        assert!(yaml.contains("4317"), "YAML should have gRPC port\n{yaml}");
        assert!(yaml.contains("4318"), "YAML should have HTTP port\n{yaml}");
    }

    #[test]
    fn contains_prometheus_exporter() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("exporters:"),
            "YAML should have exporters section\n{yaml}"
        );
        assert!(
            yaml.contains("prometheus:"),
            "YAML should have prometheus exporter\n{yaml}"
        );
        assert!(
            yaml.contains("8889"),
            "YAML should have prometheus port\n{yaml}"
        );
    }

    #[test]
    fn pipeline_has_receivers_and_exporters() {
        let yaml = generate_agent_config(&ddsketch_cfg(), "ws://ctrl:4320/v1/opamp").unwrap();
        // Ensure the pipeline block references both receiver and exporter keys.
        assert!(
            yaml.contains("- otlp"),
            "pipeline receivers should list otlp\n{yaml}"
        );
        assert!(
            yaml.contains("- prometheus"),
            "pipeline exporters should list prometheus\n{yaml}"
        );
    }

    #[test]
    fn delta_fields_present_when_enabled() {
        let mut cfg = ddsketch_cfg();
        cfg.delta_transmission = true;
        cfg.delta_threshold = 1.0;
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("delta_transmission: true"),
            "YAML should contain delta_transmission: true\n{yaml}"
        );
        assert!(
            yaml.contains("delta_threshold"),
            "YAML should contain delta_threshold\n{yaml}"
        );
    }

    #[test]
    fn delta_fields_absent_when_disabled() {
        let cfg = ddsketch_cfg(); // delta_transmission: false by default
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            !yaml.contains("delta_transmission"),
            "YAML must not contain delta_transmission when disabled\n{yaml}"
        );
        assert!(
            !yaml.contains("delta_threshold"),
            "YAML must not contain delta_threshold when disabled\n{yaml}"
        );
    }

    #[test]
    fn kll_processor() {
        let cfg = AgentCollectorConfig {
            sketch_type: SketchType::KLL,
            sketch_params: SketchParams {
                k: 200,
                quantiles: vec![0.5, 0.99],
                ..Default::default()
            },
            mode: ProcessorMode::Window,
            window_duration: Some(std::time::Duration::from_secs(300)),
            output_mode: OutputMode::Sketch,
            aggregate_by: vec![],
            label_matchers: vec![],
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            file_output_path: None,
        };
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("kll:"), "YAML should contain 'kll:'\n{yaml}");
        assert!(yaml.contains("k:"), "YAML should contain 'k:' param\n{yaml}");
        assert!(!yaml.contains("ddsketch:"), "YAML must not contain wrong processor key\n{yaml}");
    }

    #[test]
    fn countsketch_processor() {
        let cfg = AgentCollectorConfig {
            sketch_type: SketchType::CountSketch,
            sketch_params: SketchParams {
                rows: 5,
                cols: 10000,
                ..Default::default()
            },
            mode: ProcessorMode::Batch,
            window_duration: None,
            output_mode: OutputMode::Sketch,
            aggregate_by: vec![],
            label_matchers: vec![],
            enable_self_monitoring: true,
            transmit_sketch: true,
            drop_original: true,
            delta_transmission: false,
            delta_threshold: 0.0,
            file_output_path: None,
        };
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("countsketch:"),
            "YAML should contain 'countsketch:'\n{yaml}"
        );
        assert!(
            !yaml.contains("countminsketch:"),
            "YAML must not contain 'countminsketch:' for CountSketch\n{yaml}"
        );
    }

    /// Verifies that for every sketch type the processor key in the `processors:`
    /// section and the key listed under `service.pipelines.metrics.processors`
    /// are identical.  This guards against the processor map and the pipeline
    /// reference going out of sync.
    #[test]
    fn all_sketch_types_processor_key_matches_pipeline_ref() {
        let cases: &[(&str, SketchType, SketchParams)] = &[
            ("ddsketch", SketchType::DDSketch, SketchParams { relative_accuracy: 0.01, ..Default::default() }),
            ("kll",      SketchType::KLL,      SketchParams { k: 200, ..Default::default() }),
            ("hll",      SketchType::HLL,      SketchParams { precision: 14, ..Default::default() }),
            ("countsketch",    SketchType::CountSketch,    SketchParams { rows: 5, cols: 10000, ..Default::default() }),
            ("countminsketch", SketchType::CountMinSketch, SketchParams { rows: 5, cols: 2048, ..Default::default() }),
        ];

        for (expected_key, sketch_type, sketch_params) in cases {
            let cfg = AgentCollectorConfig {
                sketch_type: sketch_type.clone(),
                sketch_params: sketch_params.clone(),
                mode: ProcessorMode::Batch,
                window_duration: None,
                output_mode: OutputMode::Sketch,
                aggregate_by: vec![],
                label_matchers: vec![],
                enable_self_monitoring: true,
                transmit_sketch: true,
                drop_original: true,
                delta_transmission: false,
                delta_threshold: 0.0,
                file_output_path: None,
            };
            let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();

            // Processor section key present.
            assert!(
                yaml.contains(&format!("{expected_key}:")),
                "sketch_type={expected_key}: YAML missing processor key '{expected_key}:'\n{yaml}"
            );
            // Pipeline processor list references the same key.
            assert!(
                yaml.contains(&format!("- {expected_key}")),
                "sketch_type={expected_key}: pipeline processor list missing '- {expected_key}'\n{yaml}"
            );
            // No other sketch type key should appear as a processor.
            for (other_key, _, _) in cases {
                if other_key == expected_key { continue; }
                assert!(
                    !yaml.contains(&format!("{other_key}:")),
                    "sketch_type={expected_key}: YAML must not contain foreign key '{other_key}:'\n{yaml}"
                );
            }
        }
    }

    #[test]
    fn file_exporter_included_when_path_set() {
        let mut cfg = ddsketch_cfg();
        cfg.file_output_path = Some("/tmp/sketch_output.jsonl".into());
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            yaml.contains("file:"),
            "YAML should contain file exporter section\n{yaml}"
        );
        assert!(
            yaml.contains("/tmp/sketch_output.jsonl"),
            "YAML should contain the configured file path\n{yaml}"
        );
        assert!(
            yaml.contains("- file"),
            "pipeline exporters should list file\n{yaml}"
        );
    }

    #[test]
    fn file_exporter_absent_when_path_not_set() {
        let cfg = ddsketch_cfg(); // file_output_path: None by default
        let yaml = generate_agent_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(
            !yaml.contains("file:"),
            "YAML must not contain file exporter when path is not set\n{yaml}"
        );
    }
}
