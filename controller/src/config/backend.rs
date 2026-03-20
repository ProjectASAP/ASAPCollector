use anyhow::Context;
use crate::types::*;

/// Generates an OTel collector YAML string for the backend merge collector.
pub fn generate_backend_config(cfg: &BackendCollectorConfig, opamp_endpoint: &str) -> anyhow::Result<String> {
    let processor_key = format!("{}_merge", cfg.merge_sketch_type);

    let doc = serde_yaml::to_value(&serde_json::json!({
        "extensions": {
            "opamp": { "server": { "ws": { "endpoint": opamp_endpoint } } }
        },
        "processors": {
            &processor_key: {
                "mode":     "merge",
                "group_by": cfg.group_by
            }
        },
        "service": {
            "extensions": ["opamp"],
            "pipelines": {
                "metrics": { "processors": [&processor_key] }
            }
        }
    })).context("build backend doc")?;

    serde_yaml::to_string(&doc).context("serialize backend config")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_merge_key() {
        let cfg = BackendCollectorConfig {
            merge_sketch_type: SketchType::DDSketch,
            group_by: vec!["host.name".into()],
        };
        let yaml = generate_backend_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("ddsketch_merge"), "YAML should contain merge key\n{yaml}");
        assert!(yaml.contains("host.name"),      "YAML should contain group_by\n{yaml}");
    }

    #[test]
    fn hll_merge_key() {
        let cfg = BackendCollectorConfig {
            merge_sketch_type: SketchType::HLL,
            group_by: vec![],
        };
        let yaml = generate_backend_config(&cfg, "ws://ctrl:4320/v1/opamp").unwrap();
        assert!(yaml.contains("hll_merge"), "{yaml}");
    }

    #[test]
    fn contains_opamp_endpoint() {
        let ep = "ws://custom-ctrl:9000/v1/opamp";
        let cfg = BackendCollectorConfig { merge_sketch_type: SketchType::KLL, group_by: vec![] };
        let yaml = generate_backend_config(&cfg, ep).unwrap();
        assert!(yaml.contains(ep), "YAML should contain endpoint\n{yaml}");
    }
}
