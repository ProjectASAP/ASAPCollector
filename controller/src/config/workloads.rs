//! Declarative workload registration.
//!
//! Loads a YAML file describing workloads and their assignments so the
//! controller can pre-populate the plan store and assign workloads to
//! agents on connect without requiring an explicit HTTP `POST /api/v1/plan`.

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

/// A single workload entry from the workloads YAML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadEntry {
    /// Metric name this workload targets (e.g. `http_request_duration_seconds`).
    pub metric_name: String,
    /// PromQL / SQL query string for the planner.
    #[serde(default)]
    pub query_string: Option<String>,
    /// Required accuracy SLA (0.0 – 1.0).
    #[serde(default = "default_accuracy_sla")]
    pub accuracy_sla: f64,
    /// Role that should receive this workload (e.g. `"agent"`, `"backend"`).
    #[serde(default = "default_role")]
    pub assign_to_role: String,
}

fn default_accuracy_sla() -> f64 { 0.01 }
fn default_role() -> String { "agent".into() }

/// Registry of declarative workloads loaded from a YAML file.
#[derive(Debug, Clone)]
pub struct WorkloadRegistry {
    entries: Vec<WorkloadEntry>,
}

impl WorkloadRegistry {
    /// Load from a YAML file. Returns an empty registry on any error.
    pub fn load(path: &str) -> Self {
        match std::fs::read_to_string(path) {
            Ok(contents) => match serde_yaml::from_str::<Vec<WorkloadEntry>>(&contents) {
                Ok(entries) => {
                    info!(path, count = entries.len(), "loaded workload registry");
                    Self { entries }
                }
                Err(e) => {
                    warn!(path, error = %e, "invalid workloads YAML; using empty registry");
                    Self { entries: vec![] }
                }
            },
            Err(_) => {
                info!(path, "workloads file not found; using empty registry");
                Self { entries: vec![] }
            }
        }
    }

    /// Create an empty registry (no file).
    pub fn empty() -> Self {
        Self { entries: vec![] }
    }

    /// Returns all workload entries.
    pub fn entries(&self) -> &[WorkloadEntry] {
        &self.entries
    }

    /// Returns workload entries assigned to a given role.
    pub fn for_role(&self, role: &str) -> Vec<&WorkloadEntry> {
        self.entries.iter()
            .filter(|e| e.assign_to_role.eq_ignore_ascii_case(role))
            .collect()
    }

    /// Returns the first workload entry for a given role, if any.
    pub fn first_for_role(&self, role: &str) -> Option<&WorkloadEntry> {
        self.entries.iter()
            .find(|e| e.assign_to_role.eq_ignore_ascii_case(role))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_empty_on_missing_file() {
        let reg = WorkloadRegistry::load("/nonexistent/workloads.yaml");
        assert!(reg.entries().is_empty());
    }

    #[test]
    fn empty_registry() {
        let reg = WorkloadRegistry::empty();
        assert!(reg.entries().is_empty());
        assert!(reg.first_for_role("agent").is_none());
    }

    #[test]
    fn deserialize_entries() {
        let yaml = r#"
- metric_name: latency
  query_string: "histogram_quantile(0.99, rate(http_duration_bucket[5m]))"
  accuracy_sla: 0.01
  assign_to_role: agent
- metric_name: error_count
  accuracy_sla: 0.05
  assign_to_role: backend
"#;
        let entries: Vec<WorkloadEntry> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].metric_name, "latency");
        assert_eq!(entries[0].assign_to_role, "agent");
        assert!(entries[0].query_string.is_some());
        assert_eq!(entries[1].metric_name, "error_count");
        assert!(entries[1].query_string.is_none());
    }

    #[test]
    fn for_role_filters_correctly() {
        let reg = WorkloadRegistry {
            entries: vec![
                WorkloadEntry {
                    metric_name: "a".into(),
                    query_string: None,
                    accuracy_sla: 0.01,
                    assign_to_role: "agent".into(),
                },
                WorkloadEntry {
                    metric_name: "b".into(),
                    query_string: None,
                    accuracy_sla: 0.05,
                    assign_to_role: "backend".into(),
                },
                WorkloadEntry {
                    metric_name: "c".into(),
                    query_string: None,
                    accuracy_sla: 0.02,
                    assign_to_role: "agent".into(),
                },
            ],
        };
        assert_eq!(reg.for_role("agent").len(), 2);
        assert_eq!(reg.for_role("backend").len(), 1);
        assert_eq!(reg.first_for_role("agent").unwrap().metric_name, "a");
    }
}
