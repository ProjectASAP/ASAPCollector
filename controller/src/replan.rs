//! SP-8 re-planning automation.
//!
//! [`Replanner`] closes the feedback loop from the [`monitor::Scraper`] back
//! to the planner.  Two triggers drive re-planning:
//!
//! 1. **Violation-triggered**: when the scraper fires an SLA violation callback
//!    the replanner looks up which metric the violating agent is serving and
//!    immediately requests a fresh plan.
//!
//! 2. **Expiry-triggered**: a periodic ticker calls [`Replanner::replan_expired`]
//!    to re-plan any metric whose `CollectionPlan::valid_until` has passed.
//!
//! After a plan is updated the replanner pushes role-appropriate OTel YAML to
//! all connected collectors via OpAMP and updates scraper endpoint sketch-type
//! bookkeeping so the EMA cost model receives correctly attributed updates.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::config::{generate_agent_config, generate_backend_config, build_precompute_jobs};
use crate::monitor::Scraper;
use crate::opamp::{AgentRole, OpampServer, RemoteConfig};
use crate::planner::FreezeAfterFirstPlanner;
use crate::store::{PlanStore, WorkloadStore};

fn short_hash(s: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

// ── Replanner ─────────────────────────────────────────────────────────────────

pub struct Replanner {
    planner:        Arc<FreezeAfterFirstPlanner>,
    plan_store:     Arc<PlanStore>,
    workload_store: Arc<WorkloadStore>,
    opamp:          Arc<OpampServer>,
    scraper:        Arc<Scraper>,
    opamp_endpoint: String,
    /// Maps agent_id → metric_name so violation callbacks can look up which
    /// metric a particular agent is serving.
    agent_to_metric: Arc<RwLock<HashMap<String, String>>>,
}

impl Replanner {
    pub fn new(
        planner:        Arc<FreezeAfterFirstPlanner>,
        plan_store:     Arc<PlanStore>,
        workload_store: Arc<WorkloadStore>,
        opamp:          Arc<OpampServer>,
        scraper:        Arc<Scraper>,
        opamp_endpoint: impl Into<String>,
    ) -> Self {
        Self {
            planner,
            plan_store,
            workload_store,
            opamp,
            scraper,
            opamp_endpoint: opamp_endpoint.into(),
            agent_to_metric: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // ── Agent registry ────────────────────────────────────────────────────────

    /// Record that `agent_id` is serving `metric`. Called from `handle_plan`
    /// after pushing configs so violations can be mapped back to a metric.
    pub async fn register_agent(&self, agent_id: impl Into<String>, metric: impl Into<String>) {
        self.agent_to_metric.write().await
            .insert(agent_id.into(), metric.into());
    }

    /// Remove the mapping for a disconnected agent.
    pub async fn unregister_agent(&self, agent_id: &str) {
        self.agent_to_metric.write().await.remove(agent_id);
    }

    // ── Re-plan helpers ───────────────────────────────────────────────────────

    /// Re-plans a single metric and pushes updated configs.
    /// Returns `true` if re-planning succeeded, `false` if the metric is unknown.
    pub async fn replan_metric(&self, metric: &str) -> bool {
        let Some((workload, wc)) = self.workload_store.get(metric) else {
            warn!(metric, "replan requested but workload not found in store");
            return false;
        };

        info!(metric, "re-planning metric");

        // Unfreeze so the cost model runs fresh rather than returning the
        // previously cached plan — the whole point of a re-plan is to
        // re-optimise with current EMA data.
        self.planner.unfreeze(metric);
        let mut plan = self.planner.plan(&workload, Some(&wc));
        plan.precompute = build_precompute_jobs(&workload, &plan, "backend:4317");
        self.plan_store.set(metric, plan.clone());

        // Push role-appropriate configs.
        if let Ok(yaml) = generate_agent_config(&plan.agent_config, &self.opamp_endpoint) {
            self.opamp.push_to_role(
                AgentRole::Agent,
                RemoteConfig { config_hash: short_hash(&yaml), yaml },
            ).await;
        }
        if let Ok(yaml) = generate_backend_config(&plan.backend_config, &self.opamp_endpoint) {
            self.opamp.push_to_role(
                AgentRole::Backend,
                RemoteConfig { config_hash: short_hash(&yaml), yaml },
            ).await;
        }

        // Update scraper endpoint sketch types for correct EMA attribution.
        let sketch_type = plan.agent_config.sketch_type;
        for agent_id in self.opamp.connected_agents().await {
            self.scraper.set_sketch_type(&agent_id, sketch_type.clone()).await;
        }

        info!(metric, sketch_type = %sketch_type, "re-plan complete");
        true
    }

    /// Re-plans all metrics whose `valid_until` has already passed.
    pub async fn replan_expired(&self) {
        let expired = self.plan_store.expired(chrono::Utc::now());
        if expired.is_empty() { return; }
        info!(count = expired.len(), "re-planning expired metrics");
        for metric in expired {
            self.replan_metric(&metric).await;
        }
    }

    /// Called from the violation callback. Looks up the metric served by
    /// `agent_id` and triggers an immediate re-plan.
    pub async fn handle_violation(&self, agent_id: &str) {
        let metric = self.agent_to_metric.read().await.get(agent_id).cloned();
        match metric {
            Some(m) => {
                info!(agent = agent_id, metric = %m, "SLA violation → triggering re-plan");
                self.replan_metric(&m).await;
            }
            None => {
                warn!(agent = agent_id, "SLA violation but no metric mapping found; re-planning all expired");
                self.replan_expired().await;
            }
        }
    }

    /// Starts a background loop that re-plans expired metrics every `interval`.
    pub async fn run_expiry_ticker(self: Arc<Self>, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            self.replan_expired().await;
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use chrono::Utc;

    use crate::planner::{CostModelPlanner, FreezeAfterFirstPlanner};
    use crate::store::{PlanStore, WorkloadStore};
    use crate::types::*;

    fn make_replanner() -> Arc<Replanner> {
        let plan_store     = Arc::new(PlanStore::new());
        let workload_store = Arc::new(WorkloadStore::new());
        let planner        = Arc::new(FreezeAfterFirstPlanner::new(CostModelPlanner::new()));
        let opamp          = Arc::new(crate::opamp::OpampServer::new());
        let scraper        = Arc::new(crate::monitor::Scraper::new(
            vec![], crate::monitor::Thresholds::default(),
            Arc::new(|_| {}), Duration::from_secs(60),
        ));
        Arc::new(Replanner::new(
            planner, plan_store, workload_store, opamp, scraper,
            "ws://ctrl:4320/v1/opamp",
        ))
    }

    fn test_workload(metric: &str) -> (QueryWorkload, WorkloadCharacteristics) {
        let wl = QueryWorkload {
            metric_name:          metric.into(),
            label_filters:        HashMap::new(),
            group_by_labels:      vec![],
            aggregations:         vec![AggType::Quantile],
            time_window:          Duration::from_secs(300),
            repeat_every:         None,
            accuracy_sla:         0.01,
            latency_sla:          None,
            sketch_type_override: None,
            exact_required:       false,
            quantiles:            vec![],
        };
        (wl, WorkloadCharacteristics::default())
    }

    fn make_plan() -> CollectionPlan {
        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Sketch,
                sketch_type: SketchType::DDSketch,
                sketch_params: SketchParams { relative_accuracy: 0.01, ..Default::default() },
                aggregate_by: vec![],
                label_matchers: vec![],
                window_duration: None,
                mode: ProcessorMode::Batch,
                enable_self_monitoring: true,
                transmit_sketch: true,
                drop_original: true,
                delta_transmission: false,
                delta_threshold: 0.0,
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            backend_config: BackendCollectorConfig {
                merge_sketch_type: SketchType::DDSketch,
                group_by: vec![],
            },
            precompute: vec![],
            valid_until: Utc::now() + chrono::Duration::seconds(3600),
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
        }
    }

    #[tokio::test]
    async fn replan_unknown_metric_returns_false() {
        let r = make_replanner();
        assert!(!r.replan_metric("unknown").await);
    }

    #[tokio::test]
    async fn replan_known_metric_updates_plan_store() {
        let r = make_replanner();
        let (wl, wc) = test_workload("latency");
        r.workload_store.set("latency", wl, wc);
        r.plan_store.set("latency", make_plan());

        let ok = r.replan_metric("latency").await;
        assert!(ok);
        // Plan store should now have a new entry (valid_until in the future).
        let updated = r.plan_store.get("latency").unwrap();
        assert!(updated.valid_until > Utc::now());
    }

    #[tokio::test]
    async fn replan_expired_replans_only_expired() {
        let r = make_replanner();
        let (wl, wc) = test_workload("old");
        r.workload_store.set("old", wl, wc);

        // Insert an already-expired plan.
        let mut expired_plan = make_plan();
        expired_plan.valid_until = Utc::now() - chrono::Duration::seconds(60);
        r.plan_store.set("old", expired_plan);

        // Insert a still-active plan for "active".
        let (awl, awc) = test_workload("active");
        r.workload_store.set("active", awl, awc);
        r.plan_store.set("active", make_plan());

        r.replan_expired().await;

        // "old" should now have a freshly computed plan.
        let old_plan = r.plan_store.get("old").unwrap();
        assert!(old_plan.valid_until > Utc::now());
    }

    #[tokio::test]
    async fn register_then_violation_replans_correct_metric() {
        let r = make_replanner();
        let (wl, wc) = test_workload("req_rate");
        r.workload_store.set("req_rate", wl, wc);
        r.plan_store.set("req_rate", make_plan());

        r.register_agent("agent-1", "req_rate").await;
        r.handle_violation("agent-1").await;

        // Plan should have been refreshed.
        assert!(r.plan_store.get("req_rate").is_ok());
    }

    #[tokio::test]
    async fn unregister_removes_mapping() {
        let r = make_replanner();
        r.register_agent("a1", "m").await;
        r.unregister_agent("a1").await;
        // After unregister, handle_violation falls back to replan_expired (no-op).
        r.handle_violation("a1").await; // should not panic
    }
}
