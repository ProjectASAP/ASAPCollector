/// Freeze-after-first planner.
///
/// Runs the full cost-model optimisation **once** per metric (on the first
/// POST /api/v1/plan call for that metric name) and then returns the same
/// `CollectionPlan` for every subsequent request — even if the workload
/// characteristics change.
///
/// This is the intended production default: you get a data-driven initial
/// plan without the risk of live re-optimisation diverging mid-stream.
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::types::{CollectionPlan, QueryWorkload, WorkloadCharacteristics};
use super::cost_model::CostModelPlanner;

pub struct FreezeAfterFirstPlanner {
    inner: CostModelPlanner,
    cache: Arc<RwLock<HashMap<String, CollectionPlan>>>,
}

impl FreezeAfterFirstPlanner {
    pub fn new(inner: CostModelPlanner) -> Self {
        Self {
            inner,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Return the frozen plan for this metric, or run the cost model and
    /// freeze the result if this is the first request for the metric.
    pub fn plan(
        &self,
        workload: &QueryWorkload,
        wc: Option<&WorkloadCharacteristics>,
    ) -> CollectionPlan {
        let key = &workload.metric_name;

        // Fast path: return the cached plan if one exists.
        {
            let cache = self.cache.read().unwrap();
            if let Some(plan) = cache.get(key) {
                return plan.clone();
            }
        }

        // Slow path: first request for this metric — run cost optimisation.
        let plan = self.inner.plan(workload, wc);
        self.cache.write().unwrap().insert(key.clone(), plan.clone());
        plan
    }

    /// Explicitly clear the frozen plan for a metric so the next request
    /// triggers a fresh cost-model run.  Called by the rollback handler or
    /// any future re-plan endpoint.
    pub fn unfreeze(&self, metric: &str) {
        self.cache.write().unwrap().remove(metric);
    }

    /// Return the metric names for which a frozen plan exists.
    pub fn frozen_metrics(&self) -> Vec<String> {
        self.cache.read().unwrap().keys().cloned().collect()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;
    use crate::types::AggType;

    fn workload(metric: &str) -> QueryWorkload {
        QueryWorkload {
            metric_name:         metric.into(),
            label_filters:       HashMap::new(),
            group_by_labels:     vec![],
            aggregations:        vec![AggType::Quantile],
            time_window:         Duration::from_secs(300),
            repeat_every:        None,
            accuracy_sla:        0.01,
            latency_sla:         None,
            sketch_type_override: None,
            exact_required:      false,
            quantiles:           vec![0.99],
        }
    }

    fn planner() -> FreezeAfterFirstPlanner {
        FreezeAfterFirstPlanner::new(CostModelPlanner::new())
    }

    #[test]
    fn first_call_produces_a_plan() {
        let p = planner();
        let plan = p.plan(&workload("latency"), None);
        // Cost model picks the cheapest sketch that meets the SLA; just verify
        // we got a sketch-mode plan (not raw passthrough).
        assert!(plan.agent_config.transmit_sketch);
    }

    #[test]
    fn second_call_returns_same_plan() {
        let p = planner();
        let first  = p.plan(&workload("latency"), None);
        // Change the workload — the frozen planner must ignore it.
        let mut w2 = workload("latency");
        w2.aggregations = vec![AggType::Cardinality];
        let second = p.plan(&w2, None);
        assert_eq!(
            first.agent_config.sketch_type,
            second.agent_config.sketch_type,
            "frozen plan must not change even when workload changes"
        );
    }

    #[test]
    fn different_metrics_get_independent_plans() {
        let p = planner();
        let a = p.plan(&workload("metric_a"), None);
        let b = p.plan(&workload("metric_b"), None);
        // Both plans are valid (exact sketch type may differ by cost model
        // internals, but we just check they are independently produced).
        let _ = (a, b);
        assert_eq!(p.frozen_metrics().len(), 2);
    }

    #[test]
    fn unfreeze_allows_re_plan() {
        let p = planner();
        let first = p.plan(&workload("latency"), None);
        p.unfreeze("latency");
        assert!(p.frozen_metrics().is_empty());
        // After unfreeze the planner will run the cost model again on the same
        // workload and should produce an equivalent plan.
        let second = p.plan(&workload("latency"), None);
        assert_eq!(
            first.agent_config.sketch_type,
            second.agent_config.sketch_type,
            "same workload after unfreeze should produce the same sketch type"
        );
    }

    #[test]
    fn frozen_metrics_lists_all_seen_metrics() {
        let p = planner();
        p.plan(&workload("cpu"), None);
        p.plan(&workload("mem"), None);
        p.plan(&workload("cpu"), None); // repeat — should not double-count
        let mut metrics = p.frozen_metrics();
        metrics.sort();
        assert_eq!(metrics, vec!["cpu", "mem"]);
    }
}
