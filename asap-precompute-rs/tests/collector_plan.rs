use asap_precompute_rs::{CollectorPlan, CollectorPlanError, SketchType};
use serde_json::json;

fn plan(materializations: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "collector_id": "edge-a",
        "envelope": {
            "plan_id": 42,
            "generated_at_unix_ms": 10000,
            "planner_revision": "3afcba68f4e8397fb81e2be988f47120f63f7a39",
            "capability_snapshot_id": "caps-7"
        },
        "materializations": materializations
    }))
    .unwrap()
}

#[test]
fn backend_quantile_plan_projects_without_replanning() {
    let bytes = plan(json!([{
        "query_id": "q-quantile",
        "metric": "request_duration_seconds",
        "algorithm": "ddsketch",
        "parameters": {"alpha": 0.01},
        "group_by": ["service"],
        "window_secs": 60,
        "evidence_source": null
    }]));
    let plan = CollectorPlan::from_json(&bytes, "edge-a").unwrap();
    let configs = plan.to_precompute_config_set().unwrap();
    assert_eq!(configs.version, 42);
    assert_eq!(configs.configs.len(), 1);
    let config = &configs.configs[0];
    assert_eq!(config.sketch_type, SketchType::DDSketch);
    assert_eq!(config.metric_name, "request_duration_seconds");
    assert_eq!(config.aggregate_by, vec!["service"]);
    assert_eq!(config.sketch_params["relative_accuracy"], 0.01);
    assert!(config.transmit_sketch);
}

#[test]
fn certified_topk_plan_preserves_heap_and_evidence_contract() {
    let bytes = plan(json!([{
        "query_id": "q-topk",
        "metric": "requests_total",
        "algorithm": "countsketchwithheap",
        "parameters": {"width": 512, "depth": 5, "heap_size": 10},
        "group_by": ["zone"],
        "window_secs": 30,
        "evidence_source": "runtime-margin-monitor"
    }]));
    let configs = CollectorPlan::from_json(&bytes, "edge-a")
        .unwrap()
        .to_precompute_config_set()
        .unwrap();
    let config = &configs.configs[0];
    assert_eq!(config.sketch_type, SketchType::CountSketch);
    assert_eq!(config.sketch_params["heap_size"], 10.0);
}

#[test]
fn topk_without_evidence_is_rejected() {
    let bytes = plan(json!([{
        "query_id": "q-topk",
        "metric": "requests_total",
        "algorithm": "cmswithheap",
        "parameters": {"width": 512, "depth": 5, "heap_size": 10},
        "group_by": [],
        "window_secs": 30,
        "evidence_source": null
    }]));
    assert!(matches!(
        CollectorPlan::from_json(&bytes, "edge-a"),
        Err(CollectorPlanError::Materialization { .. })
    ));
}

#[test]
fn one_invalid_materialization_rejects_the_whole_plan() {
    let bytes = plan(json!([
        {
            "query_id": "good",
            "metric": "latency",
            "algorithm": "kll",
            "parameters": {"k": 269},
            "group_by": [],
            "window_secs": 60,
            "evidence_source": null
        },
        {
            "query_id": "bad",
            "metric": "users",
            "algorithm": "hll",
            "parameters": {},
            "group_by": [],
            "window_secs": 60,
            "evidence_source": null
        }
    ]));
    assert!(CollectorPlan::from_json(&bytes, "edge-a").is_err());
}

#[test]
fn plan_cannot_be_applied_to_another_collector() {
    assert!(matches!(
        CollectorPlan::from_json(&plan(json!([])), "edge-b"),
        Err(CollectorPlanError::WrongTarget { .. })
    ));
}
