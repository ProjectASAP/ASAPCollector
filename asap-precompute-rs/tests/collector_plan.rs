use asap_precompute_rs::collector_plan::{
    CollectorPlanLifecycle, PlanPhase, SamplingEstimator, SamplingPolicy, SummaryWindowFramework,
    TransmissionMode,
};
use asap_precompute_rs::frame_identity::{FrameSequencer, SummaryFrameKind};
use asap_precompute_rs::{CollectorPlan, CollectorPlanError, SketchType};
use serde_json::json;

fn plan(materializations: serde_json::Value) -> Vec<u8> {
    let mut materializations = materializations;
    let mut rules = Vec::new();
    for (index, materialization) in materializations
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        let fingerprint = 9_001 + index as u64;
        let emit_every_ms = materialization["window_secs"].as_u64().unwrap() * 1_000;
        materialization
            .as_object_mut()
            .unwrap()
            .insert("materialization".into(), json!(fingerprint));
        let object = materialization.as_object_mut().unwrap();
        object
            .entry("abstract_window_framework")
            .or_insert(json!("tumbling"));
        object
            .entry("window_implementation_id")
            .or_insert(json!("collector-tumbling-v1"));
        object
            .entry("pane_secs")
            .or_insert(json!(emit_every_ms / 1_000));
        object
            .entry("state_layout")
            .or_insert(json!("anchored-pane-v1"));
        materialization.as_object_mut().unwrap().insert(
            "lifecycle".into(),
            json!({
                "kind": "continuously_maintained",
                "maintenance_mode": "incremental",
                "evaluation_schedule": "per_update",
                "output_representation": "summary_state"
            }),
        );
        rules.push(json!({
            "materialization": fingerprint,
            "producer_id": "edge-a",
            "schema_id": format!("summary-state-v1-{fingerprint}"),
            "mode": "full",
            "encoding": "sketchlib_protobuf_v1",
            "emit_every_ms": emit_every_ms,
            "full_checkpoint_every_ms": null,
            "destination_ref": "asap-backend-primary"
        }));
    }
    serde_json::to_vec(&json!({
        "collector_id": "edge-a",
        "envelope": {
            "plan_id": 42,
            "plan_version": 1,
            "generated_at_unix_ms": 10_000,
            "activation_unix_ms": 11_000,
            "expiry_unix_ms": null,
            "backend_compat": "asap-query-backend.v1",
            "planner_revision": "264937ec4a06e260920c7e583bffed34cc07dd64",
            "capability_snapshot_id": "caps-7"
        },
        "materializations": materializations,
        "transmission_rules": rules
    }))
    .unwrap()
}

fn quantile_plan() -> CollectorPlan {
    CollectorPlan::from_json(
        &plan(json!([{
            "query_id": "q",
            "metric": "request_duration_seconds",
            "algorithm": "ddsketch",
            "parameters": {"alpha": 0.01},
            "group_by": ["service"],
            "window_secs": 60,
            "evidence_source": null
        }])),
        "edge-a",
    )
    .unwrap()
}

#[test]
fn backend_quantile_plan_projects_without_replanning() {
    let configs = quantile_plan().to_precompute_config_set().unwrap();
    assert_eq!(configs.version, 1);
    assert_eq!(configs.configs.len(), 1);
    let config = &configs.configs[0];
    assert_eq!(config.agg_id, 9_001);
    assert_eq!(config.sketch_type, SketchType::DDSketch);
    assert_eq!(config.metric_name, "request_duration_seconds");
    assert_eq!(config.aggregate_by, vec!["service"]);
    assert_eq!(config.sketch_params["relative_accuracy"], 0.01);
    assert!(config.transmit_sketch);
}

#[test]
fn unsupported_planner_window_realization_is_rejected() {
    let mut plan = quantile_plan();
    plan.materializations[0].abstract_window_framework = SummaryWindowFramework::Sliding;
    assert!(matches!(
        plan.to_precompute_config_set(),
        Err(CollectorPlanError::Materialization { .. })
    ));

    let mut plan = quantile_plan();
    plan.materializations[0].pane_secs = 30;
    assert!(matches!(
        plan.to_precompute_config_set(),
        Err(CollectorPlanError::Materialization { .. })
    ));
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
fn runtime_policy_projects_sampling_delta_and_gos() {
    let mut value: serde_json::Value = serde_json::from_slice(&plan(json!([{
        "query_id": "q-frequency",
        "metric": "requests_total",
        "algorithm": "countsketch",
        "parameters": {"width": 512, "depth": 5},
        "group_by": ["zone"],
        "window_secs": 30,
        "evidence_source": null
    }])))
    .unwrap();
    value["transmission_rules"][0]["mode"] = json!("delta");
    value["transmission_rules"][0]["full_checkpoint_every_ms"] = json!(60_000);
    value["transmission_rules"][0]["runtime_policy"] = json!({
        "sampling": {"mode": "disabled"},
        "delta": {
            "absolute_threshold": 4.0,
            "gos": {"epsilon_staleness": 0.05, "sites": 4, "threshold_mode": "isotropic"}
        }
    });
    let parsed = CollectorPlan::from_json(&serde_json::to_vec(&value).unwrap(), "edge-a").unwrap();
    let config = &parsed.to_precompute_config_set().unwrap().configs[0];
    assert!(config.delta_transmission);
    assert_eq!(config.delta_threshold, 4);
    assert_eq!(config.sketch_params["gos_delta_epsilon"], 0.05);
    assert_eq!(config.sketch_params["gos_sites"], 4.0);
}

#[test]
fn hll_hash_sampling_is_bound_to_the_runtime_config() {
    let mut value: serde_json::Value = serde_json::from_slice(&plan(json!([{
        "query_id": "q-cardinality",
        "metric": "users",
        "algorithm": "hll",
        "parameters": {"precision": 14},
        "group_by": [],
        "window_secs": 30,
        "evidence_source": null
    }])))
    .unwrap();
    value["transmission_rules"][0]["runtime_policy"] = json!({
        "sampling": {"mode": "fixed", "probability": 0.25, "estimator": "hash_threshold"},
        "delta": null
    });
    let parsed = CollectorPlan::from_json(&serde_json::to_vec(&value).unwrap(), "edge-a").unwrap();
    assert!(matches!(
        parsed.transmission_rules[0].runtime_policy.sampling,
        SamplingPolicy::Fixed {
            probability,
            estimator: SamplingEstimator::HashThreshold
        } if probability == 0.25
    ));
    assert_eq!(
        parsed.to_precompute_config_set().unwrap().configs[0].sketch_params["sample_p"],
        0.25
    );
}

#[test]
fn staged_activation_is_atomic_and_versioned() {
    let first = quantile_plan();
    let mut lifecycle = CollectorPlanLifecycle::default();
    lifecycle.stage(first.clone(), "edge-a", 10_500).unwrap();
    assert!(lifecycle.active().is_none());
    assert!(lifecycle.activate(42, 1, 10_999).is_err());
    lifecycle.activate(42, 1, 11_000).unwrap();
    assert_eq!(lifecycle.active().unwrap().envelope.plan_version, 1);

    let mut second = first;
    second.envelope.plan_version = 2;
    second.envelope.activation_unix_ms = 12_000;
    lifecycle.stage(second, "edge-a", 11_500).unwrap();
    assert_eq!(lifecycle.active().unwrap().envelope.plan_version, 1);
    lifecycle.activate(42, 2, 12_000).unwrap();
    assert_eq!(lifecycle.active().unwrap().envelope.plan_version, 2);
    assert!(lifecycle
        .statuses()
        .iter()
        .any(|status| status.plan_version == 1 && status.phase == PlanPhase::Draining));
    lifecycle.retire_drained();
    assert!(lifecycle
        .statuses()
        .iter()
        .any(|status| status.plan_version == 1 && status.phase == PlanPhase::Retired));
}

#[test]
fn frame_sequence_and_reserved_attributes_match_backend_contract() {
    let mut parsed = quantile_plan();
    parsed.transmission_rules[0].mode = TransmissionMode::Delta;
    parsed.transmission_rules[0].full_checkpoint_every_ms = Some(60_000);
    parsed.transmission_rules[0].runtime_policy.delta =
        Some(asap_precompute_rs::collector_plan::DeltaPolicy {
            absolute_threshold: 1.0,
            gos: None,
        });
    let rule = parsed.transmission_rules[0].clone();
    let mut sequencer = FrameSequencer::default();
    let full = sequencer
        .next(
            &parsed,
            &rule,
            "boot-7",
            "service=checkout,zone=a",
            100,
            200,
            1_000,
        )
        .unwrap();
    let delta = sequencer
        .next(
            &parsed,
            &rule,
            "boot-7",
            "service=checkout,zone=a",
            100,
            200,
            2_000,
        )
        .unwrap();
    let checkpoint = sequencer
        .next(
            &parsed,
            &rule,
            "boot-7",
            "service=checkout,zone=a",
            100,
            200,
            61_000,
        )
        .unwrap();
    assert_eq!(full.kind, SummaryFrameKind::Full);
    assert_eq!(delta.kind, SummaryFrameKind::Delta);
    assert_eq!(delta.base_checkpoint_id, full.checkpoint_id);
    assert_eq!(checkpoint.kind, SummaryFrameKind::Full);
    assert_eq!(checkpoint.sequence, 3);

    let attributes = delta.otlp_attributes();
    let expected_keys = [
        "asap.frame.backend_compat",
        "asap.frame.base_checkpoint_id",
        "asap.frame.encoding",
        "asap.frame.identity_version",
        "asap.frame.kind",
        "asap.frame.materialization",
        "asap.frame.plan_id",
        "asap.frame.plan_version",
        "asap.frame.producer_epoch",
        "asap.frame.producer_id",
        "asap.frame.schema_id",
        "asap.frame.sequence",
        "asap.frame.series_identity",
    ];
    assert_eq!(
        attributes.keys().map(String::as_str).collect::<Vec<_>>(),
        expected_keys
    );
    assert_eq!(attributes["asap.frame.kind"], "delta");
    assert_eq!(
        attributes["asap.frame.base_checkpoint_id"],
        full.checkpoint_id.unwrap()
    );

    let next_window = sequencer
        .next(
            &parsed,
            &rule,
            "boot-7",
            "service=checkout,zone=a",
            200,
            300,
            2_000,
        )
        .unwrap();
    let next_epoch = sequencer
        .next(
            &parsed,
            &rule,
            "boot-8",
            "service=checkout,zone=a",
            100,
            200,
            2_000,
        )
        .unwrap();
    let next_series = sequencer
        .next(
            &parsed,
            &rule,
            "boot-7",
            "service=checkout,zone=b",
            100,
            200,
            2_000,
        )
        .unwrap();
    assert_eq!(
        (next_window.sequence, next_window.kind),
        (1, SummaryFrameKind::Full)
    );
    assert_eq!(
        (next_epoch.sequence, next_epoch.kind),
        (1, SummaryFrameKind::Full)
    );
    assert_eq!(
        (next_series.sequence, next_series.kind),
        (1, SummaryFrameKind::Full)
    );
}

#[test]
fn backend_contract_fixture_is_accepted_as_one_atomic_plan() {
    let bytes = include_bytes!("fixtures/collector_plan_v1.json");
    let parsed = CollectorPlan::from_json(bytes, "edge-a").unwrap();
    assert_eq!(parsed.envelope.backend_compat, "asap-query-backend.v1");
    assert_eq!(
        parsed.materializations.len(),
        parsed.transmission_rules.len()
    );
}

#[test]
fn missing_or_wrong_transmission_binding_rejects_the_whole_plan() {
    let mut missing: serde_json::Value = serde_json::from_slice(&plan(json!([{
        "query_id": "q", "metric": "users", "algorithm": "hll",
        "parameters": {"precision": 14}, "group_by": [], "window_secs": 30,
        "evidence_source": null
    }])))
    .unwrap();
    missing["transmission_rules"] = json!([]);
    assert!(CollectorPlan::from_json(&serde_json::to_vec(&missing).unwrap(), "edge-a").is_err());

    let mut wrong: serde_json::Value = serde_json::from_slice(&plan(json!([{
        "query_id": "q", "metric": "users", "algorithm": "hll",
        "parameters": {"precision": 14}, "group_by": [], "window_secs": 30,
        "evidence_source": null
    }])))
    .unwrap();
    wrong["transmission_rules"][0]["producer_id"] = json!("edge-b");
    assert!(CollectorPlan::from_json(&serde_json::to_vec(&wrong).unwrap(), "edge-a").is_err());
}

#[test]
fn topk_without_evidence_is_rejected() {
    let bytes = plan(json!([{
        "query_id": "q-topk", "metric": "requests_total", "algorithm": "cmswithheap",
        "parameters": {"width": 512, "depth": 5, "heap_size": 10}, "group_by": [],
        "window_secs": 30, "evidence_source": null
    }]));
    assert!(matches!(
        CollectorPlan::from_json(&bytes, "edge-a"),
        Err(CollectorPlanError::Materialization { .. })
    ));
}

#[test]
fn unsupported_planner_lifecycle_is_rejected() {
    let mut value: serde_json::Value = serde_json::from_slice(&plan(json!([{
        "query_id": "q", "metric": "requests_total", "algorithm": "hll",
        "parameters": {"precision": 14}, "group_by": [], "window_secs": 30,
        "evidence_source": null
    }])))
    .unwrap();
    value["materializations"][0]["lifecycle"]["kind"] = json!("ephemeral");
    assert!(matches!(
        CollectorPlan::from_json(&serde_json::to_vec(&value).unwrap(), "edge-a"),
        Err(CollectorPlanError::Materialization { .. })
    ));
}

#[test]
fn one_invalid_materialization_rejects_the_whole_plan() {
    let bytes = plan(json!([
        {"query_id": "good", "metric": "latency", "algorithm": "kll",
         "parameters": {"k": 269}, "group_by": [], "window_secs": 60, "evidence_source": null},
        {"query_id": "bad", "metric": "users", "algorithm": "hll",
         "parameters": {}, "group_by": [], "window_secs": 60, "evidence_source": null}
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
