mod algebra;
mod analyzer;
mod config;
mod monitor;
mod opamp;
mod planner;
mod query_parser;
mod replan;
mod store;
mod types;

use std::sync::Arc;
use std::time::Duration;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use tracing::{info, warn};

use algebra::{QueryOptimizer, SketchAllocator};
use analyzer::{Analyzer, QuerySpec};
use config::{generate_agent_config, generate_backend_config, build_precompute_jobs};
use config::WorkloadRegistry;
use types::AgentCollectorConfig;
use config::generate_backend_config_staged;
use monitor::{Endpoint, Scraper, ScrapedData, Thresholds, Violation};
use opamp::{AgentRole, OpampServer, RemoteConfig};
use planner::{CostModelPlanner, BaselinePlanner, ObjectiveWeights, OnlineMetricsStore, init_online_store, pareto_frontier, select_best};
use planner::online_cost_model;
use planner::stage_split::split_expr_by_stage;
use planner::tco;
use algebra::physical::physical_plan_to_staged;
use query_parser::parse_query_expr;
use replan::Replanner;
use store::{PlanStore, WorkloadStore};
use types::StageResourceBudgets;

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    analyzer:          Arc<Analyzer>,
    planner:           Arc<BaselinePlanner>,
    store:             Arc<PlanStore>,
    workload_store:    Arc<WorkloadStore>,
    opamp:             Arc<OpampServer>,
    scraper:           Arc<Scraper>,
    replanner:         Arc<Replanner>,
    online_store:      OnlineMetricsStore,
    opamp_endpoint:    String,
    workload_registry: Arc<WorkloadRegistry>,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let api_addr   = std::env::var("CONTROLLER_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".into());
    let opamp_addr = std::env::var("CONTROLLER_OPAMP_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:4320".into());
    let opamp_ep   = std::env::var("CONTROLLER_OPAMP_ENDPOINT")
        .unwrap_or_else(|_| "ws://controller:4320/v1/opamp".into());
    let scrape_interval = Duration::from_secs(
        std::env::var("CONTROLLER_SCRAPE_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60u64),
    );

    // ── SP-5: Online EMA cost store ───────────────────────────────────────────
    let online_store = init_online_store();

    // ── SP-8: Prometheus scraper ──────────────────────────────────────────────
    // Violations are forwarded to the Replanner (built below).
    // We use an Arc<RwLock<Option<Arc<Replanner>>>> as a late-binding cell so
    // the scraper can hold a reference even though the Replanner is built after it.
    let replanner_cell: Arc<tokio::sync::RwLock<Option<Arc<Replanner>>>> =
        Arc::new(tokio::sync::RwLock::new(None));
    let registry_cell: Arc<tokio::sync::RwLock<Option<Arc<WorkloadRegistry>>>> =
        Arc::new(tokio::sync::RwLock::new(None));

    let scraper: Arc<Scraper> = {
        let ema  = Arc::clone(&online_store);
        let cell = Arc::clone(&replanner_cell);
        Arc::new(
            Scraper::new(
                vec![],
                Thresholds::default(),
                Arc::new(move |v: Violation| {
                    warn!(agent = %v.agent_id, kind = %v.kind,
                          observed = v.observed, threshold = v.threshold,
                          "SLA violation detected — triggering re-plan");
                    let cell = Arc::clone(&cell);
                    let agent_id = v.agent_id.clone();
                    tokio::spawn(async move {
                        if let Some(r) = cell.read().await.as_ref() {
                            r.handle_violation(&agent_id).await;
                        }
                    });
                }),
                scrape_interval,
            )
            .with_on_metrics(Arc::new(move |data: ScrapedData| {
                // Update EMA only when we know the sketch type and have a
                // CPU-per-sample estimate (requires at least 2 scrapes).
                if let (Some(st), Some(cpu)) = (data.sketch_type, data.cpu_micros_per_sample) {
                    let ema = Arc::clone(&ema);
                    tokio::spawn(async move {
                        planner::online_cost_model::update(
                            &ema, &st, data.sketch_size_bytes, cpu,
                        ).await;
                    });
                }
            })),
        )
    };

    // ── OpAMP server with connect/disconnect hooks ────────────────────────────
    let opamp_srv: Arc<OpampServer> = {
        let sc = Arc::clone(&scraper);
        let sd = Arc::clone(&scraper);
        let connect_cell = Arc::clone(&replanner_cell);
        let connect_registry = Arc::clone(&registry_cell);
        Arc::new(
            OpampServer::new()
                .with_on_connect(move |agent_id, _role| {
                    // Convention: agent metrics endpoint at http://<agent_id>/metrics.
                    // Collectors should set their agent-id to "<host>:<port>" so this
                    // resolves correctly, or override CONTROLLER_METRICS_PATH.
                    let url     = format!("http://{agent_id}/metrics");
                    let sc      = Arc::clone(&sc);
                    let id_copy = agent_id.clone();
                    let cell    = Arc::clone(&connect_cell);
                    let reg     = Arc::clone(&connect_registry);
                    let aid     = agent_id.clone();
                    tokio::spawn(async move {
                        sc.add_endpoint(Endpoint::new(id_copy, url)).await;
                        if let Some(r) = cell.read().await.as_ref() {
                            // Push the current plan config if this agent has a prior assignment.
                            let pushed = r.push_config_to_agent(&aid).await;
                            // If the agent has no prior assignment, assign it a workload
                            // from the registry (if available).
                            if !pushed {
                                if let Some(registry) = reg.read().await.as_ref() {
                                    if let Some(entry) = registry.first_for_role("agent") {
                                        r.register_agent(&aid, &entry.metric_name).await;
                                        r.push_config_to_agent(&aid).await;
                                    }
                                }
                            }
                        }
                    });
                })
                .with_on_disconnect(move |agent_id| {
                    let sd = Arc::clone(&sd);
                    tokio::spawn(async move {
                        sd.remove_endpoint(&agent_id).await;
                    });
                }),
        )
    };

    // ── Sketch defaults (YAML-configurable) ────────────────────────────────
    let sketch_defaults_path = std::env::var("CONTROLLER_SKETCH_DEFAULTS")
        .unwrap_or_else(|_| "sketch_params_default.yml".into());
    let sketch_defaults = types::SketchDefaults::load(&sketch_defaults_path);
    info!(path = %sketch_defaults_path, "loaded sketch defaults");

    // ── BaselinePlanner backed by live EMA data ─────────────────────────────
    // Runs the full cost-model optimisation once per metric on the first
    // request, then locks in that plan as the baseline.  The Replanner resets
    // and re-optimises on SLA violation or plan expiry.
    let planner = Arc::new(BaselinePlanner::new(
        CostModelPlanner::new()
            .with_sketch_defaults(sketch_defaults)
            .with_online_store(Arc::clone(&online_store)),
    ));

    let plan_store     = Arc::new(PlanStore::new());
    let workload_store = Arc::new(WorkloadStore::new());

    // ── Declarative workload registry ────────────────────────────────────────
    let workloads_path = std::env::var("CONTROLLER_WORKLOADS")
        .unwrap_or_else(|_| "workloads.yaml".into());
    let workload_registry = Arc::new(WorkloadRegistry::load(&workloads_path));

    // Pre-populate PlanStore from the registry so agents get a config immediately.
    {
        let analyzer = Analyzer::new();
        for entry in workload_registry.entries() {
            let spec = analyzer::QuerySpec {
                query_string:    entry.query_string.clone(),
                metric_name:     entry.metric_name.clone(),
                label_filters:   Default::default(),
                group_by_labels: vec![],
                aggregations:    vec!["quantile".into()],
                time_window:     "5m".into(),
                repeat_every:    None,
                accuracy_sla:    entry.accuracy_sla,
                latency_sla:     None,
                sketch_type:     None,
                workload:        types::WorkloadCharacteristics::default(),
                file_output_path: None,
            };
            match analyzer.analyze(spec) {
                Ok(wl) => {
                    let wc = types::WorkloadCharacteristics::default();
                    let plan = planner.plan(&wl, Some(&wc));
                    let metric_name = wl.metric_name.clone();
                    plan_store.set(&metric_name, plan);
                    workload_store.set(&metric_name, wl, wc);
                }
                Err(e) => {
                    warn!(metric = %entry.metric_name, error = %e,
                        "failed to pre-populate plan from workload registry");
                }
            }
        }
    }

    // ── Replanner — closes the SP-8 feedback loop ─────────────────────────────
    let replanner = Arc::new(Replanner::new(
        Arc::clone(&planner),
        Arc::clone(&plan_store),
        Arc::clone(&workload_store),
        Arc::clone(&opamp_srv),
        Arc::clone(&scraper),
        opamp_ep.clone(),
    ));
    // Bind the late-binding cells so callbacks can reach the replanner and registry.
    *replanner_cell.write().await = Some(Arc::clone(&replanner));
    *registry_cell.write().await = Some(Arc::clone(&workload_registry));

    let replan_interval = Duration::from_secs(
        std::env::var("CONTROLLER_REPLAN_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300u64), // re-check plan expiry every 5 minutes
    );

    let state = AppState {
        analyzer:          Arc::new(Analyzer::new()),
        planner,
        store:             Arc::clone(&plan_store),
        workload_store:    Arc::clone(&workload_store),
        opamp:             Arc::clone(&opamp_srv),
        scraper:           Arc::clone(&scraper),
        replanner:         Arc::clone(&replanner),
        online_store:      Arc::clone(&online_store),
        opamp_endpoint:    opamp_ep,
        workload_registry: Arc::clone(&workload_registry),
    };

    // ── Background tasks ──────────────────────────────────────────────────────
    tokio::spawn(Arc::clone(&scraper).run());
    tokio::spawn(Arc::clone(&replanner).run_expiry_ticker(replan_interval));

    // ── OpAMP WebSocket listener ──────────────────────────────────────────────
    let opamp_router = Router::new()
        .route("/v1/opamp", get(OpampServer::ws_handler))
        .with_state(Arc::clone(&opamp_srv));

    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(&opamp_addr).await.unwrap();
        info!("OpAMP server listening on {opamp_addr}");
        axum::serve(listener, opamp_router).await.unwrap();
    });

    // ── HTTP API ──────────────────────────────────────────────────────────────
    let app = Router::new()
        .route("/api/v1/plan",                    post(handle_plan))
        .route("/api/v1/plan/pareto",             post(handle_pareto))
        .route("/api/v1/plan/:metric",            get(handle_get_plan))
        .route("/api/v1/plan/:metric/rollback",   post(handle_rollback))
        .route("/api/v1/plan/:metric/diff",       get(handle_plan_diff))
        .route("/api/v1/agents",                  get(handle_agents))
        .route("/api/v1/config/:metric",          get(handle_get_config))
        .route("/api/v1/collector-config/agent",  get(handle_bootstrap_agent_config))
        .route("/api/v1/collector-config/backend", get(handle_bootstrap_backend_config))
        .route("/api/v1/cost-model",              get(handle_cost_model))
        .route("/api/v1/tco",                     post(handle_tco))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&api_addr).await.unwrap();
    info!("controller API listening on {api_addr}");
    axum::serve(listener, app).await.unwrap();
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn handle_plan(
    State(st): State<AppState>,
    Json(spec): Json<QuerySpec>,
) -> impl IntoResponse {
    let wc           = spec.workload.clone();
    let query_string = spec.query_string.clone();
    let file_output_path = spec.file_output_path.clone();
    let workload = match st.analyzer.analyze(spec) {
        Ok(w)  => w,
        Err(e) => return (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response(),
    };

    let mut plan = st.planner.plan(&workload, Some(&wc));
    plan.agent_config.file_output_path = file_output_path;

    // ── SP-9: single QueryExpr pipeline — parse → optimise → stage-split ─────
    // When query_string is present, run the full algebra pipeline and attach
    // the StagedPlan.  The SP-3 flat assignment remains the fallback when no
    // query_string is supplied.
    if let Some(ref qs) = query_string {
        match parse_query_expr(qs) {
            Err(e) => warn!(query = %qs, error = %e, "parse_query_expr failed; skipping staged_plan"),
            Ok(qe) => {
                let raw_bps = plan.transmission_cost_summary.raw_bytes_per_sec;
                let budgets = StageResourceBudgets::from_workload_chars(&wc);
                let constraints = algebra::optimizer::DeploymentConstraints::from_budgets(&budgets);
                let (opt_qe, _) = QueryOptimizer::with_constraints(raw_bps, constraints).optimize(qe);
                let (staged, _physical_tree) = physical_plan_to_staged(&opt_qe, &budgets);
                plan.staged_plan = Some(staged);
            }
        }
    }

    plan.precompute = build_precompute_jobs(&workload, &plan, "backend:4317");
    st.store.set(&workload.metric_name, plan.clone());
    // Persist workload so the replanner can re-run plan() without the original spec.
    let wc_for_algebra = wc.clone();
    st.workload_store.set(&workload.metric_name, workload.clone(), wc);

    // ── Push agent config to agent-role collectors ────────────────────────────
    if let Ok(agent_yaml) = generate_agent_config(&plan.agent_config, &st.opamp_endpoint) {
        let hash = short_hash(&agent_yaml);
        st.opamp.push_to_role(
            AgentRole::Agent,
            RemoteConfig { config_hash: hash, yaml: agent_yaml },
        ).await;
    }

    // ── Push backend config to backend-role collectors ────────────────────────
    // SP-9: pass the BackendSubPlan so the YAML gains a dedup processor when needed.
    let backend_staged = plan.staged_plan.as_ref().map(|sp| &sp.backend);
    if let Ok(backend_yaml) = generate_backend_config_staged(
        &plan.backend_config, backend_staged, &st.opamp_endpoint,
    ) {
        let hash = short_hash(&backend_yaml);
        st.opamp.push_to_role(
            AgentRole::Backend,
            RemoteConfig { config_hash: hash, yaml: backend_yaml },
        ).await;
    }

    // ── Update scrape-endpoint sketch types and agent→metric mapping ──────────
    let sketch_type = plan.agent_config.sketch_type.clone();
    for agent_id in st.opamp.connected_agents().await {
        st.scraper.set_sketch_type(&agent_id, sketch_type.clone()).await;
        st.replanner.register_agent(&agent_id, &workload.metric_name).await;
    }

    // ── Algebra pipeline: parse → optimise → allocate ─────────────────────────
    let raw_bps = plan.transmission_cost_summary.raw_bytes_per_sec;
    let plan_summary = query_string.as_deref().and_then(|qs| {
        match parse_query_expr(qs) {
            Err(e) => {
                warn!(query = qs, error = %e, "parse_query_expr failed; skipping plan_summary");
                None
            }
            Ok(qe) => {
                let budgets = StageResourceBudgets::from_workload_chars(&wc_for_algebra);
                let constraints = algebra::optimizer::DeploymentConstraints::from_budgets(&budgets);
                let (opt_qe, _iters) = QueryOptimizer::with_constraints(raw_bps, constraints).optimize(qe);
                let plan_node = SketchAllocator::new(budgets, raw_bps).allocate(opt_qe);
                Some(plan_node.summarise(raw_bps))
            }
        }
    });

    let agents = st.opamp.connected_agents().await;
    let cost = &plan.transmission_cost_summary;
    (StatusCode::OK, Json(json!({
        "metric":              workload.metric_name,
        "sketch_type":         plan.agent_config.sketch_type.to_string(),
        "mode":                plan.agent_config.mode.to_string(),
        "aggregate_by":        plan.agent_config.aggregate_by,
        "valid_until":         plan.valid_until,
        "agents_notified":     agents.len(),
        "precompute_jobs":     plan.precompute.len(),
        "delta_decision":      plan.delta_decision,
        "staged_plan":         plan.staged_plan,
        "transmission_costs": {
            "raw_bytes_per_sec":                   cost.raw_bytes_per_sec,
            "sketch_full_bytes_per_sec":            cost.sketch_full_bytes_per_sec,
            "sketch_delta_bytes_per_sec":           cost.sketch_delta_bytes_per_sec,
            "delta_cpu_overhead_micros_per_sample": cost.delta_cpu_overhead_micros_per_sample,
            "delta_memory_overhead_bytes":          cost.delta_memory_overhead_bytes,
            "estimated_fill_rate":                  cost.estimated_fill_rate,
            "flush_rate_hz":                        cost.flush_rate_hz,
        },
        "plan_summary": plan_summary,
    }))).into_response()
}

/// Request body for `POST /api/v1/plan/pareto`.
#[derive(serde::Deserialize)]
struct ParetoRequest {
    #[serde(flatten)]
    spec:    QuerySpec,
    #[serde(default)]
    weights: ObjectiveWeights,
}

/// Returns the Pareto frontier of collection plans for the given workload.
/// Each point is annotated with bandwidth, CPU, memory and accuracy objectives.
/// The caller can specify `weights` to get the frontier sorted by their
/// preferred trade-off.
async fn handle_pareto(
    State(st): State<AppState>,
    Json(req): Json<ParetoRequest>,
) -> impl IntoResponse {
    let wc = req.spec.workload.clone();
    let workload = match st.analyzer.analyze(req.spec) {
        Ok(w)  => w,
        Err(e) => return (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response(),
    };

    let frontier = pareto_frontier(&workload, &wc, req.weights, Some(&st.online_store));

    if frontier.is_empty() {
        return (StatusCode::UNPROCESSABLE_ENTITY,
            "no sketch meets the accuracy SLA for the given workload").into_response();
    }

    let best = select_best(&frontier, req.weights)
        .map(|p| p.sketch_type.to_string());

    let points: Vec<serde_json::Value> = frontier.iter().map(|p| json!({
        "sketch_type":             p.sketch_type.to_string(),
        "bandwidth_bytes_per_sec": p.bandwidth_bytes_per_sec,
        "cpu_micros_per_sample":   p.cpu_micros_per_sample,
        "memory_bytes":            p.memory_bytes,
        "estimated_error":         p.estimated_error,
    })).collect();

    (StatusCode::OK, Json(json!({
        "metric":   workload.metric_name,
        "frontier": points,
        "best":     best,
    }))).into_response()
}

async fn handle_get_plan(
    State(st): State<AppState>,
    Path(metric): Path<String>,
) -> impl IntoResponse {
    match st.store.get(&metric) {
        Ok(plan) => (StatusCode::OK, Json(json!({
            "metric":      metric,
            "sketch_type": plan.agent_config.sketch_type.to_string(),
            "valid_until": plan.valid_until,
        }))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn handle_rollback(
    State(st): State<AppState>,
    Path(metric): Path<String>,
) -> impl IntoResponse {
    // Reset the baseline so the next POST /api/v1/plan re-runs the cost
    // model and establishes a fresh baseline plan for this metric.
    st.planner.reset(&metric);
    match st.store.rollback(&metric) {
        Ok(plan) => {
            if let Ok(yaml) = generate_agent_config(&plan.agent_config, &st.opamp_endpoint) {
                st.opamp.push_to_role(AgentRole::Agent, RemoteConfig {
                    config_hash: short_hash(&yaml), yaml,
                }).await;
            }
            if let Ok(yaml) = generate_backend_config(&plan.backend_config, &st.opamp_endpoint) {
                st.opamp.push_to_role(AgentRole::Backend, RemoteConfig {
                    config_hash: short_hash(&yaml), yaml,
                }).await;
            }
            (StatusCode::OK, Json(json!({ "metric": metric, "rolled_back": true }))).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn handle_agents(State(st): State<AppState>) -> impl IntoResponse {
    Json(st.opamp.connected_agents_with_roles().await)
}

/// Returns a complete OTel collector YAML for the named metric's current plan.
/// Collectors can use this with the HTTP config provider:
///   --config=http://controller:8080/api/v1/config/<metric>
async fn handle_get_config(
    State(st): State<AppState>,
    Path(metric): Path<String>,
) -> impl IntoResponse {
    match st.store.get(&metric) {
        Ok(plan) => match generate_agent_config(&plan.agent_config, &st.opamp_endpoint) {
            Ok(yaml) => (
                StatusCode::OK,
                [("content-type", "application/yaml")],
                yaml,
            ).into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// Bootstrap YAML config for agent collectors.
///
/// Collectors start with:
///   `./collector --config "http://controller:8080/api/v1/collector-config/agent"`
///
/// Returns a minimal valid OTel Collector YAML with OTLP receiver + batch
/// processor + prometheus exporter.  The controller can later push updated
/// configs via OpAMP or the collector can re-fetch on reload.
async fn handle_bootstrap_agent_config(
    State(st): State<AppState>,
) -> impl IntoResponse {
    // Use a default DDSketch config as the bootstrap.
    let cfg = AgentCollectorConfig {
        output_mode:          types::OutputMode::Sketch,
        sketch_type:          types::SketchType::DDSketch,
        sketch_params:        types::SketchParams::default(),
        aggregate_by:         vec![],
        label_matchers:       vec![],
        window_duration:      Some(std::time::Duration::from_secs(60)),
        mode:                 types::ProcessorMode::Window,
        enable_self_monitoring: true,
        transmit_sketch:      true,
        drop_original:        true,
        delta_transmission:   false,
        delta_threshold:      0.0,
        file_output_path:     None,
        enable_series_id:     false,
        series_id_ttl_secs:   300,
    };
    match generate_agent_config(&cfg, &st.opamp_endpoint) {
        Ok(yaml) => (
            StatusCode::OK,
            [("content-type", "application/yaml")],
            yaml,
        ).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Bootstrap YAML config for backend (merge) collectors.
async fn handle_bootstrap_backend_config(
    State(st): State<AppState>,
) -> impl IntoResponse {
    let cfg = types::BackendCollectorConfig {
        merge_sketch_type: types::SketchType::DDSketch,
        group_by:          vec![],
    };
    match generate_backend_config(&cfg, &st.opamp_endpoint) {
        Ok(yaml) => (
            StatusCode::OK,
            [("content-type", "application/yaml")],
            yaml,
        ).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Returns the diff between the current and previous plan for `metric`.
/// 404 if the metric has no plan, 200 with `null` data if no previous plan exists.
async fn handle_plan_diff(
    State(st): State<AppState>,
    Path(metric): Path<String>,
) -> impl IntoResponse {
    match st.store.diff(&metric) {
        Ok(Some(diff)) => (StatusCode::OK, Json(json!({
            "metric": metric,
            "has_diff": true,
            "diff": diff,
        }))).into_response(),
        Ok(None) => (StatusCode::OK, Json(json!({
            "metric": metric,
            "has_diff": false,
        }))).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// Returns the current EMA cost model state — blended benchmark + observed costs
/// per sketch type.  Useful for diagnosing whether the online cost model has
/// received sufficient observations to meaningfully influence plan selection.
async fn handle_cost_model(State(st): State<AppState>) -> impl IntoResponse {
    let table = online_cost_model::effective_table(&st.online_store);
    let raw   = st.online_store.try_read();

    let entries: Vec<serde_json::Value> = table.iter().map(|(sketch_type, costs)| {
        let observations = raw.as_ref().ok()
            .and_then(|m| m.get(sketch_type))
            .map(|o| o.observations)
            .unwrap_or(0);
        json!({
            "sketch_type":               sketch_type.to_string(),
            "bw_bytes_per_series_per_sec": costs.bytes_per_series_per_sec,
            "cpu_micros_per_sample":       costs.cpu_micros_per_sample,
            "base_memory_bytes":           costs.base_memory_bytes,
            "relative_error":              costs.relative_error_at_default,
            "observations":                observations,
        })
    }).collect();

    (StatusCode::OK, Json(json!({ "sketches": entries }))).into_response()
}

// ── TCO endpoint ─────────────────────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct TcoRequest {
    workload: tco::TcoWorkload,
    pricing: Option<tco::CloudPricing>,
}

async fn handle_tco(Json(req): Json<TcoRequest>) -> impl IntoResponse {
    let pricing = req.pricing.unwrap_or_default();
    let estimate = tco::estimate_tco(&req.workload, &pricing);
    (StatusCode::OK, Json(estimate))
}

fn short_hash(s: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

// ── Test helpers ──────────────────────────────────────────────────────────────

/// Builds a minimal `AppState` + `Router` for integration tests.
/// No background tasks are started; OpAMP/scraper hold no real connections.
#[cfg(test)]
fn test_app() -> (AppState, axum::Router) {
    let online_store   = init_online_store();
    let plan_store     = Arc::new(PlanStore::new());
    let workload_store = Arc::new(WorkloadStore::new());
    let opamp          = Arc::new(OpampServer::new());
    let scraper        = Arc::new(Scraper::new(
        vec![], Thresholds::default(), Arc::new(|_| {}), Duration::from_secs(60),
    ));
    let planner = Arc::new(BaselinePlanner::new(
        CostModelPlanner::new().with_online_store(Arc::clone(&online_store)),
    ));
    let replanner = Arc::new(Replanner::new(
        Arc::clone(&planner),
        Arc::clone(&plan_store),
        Arc::clone(&workload_store),
        Arc::clone(&opamp),
        Arc::clone(&scraper),
        "ws://ctrl:4320/v1/opamp",
    ));
    let state = AppState {
        analyzer:          Arc::new(Analyzer::new()),
        planner,
        store:             Arc::clone(&plan_store),
        workload_store:    Arc::clone(&workload_store),
        opamp,
        scraper,
        replanner,
        online_store,
        opamp_endpoint:    "ws://ctrl:4320/v1/opamp".into(),
        workload_registry: Arc::new(WorkloadRegistry::empty()),
    };
    let router = axum::Router::new()
        .route("/api/v1/plan",                  axum::routing::post(handle_plan))
        .route("/api/v1/plan/pareto",           axum::routing::post(handle_pareto))
        .route("/api/v1/plan/:metric",          axum::routing::get(handle_get_plan))
        .route("/api/v1/plan/:metric/rollback", axum::routing::post(handle_rollback))
        .route("/api/v1/plan/:metric/diff",     axum::routing::get(handle_plan_diff))
        .route("/api/v1/agents",                axum::routing::get(handle_agents))
        .route("/api/v1/cost-model",            axum::routing::get(handle_cost_model))
        .route("/api/v1/tco",                   axum::routing::post(handle_tco))
        .with_state(state.clone());
    (state, router)
}

#[cfg(test)]
mod api_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn plan_spec(metric: &str) -> serde_json::Value {
        serde_json::json!({
            "metric_name":  metric,
            "aggregations": ["quantile"],
            "time_window":  "5m",
            "accuracy_sla": 0.01
        })
    }

    // ── POST /api/v1/plan ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn plan_happy_path() {
        let (_, app) = test_app();
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/plan")
            .header("content-type", "application/json")
            .body(Body::from(plan_spec("latency").to_string()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["metric"], "latency");
        assert!(body["sketch_type"].as_str().is_some());
        assert!(body["valid_until"].as_str().is_some());
    }

    #[tokio::test]
    async fn plan_invalid_spec_returns_422() {
        let (_, app) = test_app();
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/plan")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"metric_name":"","aggregations":["quantile"],"time_window":"5m","accuracy_sla":0.01}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn plan_invalid_aggregation_returns_422() {
        let (_, app) = test_app();
        let bad = serde_json::json!({
            "metric_name": "m", "aggregations": ["histogram"],
            "time_window": "5m", "accuracy_sla": 0.01
        });
        let req = Request::builder()
            .method("POST").uri("/api/v1/plan")
            .header("content-type", "application/json")
            .body(Body::from(bad.to_string())).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // ── GET /api/v1/plan/:metric ──────────────────────────────────────────────

    #[tokio::test]
    async fn get_plan_not_found_returns_404() {
        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/plan/nonexistent").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_plan_after_post() {
        let (_, app) = test_app();
        // POST first
        let post_req = Request::builder()
            .method("POST").uri("/api/v1/plan")
            .header("content-type", "application/json")
            .body(Body::from(plan_spec("cpu").to_string())).unwrap();
        let post_resp = app.clone().oneshot(post_req).await.unwrap();
        assert_eq!(post_resp.status(), StatusCode::OK);
        // Then GET
        let get_req = Request::builder()
            .uri("/api/v1/plan/cpu").body(Body::empty()).unwrap();
        let get_resp = app.oneshot(get_req).await.unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = body_json(get_resp).await;
        assert_eq!(body["metric"], "cpu");
    }

    // ── POST /api/v1/plan/:metric/rollback ────────────────────────────────────

    #[tokio::test]
    async fn rollback_no_previous_returns_400() {
        let (st, app) = test_app();
        // Seed one plan directly.
        use crate::planner::rules::RulesPlanner;
        let wl = crate::types::QueryWorkload {
            metric_name: "m".into(),
            label_filters: std::collections::HashMap::new(),
            group_by_labels: vec![],
            aggregations: vec![crate::types::AggType::Quantile],
            time_window: std::time::Duration::from_secs(300),
            repeat_every: None, accuracy_sla: 0.01, latency_sla: None,
            sketch_type_override: None, exact_required: false, quantiles: vec![],
        };
        st.store.set("m", RulesPlanner::new().plan(&wl));
        let req = Request::builder()
            .method("POST").uri("/api/v1/plan/m/rollback")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rollback_not_found_returns_400() {
        let (_, app) = test_app();
        let req = Request::builder()
            .method("POST").uri("/api/v1/plan/ghost/rollback")
            .body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── GET /api/v1/plan/:metric/diff ─────────────────────────────────────────

    #[tokio::test]
    async fn diff_no_previous_returns_has_diff_false() {
        let (_, app) = test_app();
        // POST a plan once.
        let req = Request::builder()
            .method("POST").uri("/api/v1/plan")
            .header("content-type", "application/json")
            .body(Body::from(plan_spec("rtt").to_string())).unwrap();
        app.clone().oneshot(req).await.unwrap();
        // Diff should exist but has_diff=false (only one version).
        let req = Request::builder()
            .uri("/api/v1/plan/rtt/diff").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["has_diff"], false);
    }

    #[tokio::test]
    async fn diff_not_found_returns_404() {
        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/plan/ghost/diff").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // ── GET /api/v1/cost-model ────────────────────────────────────────────────

    #[tokio::test]
    async fn cost_model_returns_all_sketch_types() {
        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/cost-model").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let sketches = body["sketches"].as_array().unwrap();
        assert!(sketches.len() >= 4, "expected at least 4 sketch types");
        for s in sketches {
            assert!(s["sketch_type"].as_str().is_some());
            assert!(s["observations"].as_u64().is_some());
        }
    }

    // ── POST /api/v1/plan/pareto ──────────────────────────────────────────────

    #[tokio::test]
    async fn pareto_returns_frontier_for_quantile() {
        let (_, app) = test_app();
        let body = serde_json::json!({
            "metric_name": "latency", "aggregations": ["quantile"],
            "time_window": "5m", "accuracy_sla": 0.02,
            "weights": { "bandwidth": 0.7, "cpu": 0.2, "memory": 0.1 }
        });
        let req = Request::builder()
            .method("POST").uri("/api/v1/plan/pareto")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let frontier = body["frontier"].as_array().unwrap();
        assert!(!frontier.is_empty(), "frontier should not be empty");
        assert!(body["best"].as_str().is_some(), "best sketch should be set");
    }

    // ── GET /api/v1/agents ────────────────────────────────────────────────────

    #[tokio::test]
    async fn agents_returns_empty_map_initially() {
        let (_, app) = test_app();
        let req = Request::builder()
            .uri("/api/v1/agents").body(Body::empty()).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        // No agents connected → empty object.
        assert_eq!(body, serde_json::json!({}));
    }

    // ── POST /api/v1/tco ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn tco_returns_valid_estimate() {
        let (_, app) = test_app();
        let body = serde_json::json!({
            "workload": {
                "series_count": 100000,
                "samples_per_sec": 1.0,
                "bytes_per_sample": 100,
                "scrape_interval_secs": 15,
                "queries_per_sec": 1.0,
                "query_window_secs": 300,
                "retention_days": 30,
                "sketch_compression_ratio": 0.05,
                "delta_compression_ratio": 0.3
            }
        });
        let req = Request::builder()
            .method("POST").uri("/api/v1/tco")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert!(body["before"]["total_dollars"].as_f64().unwrap() > 0.0);
        assert!(body["after"]["total_dollars"].as_f64().unwrap() > 0.0);
        assert!(body["savings_percent"].as_f64().unwrap() > 0.0);
    }

    // ── Integration: controller ↔ collector wiring ─────────────────────────

    /// Helper: start an OpAMP WebSocket server on a random port.
    /// Returns the (server Arc, local addr string).
    async fn start_opamp_server(opamp: Arc<OpampServer>) -> String {
        let router = axum::Router::new()
            .route("/v1/opamp", axum::routing::get(OpampServer::ws_handler))
            .with_state(opamp);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("127.0.0.1:{}", addr.port())
    }

    /// Connect a mock agent via WebSocket, returning the stream.
    async fn connect_agent(
        opamp_addr: &str,
        agent_id: &str,
        role: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let url = format!("ws://{opamp_addr}/v1/opamp");
        let mut req = url.into_client_request().unwrap();
        req.headers_mut().insert("X-Agent-ID", agent_id.parse().unwrap());
        req.headers_mut().insert("X-Agent-Role", role.parse().unwrap());
        let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        ws
    }

    /// Read the next binary WebSocket frame, decode as OpAMP ServerToAgent,
    /// and extract the YAML config body.
    async fn recv_config_yaml(
        ws: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    ) -> String {
        use tokio_tungstenite::tungstenite::Message;
        let msg = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            futures_util::StreamExt::next(ws),
        ).await.expect("timeout waiting for config push")
         .expect("stream ended")
         .expect("ws error");
        match msg {
            Message::Binary(data) => {
                let sta = <crate::opamp::opamp_proto::ServerToAgent as prost::Message>::decode(
                    data.as_slice(),
                ).expect("decode ServerToAgent");
                let rc = sta.remote_config.expect("remote_config present");
                let cm = rc.config.expect("config present");
                let file = cm.config_map.get("").expect("empty-key config file");
                String::from_utf8(file.body.clone()).expect("yaml is utf8")
            }
            other => panic!("expected binary frame, got {other:?}"),
        }
    }

    /// Test 1: Agent connects with workloads.yaml pre-populated, receives config on connect.
    #[tokio::test]
    async fn agent_receives_config_on_connect_via_workload_registry() {
        // Build a full AppState with a workload registry entry.
        let online_store   = init_online_store();
        let plan_store     = Arc::new(PlanStore::new());
        let workload_store = Arc::new(WorkloadStore::new());
        let opamp          = Arc::new(OpampServer::new());
        let scraper        = Arc::new(Scraper::new(
            vec![], Thresholds::default(), Arc::new(|_| {}), Duration::from_secs(60),
        ));
        let planner = Arc::new(BaselinePlanner::new(
            CostModelPlanner::new().with_online_store(Arc::clone(&online_store)),
        ));

        // Pre-populate plan store (simulating what main() does with workload registry).
        let analyzer = Analyzer::new();
        let spec = analyzer::QuerySpec {
            query_string:    None,
            metric_name:     "http_latency".into(),
            label_filters:   Default::default(),
            group_by_labels: vec![],
            aggregations:    vec!["quantile".into()],
            time_window:     "5m".into(),
            repeat_every:    None,
            accuracy_sla:    0.01,
            latency_sla:     None,
            sketch_type:     None,
            workload:        types::WorkloadCharacteristics::default(),
            file_output_path: None,
        };
        let wl = analyzer.analyze(spec).unwrap();
        let wc = types::WorkloadCharacteristics::default();
        let plan = planner.plan(&wl, Some(&wc));
        plan_store.set("http_latency", plan);
        workload_store.set("http_latency", wl, wc);

        // Build replanner and late-binding cells.
        let replanner_cell: Arc<tokio::sync::RwLock<Option<Arc<Replanner>>>> =
            Arc::new(tokio::sync::RwLock::new(None));
        let registry_cell: Arc<tokio::sync::RwLock<Option<Arc<WorkloadRegistry>>>> =
            Arc::new(tokio::sync::RwLock::new(None));

        let opamp_ep = "ws://127.0.0.1:0/v1/opamp".to_string();

        // Wire on_connect callback — same logic as main().
        let sc = Arc::clone(&scraper);
        let connect_cell = Arc::clone(&replanner_cell);
        let connect_registry = Arc::clone(&registry_cell);
        let opamp_srv = Arc::new(
            OpampServer::new()
                .with_on_connect(move |agent_id, _role| {
                    let url = format!("http://{agent_id}/metrics");
                    let sc = Arc::clone(&sc);
                    let id_copy = agent_id.clone();
                    let cell = Arc::clone(&connect_cell);
                    let reg = Arc::clone(&connect_registry);
                    let aid = agent_id.clone();
                    tokio::spawn(async move {
                        sc.add_endpoint(Endpoint::new(id_copy, url)).await;
                        if let Some(r) = cell.read().await.as_ref() {
                            let pushed = r.push_config_to_agent(&aid).await;
                            if !pushed {
                                if let Some(registry) = reg.read().await.as_ref() {
                                    if let Some(entry) = registry.first_for_role("agent") {
                                        r.register_agent(&aid, &entry.metric_name).await;
                                        r.push_config_to_agent(&aid).await;
                                    }
                                }
                            }
                        }
                    });
                }),
        );

        let replanner = Arc::new(Replanner::new(
            Arc::clone(&planner),
            Arc::clone(&plan_store),
            Arc::clone(&workload_store),
            Arc::clone(&opamp_srv),
            Arc::clone(&scraper),
            opamp_ep,
        ));

        // Build a workload registry with one entry matching the pre-populated plan.
        let registry = Arc::new(WorkloadRegistry::load("/nonexistent")); // empty
        // We'll create one inline with the correct metric name.
        let yaml = "- metric_name: http_latency\n  accuracy_sla: 0.01\n  assign_to_role: agent\n";
        let entries: Vec<crate::config::workloads::WorkloadEntry> =
            serde_yaml::from_str(yaml).unwrap();
        // WorkloadRegistry doesn't have a public constructor from entries, so we
        // test via the first_for_role interface that the on_connect path uses.
        // Bind the cells.
        *replanner_cell.write().await = Some(Arc::clone(&replanner));
        // We need a registry that returns "http_latency". Load trick:
        let tmp_path = "/tmp/datacollector_test_workloads.yaml";
        std::fs::write(tmp_path, yaml).unwrap();
        let registry = Arc::new(WorkloadRegistry::load(tmp_path));
        *registry_cell.write().await = Some(Arc::clone(&registry));

        // Start OpAMP WS server.
        let addr = start_opamp_server(Arc::clone(&opamp_srv)).await;

        // Connect a mock agent.
        let mut ws = connect_agent(&addr, "test-agent-1", "agent").await;

        // The on_connect callback should assign the workload and push config.
        let yaml_config = recv_config_yaml(&mut ws).await;

        // Verify the config has the expected sketch processor.
        assert!(
            yaml_config.contains("ddsketch:") || yaml_config.contains("KLL:"),
            "expected a sketch processor in the pushed config:\n{yaml_config}"
        );
        // Verify OpAMP extension is present.
        assert!(
            yaml_config.contains("opamp"),
            "pushed config should include opamp extension:\n{yaml_config}"
        );

        std::fs::remove_file(tmp_path).ok();
    }

    /// Test 2: Re-plan pushes config only to agents registered for that metric.
    #[tokio::test]
    async fn replan_pushes_only_to_registered_agent() {
        let online_store   = init_online_store();
        let plan_store     = Arc::new(PlanStore::new());
        let workload_store = Arc::new(WorkloadStore::new());
        let opamp_srv      = Arc::new(OpampServer::new());
        let scraper        = Arc::new(Scraper::new(
            vec![], Thresholds::default(), Arc::new(|_| {}), Duration::from_secs(60),
        ));
        let planner = Arc::new(BaselinePlanner::new(
            CostModelPlanner::new().with_online_store(Arc::clone(&online_store)),
        ));

        // Seed workload + plan for "metric_a".
        let analyzer = Analyzer::new();
        let spec = analyzer::QuerySpec {
            query_string:    None,
            metric_name:     "metric_a".into(),
            label_filters:   Default::default(),
            group_by_labels: vec![],
            aggregations:    vec!["quantile".into()],
            time_window:     "5m".into(),
            repeat_every:    None,
            accuracy_sla:    0.01,
            latency_sla:     None,
            sketch_type:     None,
            workload:        types::WorkloadCharacteristics::default(),
            file_output_path: None,
        };
        let wl = analyzer.analyze(spec).unwrap();
        let wc = types::WorkloadCharacteristics::default();
        let plan = planner.plan(&wl, Some(&wc));
        plan_store.set("metric_a", plan);
        workload_store.set("metric_a", wl, wc);

        let replanner = Arc::new(Replanner::new(
            Arc::clone(&planner),
            Arc::clone(&plan_store),
            Arc::clone(&workload_store),
            Arc::clone(&opamp_srv),
            Arc::clone(&scraper),
            "ws://ctrl:4320/v1/opamp",
        ));

        // Start OpAMP server and connect two agents.
        let addr = start_opamp_server(Arc::clone(&opamp_srv)).await;
        let mut ws_a = connect_agent(&addr, "agent-a", "agent").await;
        let mut ws_b = connect_agent(&addr, "agent-b", "agent").await;
        // Let connections register.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Register agent-a for metric_a, agent-b is NOT registered for metric_a.
        replanner.register_agent("agent-a", "metric_a").await;
        replanner.register_agent("agent-b", "metric_b").await;

        // Trigger replan for metric_a.
        let ok = replanner.replan_metric("metric_a").await;
        assert!(ok, "replan should succeed");

        // agent-a should receive a config push.
        let yaml_a = recv_config_yaml(&mut ws_a).await;
        assert!(!yaml_a.is_empty(), "agent-a should have received config");

        // agent-b should NOT receive anything (timeout).
        let result_b = tokio::time::timeout(
            Duration::from_millis(500),
            futures_util::StreamExt::next(&mut ws_b),
        ).await;
        assert!(
            result_b.is_err(),
            "agent-b should NOT receive config for metric_a replan"
        );
    }

    /// Test 3: Generated agent YAML contains extensions.opamp with correct endpoint.
    #[tokio::test]
    async fn generated_agent_yaml_contains_opamp_extension() {
        let endpoint = "ws://my-controller:4320/v1/opamp";
        let cfg = AgentCollectorConfig {
            output_mode:          types::OutputMode::Sketch,
            sketch_type:          types::SketchType::DDSketch,
            sketch_params:        types::SketchParams::default(),
            aggregate_by:         vec![],
            label_matchers:       vec![],
            window_duration:      Some(Duration::from_secs(60)),
            mode:                 types::ProcessorMode::Window,
            enable_self_monitoring: true,
            transmit_sketch:      true,
            drop_original:        true,
            delta_transmission:   false,
            delta_threshold:      0.0,
            file_output_path:     None,
            enable_series_id:     false,
            series_id_ttl_secs:   300,
        };
        let yaml = generate_agent_config(&cfg, endpoint).unwrap();

        // Parse the YAML to verify structure, not just substring matches.
        let doc: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();

        // 1. extensions.opamp.server.ws.endpoint matches the parameter.
        let opamp_ext = &doc["extensions"]["opamp"];
        assert!(
            !opamp_ext.is_null(),
            "YAML missing extensions.opamp:\n{yaml}"
        );
        let ws_endpoint = opamp_ext["server"]["ws"]["endpoint"].as_str().unwrap();
        assert_eq!(
            ws_endpoint, endpoint,
            "OpAMP endpoint mismatch"
        );

        // 2. service.extensions list includes "opamp".
        let svc_exts = doc["service"]["extensions"].as_sequence().unwrap();
        let has_opamp = svc_exts.iter().any(|v| v.as_str() == Some("opamp"));
        assert!(
            has_opamp,
            "service.extensions should include 'opamp':\n{yaml}"
        );

        // 3. The YAML is complete: has receivers, processors, exporters, service.pipelines.
        assert!(doc["receivers"]["otlp"].is_mapping(), "missing receivers.otlp");
        assert!(doc["exporters"]["prometheus"].is_mapping(), "missing exporters.prometheus");
        let pipeline = &doc["service"]["pipelines"]["metrics"];
        assert!(pipeline["receivers"].is_sequence(), "missing pipeline receivers");
        assert!(pipeline["processors"].is_sequence(), "missing pipeline processors");
        assert!(pipeline["exporters"].is_sequence(), "missing pipeline exporters");
    }

    #[tokio::test]
    async fn tco_with_custom_pricing() {
        let (_, app) = test_app();
        let body = serde_json::json!({
            "workload": {
                "series_count": 50000,
                "samples_per_sec": 1.0,
                "bytes_per_sample": 100,
                "scrape_interval_secs": 15,
                "queries_per_sec": 1.0,
                "query_window_secs": 300,
                "retention_days": 30,
                "sketch_compression_ratio": 0.05,
                "delta_compression_ratio": 0.3
            },
            "pricing": {
                "grafana_per_1k_series_1dpm": 8.0,
                "s3_storage_per_gb_month": 0.023,
                "s3_put_per_1k": 0.005,
                "s3_get_per_1k": 0.0004,
                "s3_transfer_per_gb": 0.09,
                "ec2_sketch_instance_per_hour": 0.384
            }
        });
        let req = Request::builder()
            .method("POST").uri("/api/v1/tco")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())).unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        // With higher Grafana pricing, before cost should be higher.
        assert!(body["before"]["ingestion_dollars"].as_f64().unwrap() > 0.0);
        assert!(body["monthly_savings_dollars"].as_f64().unwrap() > 0.0);
    }
}
