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
use config::generate_backend_config_staged;
use monitor::{Endpoint, Scraper, ScrapedData, Thresholds, Violation};
use opamp::{AgentRole, OpampServer, RemoteConfig};
use planner::{CostModelPlanner, BaselinePlanner, ObjectiveWeights, OnlineMetricsStore, init_online_store, pareto_frontier, select_best};
use planner::online_cost_model;
use planner::stage_split::split_expr_by_stage;
use query_parser::parse_query_expr;
use replan::Replanner;
use store::{PlanStore, WorkloadStore};
use types::StageResourceBudgets;

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    analyzer:        Arc<Analyzer>,
    planner:         Arc<BaselinePlanner>,
    store:           Arc<PlanStore>,
    workload_store:  Arc<WorkloadStore>,
    opamp:           Arc<OpampServer>,
    scraper:         Arc<Scraper>,
    replanner:       Arc<Replanner>,
    online_store:    OnlineMetricsStore,
    opamp_endpoint:  String,
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
        Arc::new(
            OpampServer::new()
                .with_on_connect(move |agent_id, _role| {
                    // Convention: agent metrics endpoint at http://<agent_id>/metrics.
                    // Collectors should set their agent-id to "<host>:<port>" so this
                    // resolves correctly, or override CONTROLLER_METRICS_PATH.
                    let url     = format!("http://{agent_id}/metrics");
                    let sc      = Arc::clone(&sc);
                    let id_copy = agent_id.clone();
                    tokio::spawn(async move {
                        sc.add_endpoint(Endpoint::new(id_copy, url)).await;
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

    // ── BaselinePlanner backed by live EMA data ─────────────────────────────
    // Runs the full cost-model optimisation once per metric on the first
    // request, then locks in that plan as the baseline.  The Replanner resets
    // and re-optimises on SLA violation or plan expiry.
    let planner = Arc::new(BaselinePlanner::new(
        CostModelPlanner::new().with_online_store(Arc::clone(&online_store)),
    ));

    let plan_store     = Arc::new(PlanStore::new());
    let workload_store = Arc::new(WorkloadStore::new());

    // ── Replanner — closes the SP-8 feedback loop ─────────────────────────────
    let replanner = Arc::new(Replanner::new(
        Arc::clone(&planner),
        Arc::clone(&plan_store),
        Arc::clone(&workload_store),
        Arc::clone(&opamp_srv),
        Arc::clone(&scraper),
        opamp_ep.clone(),
    ));
    // Bind the late-binding cell so the violation callback can reach the replanner.
    *replanner_cell.write().await = Some(Arc::clone(&replanner));

    let replan_interval = Duration::from_secs(
        std::env::var("CONTROLLER_REPLAN_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300u64), // re-check plan expiry every 5 minutes
    );

    let state = AppState {
        analyzer:       Arc::new(Analyzer::new()),
        planner,
        store:          Arc::clone(&plan_store),
        workload_store: Arc::clone(&workload_store),
        opamp:          Arc::clone(&opamp_srv),
        scraper:        Arc::clone(&scraper),
        replanner:      Arc::clone(&replanner),
        online_store:   Arc::clone(&online_store),
        opamp_endpoint: opamp_ep,
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
        .route("/api/v1/cost-model",              get(handle_cost_model))
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
    let workload = match st.analyzer.analyze(spec) {
        Ok(w)  => w,
        Err(e) => return (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response(),
    };

    let mut plan = st.planner.plan(&workload, Some(&wc));

    // ── SP-9: single QueryExpr pipeline — parse → optimise → stage-split ─────
    // When query_string is present, run the full algebra pipeline and attach
    // the StagedPlan.  The SP-3 flat assignment remains the fallback when no
    // query_string is supplied.
    if let Some(ref qs) = query_string {
        match parse_query_expr(qs) {
            Err(e) => warn!(query = %qs, error = %e, "parse_query_expr failed; skipping staged_plan"),
            Ok(qe) => {
                let raw_bps = plan.transmission_cost_summary.raw_bytes_per_sec;
                let (opt_qe, _) = QueryOptimizer::new(raw_bps).optimize(qe);
                let budgets = StageResourceBudgets::from_workload_chars(&wc);
                plan.staged_plan = Some(split_expr_by_stage(&opt_qe, &budgets));
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
                let (opt_qe, _iters) = QueryOptimizer::new(raw_bps).optimize(qe);
                let budgets = StageResourceBudgets::from_workload_chars(&wc_for_algebra);
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
        analyzer:       Arc::new(Analyzer::new()),
        planner,
        store:          Arc::clone(&plan_store),
        workload_store: Arc::clone(&workload_store),
        opamp,
        scraper,
        replanner,
        online_store,
        opamp_endpoint: "ws://ctrl:4320/v1/opamp".into(),
    };
    let router = axum::Router::new()
        .route("/api/v1/plan",                  axum::routing::post(handle_plan))
        .route("/api/v1/plan/pareto",           axum::routing::post(handle_pareto))
        .route("/api/v1/plan/:metric",          axum::routing::get(handle_get_plan))
        .route("/api/v1/plan/:metric/rollback", axum::routing::post(handle_rollback))
        .route("/api/v1/plan/:metric/diff",     axum::routing::get(handle_plan_diff))
        .route("/api/v1/agents",                axum::routing::get(handle_agents))
        .route("/api/v1/cost-model",            axum::routing::get(handle_cost_model))
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
}
