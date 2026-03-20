mod analyzer;
mod config;
mod monitor;
mod opamp;
mod planner;
mod store;
mod types;

use std::sync::Arc;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use tracing::info;

use analyzer::{Analyzer, QuerySpec};
use config::{generate_agent_config, build_precompute_jobs};
use opamp::{OpampServer, RemoteConfig};
use planner::CostModelPlanner;
use store::PlanStore;

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    analyzer:       Arc<Analyzer>,
    planner:        Arc<CostModelPlanner>,
    store:          Arc<PlanStore>,
    opamp:          Arc<OpampServer>,
    opamp_endpoint: String,
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

    let opamp_srv = Arc::new(OpampServer::new());

    let state = AppState {
        analyzer:       Arc::new(Analyzer::new()),
        planner:        Arc::new(CostModelPlanner::new()),
        store:          Arc::new(PlanStore::new()),
        opamp:          Arc::clone(&opamp_srv),
        opamp_endpoint: opamp_ep,
    };

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
        .route("/api/v1/plan/:metric",            get(handle_get_plan))
        .route("/api/v1/plan/:metric/rollback",   post(handle_rollback))
        .route("/api/v1/agents",                  get(handle_agents))
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
    let workload = match st.analyzer.analyze(spec) {
        Ok(w)  => w,
        Err(e) => return (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response(),
    };

    let mut plan = st.planner.plan(&workload);
    plan.precompute = build_precompute_jobs(&workload, &plan, "backend:4317");
    st.store.set(&workload.metric_name, plan.clone());

    if let Ok(yaml) = generate_agent_config(&plan.agent_config, &st.opamp_endpoint) {
        let hash = short_hash(&yaml);
        st.opamp.push_all(RemoteConfig { config_hash: hash, yaml }).await;
    }

    let agents = st.opamp.connected_agents().await;
    (StatusCode::OK, Json(json!({
        "metric":            workload.metric_name,
        "sketch_type":       plan.agent_config.sketch_type.to_string(),
        "mode":              plan.agent_config.mode.to_string(),
        "aggregate_by":      plan.agent_config.aggregate_by,
        "valid_until":       plan.valid_until,
        "agents_notified":   agents.len(),
        "precompute_jobs":   plan.precompute.len(),
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
    match st.store.rollback(&metric) {
        Ok(plan) => {
            if let Ok(yaml) = generate_agent_config(&plan.agent_config, &st.opamp_endpoint) {
                st.opamp.push_all(RemoteConfig { config_hash: short_hash(&yaml), yaml }).await;
            }
            (StatusCode::OK, Json(json!({ "metric": metric, "rolled_back": true }))).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn handle_agents(State(st): State<AppState>) -> impl IntoResponse {
    Json(st.opamp.connected_agents().await)
}

fn short_hash(s: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}
