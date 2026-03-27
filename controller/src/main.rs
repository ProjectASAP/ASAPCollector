mod analyzer;
mod config;
mod monitor;
mod opamp;
mod planner;
mod query_parser;
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

use analyzer::{Analyzer, QuerySpec};
use config::{generate_agent_config, generate_backend_config, build_precompute_jobs};
use monitor::{Endpoint, Scraper, ScrapedData, Thresholds, Violation};
use opamp::{AgentRole, OpampServer, RemoteConfig};
use planner::{CostModelPlanner, OnlineMetricsStore, init_online_store};
use store::PlanStore;

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    analyzer:       Arc<Analyzer>,
    planner:        Arc<CostModelPlanner>,
    store:          Arc<PlanStore>,
    opamp:          Arc<OpampServer>,
    scraper:        Arc<Scraper>,
    online_store:   OnlineMetricsStore,
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
    let scrape_interval = Duration::from_secs(
        std::env::var("CONTROLLER_SCRAPE_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60u64),
    );

    // ── SP-5: Online EMA cost store ───────────────────────────────────────────
    let online_store = init_online_store();

    // ── SP-8: Prometheus scraper ──────────────────────────────────────────────
    // Endpoints are added dynamically as collectors connect via OpAMP.
    let scraper: Arc<Scraper> = {
        let ema = Arc::clone(&online_store);
        Arc::new(
            Scraper::new(
                vec![],
                Thresholds::default(),
                Arc::new(|v: Violation| {
                    warn!(agent = %v.agent_id, kind = %v.kind,
                          observed = v.observed, threshold = v.threshold,
                          "SLA violation detected — queuing re-plan");
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

    // ── CostModelPlanner backed by live EMA data ──────────────────────────────
    let planner = Arc::new(
        CostModelPlanner::new().with_online_store(Arc::clone(&online_store)),
    );

    let state = AppState {
        analyzer:     Arc::new(Analyzer::new()),
        planner,
        store:        Arc::new(PlanStore::new()),
        opamp:        Arc::clone(&opamp_srv),
        scraper:      Arc::clone(&scraper),
        online_store: Arc::clone(&online_store),
        opamp_endpoint: opamp_ep,
    };

    // ── Start scraper background loop ─────────────────────────────────────────
    tokio::spawn(Arc::clone(&scraper).run());

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
        .route("/api/v1/config/:metric",          get(handle_get_config))
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
    let wc = spec.workload.clone();
    let workload = match st.analyzer.analyze(spec) {
        Ok(w)  => w,
        Err(e) => return (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()).into_response(),
    };

    let mut plan = st.planner.plan(&workload, Some(&wc));
    plan.precompute = build_precompute_jobs(&workload, &plan, "backend:4317");
    st.store.set(&workload.metric_name, plan.clone());

    // ── Push agent config to agent-role collectors ────────────────────────────
    if let Ok(agent_yaml) = generate_agent_config(&plan.agent_config, &st.opamp_endpoint) {
        let hash = short_hash(&agent_yaml);
        st.opamp.push_to_role(
            AgentRole::Agent,
            RemoteConfig { config_hash: hash, yaml: agent_yaml },
        ).await;
    }

    // ── Push backend config to backend-role collectors ────────────────────────
    if let Ok(backend_yaml) = generate_backend_config(&plan.backend_config, &st.opamp_endpoint) {
        let hash = short_hash(&backend_yaml);
        st.opamp.push_to_role(
            AgentRole::Backend,
            RemoteConfig { config_hash: hash, yaml: backend_yaml },
        ).await;
    }

    // ── Update scrape-endpoint sketch types for EMA attribution ───────────────
    let sketch_type = plan.agent_config.sketch_type.clone();
    for agent_id in st.opamp.connected_agents().await {
        st.scraper.set_sketch_type(&agent_id, sketch_type.clone()).await;
    }

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
        "transmission_costs": {
            "raw_bytes_per_sec":                   cost.raw_bytes_per_sec,
            "sketch_full_bytes_per_sec":            cost.sketch_full_bytes_per_sec,
            "sketch_delta_bytes_per_sec":           cost.sketch_delta_bytes_per_sec,
            "delta_cpu_overhead_micros_per_sample": cost.delta_cpu_overhead_micros_per_sample,
            "delta_memory_overhead_bytes":          cost.delta_memory_overhead_bytes,
            "estimated_fill_rate":                  cost.estimated_fill_rate,
            "flush_rate_hz":                        cost.flush_rate_hz,
        },
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

fn short_hash(s: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}
