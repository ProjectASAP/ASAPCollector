/// OpAMP server implemented over WebSocket via axum.
///
/// Each OTel collector connects as an "agent" identified by the `X-Agent-ID`
/// request header.  The role (`agent` vs `backend`) is determined by the
/// optional `X-Agent-Role` header (defaults to `agent`).
///
/// The server pushes `RemoteConfig` JSON messages to agents and receives
/// `AgentStatus` messages back.  Optional `on_connect` / `on_disconnect`
/// callbacks allow callers to register/deregister scrape endpoints.
use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

// ── Wire types ────────────────────────────────────────────────────────────────

/// Config message pushed to an agent collector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    pub config_hash: String,
    pub yaml:        String,
}

/// Status report sent back from an agent after applying a config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentStatus {
    pub agent_id:    String,
    pub config_hash: String,
    pub healthy:     bool,
    pub error:       Option<String>,
}

// ── Role ──────────────────────────────────────────────────────────────────────

/// Role of a connected OTel collector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentRole {
    /// Edge / agent collector that produces sketches.
    Agent,
    /// Backend / aggregation collector that merges sketches.
    Backend,
}

impl AgentRole {
    fn from_header(value: &str) -> Self {
        match value.trim().to_lowercase().as_str() {
            "backend" => AgentRole::Backend,
            _         => AgentRole::Agent,
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

type AgentMap = HashMap<String, (mpsc::Sender<RemoteConfig>, AgentRole)>;

pub type OnConnectFn    = Arc<dyn Fn(String, AgentRole) + Send + Sync>;
pub type OnDisconnectFn = Arc<dyn Fn(String)            + Send + Sync>;

pub struct OpampServer {
    agents:        Arc<RwLock<AgentMap>>,
    on_connect:    Option<OnConnectFn>,
    on_disconnect: Option<OnDisconnectFn>,
}

impl Default for OpampServer {
    fn default() -> Self {
        Self {
            agents:        Arc::new(RwLock::new(HashMap::new())),
            on_connect:    None,
            on_disconnect: None,
        }
    }
}

impl Clone for OpampServer {
    fn clone(&self) -> Self {
        Self {
            agents:        Arc::clone(&self.agents),
            on_connect:    self.on_connect.clone(),
            on_disconnect: self.on_disconnect.clone(),
        }
    }
}

impl OpampServer {
    pub fn new() -> Self { Self::default() }

    /// Register a callback invoked when an agent connects.
    pub fn with_on_connect(mut self, f: impl Fn(String, AgentRole) + Send + Sync + 'static) -> Self {
        self.on_connect = Some(Arc::new(f));
        self
    }

    /// Register a callback invoked when an agent disconnects.
    pub fn with_on_disconnect(mut self, f: impl Fn(String) + Send + Sync + 'static) -> Self {
        self.on_disconnect = Some(Arc::new(f));
        self
    }

    /// axum handler: `GET /v1/opamp` — upgrades to WebSocket.
    /// Agents must include `X-Agent-ID: <id>` in the upgrade request.
    /// Optional `X-Agent-Role: agent|backend` (default: `agent`).
    pub async fn ws_handler(
        ws:      WebSocketUpgrade,
        headers: HeaderMap,
        State(srv): State<Arc<OpampServer>>,
    ) -> impl IntoResponse {
        let agent_id = headers
            .get("X-Agent-ID")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        if agent_id.is_empty() {
            return (StatusCode::BAD_REQUEST, "X-Agent-ID header required").into_response();
        }

        let role = headers
            .get("X-Agent-Role")
            .and_then(|v| v.to_str().ok())
            .map(AgentRole::from_header)
            .unwrap_or(AgentRole::Agent);

        ws.on_upgrade(move |socket| handle_socket(socket, agent_id, role, srv))
            .into_response()
    }

    /// Pushes a config to a specific agent. Returns false if not connected.
    pub async fn push(&self, agent_id: &str, cfg: RemoteConfig) -> bool {
        if let Some((tx, _)) = self.agents.read().await.get(agent_id) {
            tx.send(cfg).await.is_ok()
        } else {
            false
        }
    }

    /// Broadcasts a config to every connected agent regardless of role.
    pub async fn push_all(&self, cfg: RemoteConfig) {
        let ids: Vec<String> = self.agents.read().await.keys().cloned().collect();
        for id in ids { self.push(&id, cfg.clone()).await; }
    }

    /// Broadcasts a config only to agents matching the given role.
    pub async fn push_to_role(&self, role: AgentRole, cfg: RemoteConfig) {
        let ids: Vec<String> = self.agents.read().await
            .iter()
            .filter(|(_, (_, r))| *r == role)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids { self.push(&id, cfg.clone()).await; }
    }

    /// Returns the IDs of currently connected agents (all roles).
    pub async fn connected_agents(&self) -> Vec<String> {
        self.agents.read().await.keys().cloned().collect()
    }

    /// Returns a map of agent_id → role for all connected agents.
    pub async fn connected_agents_with_roles(&self) -> HashMap<String, AgentRole> {
        self.agents.read().await
            .iter()
            .map(|(id, (_, role))| (id.clone(), role.clone()))
            .collect()
    }
}

async fn handle_socket(socket: WebSocket, agent_id: String, role: AgentRole, srv: Arc<OpampServer>) {
    let (tx, mut rx) = mpsc::channel::<RemoteConfig>(16);
    srv.agents.write().await.insert(agent_id.clone(), (tx, role.clone()));
    info!(agent = %agent_id, ?role, "agent connected");

    if let Some(cb) = &srv.on_connect {
        cb(agent_id.clone(), role);
    }

    let (mut ws_tx, mut ws_rx) = socket.split();

    // Forward channel messages → WebSocket.
    let writer_id = agent_id.clone();
    let write_task = tokio::spawn(async move {
        while let Some(cfg) = rx.recv().await {
            let json = serde_json::to_string(&cfg).unwrap_or_default();
            if ws_tx.send(Message::Text(json.into())).await.is_err() { break; }
            info!(agent = %writer_id, hash = %cfg.config_hash, "config pushed");
        }
    });

    // Receive AgentStatus messages.
    while let Some(Ok(msg)) = ws_rx.next().await {
        match msg {
            Message::Text(text) => match serde_json::from_str::<AgentStatus>(&text) {
                Ok(s) => info!(agent = %s.agent_id, healthy = s.healthy, "agent status"),
                Err(_) => warn!(agent = %agent_id, "unexpected message"),
            },
            Message::Close(_) => break,
            _ => {}
        }
    }

    write_task.abort();
    srv.agents.write().await.remove(&agent_id);
    info!(agent = %agent_id, "agent disconnected");

    if let Some(cb) = &srv.on_disconnect {
        cb(agent_id);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, routing::get};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn no_agents_push_returns_false() {
        let srv = Arc::new(OpampServer::new());
        let sent = srv.push("unknown", RemoteConfig {
            config_hash: "h".into(), yaml: "y".into()
        }).await;
        assert!(!sent);
    }

    #[tokio::test]
    async fn connected_agents_empty_initially() {
        let srv = Arc::new(OpampServer::new());
        assert!(srv.connected_agents().await.is_empty());
    }

    #[test]
    fn role_from_header() {
        assert_eq!(AgentRole::from_header("backend"), AgentRole::Backend);
        assert_eq!(AgentRole::from_header("agent"),   AgentRole::Agent);
        assert_eq!(AgentRole::from_header(""),        AgentRole::Agent);
        assert_eq!(AgentRole::from_header("BACKEND"), AgentRole::Backend);
    }
}
