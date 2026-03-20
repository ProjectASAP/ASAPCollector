/// OpAMP server implemented over WebSocket via axum.
///
/// Each OTel collector connects as an "agent" identified by the `X-Agent-ID`
/// request header.  The server pushes `RemoteConfig` JSON messages to agents
/// and receives `AgentStatus` messages back.
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

// ── Server ────────────────────────────────────────────────────────────────────

type AgentMap = HashMap<String, mpsc::Sender<RemoteConfig>>;

#[derive(Clone, Default)]
pub struct OpampServer {
    agents: Arc<RwLock<AgentMap>>,
}

impl OpampServer {
    pub fn new() -> Self { Self::default() }

    /// axum handler: `GET /v1/opamp` — upgrades to WebSocket.
    /// Agents must include `X-Agent-ID: <id>` in the upgrade request.
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
        ws.on_upgrade(move |socket| handle_socket(socket, agent_id, srv))
            .into_response()
    }

    /// Pushes a config to a specific agent. Returns false if not connected.
    pub async fn push(&self, agent_id: &str, cfg: RemoteConfig) -> bool {
        if let Some(tx) = self.agents.read().await.get(agent_id) {
            tx.send(cfg).await.is_ok()
        } else {
            false
        }
    }

    /// Broadcasts a config to every connected agent.
    pub async fn push_all(&self, cfg: RemoteConfig) {
        let ids: Vec<String> = self.agents.read().await.keys().cloned().collect();
        for id in ids { self.push(&id, cfg.clone()).await; }
    }

    /// Returns the IDs of currently connected agents.
    pub async fn connected_agents(&self) -> Vec<String> {
        self.agents.read().await.keys().cloned().collect()
    }
}

async fn handle_socket(socket: WebSocket, agent_id: String, srv: Arc<OpampServer>) {
    let (tx, mut rx) = mpsc::channel::<RemoteConfig>(16);
    srv.agents.write().await.insert(agent_id.clone(), tx);
    info!(agent = %agent_id, "agent connected");

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
}
