/// OpAMP server implemented over WebSocket via axum.
///
/// Speaks the standard OpAMP protobuf protocol (ServerToAgent / AgentToServer)
/// so the `opampextension` in OTel Collectors can connect directly.
///
/// Each OTel collector connects as an "agent" identified by the `X-Agent-ID`
/// request header.  The role (`agent` vs `backend`) is determined by the
/// optional `X-Agent-Role` header (defaults to `agent`).
///
/// The server pushes `RemoteConfig` as an OpAMP `ServerToAgent.remote_config`
/// message (protobuf binary frame) and receives `AgentToServer` status reports.
use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

/// Generated OpAMP protobuf types (from proto/opamp.proto).
pub mod opamp_proto {
    include!(concat!(env!("OUT_DIR"), "/opamp.proto.rs"));
}

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

    // Forward channel messages → WebSocket as standard OpAMP protobuf.
    //
    // OpAMP WS wire format prepends each binary frame with a varint
    // header (`uint64(0)` today). See `opamp-go/internal/wsmessage.go`.
    // Without the header, the agent's `DecodeWSMessage` falls back to
    // "old format" and decodes successfully — which is why pushes
    // worked even before this fix. We add the header for spec
    // conformance so the agent never has to take the legacy path.
    let writer_id = agent_id.clone();
    let write_task = tokio::spawn(async move {
        while let Some(cfg) = rx.recv().await {
            // Build standard OpAMP ServerToAgent with RemoteConfig.
            let server_to_agent = encode_remote_config(&cfg);
            let mut payload = Vec::new();
            if server_to_agent.encode(&mut payload).is_err() {
                warn!(agent = %writer_id, "failed to encode OpAMP protobuf");
                continue;
            }
            // Prepend the wsMsgHeader varint (zero byte today; the
            // varint is `0u64`, which encodes to a single 0x00).
            let mut buf = Vec::with_capacity(1 + payload.len());
            buf.push(0u8);
            buf.extend_from_slice(&payload);
            if ws_tx.send(Message::Binary(buf.into())).await.is_err() { break; }
            info!(agent = %writer_id, hash = %cfg.config_hash, "config pushed (OpAMP protobuf)");
        }
    });

    // Receive AgentToServer protobuf messages.
    //
    // Strip the OpAMP wire-format header before decoding. Per
    // `opamp-go/internal/wsmessage.go::DecodeWSMessage`, the spec
    // header is a varint-encoded `uint64(0)` and is detected by a
    // leading 0 byte. Older clients send raw protobuf with no header
    // — in that case the first byte is the protobuf field tag and is
    // never zero (a tag-0 wire type is illegal), so the
    // "first-byte-is-zero" check is unambiguous.
    while let Some(Ok(msg)) = ws_rx.next().await {
        match msg {
            Message::Binary(data) => {
                let payload: &[u8] = if !data.is_empty() && data[0] == 0 {
                    // Spec format. Decode the varint header (always
                    // 0 today) and skip it.
                    match prost::encoding::decode_varint(&mut &data[..]) {
                        Ok(_hdr) => {
                            // Recompute consumed bytes = varint length.
                            // For the canonical zero header this is 1
                            // byte; for any future non-zero header
                            // it's `n` bytes from the unsigned LEB128
                            // encoding.
                            let mut tmp: &[u8] = data.as_ref();
                            let _ = prost::encoding::decode_varint(&mut tmp);
                            let consumed = data.len() - tmp.len();
                            &data[consumed..]
                        }
                        Err(_) => &data[..],
                    }
                } else {
                    &data[..]
                };
                match opamp_proto::AgentToServer::decode(payload) {
                    Ok(ats) => {
                        info!(agent = %agent_id, "received AgentToServer (OpAMP protobuf)");
                        // Log effective config if reported. Triggered by
                        // the `ReportFullState` flag on our outgoing
                        // ServerToAgent (see `encode_remote_config`).
                        if let Some(ec) = &ats.effective_config {
                            if let Some(cm) = &ec.config_map {
                                for (name, file) in &cm.config_map {
                                    let body_preview = String::from_utf8_lossy(
                                        &file.body[..file.body.len().min(160)]
                                    );
                                    info!(
                                        agent = %agent_id,
                                        config_name = %name,
                                        bytes = file.body.len(),
                                        preview = %body_preview.replace('\n', " ⏎ "),
                                        "agent reported effective config",
                                    );
                                }
                            }
                        }
                        // Log remote-config apply state — this is the
                        // signal that "the agent received our pushed
                        // RemoteConfig, attempted to apply it, and ended
                        // up in {Applied | Failed | Applying}".
                        // RemoteConfigStatuses enum values:
                        //   0 = Unset
                        //   1 = Applied
                        //   2 = Applying
                        //   3 = Failed
                        if let Some(rcs) = &ats.remote_config_status {
                            let status_str = match rcs.status {
                                0 => "Unset",
                                1 => "Applied",
                                2 => "Applying",
                                3 => "Failed",
                                n => {
                                    // Future spec-defined values fall through
                                    // here; surface the raw int rather than
                                    // claim a meaning.
                                    return_unknown_status(n)
                                }
                            };
                            let last_hash_hex = rcs
                                .last_remote_config_hash
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<String>();
                            if rcs.status == 3 {
                                warn!(
                                    agent = %agent_id,
                                    status = status_str,
                                    last_hash = %last_hash_hex,
                                    error = %rcs.error_message,
                                    "agent reported remote-config status",
                                );
                            } else {
                                info!(
                                    agent = %agent_id,
                                    status = status_str,
                                    last_hash = %last_hash_hex,
                                    "agent reported remote-config status",
                                );
                            }
                        }
                        // Log health if reported.
                        if let Some(health) = &ats.health {
                            info!(agent = %agent_id, healthy = health.healthy, "agent health");
                        }
                    }
                    Err(e) => warn!(agent = %agent_id, error = %e, "failed to decode AgentToServer"),
                }
            }
            // Also accept JSON for backward compatibility.
            Message::Text(text) => match serde_json::from_str::<AgentStatus>(&text) {
                Ok(s) => info!(agent = %s.agent_id, healthy = s.healthy, "agent status (legacy JSON)"),
                Err(_) => warn!(agent = %agent_id, "unexpected text message"),
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

/// Encode a `RemoteConfig` as a standard OpAMP `ServerToAgent` protobuf message.
///
/// The YAML config body is wrapped in:
///   ServerToAgent.remote_config.config.config_map[""].body = yaml_bytes
///
/// Format an unknown `RemoteConfigStatuses` int as a stable string
/// for logs. Pulled out into a helper to keep the match arm above
/// borrow-checker-friendly (returning a `&'static str`).
fn return_unknown_status(n: i32) -> &'static str {
    // Leak the formatted int into a `'static str` only if needed.
    // For diagnostic logs we accept the cost of a Box::leak per
    // unrecognised value since this is an "out-of-spec status"
    // signal that should be rare. Avoids reworking the surrounding
    // match into String.
    Box::leak(format!("Unknown({})", n).into_boxed_str())
}

/// This is the standard OpAMP way to push collector configuration.
/// The opampextension in the OTel Collector decodes this and applies the config.
///
/// Two protocol bits the controller sets per spec:
///
/// 1. `flags = ReportFullState` (`0x01`) asks the agent's next
///    `AgentToServer` to include the full status block —
///    `effective_config` (the YAML the agent ended up running)
///    and `remote_config_status` (Applied / Failed / Applying).
///    Without this, the agent is allowed to elide both fields as
///    an optimization once the controller has acknowledged a
///    given sequence_num, and we lose visibility into whether the
///    push actually took.
///
/// 2. `capabilities` advertises what the controller can accept
///    back. `AcceptsStatus` is mandatory; `OffersRemoteConfig`
///    must be set whenever we send `remote_config`;
///    `AcceptsEffectiveConfig` tells the agent it's worth
///    populating the field (some agents skip it if the server
///    didn't claim it could parse it).
fn encode_remote_config(cfg: &RemoteConfig) -> opamp_proto::ServerToAgent {
    let config_file = opamp_proto::AgentConfigFile {
        body: cfg.yaml.as_bytes().to_vec(),
        content_type: "text/yaml".to_string(),
    };

    let mut config_map = HashMap::new();
    config_map.insert(String::new(), config_file); // empty key = single config file

    let agent_config_map = opamp_proto::AgentConfigMap { config_map };

    let remote_config = opamp_proto::AgentRemoteConfig {
        config: Some(agent_config_map),
        config_hash: cfg.config_hash.as_bytes().to_vec(),
    };

    // ServerToAgentFlags_ReportFullState = 0x01.
    const FLAG_REPORT_FULL_STATE: u64 = 0x0000_0001;
    // ServerCapabilities bitmask:
    //   AcceptsStatus            = 0x01
    //   OffersRemoteConfig       = 0x02
    //   AcceptsEffectiveConfig   = 0x04
    const CAPS_DEFAULT: u64 = 0x01 | 0x02 | 0x04;

    opamp_proto::ServerToAgent {
        remote_config: Some(remote_config),
        flags: FLAG_REPORT_FULL_STATE,
        capabilities: CAPS_DEFAULT,
        ..Default::default()
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

    /// Helper: start a real OpAMP server on a random port, return the server
    /// Arc and the bound address.
    async fn start_server() -> (Arc<OpampServer>, std::net::SocketAddr) {
        let srv = Arc::new(OpampServer::new());
        let app = Router::new()
            .route("/v1/opamp", get(OpampServer::ws_handler))
            .with_state(Arc::clone(&srv));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (srv, addr)
    }

    /// Connect a WebSocket client with a given agent-id and role.
    async fn connect_ws_client(
        addr: std::net::SocketAddr,
        agent_id: &str,
        role: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        use tokio_tungstenite::tungstenite::http::Request;
        let req = Request::builder()
            .uri(format!("ws://{addr}/v1/opamp"))
            .header("Host", addr.to_string())
            .header("X-Agent-ID", agent_id)
            .header("X-Agent-Role", role)
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("Sec-WebSocket-Version", "13")
            .body(())
            .unwrap();
        let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        ws
    }

    /// `push_to_role` delivers a RemoteConfig to a connected agent-role client.
    #[tokio::test]
    async fn push_to_role_delivers_yaml_to_agent_role() {
        let (srv, addr) = start_server().await;
        let mut agent_ws = connect_ws_client(addr, "agent-1", "agent").await;

        // Allow the server to register the connection.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let yaml_payload = "ddsketch:\n  mode: window\n";
        srv.push_to_role(AgentRole::Agent, RemoteConfig {
            config_hash: "hash-1".into(),
            yaml: yaml_payload.to_string(),
        }).await;

        let msg = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            futures_util::StreamExt::next(&mut agent_ws),
        )
        .await
        .expect("timed out waiting for message")
        .unwrap()
        .unwrap();

        // Decode standard OpAMP protobuf binary frame.
        let data = msg.into_data();
        let sta = opamp_proto::ServerToAgent::decode(data.as_ref()).expect("valid protobuf");
        let rc = sta.remote_config.expect("should have remote_config");
        let config = rc.config.expect("should have config");
        let file = config.config_map.get("").expect("should have empty-key entry");
        let yaml = String::from_utf8(file.body.clone()).unwrap();
        assert_eq!(yaml, yaml_payload, "delivered yaml must match");
        assert_eq!(String::from_utf8(rc.config_hash).unwrap(), "hash-1", "delivered hash must match");
    }

    /// `push_to_role(Agent)` must not deliver to a backend-role client.
    #[tokio::test]
    async fn push_to_agent_role_does_not_reach_backend_role() {
        use futures_util::StreamExt;
        let (srv, addr) = start_server().await;
        let mut agent_ws   = connect_ws_client(addr, "agent-1",   "agent").await;
        let mut backend_ws = connect_ws_client(addr, "backend-1", "backend").await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        srv.push_to_role(AgentRole::Agent, RemoteConfig {
            config_hash: "hash-agent".into(),
            yaml: "ddsketch:\n  mode: batch\n".to_string(),
        }).await;

        // Agent must receive the message.
        let msg = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            agent_ws.next(),
        )
        .await
        .expect("agent timed out")
        .unwrap()
        .unwrap();
        let data = msg.into_data();
        let sta = opamp_proto::ServerToAgent::decode(data.as_ref()).expect("valid protobuf");
        let rc = sta.remote_config.expect("should have remote_config");
        assert_eq!(String::from_utf8(rc.config_hash).unwrap(), "hash-agent");

        // Backend must receive nothing within a short window.
        let backend_result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            backend_ws.next(),
        )
        .await;
        assert!(
            backend_result.is_err(),
            "backend-role client must not receive agent-role push"
        );
    }
}
