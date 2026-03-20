// Package opamp implements a lightweight OpAMP server that pushes OTel
// collector configs over persistent WebSocket connections.
//
// The OpAMP spec is at https://github.com/open-telemetry/opamp-spec
// This implementation covers the subset needed for Phase 1:
//   - Agents connect via WebSocket
//   - Server pushes RemoteConfig messages (YAML bytes) to connected agents
//   - Agents are identified by a string ID (e.g., "agent-east-1")
package opamp

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// RemoteConfig is the message the server sends to agents.
type RemoteConfig struct {
	// ConfigHash is a simple hash of the YAML so agents can detect no-ops.
	ConfigHash string `json:"config_hash"`
	// YAML is the full OTel collector YAML to apply.
	YAML string `json:"yaml"`
	// IssuedAt is when this config was generated.
	IssuedAt time.Time `json:"issued_at"`
}

// AgentStatus is the message agents send back after applying a config.
type AgentStatus struct {
	AgentID    string    `json:"agent_id"`
	ConfigHash string    `json:"config_hash"`
	Healthy    bool      `json:"healthy"`
	Error      string    `json:"error,omitempty"`
	ReportedAt time.Time `json:"reported_at"`
}

// agent represents a connected collector instance.
type agent struct {
	id     string
	conn   *websocket.Conn
	send   chan RemoteConfig
	logger *slog.Logger
}

// Server is the OpAMP WebSocket server.
type Server struct {
	upgrader websocket.Upgrader
	logger   *slog.Logger

	mu     sync.RWMutex
	agents map[string]*agent // keyed by agent ID
}

// New creates a new OpAMP server.
func New(logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		logger: logger,
		agents: make(map[string]*agent),
	}
}

// Handler returns an http.HandlerFunc that accepts WebSocket connections from
// OTel collectors. Agents must include an "X-Agent-ID" header.
func (s *Server) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		agentID := r.Header.Get("X-Agent-ID")
		if agentID == "" {
			http.Error(w, "X-Agent-ID header required", http.StatusBadRequest)
			return
		}

		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			s.logger.Error("websocket upgrade failed", "agent", agentID, "err", err)
			return
		}

		a := &agent{
			id:     agentID,
			conn:   conn,
			send:   make(chan RemoteConfig, 4),
			logger: s.logger.With("agent", agentID),
		}

		s.mu.Lock()
		s.agents[agentID] = a
		s.mu.Unlock()

		s.logger.Info("agent connected", "agent", agentID)

		go a.writeLoop()
		a.readLoop(func(status AgentStatus) {
			s.logger.Info("agent status", "agent", status.AgentID,
				"healthy", status.Healthy, "config_hash", status.ConfigHash)
		})

		s.mu.Lock()
		delete(s.agents, agentID)
		s.mu.Unlock()

		s.logger.Info("agent disconnected", "agent", agentID)
	}
}

// Push sends a RemoteConfig to a specific agent. If the agent is not connected
// it returns false.
func (s *Server) Push(agentID string, cfg RemoteConfig) bool {
	s.mu.RLock()
	a, ok := s.agents[agentID]
	s.mu.RUnlock()

	if !ok {
		return false
	}

	select {
	case a.send <- cfg:
		return true
	default:
		s.logger.Warn("agent send buffer full, dropping config", "agent", agentID)
		return false
	}
}

// PushAll broadcasts a RemoteConfig to every connected agent.
func (s *Server) PushAll(cfg RemoteConfig) {
	s.mu.RLock()
	ids := make([]string, 0, len(s.agents))
	for id := range s.agents {
		ids = append(ids, id)
	}
	s.mu.RUnlock()

	for _, id := range ids {
		s.Push(id, cfg)
	}
}

// ConnectedAgents returns the IDs of currently connected agents.
func (s *Server) ConnectedAgents() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]string, 0, len(s.agents))
	for id := range s.agents {
		out = append(out, id)
	}
	return out
}

// ── agent loops ───────────────────────────────────────────────────────────────

func (a *agent) writeLoop() {
	for cfg := range a.send {
		data, err := json.Marshal(cfg)
		if err != nil {
			a.logger.Error("marshal remote config", "err", err)
			continue
		}
		if err := a.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			a.logger.Error("write to agent", "err", err)
			return
		}
		a.logger.Info("config pushed", "config_hash", cfg.ConfigHash)
	}
}

func (a *agent) readLoop(onStatus func(AgentStatus)) {
	defer a.conn.Close()
	for {
		_, msg, err := a.conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway) {
				a.logger.Error("read from agent", "err", err)
			}
			return
		}
		var status AgentStatus
		if err := json.Unmarshal(msg, &status); err != nil {
			a.logger.Warn("unexpected message from agent", "raw", string(msg))
			continue
		}
		status.ReportedAt = time.Now()
		onStatus(status)
	}
}
