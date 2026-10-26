// Package server exposes the relay over HTTP and WebSocket.
package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/coder/websocket"

	"github.com/sparshchadha/sparshur-service/internal/config"
	"github.com/sparshchadha/sparshur-service/internal/relay"
)

// Server wires HTTP routes to the relay hub.
type Server struct {
	cfg config.Config
	hub *relay.Hub
	log *slog.Logger
}

// New creates a server backed by hub.
func New(cfg config.Config, hub *relay.Hub, log *slog.Logger) *Server {
	return &Server{cfg: cfg, hub: hub, log: log}
}

// Handler returns the HTTP routes:
//
//	GET  /healthz                     liveness probe
//	GET  /readyz                      readiness probe with occupancy stats
//	POST /v1/rooms                    mint a new random room ID
//	GET  /v1/rooms/{roomID}/ws?role=  WebSocket relay (role = mac | android)
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.HandleFunc("POST /v1/rooms", s.handleCreateRoom)
	mux.HandleFunc("GET /v1/rooms/{roomID}/ws", s.handleWS)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "stats": s.hub.Stats()})
}

func (s *Server) handleCreateRoom(w http.ResponseWriter, _ *http.Request) {
	id, err := relay.NewRoomID()
	if err != nil {
		s.log.Error("generate room id", "err", err)
		writeError(w, http.StatusInternalServerError, "could not generate room id")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"room_id": id})
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("roomID")
	if !relay.ValidRoomID(roomID) {
		writeError(w, http.StatusBadRequest, "invalid room id")
		return
	}
	role, ok := relay.ParseRole(r.URL.Query().Get("role"))
	if !ok {
		writeError(w, http.StatusBadRequest, `role must be "mac" or "android"`)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns: s.cfg.AllowedOrigins,
		// Payloads are E2EE ciphertext and do not compress; skip the CPU cost.
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		// Accept has already written an HTTP error response.
		s.log.Debug("websocket accept failed", "err", err)
		return
	}

	s.hub.Serve(r.Context(), conn, roomID, role, relay.Options{
		MaxMessageBytes: s.cfg.MaxMessageBytes,
		SendQueueSize:   s.cfg.SendQueueSize,
		PingInterval:    s.cfg.PingInterval,
		WriteTimeout:    s.cfg.WriteTimeout,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
