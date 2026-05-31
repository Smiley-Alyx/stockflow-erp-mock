package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
)

type Server struct {
	httpServer *http.Server
	ready      atomic.Bool
}

type statusResponse struct {
	Status string `json:"status"`
}

func New(address string, logger *slog.Logger) *Server {
	server := &Server{}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", server.handleHealth)
	mux.HandleFunc("GET /ready", server.handleReady)

	server.httpServer = &http.Server{
		Addr:    address,
		Handler: requestLogger(logger, mux),
	}

	return server
}

func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, statusResponse{Status: "not_ready"})
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "ready"})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(payload)
}

func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Info("http request", "method", r.Method, "path", r.URL.Path)
		next.ServeHTTP(w, r)
	})
}
