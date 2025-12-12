package http

import (
	"context"
	"net/http"
	"sync"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/rs/zerolog"
)

// Server represents an HTTP server
type Server struct {
	storage    storage.StorageBackend
	httpServer *http.Server
	addr       string
	log        zerolog.Logger
	ready      bool
	mu         sync.RWMutex
	router     *Router
}

// NewServer creates a new HTTP server
func NewServer(addr string, storage storage.StorageBackend) *Server {
	s := &Server{
		storage: storage,
		addr:    addr,
		log:     logger.WithComponent("http"),
	}

	s.router = NewRouter(storage)

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: s.router.mux,
	}

	return s
}

// Start starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ready {
		return nil
	}

	s.log.Info().Str("addr", s.addr).Msg("Starting HTTP server")

	// Start server in a goroutine
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error().Err(err).Msg("HTTP server error")
		}
	}()

	s.ready = true
	s.log.Info().Str("addr", s.addr).Msg("HTTP server started")

	return nil
}

// Stop gracefully stops the HTTP server
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.ready {
		return nil
	}

	s.log.Info().Msg("Stopping HTTP server")

	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.httpServer.Close()
		return err
	}

	s.ready = false
	s.log.Info().Msg("HTTP server stopped")

	return nil
}

// Ready returns true if the server is ready
func (s *Server) Ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ready
}
