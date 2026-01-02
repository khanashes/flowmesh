package metrics

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/logger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

// Server represents a standalone metrics HTTP server
type Server struct {
	httpServer *http.Server
	addr       string
	registry   *prometheus.Registry
	log        zerolog.Logger
	ready      bool
	mu         sync.RWMutex
}

// NewServer creates a new metrics server
func NewServer(addr string, registry *prometheus.Registry) *Server {
	return &Server{
		addr:     addr,
		registry: registry,
		log:      logger.WithComponent("metrics.server"),
	}
}

// Start starts the metrics server
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ready {
		return nil
	}

	// Create HTTP handler with custom registry
	mux := http.NewServeMux()
	if s.registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{}))
	} else {
		// Fallback to default registry
		mux.Handle("/metrics", promhttp.Handler())
	}

	s.httpServer = &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	s.log.Info().Str("addr", s.addr).Msg("Starting metrics server")

	// Start server in a goroutine
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error().Err(err).Msg("Metrics server error")
		}
	}()

	s.ready = true
	s.log.Info().Str("addr", s.addr).Msg("Metrics server started")

	return nil
}

// Stop gracefully stops the metrics server
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.ready {
		return nil
	}

	s.log.Info().Msg("Stopping metrics server")

	if err := s.httpServer.Shutdown(ctx); err != nil {
		//nolint:errcheck // Ignore close error if shutdown failed
		_ = s.httpServer.Close()
		return err
	}

	s.ready = false
	s.log.Info().Msg("Metrics server stopped")

	return nil
}

// Ready returns true if the server is ready
func (s *Server) Ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ready
}
