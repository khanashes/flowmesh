package api

import (
	"context"
	"sync"

	"github.com/flowmesh/engine/internal/api/auth"
	grpcapi "github.com/flowmesh/engine/internal/api/grpc"
	httpapi "github.com/flowmesh/engine/internal/api/http"
	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/rs/zerolog"
)

// Server manages both gRPC and HTTP servers
type Server struct {
	storage    storage.StorageBackend
	grpcServer *grpcapi.Server
	httpServer *httpapi.Server
	tokenStore auth.TokenStore
	log        zerolog.Logger
	ready      bool
	mu         sync.RWMutex
}

// Config holds configuration for the API server
type Config struct {
	GRPCAddr string
	HTTPAddr string
}

// NewServer creates a new API server
func NewServer(cfg Config, storage storage.StorageBackend) *Server {
	s := &Server{
		storage:    storage,
		tokenStore: auth.NewInMemoryTokenStore(),
		log:        logger.WithComponent("api"),
	}

	// Create default token for development/testing
	if _, err := s.tokenStore.(*auth.InMemoryTokenStore).AddDefaultToken(); err != nil {
		s.log.Warn().Err(err).Msg("Failed to create default token")
	}

	s.grpcServer = grpcapi.NewServer(cfg.GRPCAddr, storage, s.tokenStore)
	s.httpServer = httpapi.NewServer(cfg.HTTPAddr, storage)

	return s
}

// Start starts both gRPC and HTTP servers
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ready {
		return nil
	}

	s.log.Info().Msg("Starting API server")

	// Start storage first
	if err := s.storage.Start(ctx); err != nil {
		return err
	}

	// Start gRPC server
	if err := s.grpcServer.Start(ctx); err != nil {
		return err
	}

	// Start HTTP server
	if err := s.httpServer.Start(ctx); err != nil {
		// Stop gRPC server if HTTP fails
		s.grpcServer.Stop(ctx)
		return err
	}

	s.ready = true
	s.log.Info().Msg("API server started")

	return nil
}

// Stop gracefully stops both servers
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.ready {
		return nil
	}

	s.log.Info().Msg("Stopping API server")

	// Stop HTTP server first
	if err := s.httpServer.Stop(ctx); err != nil {
		s.log.Warn().Err(err).Msg("Error stopping HTTP server")
	}

	// Stop gRPC server
	if err := s.grpcServer.Stop(ctx); err != nil {
		s.log.Warn().Err(err).Msg("Error stopping gRPC server")
	}

	// Stop storage
	if err := s.storage.Stop(ctx); err != nil {
		s.log.Warn().Err(err).Msg("Error stopping storage")
	}

	s.ready = false
	s.log.Info().Msg("API server stopped")

	return nil
}

// Ready returns true if the server is ready
func (s *Server) Ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ready && s.grpcServer.Ready() && s.httpServer.Ready() && s.storage.Ready()
}

// TokenStore returns the token store (for testing/admin purposes)
func (s *Server) TokenStore() auth.TokenStore {
	return s.tokenStore
}
