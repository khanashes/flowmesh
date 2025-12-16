package grpc

import (
	"context"
	"net"
	"sync"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

// Server represents a gRPC server
type Server struct {
	storage    storage.StorageBackend
	grpcServer *grpc.Server
	addr       string
	log        zerolog.Logger
	ready      bool
	mu         sync.RWMutex
	healthSvc  *HealthService
	streamSvc  *StreamService
	queueSvc   *QueueService
	kvSvc      *KVService
	schemaSvc  *SchemaService
	replaySvc  *ReplayService
}

// NewServer creates a new gRPC server
func NewServer(addr string, storage storage.StorageBackend) *Server {
	s := &Server{
		storage:   storage,
		addr:      addr,
		log:       logger.WithComponent("grpc"),
		healthSvc: NewHealthService(storage),
		streamSvc: NewStreamService(storage),
		queueSvc:  NewQueueService(storage),
		kvSvc:     NewKVService(storage),
		schemaSvc: NewSchemaService(storage),
		replaySvc: NewReplayService(storage),
	}

	// Create gRPC server with interceptors
	s.grpcServer = grpc.NewServer(
		grpc.UnaryInterceptor(s.unaryInterceptorChain()),
	)

	return s
}

// Start starts the gRPC server
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ready {
		return nil
	}

	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	s.log.Info().Str("addr", s.addr).Msg("Starting gRPC server")

	// Register services
	s.registerServices()

	// Start server in a goroutine
	go func() {
		if err := s.grpcServer.Serve(listener); err != nil {
			s.log.Error().Err(err).Msg("gRPC server error")
		}
	}()

	s.ready = true
	s.log.Info().Str("addr", s.addr).Msg("gRPC server started")

	return nil
}

// Stop gracefully stops the gRPC server
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.ready {
		return nil
	}

	s.log.Info().Msg("Stopping gRPC server")

	// Graceful stop with context
	stopped := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-ctx.Done():
		// Context expired, force stop
		s.grpcServer.Stop()
		return ctx.Err()
	case <-stopped:
		// Graceful stop completed
	}

	s.ready = false
	s.log.Info().Msg("gRPC server stopped")

	return nil
}

// Ready returns true if the server is ready
func (s *Server) Ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ready
}

// registerServices registers all gRPC services
func (s *Server) registerServices() {
	// Register health service
	flowmeshpb.RegisterHealthServiceServer(s.grpcServer, s.healthSvc)

	// Register stream service
	flowmeshpb.RegisterStreamServiceServer(s.grpcServer, s.streamSvc)

	// Register queue service
	flowmeshpb.RegisterQueueServiceServer(s.grpcServer, s.queueSvc)

	// Register KV service
	flowmeshpb.RegisterKVServiceServer(s.grpcServer, s.kvSvc)

	// Register schema service
	flowmeshpb.RegisterSchemaServiceServer(s.grpcServer, s.schemaSvc)
	flowmeshpb.RegisterReplayServiceServer(s.grpcServer, s.replaySvc)
}
