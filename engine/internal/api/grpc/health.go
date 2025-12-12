package grpc

import (
	"context"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/storage"
)

// HealthService implements the HealthService gRPC service
type HealthService struct {
	flowmeshpb.UnimplementedHealthServiceServer
	storage storage.StorageBackend
}

// NewHealthService creates a new health service
func NewHealthService(storage storage.StorageBackend) *HealthService {
	return &HealthService{
		storage: storage,
	}
}

// HealthCheck checks if the server is running
func (s *HealthService) HealthCheck(ctx context.Context, req *flowmeshpb.HealthCheckRequest) (*flowmeshpb.HealthCheckResponse, error) {
	return &flowmeshpb.HealthCheckResponse{
		Status: &flowmeshpb.Status{
			Code:    0, // OK
			Message: "healthy",
		},
	}, nil
}

// ReadinessCheck checks if the server is ready to serve requests
func (s *HealthService) ReadinessCheck(ctx context.Context, req *flowmeshpb.ReadinessCheckRequest) (*flowmeshpb.ReadinessCheckResponse, error) {
	ready := s.storage != nil && s.storage.Ready()

	var status *flowmeshpb.Status
	if ready {
		status = &flowmeshpb.Status{
			Code:    0, // OK
			Message: "ready",
		}
	} else {
		status = &flowmeshpb.Status{
			Code:    503, // Service Unavailable
			Message: "not ready",
		}
	}

	return &flowmeshpb.ReadinessCheckResponse{
		Status: status,
		Ready:  ready,
	}, nil
}
