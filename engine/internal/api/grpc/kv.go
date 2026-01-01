package grpc

import (
	"context"
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/api/validation"
	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage"
	kverrors "github.com/flowmesh/engine/internal/storage/kv"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// KVService implements the KVService gRPC service
type KVService struct {
	flowmeshpb.UnimplementedKVServiceServer
	storage storage.StorageBackend
	log     zerolog.Logger
}

// NewKVService creates a new KV service
func NewKVService(storage storage.StorageBackend) *KVService {
	return &KVService{
		storage: storage,
		log:     logger.WithComponent("grpc.kv"),
	}
}

// Set sets a key-value pair with optional TTL
func (s *KVService) Set(ctx context.Context, req *flowmeshpb.SetRequest) (*flowmeshpb.SetResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}
	if len(req.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "value cannot be empty")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is kv
	if req.ResourcePath.ResourceType != "kv" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'kv'")
	}

	// Convert TTL
	var ttl time.Duration
	if req.TtlSeconds > 0 {
		ttl = time.Duration(req.TtlSeconds) * time.Second
		if ttl > 365*24*time.Hour {
			return nil, status.Error(codes.InvalidArgument, "ttl_seconds cannot exceed 31536000 (1 year)")
		}
	}

	// Set key-value pair
	kvMgr := s.storage.KVManager()
	err = kvMgr.Set(ctx, resourcePath, req.Key, req.Value, storage.KVSetOptions{
		TTL: ttl,
	})
	if err != nil {
		return nil, s.mapKVError(err, resourcePath)
	}

	return &flowmeshpb.SetResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "key-value pair set successfully",
		},
	}, nil
}

// Get retrieves a value by key
func (s *KVService) Get(ctx context.Context, req *flowmeshpb.GetRequest) (*flowmeshpb.GetResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is kv
	if req.ResourcePath.ResourceType != "kv" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'kv'")
	}

	// Get value
	kvMgr := s.storage.KVManager()
	value, err := kvMgr.Get(ctx, resourcePath, req.Key)
	if err != nil {
		return nil, s.mapKVError(err, resourcePath)
	}

	return &flowmeshpb.GetResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "value retrieved successfully",
		},
		Value: value,
	}, nil
}

// Delete deletes a key
func (s *KVService) Delete(ctx context.Context, req *flowmeshpb.DeleteRequest) (*flowmeshpb.DeleteResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is kv
	if req.ResourcePath.ResourceType != "kv" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'kv'")
	}

	// Delete key
	kvMgr := s.storage.KVManager()
	err = kvMgr.Delete(ctx, resourcePath, req.Key)
	if err != nil {
		return nil, s.mapKVError(err, resourcePath)
	}

	return &flowmeshpb.DeleteResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "key deleted successfully",
		},
	}, nil
}

// Exists checks if a key exists
func (s *KVService) Exists(ctx context.Context, req *flowmeshpb.ExistsRequest) (*flowmeshpb.ExistsResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is kv
	if req.ResourcePath.ResourceType != "kv" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'kv'")
	}

	// Check if key exists
	kvMgr := s.storage.KVManager()
	exists, err := kvMgr.Exists(ctx, resourcePath, req.Key)
	if err != nil {
		return nil, s.mapKVError(err, resourcePath)
	}

	return &flowmeshpb.ExistsResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "existence check completed",
		},
		Exists: exists,
	}, nil
}

// ListKeys lists all keys, optionally filtered by prefix
func (s *KVService) ListKeys(
	ctx context.Context,
	req *flowmeshpb.ListKeysRequest,
) (*flowmeshpb.ListKeysResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(
		req.ResourcePath.Tenant,
		req.ResourcePath.Namespace,
		req.ResourcePath.ResourceType,
		req.ResourcePath.Name,
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Validate resource type is kv
	if req.ResourcePath.ResourceType != "kv" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'kv'")
	}

	// List keys
	kvMgr := s.storage.KVManager()
	keys, err := kvMgr.ListKeys(ctx, resourcePath, req.Prefix)
	if err != nil {
		return nil, s.mapKVError(err, resourcePath)
	}

	return &flowmeshpb.ListKeysResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "keys listed successfully",
		},
		Keys: keys,
	}, nil
}

// mapKVError maps storage KV errors to gRPC status codes
func (s *KVService) mapKVError(err error, resourcePath string) error {
	if err == nil {
		return nil
	}

	switch e := err.(type) {
	case kverrors.KVStoreNotFoundError:
		return status.Error(codes.NotFound, e.Error())
	case kverrors.KeyNotFoundError:
		return status.Error(codes.NotFound, e.Error())
	case kverrors.KeyExpiredError:
		return status.Error(codes.NotFound, e.Error())
	case kverrors.InvalidKeyError:
		return status.Error(codes.InvalidArgument, e.Error())
	case kverrors.InvalidValueError:
		return status.Error(codes.InvalidArgument, e.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
