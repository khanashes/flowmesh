package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/api/validation"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	schemaerrors "github.com/flowmesh/engine/internal/storage/schema"
)

// SchemaService implements the SchemaService gRPC service
type SchemaService struct {
	flowmeshpb.UnimplementedSchemaServiceServer
	storage storage.StorageBackend
}

// NewSchemaService creates a new schema service
func NewSchemaService(storage storage.StorageBackend) *SchemaService {
	return &SchemaService{
		storage: storage,
	}
}

// RegisterSchema registers a new schema version
func (s *SchemaService) RegisterSchema(ctx context.Context, req *flowmeshpb.RegisterSchemaRequest) (*flowmeshpb.RegisterSchemaResponse, error) {
	// Validate request
	if req.Tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant is required")
	}
	if req.SchemaId == "" {
		return nil, status.Error(codes.InvalidArgument, "schema_id is required")
	}
	if req.SchemaType == "" {
		return nil, status.Error(codes.InvalidArgument, "schema_type is required")
	}
	if req.Version <= 0 {
		return nil, status.Error(codes.InvalidArgument, "version must be positive")
	}
	if len(req.Definition) == 0 {
		return nil, status.Error(codes.InvalidArgument, "definition cannot be empty")
	}

	// Validate schema definition is valid JSON Schema
	if req.SchemaType == "jsonschema" {
		if err := validation.ValidateSchemaDefinition(req.Definition); err != nil {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid schema definition: %v", err))
		}
	}

	// Register schema
	schemaReg := s.storage.SchemaRegistry()
	if err := schemaReg.RegisterSchema(ctx, req.Tenant, req.SchemaId, req.SchemaType, req.Version, req.Definition); err != nil {
		return nil, s.mapSchemaError(err, req.SchemaId)
	}

	return &flowmeshpb.RegisterSchemaResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "schema registered successfully",
		},
	}, nil
}

// GetSchema retrieves a schema by ID and version
func (s *SchemaService) GetSchema(ctx context.Context, req *flowmeshpb.GetSchemaRequest) (*flowmeshpb.GetSchemaResponse, error) {
	// Validate request
	if req.Tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant is required")
	}
	if req.SchemaId == "" {
		return nil, status.Error(codes.InvalidArgument, "schema_id is required")
	}

	schemaReg := s.storage.SchemaRegistry()
	var schemaDef *schemaerrors.Schema
	var err error

	if req.Version > 0 {
		schemaDef, err = schemaReg.GetSchema(ctx, req.Tenant, req.SchemaId, req.Version)
	} else {
		schemaDef, err = schemaReg.GetLatestSchema(ctx, req.Tenant, req.SchemaId)
	}

	if err != nil {
		return nil, s.mapSchemaError(err, req.SchemaId)
	}

	return &flowmeshpb.GetSchemaResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "schema retrieved successfully",
		},
		Schema: &flowmeshpb.Schema{
			Id:         schemaDef.ID,
			Type:       schemaDef.Type,
			Version:    schemaDef.Version,
			Definition: schemaDef.Definition,
			CreatedAt:  schemaDef.CreatedAt.UnixNano(),
		},
	}, nil
}

// ListSchemas lists all schemas, optionally filtered by schema ID
func (s *SchemaService) ListSchemas(ctx context.Context, req *flowmeshpb.ListSchemasRequest) (*flowmeshpb.ListSchemasResponse, error) {
	// Validate request
	if req.Tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant is required")
	}

	schemaReg := s.storage.SchemaRegistry()
	schemas, err := schemaReg.ListSchemas(ctx, req.Tenant, req.SchemaId)
	if err != nil {
		return nil, s.mapSchemaError(err, req.SchemaId)
	}

	// Convert to proto
	protoSchemas := make([]*flowmeshpb.Schema, len(schemas))
	for i, s := range schemas {
		protoSchemas[i] = &flowmeshpb.Schema{
			Id:         s.ID,
			Type:       s.Type,
			Version:    s.Version,
			Definition: s.Definition,
			CreatedAt:  s.CreatedAt.UnixNano(),
		}
	}

	return &flowmeshpb.ListSchemasResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "schemas listed successfully",
		},
		Schemas: protoSchemas,
	}, nil
}

// DeleteSchema deletes a schema version
func (s *SchemaService) DeleteSchema(ctx context.Context, req *flowmeshpb.DeleteSchemaRequest) (*flowmeshpb.DeleteSchemaResponse, error) {
	// Validate request
	if req.Tenant == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant is required")
	}
	if req.SchemaId == "" {
		return nil, status.Error(codes.InvalidArgument, "schema_id is required")
	}
	if req.Version <= 0 {
		return nil, status.Error(codes.InvalidArgument, "version must be positive")
	}

	schemaReg := s.storage.SchemaRegistry()
	if err := schemaReg.DeleteSchema(ctx, req.Tenant, req.SchemaId, req.Version); err != nil {
		return nil, s.mapSchemaError(err, req.SchemaId)
	}

	return &flowmeshpb.DeleteSchemaResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "schema deleted successfully",
		},
	}, nil
}

// SetResourceSchema attaches a schema to a resource (stream/queue)
func (s *SchemaService) SetResourceSchema(ctx context.Context, req *flowmeshpb.SetResourceSchemaRequest) (*flowmeshpb.SetResourceSchemaResponse, error) {
	// Validate request
	if req.ResourcePath == nil {
		return nil, status.Error(codes.InvalidArgument, "resource_path is required")
	}
	if req.SchemaId == "" {
		return nil, status.Error(codes.InvalidArgument, "schema_id is required")
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

	// Validate resource type
	if req.ResourcePath.ResourceType != "stream" && req.ResourcePath.ResourceType != "queue" {
		return nil, status.Error(codes.InvalidArgument, "resource_type must be 'stream' or 'queue'")
	}

	// Verify resource exists
	metaStore := s.storage.MetaStore()
	_, err = metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("resource not found: %s", resourcePath))
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get resource: %v", err))
	}

	// Determine schema version
	schemaVersion := req.Version
	if schemaVersion == 0 {
		// Get latest version
		schemaReg := s.storage.SchemaRegistry()
		latestSchema, err := schemaReg.GetLatestSchema(ctx, req.ResourcePath.Tenant, req.SchemaId)
		if err != nil {
			return nil, s.mapSchemaError(err, req.SchemaId)
		}
		schemaVersion = latestSchema.Version
	}

	// Update resource config with schema reference
	err = metaStore.UpdateResource(resourcePath, func(cfg *metastore.ResourceConfig) error {
		cfg.Schema = &metastore.SchemaRef{
			ID:      req.SchemaId,
			Type:    "jsonschema", // Default to jsonschema
			Version: schemaVersion,
		}
		return nil
	})
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to update resource: %v", err))
	}

	return &flowmeshpb.SetResourceSchemaResponse{
		Status: &flowmeshpb.Status{
			Code:    int32(codes.OK),
			Message: "schema attached to resource successfully",
		},
	}, nil
}

// mapSchemaError maps schema errors to gRPC status codes
func (s *SchemaService) mapSchemaError(err error, schemaID string) error {
	switch e := err.(type) {
	case schemaerrors.SchemaNotFoundError:
		return status.Error(codes.NotFound, e.Error())
	case schemaerrors.InvalidSchemaError:
		return status.Error(codes.InvalidArgument, e.Error())
	case schemaerrors.SchemaValidationError:
		return status.Error(codes.InvalidArgument, e.Error())
	default:
		return status.Error(codes.Internal, fmt.Sprintf("internal error: %v", err))
	}
}
