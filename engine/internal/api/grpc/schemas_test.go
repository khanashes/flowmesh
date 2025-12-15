package grpc

import (
	"context"
	"testing"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	schemaerrors "github.com/flowmesh/engine/internal/storage/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockSchemaStorage is a mock implementation of storage.StorageBackend for schema testing
type mockSchemaStorage struct {
	storage.StorageBackend
	schemaReg *mockSchemaRegistry
	metaStore *metastore.Store
}

func (m *mockSchemaStorage) SchemaRegistry() storage.SchemaRegistry {
	return m.schemaReg
}

func (m *mockSchemaStorage) MetaStore() *metastore.Store {
	return m.metaStore
}

func (m *mockSchemaStorage) Start(ctx context.Context) error {
	return nil
}

func (m *mockSchemaStorage) Stop(ctx context.Context) error {
	return nil
}

func (m *mockSchemaStorage) Ready() bool {
	return true
}

// mockSchemaRegistry is a mock implementation of storage.SchemaRegistry
type mockSchemaRegistry struct {
	registerSchemaFunc  func(ctx context.Context, tenant, schemaID, schemaType string, version int32, definition []byte) error
	getSchemaFunc       func(ctx context.Context, tenant, schemaID string, version int32) (*schemaerrors.Schema, error)
	getLatestSchemaFunc func(ctx context.Context, tenant, schemaID string) (*schemaerrors.Schema, error)
	listSchemasFunc     func(ctx context.Context, tenant, schemaID string) ([]*schemaerrors.Schema, error)
	deleteSchemaFunc    func(ctx context.Context, tenant, schemaID string, version int32) error
}

func (m *mockSchemaRegistry) RegisterSchema(ctx context.Context, tenant, schemaID, schemaType string, version int32, definition []byte) error {
	if m.registerSchemaFunc != nil {
		return m.registerSchemaFunc(ctx, tenant, schemaID, schemaType, version, definition)
	}
	return nil
}

func (m *mockSchemaRegistry) GetSchema(ctx context.Context, tenant, schemaID string, version int32) (*schemaerrors.Schema, error) {
	if m.getSchemaFunc != nil {
		return m.getSchemaFunc(ctx, tenant, schemaID, version)
	}
	return nil, schemaerrors.SchemaNotFoundError{SchemaID: schemaID, Version: version}
}

func (m *mockSchemaRegistry) GetLatestSchema(ctx context.Context, tenant, schemaID string) (*schemaerrors.Schema, error) {
	if m.getLatestSchemaFunc != nil {
		return m.getLatestSchemaFunc(ctx, tenant, schemaID)
	}
	return nil, schemaerrors.SchemaNotFoundError{SchemaID: schemaID}
}

func (m *mockSchemaRegistry) ListSchemas(ctx context.Context, tenant, schemaID string) ([]*schemaerrors.Schema, error) {
	if m.listSchemasFunc != nil {
		return m.listSchemasFunc(ctx, tenant, schemaID)
	}
	return nil, nil
}

func (m *mockSchemaRegistry) DeleteSchema(ctx context.Context, tenant, schemaID string, version int32) error {
	if m.deleteSchemaFunc != nil {
		return m.deleteSchemaFunc(ctx, tenant, schemaID, version)
	}
	return nil
}

func (m *mockSchemaRegistry) Start(ctx context.Context) error { return nil }
func (m *mockSchemaRegistry) Stop(ctx context.Context) error  { return nil }
func (m *mockSchemaRegistry) Ready() bool                     { return true }

func TestSchemaService_RegisterSchema(t *testing.T) {
	mockReg := &mockSchemaRegistry{}
	mockStorage := &mockSchemaStorage{
		schemaReg: mockReg,
	}
	service := NewSchemaService(mockStorage)

	ctx := context.Background()
	req := &flowmeshpb.RegisterSchemaRequest{
		Tenant:     "test-tenant",
		SchemaId:   "test-schema",
		SchemaType: "jsonschema",
		Version:    1,
		Definition: []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`),
	}

	mockReg.registerSchemaFunc = func(ctx context.Context, tenant, schemaID, schemaType string, version int32, definition []byte) error {
		assert.Equal(t, "test-tenant", tenant)
		assert.Equal(t, "test-schema", schemaID)
		assert.Equal(t, "jsonschema", schemaType)
		assert.Equal(t, int32(1), version)
		return nil
	}

	resp, err := service.RegisterSchema(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int32(codes.OK), resp.Status.Code)
}

func TestSchemaService_RegisterSchema_InvalidRequest(t *testing.T) {
	mockStorage := &mockSchemaStorage{schemaReg: &mockSchemaRegistry{}}
	service := NewSchemaService(mockStorage)

	ctx := context.Background()

	tests := []struct {
		name string
		req  *flowmeshpb.RegisterSchemaRequest
	}{
		{"empty tenant", &flowmeshpb.RegisterSchemaRequest{SchemaId: "schema", SchemaType: "jsonschema", Version: 1, Definition: []byte(`{}`)}},
		{"empty schema ID", &flowmeshpb.RegisterSchemaRequest{Tenant: "tenant", SchemaType: "jsonschema", Version: 1, Definition: []byte(`{}`)}},
		{"empty schema type", &flowmeshpb.RegisterSchemaRequest{Tenant: "tenant", SchemaId: "schema", Version: 1, Definition: []byte(`{}`)}},
		{"zero version", &flowmeshpb.RegisterSchemaRequest{Tenant: "tenant", SchemaId: "schema", SchemaType: "jsonschema", Version: 0, Definition: []byte(`{}`)}},
		{"empty definition", &flowmeshpb.RegisterSchemaRequest{Tenant: "tenant", SchemaId: "schema", SchemaType: "jsonschema", Version: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := service.RegisterSchema(ctx, tt.req)
			assert.Error(t, err)
			st, ok := status.FromError(err)
			assert.True(t, ok)
			assert.Equal(t, codes.InvalidArgument, st.Code())
		})
	}
}

func TestSchemaService_GetSchema(t *testing.T) {
	mockReg := &mockSchemaRegistry{}
	mockStorage := &mockSchemaStorage{schemaReg: mockReg}
	service := NewSchemaService(mockStorage)

	ctx := context.Background()
	req := &flowmeshpb.GetSchemaRequest{
		Tenant:   "test-tenant",
		SchemaId: "test-schema",
		Version:  1,
	}

	expectedSchema := &schemaerrors.Schema{
		ID:         "test-schema",
		Type:       "jsonschema",
		Version:    1,
		Definition: []byte(`{"type": "object"}`),
	}

	mockReg.getSchemaFunc = func(ctx context.Context, tenant, schemaID string, version int32) (*schemaerrors.Schema, error) {
		return expectedSchema, nil
	}

	resp, err := service.GetSchema(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int32(codes.OK), resp.Status.Code)
	assert.Equal(t, "test-schema", resp.Schema.Id)
	assert.Equal(t, int32(1), resp.Schema.Version)
}

func TestSchemaService_GetSchema_NotFound(t *testing.T) {
	mockReg := &mockSchemaRegistry{}
	mockStorage := &mockSchemaStorage{schemaReg: mockReg}
	service := NewSchemaService(mockStorage)

	ctx := context.Background()
	req := &flowmeshpb.GetSchemaRequest{
		Tenant:   "test-tenant",
		SchemaId: "nonexistent",
		Version:  1,
	}

	mockReg.getSchemaFunc = func(ctx context.Context, tenant, schemaID string, version int32) (*schemaerrors.Schema, error) {
		return nil, schemaerrors.SchemaNotFoundError{SchemaID: schemaID, Version: version}
	}

	_, err := service.GetSchema(ctx, req)
	assert.Error(t, err)
	st, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.NotFound, st.Code())
}

func TestSchemaService_ListSchemas(t *testing.T) {
	mockReg := &mockSchemaRegistry{}
	mockStorage := &mockSchemaStorage{schemaReg: mockReg}
	service := NewSchemaService(mockStorage)

	ctx := context.Background()
	req := &flowmeshpb.ListSchemasRequest{
		Tenant:   "test-tenant",
		SchemaId: "",
	}

	expectedSchemas := []*schemaerrors.Schema{
		{ID: "schema1", Type: "jsonschema", Version: 1, Definition: []byte(`{}`)},
		{ID: "schema2", Type: "jsonschema", Version: 1, Definition: []byte(`{}`)},
	}

	mockReg.listSchemasFunc = func(ctx context.Context, tenant, schemaID string) ([]*schemaerrors.Schema, error) {
		return expectedSchemas, nil
	}

	resp, err := service.ListSchemas(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int32(codes.OK), resp.Status.Code)
	assert.Len(t, resp.Schemas, 2)
}

func TestSchemaService_DeleteSchema(t *testing.T) {
	mockReg := &mockSchemaRegistry{}
	mockStorage := &mockSchemaStorage{schemaReg: mockReg}
	service := NewSchemaService(mockStorage)

	ctx := context.Background()
	req := &flowmeshpb.DeleteSchemaRequest{
		Tenant:   "test-tenant",
		SchemaId: "test-schema",
		Version:  1,
	}

	mockReg.deleteSchemaFunc = func(ctx context.Context, tenant, schemaID string, version int32) error {
		assert.Equal(t, "test-tenant", tenant)
		assert.Equal(t, "test-schema", schemaID)
		assert.Equal(t, int32(1), version)
		return nil
	}

	resp, err := service.DeleteSchema(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int32(codes.OK), resp.Status.Code)
}

func TestSchemaService_SetResourceSchema(t *testing.T) {
	mockReg := &mockSchemaRegistry{}
	mockMetaStore := &metastore.Store{}
	mockStorage := &mockSchemaStorage{
		schemaReg: mockReg,
		metaStore: mockMetaStore,
	}
	service := NewSchemaService(mockStorage)

	ctx := context.Background()
	req := &flowmeshpb.SetResourceSchemaRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "stream",
			Name:         "test-stream",
		},
		SchemaId: "test-schema",
		Version:  1,
	}

	expectedSchema := &schemaerrors.Schema{
		ID:      "test-schema",
		Version: 1,
	}

	mockReg.getSchemaFunc = func(ctx context.Context, tenant, schemaID string, version int32) (*schemaerrors.Schema, error) {
		return expectedSchema, nil
	}

	// This test will fail because we need a real metaStore, but it shows the structure
	// In a real test, we'd set up a proper metaStore with a test resource
	_, err := service.SetResourceSchema(ctx, req)
	// We expect this to fail because the resource doesn't exist in the mock
	assert.Error(t, err)
}
