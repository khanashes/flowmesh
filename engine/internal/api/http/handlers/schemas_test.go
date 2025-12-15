package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	schemaerrors "github.com/flowmesh/engine/internal/storage/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSchemaStorage is a mock implementation of storage.StorageBackend for schema handler testing
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

func (m *mockSchemaStorage) Start(ctx context.Context) error { return nil }
func (m *mockSchemaStorage) Stop(ctx context.Context) error  { return nil }
func (m *mockSchemaStorage) Ready() bool                     { return true }

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

func TestSchemaHandlers_RegisterSchema(t *testing.T) {
	mockReg := &mockSchemaRegistry{}
	mockStorage := &mockSchemaStorage{schemaReg: mockReg}
	handlers := NewSchemaHandlers(mockStorage)

	reqBody := RegisterSchemaRequest{
		SchemaID:   "test-schema",
		SchemaType: "jsonschema",
		Version:    1,
		Definition: []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`),
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/test-tenant/schemas", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()

	mockReg.registerSchemaFunc = func(ctx context.Context, tenant, schemaID, schemaType string, version int32, definition []byte) error {
		assert.Equal(t, "test-tenant", tenant)
		assert.Equal(t, "test-schema", schemaID)
		return nil
	}

	handlers.RegisterSchema(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp RegisterSchemaResponse
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "success", resp.Status)
}

func TestSchemaHandlers_RegisterSchema_InvalidRequest(t *testing.T) {
	mockStorage := &mockSchemaStorage{schemaReg: &mockSchemaRegistry{}}
	handlers := NewSchemaHandlers(mockStorage)

	tests := []struct {
		name    string
		reqBody RegisterSchemaRequest
	}{
		{"empty schema ID", RegisterSchemaRequest{SchemaType: "jsonschema", Version: 1, Definition: []byte(`{}`)}},
		{"empty schema type", RegisterSchemaRequest{SchemaID: "schema", Version: 1, Definition: []byte(`{}`)}},
		{"zero version", RegisterSchemaRequest{SchemaID: "schema", SchemaType: "jsonschema", Version: 0, Definition: []byte(`{}`)}},
		{"empty definition", RegisterSchemaRequest{SchemaID: "schema", SchemaType: "jsonschema", Version: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bodyBytes, _ := json.Marshal(tt.reqBody)
			req := httptest.NewRequest("POST", "/api/v1/test-tenant/schemas", bytes.NewReader(bodyBytes))
			w := httptest.NewRecorder()

			handlers.RegisterSchema(w, req)

			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestSchemaHandlers_GetSchema(t *testing.T) {
	mockReg := &mockSchemaRegistry{}
	mockStorage := &mockSchemaStorage{schemaReg: mockReg}
	handlers := NewSchemaHandlers(mockStorage)

	expectedSchema := &schemaerrors.Schema{
		ID:         "test-schema",
		Type:       "jsonschema",
		Version:    1,
		Definition: []byte(`{"type": "object"}`),
	}

	mockReg.getSchemaFunc = func(ctx context.Context, tenant, schemaID string, version int32) (*schemaerrors.Schema, error) {
		return expectedSchema, nil
	}

	req := httptest.NewRequest("GET", "/api/v1/test-tenant/schemas/test-schema/v1", nil)
	w := httptest.NewRecorder()

	handlers.GetSchema(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "success", resp["status"])
}

func TestSchemaHandlers_ListSchemas(t *testing.T) {
	mockReg := &mockSchemaRegistry{}
	mockStorage := &mockSchemaStorage{schemaReg: mockReg}
	handlers := NewSchemaHandlers(mockStorage)

	expectedSchemas := []*schemaerrors.Schema{
		{ID: "schema1", Type: "jsonschema", Version: 1, Definition: []byte(`{}`)},
		{ID: "schema2", Type: "jsonschema", Version: 1, Definition: []byte(`{}`)},
	}

	mockReg.listSchemasFunc = func(ctx context.Context, tenant, schemaID string) ([]*schemaerrors.Schema, error) {
		return expectedSchemas, nil
	}

	req := httptest.NewRequest("GET", "/api/v1/test-tenant/schemas", nil)
	w := httptest.NewRecorder()

	handlers.ListSchemas(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "success", resp["status"])
}

func TestSchemaHandlers_DeleteSchema(t *testing.T) {
	mockReg := &mockSchemaRegistry{}
	mockStorage := &mockSchemaStorage{schemaReg: mockReg}
	handlers := NewSchemaHandlers(mockStorage)

	mockReg.deleteSchemaFunc = func(ctx context.Context, tenant, schemaID string, version int32) error {
		assert.Equal(t, "test-tenant", tenant)
		assert.Equal(t, "test-schema", schemaID)
		assert.Equal(t, int32(1), version)
		return nil
	}

	req := httptest.NewRequest("DELETE", "/api/v1/test-tenant/schemas/test-schema/v1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteSchema(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp map[string]interface{}
	err := json.NewDecoder(w.Body).Decode(&resp)
	require.NoError(t, err)
	assert.Equal(t, "success", resp["status"])
}
