package schema

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestRegistry(t *testing.T) (*Registry, *metastore.Store, func()) {
	tmpDir := t.TempDir()
	metadataDir := filepath.Join(tmpDir, "metadata")

	metaStore, err := metastore.NewStore(metadataDir)
	require.NoError(t, err)

	registry := NewRegistry(metaStore)

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return registry, metaStore, cleanup
}

func TestRegistry_RegisterSchema(t *testing.T) {
	registry, _, cleanup := setupTestRegistry(t)
	defer cleanup()

	ctx := context.Background()
	tenant := "test-tenant"
	schemaID := "test-schema"
	schemaType := "jsonschema"
	version := int32(1)
	definition := []byte(`{"type": "object", "properties": {"name": {"type": "string"}}}`)

	err := registry.RegisterSchema(ctx, tenant, schemaID, schemaType, version, definition)
	require.NoError(t, err)

	// Verify schema can be retrieved
	schema, err := registry.GetSchema(ctx, tenant, schemaID, version)
	require.NoError(t, err)
	assert.Equal(t, schemaID, schema.ID)
	assert.Equal(t, schemaType, schema.Type)
	assert.Equal(t, version, schema.Version)
	assert.Equal(t, definition, schema.Definition)
}

func TestRegistry_RegisterSchema_Duplicate(t *testing.T) {
	registry, _, cleanup := setupTestRegistry(t)
	defer cleanup()

	ctx := context.Background()
	tenant := "test-tenant"
	schemaID := "test-schema"
	schemaType := "jsonschema"
	version := int32(1)
	definition := []byte(`{"type": "object"}`)

	err := registry.RegisterSchema(ctx, tenant, schemaID, schemaType, version, definition)
	require.NoError(t, err)

	// Try to register again
	err = registry.RegisterSchema(ctx, tenant, schemaID, schemaType, version, definition)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestRegistry_RegisterSchema_InvalidInput(t *testing.T) {
	registry, _, cleanup := setupTestRegistry(t)
	defer cleanup()

	ctx := context.Background()
	definition := []byte(`{"type": "object"}`)

	tests := []struct {
		name       string
		tenant     string
		schemaID   string
		schemaType string
		version    int32
		definition []byte
	}{
		{"empty tenant", "", "schema", "jsonschema", 1, definition},
		{"empty schema ID", "tenant", "", "jsonschema", 1, definition},
		{"empty schema type", "tenant", "schema", "", 1, definition},
		{"zero version", "tenant", "schema", "jsonschema", 0, definition},
		{"negative version", "tenant", "schema", "jsonschema", -1, definition},
		{"empty definition", "tenant", "schema", "jsonschema", 1, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := registry.RegisterSchema(ctx, tt.tenant, tt.schemaID, tt.schemaType, tt.version, tt.definition)
			assert.Error(t, err)
		})
	}
}

func TestRegistry_GetSchema(t *testing.T) {
	registry, _, cleanup := setupTestRegistry(t)
	defer cleanup()

	ctx := context.Background()
	tenant := "test-tenant"
	schemaID := "test-schema"
	definition := []byte(`{"type": "object"}`)

	// Register schema version 1
	err := registry.RegisterSchema(ctx, tenant, schemaID, "jsonschema", 1, definition)
	require.NoError(t, err)

	// Register schema version 2
	definition2 := []byte(`{"type": "string"}`)
	err = registry.RegisterSchema(ctx, tenant, schemaID, "jsonschema", 2, definition2)
	require.NoError(t, err)

	// Get version 1
	schema1, err := registry.GetSchema(ctx, tenant, schemaID, 1)
	require.NoError(t, err)
	assert.Equal(t, int32(1), schema1.Version)
	assert.Equal(t, definition, schema1.Definition)

	// Get version 2
	schema2, err := registry.GetSchema(ctx, tenant, schemaID, 2)
	require.NoError(t, err)
	assert.Equal(t, int32(2), schema2.Version)
	assert.Equal(t, definition2, schema2.Definition)
}

func TestRegistry_GetSchema_NotFound(t *testing.T) {
	registry, _, cleanup := setupTestRegistry(t)
	defer cleanup()

	ctx := context.Background()

	_, err := registry.GetSchema(ctx, "tenant", "nonexistent", 1)
	assert.Error(t, err)
	assert.IsType(t, SchemaNotFoundError{}, err)
}

func TestRegistry_GetLatestSchema(t *testing.T) {
	registry, _, cleanup := setupTestRegistry(t)
	defer cleanup()

	ctx := context.Background()
	tenant := "test-tenant"
	schemaID := "test-schema"

	// Register multiple versions
	err := registry.RegisterSchema(ctx, tenant, schemaID, "jsonschema", 1, []byte(`{"type": "object"}`))
	require.NoError(t, err)

	err = registry.RegisterSchema(ctx, tenant, schemaID, "jsonschema", 3, []byte(`{"type": "string"}`))
	require.NoError(t, err)

	err = registry.RegisterSchema(ctx, tenant, schemaID, "jsonschema", 2, []byte(`{"type": "number"}`))
	require.NoError(t, err)

	// Get latest (should be version 3)
	latest, err := registry.GetLatestSchema(ctx, tenant, schemaID)
	require.NoError(t, err)
	assert.Equal(t, int32(3), latest.Version)
}

func TestRegistry_ListSchemas(t *testing.T) {
	registry, _, cleanup := setupTestRegistry(t)
	defer cleanup()

	ctx := context.Background()
	tenant := "test-tenant"

	// Register multiple schemas
	err := registry.RegisterSchema(ctx, tenant, "schema1", "jsonschema", 1, []byte(`{"type": "object"}`))
	require.NoError(t, err)

	err = registry.RegisterSchema(ctx, tenant, "schema1", "jsonschema", 2, []byte(`{"type": "string"}`))
	require.NoError(t, err)

	err = registry.RegisterSchema(ctx, tenant, "schema2", "jsonschema", 1, []byte(`{"type": "number"}`))
	require.NoError(t, err)

	// List all schemas
	schemas, err := registry.ListSchemas(ctx, tenant, "")
	require.NoError(t, err)
	assert.Len(t, schemas, 3)

	// List schemas filtered by ID
	schemas, err = registry.ListSchemas(ctx, tenant, "schema1")
	require.NoError(t, err)
	assert.Len(t, schemas, 2)
}

func TestRegistry_DeleteSchema(t *testing.T) {
	registry, _, cleanup := setupTestRegistry(t)
	defer cleanup()

	ctx := context.Background()
	tenant := "test-tenant"
	schemaID := "test-schema"

	// Register schema
	err := registry.RegisterSchema(ctx, tenant, schemaID, "jsonschema", 1, []byte(`{"type": "object"}`))
	require.NoError(t, err)

	// Delete schema
	err = registry.DeleteSchema(ctx, tenant, schemaID, 1)
	require.NoError(t, err)

	// Verify schema is deleted
	_, err = registry.GetSchema(ctx, tenant, schemaID, 1)
	assert.Error(t, err)
	assert.IsType(t, SchemaNotFoundError{}, err)
}

func TestRegistry_DeleteSchema_NotFound(t *testing.T) {
	registry, _, cleanup := setupTestRegistry(t)
	defer cleanup()

	ctx := context.Background()

	err := registry.DeleteSchema(ctx, "tenant", "nonexistent", 1)
	assert.Error(t, err)
	assert.IsType(t, SchemaNotFoundError{}, err)
}
