package schema

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/rs/zerolog/log"
)

// Registry manages schema registration and retrieval
type Registry struct {
	metaStore *metastore.Store
	mu        sync.RWMutex
	// Cache of latest schema versions per schema ID
	latestVersions map[string]int32
}

// NewRegistry creates a new schema registry
func NewRegistry(metaStore *metastore.Store) *Registry {
	return &Registry{
		metaStore:      metaStore,
		latestVersions: make(map[string]int32),
	}
}

// RegisterSchema registers a new schema version
func (r *Registry) RegisterSchema(ctx context.Context, tenant, schemaID, schemaType string, version int32, definition []byte) error {
	if tenant == "" {
		return InvalidSchemaError{SchemaID: schemaID, Reason: "tenant cannot be empty"}
	}
	if schemaID == "" {
		return InvalidSchemaError{SchemaID: schemaID, Reason: "schema ID cannot be empty"}
	}
	if schemaType == "" {
		return InvalidSchemaError{SchemaID: schemaID, Reason: "schema type cannot be empty"}
	}
	if version <= 0 {
		return InvalidSchemaError{SchemaID: schemaID, Reason: "version must be positive"}
	}
	if len(definition) == 0 {
		return InvalidSchemaError{SchemaID: schemaID, Reason: "definition cannot be empty"}
	}

	// Check if schema already exists
	path := SchemaPath(tenant, schemaID, version)
	_, err := r.metaStore.GetResource(path)
	if err == nil {
		return fmt.Errorf("schema %s version %d already exists", schemaID, version)
	}
	if _, ok := err.(metastore.ResourceNotFoundError); !ok {
		return fmt.Errorf("failed to check schema existence: %w", err)
	}

	// Create schema resource config
	config := &metastore.ResourceConfig{
		Tenant:     tenant,
		Namespace:  "schemas",
		Name:       fmt.Sprintf("%s/v%d", schemaID, version),
		Type:       metastore.ResourceSchema,
		Partitions: 1,
		Schema: &metastore.SchemaRef{
			ID:         schemaID,
			Type:       schemaType,
			Version:    version,
			Definition: definition,
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	// Store schema
	if err := r.metaStore.CreateResource(config); err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}

	// Update latest version cache
	r.mu.Lock()
	currentLatest, exists := r.latestVersions[schemaID]
	if !exists || version > currentLatest {
		r.latestVersions[schemaID] = version
	}
	r.mu.Unlock()

	log.Info().
		Str("schema_id", schemaID).
		Int32("version", version).
		Str("tenant", tenant).
		Msg("Schema registered")

	return nil
}

// GetSchema retrieves a specific schema version
func (r *Registry) GetSchema(ctx context.Context, tenant, schemaID string, version int32) (*Schema, error) {
	if tenant == "" {
		return nil, InvalidSchemaError{SchemaID: schemaID, Reason: "tenant cannot be empty"}
	}
	if schemaID == "" {
		return nil, InvalidSchemaError{SchemaID: schemaID, Reason: "schema ID cannot be empty"}
	}
	if version <= 0 {
		return nil, InvalidSchemaError{SchemaID: schemaID, Reason: "version must be positive"}
	}

	path := SchemaPath(tenant, schemaID, version)
	config, err := r.metaStore.GetResource(path)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return nil, SchemaNotFoundError{SchemaID: schemaID, Version: version}
		}
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	if config.Schema == nil {
		return nil, SchemaNotFoundError{SchemaID: schemaID, Version: version}
	}

	return &Schema{
		ID:         config.Schema.ID,
		Type:       config.Schema.Type,
		Version:    config.Schema.Version,
		Definition: config.Schema.Definition,
		CreatedAt:  config.CreatedAt,
	}, nil
}

// GetLatestSchema retrieves the latest version of a schema
func (r *Registry) GetLatestSchema(ctx context.Context, tenant, schemaID string) (*Schema, error) {
	if tenant == "" {
		return nil, InvalidSchemaError{SchemaID: schemaID, Reason: "tenant cannot be empty"}
	}
	if schemaID == "" {
		return nil, InvalidSchemaError{SchemaID: schemaID, Reason: "schema ID cannot be empty"}
	}

	// Check cache first
	r.mu.RLock()
	cachedVersion, exists := r.latestVersions[schemaID]
	r.mu.RUnlock()

	if exists {
		return r.GetSchema(ctx, tenant, schemaID, cachedVersion)
	}

	// Find latest version by listing schemas
	schemas, err := r.ListSchemas(ctx, tenant, schemaID)
	if err != nil {
		return nil, err
	}

	if len(schemas) == 0 {
		return nil, SchemaNotFoundError{SchemaID: schemaID, Version: 0}
	}

	// Find latest version
	latest := schemas[0]
	for _, s := range schemas {
		if s.Version > latest.Version {
			latest = s
		}
	}

	// Update cache
	r.mu.Lock()
	r.latestVersions[schemaID] = latest.Version
	r.mu.Unlock()

	return latest, nil
}

// ListSchemas lists all schemas, optionally filtered by schema ID
func (r *Registry) ListSchemas(ctx context.Context, tenant, schemaID string) ([]*Schema, error) {
	if tenant == "" {
		return nil, InvalidSchemaError{SchemaID: schemaID, Reason: "tenant cannot be empty"}
	}

	// List all schema resources
	resources, err := r.metaStore.ListResources(tenant, "schemas", metastore.ResourceSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to list schemas: %w", err)
	}

	schemas := make([]*Schema, 0)
	for _, config := range resources {
		if config.Schema == nil {
			continue
		}

		// Filter by schema ID if specified
		if schemaID != "" && config.Schema.ID != schemaID {
			continue
		}

		schemas = append(schemas, &Schema{
			ID:         config.Schema.ID,
			Type:       config.Schema.Type,
			Version:    config.Schema.Version,
			Definition: config.Schema.Definition,
			CreatedAt:  config.CreatedAt,
		})
	}

	// Sort by schema ID, then version
	sort.Slice(schemas, func(i, j int) bool {
		if schemas[i].ID != schemas[j].ID {
			return schemas[i].ID < schemas[j].ID
		}
		return schemas[i].Version < schemas[j].Version
	})

	return schemas, nil
}

// DeleteSchema deletes a specific schema version
func (r *Registry) DeleteSchema(ctx context.Context, tenant, schemaID string, version int32) error {
	if tenant == "" {
		return InvalidSchemaError{SchemaID: schemaID, Reason: "tenant cannot be empty"}
	}
	if schemaID == "" {
		return InvalidSchemaError{SchemaID: schemaID, Reason: "schema ID cannot be empty"}
	}
	if version <= 0 {
		return InvalidSchemaError{SchemaID: schemaID, Reason: "version must be positive"}
	}

	path := SchemaPath(tenant, schemaID, version)
	if err := r.metaStore.DeleteResource(path); err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			return SchemaNotFoundError{SchemaID: schemaID, Version: version}
		}
		return fmt.Errorf("failed to delete schema: %w", err)
	}

	// Update cache if this was the latest version
	r.mu.Lock()
	if cachedVersion, exists := r.latestVersions[schemaID]; exists && cachedVersion == version {
		// Find new latest version
		schemas, err := r.ListSchemas(ctx, tenant, schemaID)
		if err == nil && len(schemas) > 0 {
			latest := schemas[0]
			for _, s := range schemas {
				if s.Version > latest.Version {
					latest = s
				}
			}
			r.latestVersions[schemaID] = latest.Version
		} else {
			delete(r.latestVersions, schemaID)
		}
	}
	r.mu.Unlock()

	log.Info().
		Str("schema_id", schemaID).
		Int32("version", version).
		Str("tenant", tenant).
		Msg("Schema deleted")

	return nil
}
