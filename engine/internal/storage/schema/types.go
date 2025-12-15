package schema

import (
	"fmt"
	"time"

	"github.com/flowmesh/engine/internal/storage/metastore"
)

// Schema represents a JSON schema definition
type Schema struct {
	// ID is the unique schema identifier
	ID string
	// Type is the schema type (e.g., "jsonschema")
	Type string
	// Version is the schema version
	Version int32
	// Definition is the raw schema definition (JSON)
	Definition []byte
	// CreatedAt is when the schema was created
	CreatedAt time.Time
}

// SchemaPath constructs the schema resource path
// Format: tenant/schemas/schema/{schemaID}/v{version}
func SchemaPath(tenant, schemaID string, version int32) string {
	return metastore.ResourcePath(tenant, "schemas", "schema", fmt.Sprintf("%s/v%d", schemaID, version))
}
