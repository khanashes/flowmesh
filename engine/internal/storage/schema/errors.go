package schema

import "fmt"

// SchemaNotFoundError indicates a schema was not found
type SchemaNotFoundError struct {
	SchemaID string
	Version  int32
}

func (e SchemaNotFoundError) Error() string {
	if e.Version > 0 {
		return fmt.Sprintf("schema not found: %s (version %d)", e.SchemaID, e.Version)
	}
	return fmt.Sprintf("schema not found: %s", e.SchemaID)
}

// InvalidSchemaError indicates an invalid schema definition
type InvalidSchemaError struct {
	SchemaID string
	Reason   string
}

func (e InvalidSchemaError) Error() string {
	return fmt.Sprintf("invalid schema %s: %s", e.SchemaID, e.Reason)
}

// SchemaValidationError indicates a payload failed schema validation
type SchemaValidationError struct {
	SchemaID string
	Version  int32
	Reason   string
}

func (e SchemaValidationError) Error() string {
	return fmt.Sprintf("schema validation failed for %s (version %d): %s", e.SchemaID, e.Version, e.Reason)
}
