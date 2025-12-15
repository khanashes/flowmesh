package validation

import (
	"bytes"
	"encoding/json"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// ValidateSchemaDefinition validates that a schema definition is valid JSON Schema
func ValidateSchemaDefinition(definition []byte) error {
	// Parse as JSON first
	var schemaJSON interface{}
	if err := json.Unmarshal(definition, &schemaJSON); err != nil {
		return err
	}

	// Try to compile the schema
	compiler := jsonschema.NewCompiler()
	schemaReader := bytes.NewReader(definition)
	if err := compiler.AddResource("schema.json", schemaReader); err != nil {
		return err
	}

	_, err := compiler.Compile("schema.json")
	return err
}
