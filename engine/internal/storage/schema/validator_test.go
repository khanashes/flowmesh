package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidator_Validate(t *testing.T) {
	validator := NewValidator()

	schemaDef := []byte(`{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "number"}
		},
		"required": ["name"]
	}`)

	validPayload := []byte(`{"name": "John", "age": 30}`)
	invalidPayload := []byte(`{"name": 123}`) // name should be string
	missingRequired := []byte(`{"age": 30}`)  // missing required "name"

	// Valid payload
	err := validator.Validate(validPayload, schemaDef)
	assert.NoError(t, err)

	// Invalid payload
	err = validator.Validate(invalidPayload, schemaDef)
	assert.Error(t, err)

	// Missing required field
	err = validator.Validate(missingRequired, schemaDef)
	assert.Error(t, err)
}

func TestValidator_Validate_InvalidJSON(t *testing.T) {
	validator := NewValidator()

	schemaDef := []byte(`{"type": "object"}`)
	invalidJSON := []byte(`{invalid json}`)

	err := validator.Validate(invalidJSON, schemaDef)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not valid JSON")
}

func TestValidator_CompileSchema(t *testing.T) {
	validator := NewValidator()

	schemaDef := []byte(`{
		"type": "object",
		"properties": {
			"name": {"type": "string"}
		}
	}`)

	// Compile schema
	schema1, err := validator.CompileSchema(schemaDef)
	require.NoError(t, err)
	assert.NotNil(t, schema1)

	// Compile again (should use cache)
	schema2, err := validator.CompileSchema(schemaDef)
	require.NoError(t, err)
	assert.Equal(t, schema1, schema2) // Should be the same instance from cache
}

func TestValidator_CompileSchema_InvalidSchema(t *testing.T) {
	validator := NewValidator()

	invalidSchema := []byte(`{"type": "invalid_type"}`)

	_, err := validator.CompileSchema(invalidSchema)
	assert.Error(t, err)
}

func TestValidator_ClearCache(t *testing.T) {
	validator := NewValidator()

	schemaDef := []byte(`{"type": "object"}`)

	// Compile schema
	_, err := validator.CompileSchema(schemaDef)
	require.NoError(t, err)

	// Clear cache
	validator.ClearCache()

	// Compile again (should work, just not cached)
	_, err = validator.CompileSchema(schemaDef)
	assert.NoError(t, err)
}
