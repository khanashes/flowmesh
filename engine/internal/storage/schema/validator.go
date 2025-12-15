package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// Validator validates payloads against JSON schemas
type Validator struct {
	mu       sync.RWMutex
	compiled map[string]*jsonschema.Schema
}

// NewValidator creates a new schema validator
func NewValidator() *Validator {
	return &Validator{
		compiled: make(map[string]*jsonschema.Schema),
	}
}

// Validate validates a payload against a schema definition
func (v *Validator) Validate(payload []byte, schemaDefinition []byte) error {
	// Parse payload as JSON
	var payloadJSON interface{}
	if err := json.Unmarshal(payload, &payloadJSON); err != nil {
		return fmt.Errorf("payload is not valid JSON: %w", err)
	}

	// Compile schema (with caching)
	schema, err := v.CompileSchema(schemaDefinition)
	if err != nil {
		return fmt.Errorf("failed to compile schema: %w", err)
	}

	// Validate payload against schema
	if err := schema.Validate(payloadJSON); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	return nil
}

// CompileSchema compiles a schema definition and caches it
func (v *Validator) CompileSchema(schemaDefinition []byte) (*jsonschema.Schema, error) {
	// Create cache key from schema definition hash
	cacheKey := string(schemaDefinition)

	// Check cache
	v.mu.RLock()
	if compiled, exists := v.compiled[cacheKey]; exists {
		v.mu.RUnlock()
		return compiled, nil
	}
	v.mu.RUnlock()

	// Compile schema
	compiler := jsonschema.NewCompiler()
	schemaReader := bytes.NewReader(schemaDefinition)
	if err := compiler.AddResource("schema.json", schemaReader); err != nil {
		return nil, fmt.Errorf("failed to add schema resource: %w", err)
	}

	schema, err := compiler.Compile("schema.json")
	if err != nil {
		return nil, fmt.Errorf("failed to compile schema: %w", err)
	}

	// Cache compiled schema
	v.mu.Lock()
	v.compiled[cacheKey] = schema
	v.mu.Unlock()

	return schema, nil
}

// ClearCache clears the compiled schema cache
func (v *Validator) ClearCache() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.compiled = make(map[string]*jsonschema.Schema)
}
