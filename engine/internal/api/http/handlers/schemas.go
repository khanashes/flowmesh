package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/flowmesh/engine/internal/api/validation"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	schemaerrors "github.com/flowmesh/engine/internal/storage/schema"
)

// SchemaHandlers provides HTTP handlers for schema operations
type SchemaHandlers struct {
	storage storage.StorageBackend
}

// NewSchemaHandlers creates new schema handlers
func NewSchemaHandlers(storage storage.StorageBackend) *SchemaHandlers {
	return &SchemaHandlers{
		storage: storage,
	}
}

// RegisterSchemaRequest represents a request to register a schema
type RegisterSchemaRequest struct {
	SchemaID   string `json:"schema_id"`
	SchemaType string `json:"schema_type"`
	Version    int32  `json:"version"`
	Definition []byte `json:"definition"`
}

// RegisterSchemaResponse represents a response to registering a schema
type RegisterSchemaResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// RegisterSchema handles POST /api/v1/schemas
func (h *SchemaHandlers) RegisterSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract tenant from path or header
	tenant := extractTenantFromPath(r.URL.Path)
	if tenant == "" {
		http.Error(w, "tenant is required", http.StatusBadRequest)
		return
	}

	// Parse request body
	var req RegisterSchemaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.SchemaID == "" {
		http.Error(w, "schema_id is required", http.StatusBadRequest)
		return
	}
	if req.SchemaType == "" {
		http.Error(w, "schema_type is required", http.StatusBadRequest)
		return
	}
	if req.Version <= 0 {
		http.Error(w, "version must be positive", http.StatusBadRequest)
		return
	}
	if len(req.Definition) == 0 {
		http.Error(w, "definition cannot be empty", http.StatusBadRequest)
		return
	}

	// Validate schema definition if jsonschema
	if req.SchemaType == "jsonschema" {
		if err := validation.ValidateSchemaDefinition(req.Definition); err != nil {
			http.Error(w, "invalid schema definition: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Register schema
	schemaReg := h.storage.SchemaRegistry()
	if err := schemaReg.RegisterSchema(r.Context(), tenant, req.SchemaID, req.SchemaType, req.Version, req.Definition); err != nil {
		h.writeError(w, err, req.SchemaID)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(RegisterSchemaResponse{
		Status:  "success",
		Message: "schema registered successfully",
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// GetSchema handles GET /api/v1/schemas/{schema_id}/v{version}
func (h *SchemaHandlers) GetSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract tenant, schema ID, and version from path
	tenant, schemaID, version := extractSchemaPathParams(r.URL.Path)
	if tenant == "" {
		http.Error(w, "tenant is required", http.StatusBadRequest)
		return
	}
	if schemaID == "" {
		http.Error(w, "schema_id is required", http.StatusBadRequest)
		return
	}

	schemaReg := h.storage.SchemaRegistry()
	var schemaDef *schemaerrors.Schema
	var err error

	if version > 0 {
		schemaDef, err = schemaReg.GetSchema(r.Context(), tenant, schemaID, version)
	} else {
		schemaDef, err = schemaReg.GetLatestSchema(r.Context(), tenant, schemaID)
	}

	if err != nil {
		h.writeError(w, err, schemaID)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"schema": map[string]interface{}{
			"id":         schemaDef.ID,
			"type":       schemaDef.Type,
			"version":    schemaDef.Version,
			"definition": string(schemaDef.Definition),
			"created_at": schemaDef.CreatedAt.UnixNano(),
		},
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// ListSchemas handles GET /api/v1/schemas
func (h *SchemaHandlers) ListSchemas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract tenant from path
	tenant := extractTenantFromPath(r.URL.Path)
	if tenant == "" {
		http.Error(w, "tenant is required", http.StatusBadRequest)
		return
	}

	// Optional schema ID filter from query parameter
	schemaID := r.URL.Query().Get("schema_id")

	schemaReg := h.storage.SchemaRegistry()
	schemas, err := schemaReg.ListSchemas(r.Context(), tenant, schemaID)
	if err != nil {
		h.writeError(w, err, schemaID)
		return
	}

	// Convert to response format
	schemaList := make([]map[string]interface{}, len(schemas))
	for i, s := range schemas {
		schemaList[i] = map[string]interface{}{
			"id":         s.ID,
			"type":       s.Type,
			"version":    s.Version,
			"definition": string(s.Definition),
			"created_at": s.CreatedAt.UnixNano(),
		}
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"schemas": schemaList,
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// DeleteSchema handles DELETE /api/v1/schemas/{schema_id}/v{version}
func (h *SchemaHandlers) DeleteSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract tenant, schema ID, and version from path
	tenant, schemaID, version := extractSchemaPathParams(r.URL.Path)
	if tenant == "" {
		http.Error(w, "tenant is required", http.StatusBadRequest)
		return
	}
	if schemaID == "" {
		http.Error(w, "schema_id is required", http.StatusBadRequest)
		return
	}
	if version <= 0 {
		http.Error(w, "version must be positive", http.StatusBadRequest)
		return
	}

	schemaReg := h.storage.SchemaRegistry()
	if err := schemaReg.DeleteSchema(r.Context(), tenant, schemaID, version); err != nil {
		h.writeError(w, err, schemaID)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": "schema deleted successfully",
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// SetResourceSchema handles PUT /api/v1/{resource_type}/{tenant}/{namespace}/{name}/schema
func (h *SchemaHandlers) SetResourceSchema(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var req struct {
		SchemaID string `json:"schema_id"`
		Version  int32  `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.SchemaID == "" {
		http.Error(w, "schema_id is required", http.StatusBadRequest)
		return
	}

	// Extract path parameters
	tenant, namespace, resourceType, name := extractResourcePathParams(r.URL.Path)
	if tenant == "" || namespace == "" || resourceType == "" || name == "" {
		http.Error(w, "invalid resource path", http.StatusBadRequest)
		return
	}

	// Validate resource type
	if resourceType != "stream" && resourceType != "queue" {
		http.Error(w, "resource_type must be 'stream' or 'queue'", http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, resourceType, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Verify resource exists
	metaStore := h.storage.MetaStore()
	_, err = metaStore.GetResource(resourcePath)
	if err != nil {
		if _, ok := err.(metastore.ResourceNotFoundError); ok {
			http.Error(w, "resource not found", http.StatusNotFound)
			return
		}
		http.Error(w, "failed to get resource: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Determine schema version
	schemaVersion := req.Version
	if schemaVersion == 0 {
		// Get latest version
		schemaReg := h.storage.SchemaRegistry()
		latestSchema, err := schemaReg.GetLatestSchema(r.Context(), tenant, req.SchemaID)
		if err != nil {
			h.writeError(w, err, req.SchemaID)
			return
		}
		schemaVersion = latestSchema.Version
	}

	// Update resource config with schema reference
	err = metaStore.UpdateResource(resourcePath, func(cfg *metastore.ResourceConfig) error {
		cfg.Schema = &metastore.SchemaRef{
			ID:      req.SchemaID,
			Type:    "jsonschema",
			Version: schemaVersion,
		}
		return nil
	})
	if err != nil {
		http.Error(w, "failed to update resource: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": "schema attached to resource successfully",
	}); err != nil {
		// Failed to encode response, but we've already written the status code
		return
	}
}

// Helper functions

func extractTenantFromPath(path string) string {
	// Path format: /api/v1/schemas or /api/v1/{tenant}/schemas/...
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "api" && parts[1] == "v1" {
		// Check if next part is a tenant or "schemas"
		if parts[2] != "schemas" && parts[2] != "streams" && parts[2] != "queues" && parts[2] != "kv" {
			return parts[2]
		}
	}
	return ""
}

func extractSchemaPathParams(path string) (tenant, schemaID string, version int32) {
	// Path format: /api/v1/{tenant}/schemas/{schema_id}/v{version}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 5 && parts[0] == "api" && parts[1] == "v1" && parts[3] == "schemas" {
		tenant = parts[2]
		schemaID = parts[4]
		if len(parts) >= 6 && strings.HasPrefix(parts[5], "v") {
			if v, err := strconv.ParseInt(strings.TrimPrefix(parts[5], "v"), 10, 32); err == nil {
				version = int32(v)
			}
		}
	}
	return
}

func extractResourcePathParams(path string) (tenant, namespace, resourceType, name string) {
	// Path format: /api/v1/{resource_type}/{tenant}/{namespace}/{name}/schema
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 6 && parts[0] == "api" && parts[1] == "v1" {
		resourceType = parts[2]
		tenant = parts[3]
		namespace = parts[4]
		name = parts[5]
	}
	return
}

func (h *SchemaHandlers) writeError(w http.ResponseWriter, err error, schemaID string) {
	switch e := err.(type) {
	case schemaerrors.SchemaNotFoundError:
		http.Error(w, e.Error(), http.StatusNotFound)
	case schemaerrors.InvalidSchemaError:
		http.Error(w, e.Error(), http.StatusBadRequest)
	case schemaerrors.SchemaValidationError:
		http.Error(w, e.Error(), http.StatusBadRequest)
	default:
		http.Error(w, "internal error: "+err.Error(), http.StatusInternalServerError)
	}
}
