package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/flowmesh/engine/internal/api/validation"
	"github.com/flowmesh/engine/internal/storage"
	kverrors "github.com/flowmesh/engine/internal/storage/kv"
	"github.com/flowmesh/engine/internal/storage/metastore"
)

// KVHandlers provides HTTP handlers for KV store operations
type KVHandlers struct {
	storage storage.StorageBackend
	wsHub   *Hub // WebSocket hub for real-time updates
}

// NewKVHandlers creates new KV handlers
func NewKVHandlers(storage storage.StorageBackend) *KVHandlers {
	return &KVHandlers{
		storage: storage,
		wsHub:   nil, // Will be set by router if WebSocket is enabled
	}
}

// SetWSHub sets the WebSocket hub
func (h *KVHandlers) SetWSHub(hub *Hub) {
	h.wsHub = hub
}

// SetRequest represents a request to set a key-value pair
type SetRequest struct {
	Value      []byte `json:"value"`
	TTLSeconds int64  `json:"ttl_seconds,omitempty"`
}

// SetResponse represents a response to setting a key-value pair
type SetResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// GetResponse represents a response to getting a value
type GetResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Value   []byte `json:"value"`
}

// DeleteResponse represents a response to deleting a key
type DeleteResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// ExistsResponse represents a response to checking if a key exists
type ExistsResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Exists  bool   `json:"exists"`
}

// ListKeysResponse represents a response to listing keys
type ListKeysResponse struct {
	Status  string   `json:"status"`
	Message string   `json:"message,omitempty"`
	Keys    []string `json:"keys"`
}

// Set handles PUT /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
func (h *KVHandlers) Set(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, key, err := extractKVKeyPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "kv", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse request body
	var req SetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Value) == 0 {
		http.Error(w, "value cannot be empty", http.StatusBadRequest)
		return
	}

	// Parse TTL from query parameter if not in body
	if req.TTLSeconds == 0 {
		if ttlStr := r.URL.Query().Get("ttl"); ttlStr != "" {
			if ttl, err := parseTTL(ttlStr); err == nil {
				req.TTLSeconds = int64(ttl.Seconds())
			}
		}
	}

	// Convert TTL
	var ttl time.Duration
	if req.TTLSeconds > 0 {
		ttl = time.Duration(req.TTLSeconds) * time.Second
		if ttl > 365*24*time.Hour {
			http.Error(w, "ttl_seconds cannot exceed 31536000 (1 year)", http.StatusBadRequest)
			return
		}
	}

	// Set key-value pair
	kvMgr := h.storage.KVManager()
	err = kvMgr.Set(r.Context(), resourcePath, key, req.Value, storage.KVSetOptions{
		TTL: ttl,
	})
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(SetResponse{
		Status:  "success",
		Message: "key-value pair set successfully",
	})

	// Broadcast KV update
	if h.wsHub != nil {
		h.wsHub.BroadcastKVUpdate(tenant, namespace, name, map[string]interface{}{
			"action": "set",
			"key":    key,
		})
	}
}

// Get handles GET /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
func (h *KVHandlers) Get(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, key, err := extractKVKeyPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "kv", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get value
	kvMgr := h.storage.KVManager()
	value, err := kvMgr.Get(r.Context(), resourcePath, key)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(GetResponse{
		Status:  "success",
		Message: "value retrieved successfully",
		Value:   value,
	})
}

// Delete handles DELETE /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
func (h *KVHandlers) Delete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, key, err := extractKVKeyPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "kv", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Delete key
	kvMgr := h.storage.KVManager()
	err = kvMgr.Delete(r.Context(), resourcePath, key)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(DeleteResponse{
		Status:  "success",
		Message: "key deleted successfully",
	})

	// Broadcast KV update
	if h.wsHub != nil {
		h.wsHub.BroadcastKVUpdate(tenant, namespace, name, map[string]interface{}{
			"action": "delete",
			"key":    key,
		})
	}
}

// Exists handles HEAD /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
func (h *KVHandlers) Exists(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, key, err := extractKVKeyPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "kv", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Check if key exists
	kvMgr := h.storage.KVManager()
	exists, err := kvMgr.Exists(r.Context(), resourcePath, key)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response (HEAD returns status code only, but we'll return JSON for consistency)
	if exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(ExistsResponse{
			Status:  "success",
			Message: "key exists",
			Exists:  true,
		})
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ExistsResponse{
			Status:  "success",
			Message: "key does not exist",
			Exists:  false,
		})
	}
}

// ListKVStoresResponse represents a response to listing KV stores
type ListKVStoresResponse struct {
	Status  string                      `json:"status"`
	Message string                      `json:"message,omitempty"`
	Stores  []*metastore.ResourceConfig `json:"stores"`
}

// ListKVStores handles GET /api/v1/kv
func (h *KVHandlers) ListKVStores(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get optional query parameters
	tenant := r.URL.Query().Get("tenant")
	namespace := r.URL.Query().Get("namespace")

	// Get meta store
	metaStore := h.storage.MetaStore()
	if metaStore == nil {
		h.writeError(w, errors.New("meta store not available"), "")
		return
	}

	// List KV store resources
	stores, err := metaStore.ListResources(tenant, namespace, metastore.ResourceKV)
	if err != nil {
		h.writeError(w, err, "")
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ListKVStoresResponse{
		Status:  "success",
		Message: "KV stores retrieved successfully",
		Stores:  stores,
	})
}

// ListKeys handles GET /api/v1/kv/{tenant}/{namespace}/{name}/keys
func (h *KVHandlers) ListKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract path parameters
	tenant, namespace, name, err := extractKVPathParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Build resource path
	resourcePath, err := validation.BuildResourcePath(tenant, namespace, "kv", name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Get prefix from query parameter
	prefix := r.URL.Query().Get("prefix")

	// List keys
	kvMgr := h.storage.KVManager()
	keys, err := kvMgr.ListKeys(r.Context(), resourcePath, prefix)
	if err != nil {
		h.writeError(w, err, resourcePath)
		return
	}

	// Write response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ListKeysResponse{
		Status:  "success",
		Message: "keys listed successfully",
		Keys:    keys,
	})
}

// Helper methods

// extractKVPathParams extracts tenant, namespace, and name from URL path
func extractKVPathParams(r *http.Request) (tenant, namespace, name string, err error) {
	// Path format: /api/v1/kv/{tenant}/{namespace}/{name}/keys
	parts := splitPath(r.URL.Path)
	if len(parts) < 6 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "kv" || parts[6] != "keys" {
		return "", "", "", http.ErrMissingFile
	}
	return parts[3], parts[4], parts[5], nil
}

// extractKVKeyPathParams extracts tenant, namespace, name, and key from URL path
func extractKVKeyPathParams(r *http.Request) (tenant, namespace, name, key string, err error) {
	// Path format: /api/v1/kv/{tenant}/{namespace}/{name}/keys/{key}
	parts := splitPath(r.URL.Path)
	if len(parts) < 8 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "kv" || parts[6] != "keys" {
		return "", "", "", "", http.ErrMissingFile
	}
	return parts[3], parts[4], parts[5], parts[7], nil
}

// parseTTL parses a TTL string (e.g., "1h", "30m", "60s")
func parseTTL(ttlStr string) (time.Duration, error) {
	return time.ParseDuration(ttlStr)
}

// writeError writes an error response
func (h *KVHandlers) writeError(w http.ResponseWriter, err error, resourcePath string) {
	switch e := err.(type) {
	case kverrors.KVStoreNotFoundError:
		http.Error(w, e.Error(), http.StatusNotFound)
	case kverrors.KeyNotFoundError:
		http.Error(w, e.Error(), http.StatusNotFound)
	case kverrors.KeyExpiredError:
		http.Error(w, e.Error(), http.StatusNotFound)
	case kverrors.InvalidKeyError:
		http.Error(w, e.Error(), http.StatusBadRequest)
	case kverrors.InvalidValueError:
		http.Error(w, e.Error(), http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
