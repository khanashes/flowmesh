package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupKVIntegrationTest(t *testing.T) (storage.StorageBackend, string, func()) {
	tmpDir := t.TempDir()

	// Create real storage backend
	storageBackend, err := storage.New(tmpDir)
	require.NoError(t, err)

	ctx := context.Background()
	err = storageBackend.Start(ctx)
	require.NoError(t, err)

	cleanup := func() {
		err := storageBackend.Stop(ctx)
		assert.NoError(t, err)
	}

	return storageBackend, tmpDir, cleanup
}

func createKVStore(t *testing.T, storageBackend storage.StorageBackend, tenant, namespace, name string) string {
	config := &metastore.ResourceConfig{
		Tenant:    tenant,
		Namespace: namespace,
		Name:      name,
		Type:      metastore.ResourceKV,
		CreatedAt: time.Now(),
	}
	config.Validate()

	resourcePath := config.GetPath()
	err := storageBackend.MetaStore().CreateResource(config)
	require.NoError(t, err)

	ctx := context.Background()
	err = storageBackend.KVManager().InitializeKVStore(ctx, resourcePath)
	require.NoError(t, err)

	return resourcePath
}

func TestKVHandlers_Integration_SetGet(t *testing.T) {
	storageBackend, _, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	handlers := NewKVHandlers(storageBackend)
	createKVStore(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Set a key-value pair
	setReqBody := SetRequest{
		Value:      []byte("test-value"),
		TTLSeconds: 0,
	}
	bodyBytes, _ := json.Marshal(setReqBody)

	req := httptest.NewRequest("PUT", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/test-key", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()

	handlers.Set(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var setResp SetResponse
	err := json.Unmarshal(w.Body.Bytes(), &setResp)
	require.NoError(t, err)
	assert.Equal(t, "success", setResp.Status)

	// Get the value
	req = httptest.NewRequest("GET", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/test-key", nil)
	w = httptest.NewRecorder()

	handlers.Get(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var getResp GetResponse
	err = json.Unmarshal(w.Body.Bytes(), &getResp)
	require.NoError(t, err)
	assert.Equal(t, "success", getResp.Status)
	assert.Equal(t, []byte("test-value"), getResp.Value)
}

func TestKVHandlers_Integration_SetWithTTL(t *testing.T) {
	storageBackend, _, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	handlers := NewKVHandlers(storageBackend)
	createKVStore(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Set a key-value pair with TTL
	setReqBody := SetRequest{
		Value:      []byte("test-value-ttl"),
		TTLSeconds: 1, // 1 second TTL
	}
	bodyBytes, _ := json.Marshal(setReqBody)

	req := httptest.NewRequest("PUT", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/test-key-ttl", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()

	handlers.Set(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Get immediately - should work
	req = httptest.NewRequest("GET", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/test-key-ttl", nil)
	w = httptest.NewRecorder()

	handlers.Get(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var getResp GetResponse
	err := json.Unmarshal(w.Body.Bytes(), &getResp)
	require.NoError(t, err)
	assert.Equal(t, []byte("test-value-ttl"), getResp.Value)

	// Wait for TTL to expire
	time.Sleep(1100 * time.Millisecond)

	// Get after TTL expires - should return not found
	req = httptest.NewRequest("GET", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/test-key-ttl", nil)
	w = httptest.NewRecorder()

	handlers.Get(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestKVHandlers_Integration_Delete(t *testing.T) {
	storageBackend, _, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	handlers := NewKVHandlers(storageBackend)
	createKVStore(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Set a key-value pair
	setReqBody := SetRequest{
		Value: []byte("test-value"),
	}
	bodyBytes, _ := json.Marshal(setReqBody)

	req := httptest.NewRequest("PUT", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/test-key", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	handlers.Set(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Delete the key
	req = httptest.NewRequest("DELETE", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/test-key", nil)
	w = httptest.NewRecorder()

	handlers.Delete(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var deleteResp DeleteResponse
	err := json.Unmarshal(w.Body.Bytes(), &deleteResp)
	require.NoError(t, err)
	assert.Equal(t, "success", deleteResp.Status)

	// Try to get deleted key - should return not found
	req = httptest.NewRequest("GET", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/test-key", nil)
	w = httptest.NewRecorder()

	handlers.Get(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestKVHandlers_Integration_Exists(t *testing.T) {
	storageBackend, _, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	handlers := NewKVHandlers(storageBackend)
	createKVStore(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Set a key-value pair
	setReqBody := SetRequest{
		Value: []byte("test-value"),
	}
	bodyBytes, _ := json.Marshal(setReqBody)

	req := httptest.NewRequest("PUT", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/test-key", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	handlers.Set(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Check if key exists
	req = httptest.NewRequest("HEAD", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/test-key", nil)
	w = httptest.NewRecorder()

	handlers.Exists(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var existsResp ExistsResponse
	err := json.Unmarshal(w.Body.Bytes(), &existsResp)
	require.NoError(t, err)
	assert.True(t, existsResp.Exists)

	// Check if non-existent key exists
	req = httptest.NewRequest("HEAD", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/nonexistent", nil)
	w = httptest.NewRecorder()

	handlers.Exists(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
	var notExistsResp ExistsResponse
	err = json.Unmarshal(w.Body.Bytes(), &notExistsResp)
	require.NoError(t, err)
	assert.False(t, notExistsResp.Exists)
}

func TestKVHandlers_Integration_ListKeys(t *testing.T) {
	storageBackend, _, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	handlers := NewKVHandlers(storageBackend)
	createKVStore(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Set multiple keys
	keys := []string{"user:1", "user:2", "order:1", "order:2", "product:1"}
	for _, key := range keys {
		setReqBody := SetRequest{
			Value: []byte("value-" + key),
		}
		bodyBytes, _ := json.Marshal(setReqBody)

		req := httptest.NewRequest("PUT", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/"+key, bytes.NewReader(bodyBytes))
		w := httptest.NewRecorder()
		handlers.Set(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	}

	// List all keys
	req := httptest.NewRequest("GET", "/api/v1/kv/test-tenant/test-ns/test-kv/keys", nil)
	w := httptest.NewRecorder()

	handlers.ListKeys(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var listResp ListKeysResponse
	err := json.Unmarshal(w.Body.Bytes(), &listResp)
	require.NoError(t, err)
	assert.Equal(t, "success", listResp.Status)
	assert.Len(t, listResp.Keys, 5)

	// List keys with prefix
	req = httptest.NewRequest("GET", "/api/v1/kv/test-tenant/test-ns/test-kv/keys?prefix=user:", nil)
	w = httptest.NewRecorder()

	handlers.ListKeys(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var prefixResp ListKeysResponse
	err = json.Unmarshal(w.Body.Bytes(), &prefixResp)
	require.NoError(t, err)
	assert.Equal(t, "success", prefixResp.Status)
	assert.Len(t, prefixResp.Keys, 2)
	assert.Contains(t, prefixResp.Keys, "user:1")
	assert.Contains(t, prefixResp.Keys, "user:2")
}

func TestKVHandlers_Integration_ErrorCases(t *testing.T) {
	storageBackend, _, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	handlers := NewKVHandlers(storageBackend)

	t.Run("KV store not found", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/kv/test-tenant/test-ns/nonexistent/keys/test-key", nil)
		w := httptest.NewRecorder()

		handlers.Get(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("Key not found", func(t *testing.T) {
		createKVStore(t, storageBackend, "test-tenant", "test-ns", "test-kv")

		req := httptest.NewRequest("GET", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/nonexistent", nil)
		w := httptest.NewRecorder()

		handlers.Get(w, req)

		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("Empty key", func(t *testing.T) {
		createKVStore(t, storageBackend, "test-tenant", "test-ns", "test-kv-empty-key")

		setReqBody := SetRequest{
			Value: []byte("test-value"),
		}
		bodyBytes, _ := json.Marshal(setReqBody)

		req := httptest.NewRequest("PUT", "/api/v1/kv/test-tenant/test-ns/test-kv-empty-key/keys/", bytes.NewReader(bodyBytes))
		w := httptest.NewRecorder()

		handlers.Set(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("Empty value", func(t *testing.T) {
		createKVStore(t, storageBackend, "test-tenant", "test-ns", "test-kv-empty-value")

		setReqBody := SetRequest{
			Value: []byte{},
		}
		bodyBytes, _ := json.Marshal(setReqBody)

		req := httptest.NewRequest("PUT", "/api/v1/kv/test-tenant/test-ns/test-kv-empty-value/keys/test-key", bytes.NewReader(bodyBytes))
		w := httptest.NewRecorder()

		handlers.Set(w, req)

		assert.Equal(t, http.StatusBadRequest, w.Code)
	})
}

func TestKVHandlers_Integration_ConcurrentOperations(t *testing.T) {
	storageBackend, _, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	handlers := NewKVHandlers(storageBackend)
	createKVStore(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Concurrent sets
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			setReqBody := SetRequest{
				Value: []byte("value-" + strconv.Itoa(idx)),
			}
			bodyBytes, _ := json.Marshal(setReqBody)

			req := httptest.NewRequest("PUT", "/api/v1/kv/test-tenant/test-ns/test-kv/keys/key-"+strconv.Itoa(idx), bytes.NewReader(bodyBytes))
			w := httptest.NewRecorder()
			handlers.Set(w, req)
			done <- w.Code == http.StatusOK
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		assert.True(t, <-done)
	}

	// Verify all keys exist
	req := httptest.NewRequest("GET", "/api/v1/kv/test-tenant/test-ns/test-kv/keys", nil)
	w := httptest.NewRecorder()
	handlers.ListKeys(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var listResp ListKeysResponse
	err := json.Unmarshal(w.Body.Bytes(), &listResp)
	require.NoError(t, err)
	assert.Len(t, listResp.Keys, 10)
}
