package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServer_StartStop(t *testing.T) {
	// Create a mock storage backend
	storageBackend := &mockStorageBackend{ready: true}

	// Create server with random port (will use :0 for automatic port assignment)
	server := NewServer(":0", storageBackend)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start server
	err := server.Start(ctx)
	require.NoError(t, err)
	assert.True(t, server.Ready())

	// Stop server
	err = server.Stop(ctx)
	require.NoError(t, err)
	assert.False(t, server.Ready())
}

func TestHealthCheck(t *testing.T) {
	storageBackend := &mockStorageBackend{ready: true}
	router := NewRouter(storageBackend)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	router.mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "healthy")
}

func TestReadinessCheck(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: true}
		router := NewRouter(storageBackend)

		req := httptest.NewRequest("GET", "/ready", nil)
		w := httptest.NewRecorder()

		router.mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "ready")
	})

	t.Run("not ready", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: false}
		router := NewRouter(storageBackend)

		req := httptest.NewRequest("GET", "/ready", nil)
		w := httptest.NewRecorder()

		router.mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Contains(t, w.Body.String(), "not ready")
	})
}

// mockStorageBackend is a mock implementation of StorageBackend for testing
type mockStorageBackend struct {
	ready bool
}

func (m *mockStorageBackend) Start(ctx context.Context) error {
	m.ready = true
	return nil
}

func (m *mockStorageBackend) Stop(ctx context.Context) error {
	m.ready = false
	return nil
}

func (m *mockStorageBackend) Ready() bool {
	return m.ready
}

// Implement other required methods with empty implementations
func (m *mockStorageBackend) MetaStore() *metastore.Store                        { return nil }
func (m *mockStorageBackend) LogManager() storage.LogManager                     { return nil }
func (m *mockStorageBackend) StreamManager() storage.StreamManager               { return nil }
func (m *mockStorageBackend) QueueManager() storage.QueueManager                 { return nil }
func (m *mockStorageBackend) KVManager() storage.KVManager                       { return nil }
func (m *mockStorageBackend) ConsumerGroupManager() storage.ConsumerGroupManager { return nil }
func (m *mockStorageBackend) SchemaRegistry() storage.SchemaRegistry             { return nil }
func (m *mockStorageBackend) Paths() *storage.StoragePaths                       { return nil }
func (m *mockStorageBackend) Validate(ctx context.Context) error                 { return nil }
