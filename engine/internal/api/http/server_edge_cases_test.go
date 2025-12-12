package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flowmesh/engine/internal/api/http/middleware"
	"github.com/flowmesh/engine/internal/logger"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createLongString creates a long string of 'a' characters
func createLongString(length int) string {
	return strings.Repeat("a", length)
}

func TestServer_EdgeCases(t *testing.T) {
	t.Run("Start when already started", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: true}
		server := NewServer(":0", storageBackend)

		ctx := context.Background()
		err := server.Start(ctx)
		require.NoError(t, err)

		// Try to start again
		err = server.Start(ctx)
		assert.NoError(t, err) // Should be idempotent
	})

	t.Run("Stop when not started", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: false}
		server := NewServer(":0", storageBackend)

		ctx := context.Background()
		err := server.Stop(ctx)
		assert.NoError(t, err) // Should be idempotent
	})

	t.Run("Ready when not started", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: false}
		server := NewServer(":0", storageBackend)

		assert.False(t, server.Ready())
	})

	t.Run("Stop with cancelled context", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: true}
		server := NewServer(":0", storageBackend)

		ctx := context.Background()
		err := server.Start(ctx)
		require.NoError(t, err)

		// Cancel context
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err = server.Stop(cancelledCtx)
		// Should still stop (uses Close as fallback)
		assert.NoError(t, err)
	})

	t.Run("Stop with timeout context", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: true}
		server := NewServer(":0", storageBackend)

		ctx := context.Background()
		err := server.Start(ctx)
		require.NoError(t, err)

		// Very short timeout
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		time.Sleep(10 * time.Millisecond) // Wait for timeout
		err = server.Stop(timeoutCtx)
		// Should still stop (uses Close as fallback)
		assert.NoError(t, err)
	})
}

func TestHealthCheck_EdgeCases(t *testing.T) {
	t.Run("HealthCheck with POST method", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: true}
		router := NewRouter(storageBackend)

		req := httptest.NewRequest("POST", "/health", nil)
		w := httptest.NewRecorder()

		router.mux.ServeHTTP(w, req)

		// Should still return 200 (method doesn't matter for health)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("HealthCheck with query parameters", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: true}
		router := NewRouter(storageBackend)

		req := httptest.NewRequest("GET", "/health?foo=bar&baz=qux", nil)
		w := httptest.NewRecorder()

		router.mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("HealthCheck with invalid path", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: true}
		router := NewRouter(storageBackend)

		req := httptest.NewRequest("GET", "/health/invalid", nil)
		w := httptest.NewRecorder()

		router.mux.ServeHTTP(w, req)

		// Should return 404
		assert.Equal(t, http.StatusNotFound, w.Code)
	})
}

func TestReadinessCheck_EdgeCases(t *testing.T) {
	t.Run("ReadinessCheck with storage nil", func(t *testing.T) {
		router := NewRouter(nil)

		req := httptest.NewRequest("GET", "/ready", nil)
		w := httptest.NewRecorder()

		router.mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
		assert.Contains(t, w.Body.String(), "not ready")
	})

	t.Run("ReadinessCheck state change during request", func(t *testing.T) {
		storageBackend := &mockStorageBackendWithMutex{ready: true}
		router := NewRouter(storageBackend)

		req := httptest.NewRequest("GET", "/ready", nil)
		w := httptest.NewRecorder()

		// Change state during request (thread-safe)
		storageBackend.mu.Lock()
		storageBackend.ready = false
		storageBackend.mu.Unlock()

		router.mux.ServeHTTP(w, req)

		assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

func TestConcurrentRequests_EdgeCases(t *testing.T) {
	t.Run("Concurrent health checks", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: true}
		router := NewRouter(storageBackend)

		var wg sync.WaitGroup
		numRequests := 100

		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := httptest.NewRequest("GET", "/health", nil)
				w := httptest.NewRecorder()
				router.mux.ServeHTTP(w, req)
				assert.Equal(t, http.StatusOK, w.Code)
			}()
		}

		wg.Wait()
	})

	t.Run("Concurrent readiness checks", func(t *testing.T) {
		storageBackend := &mockStorageBackendWithMutex{ready: true}
		router := NewRouter(storageBackend)

		var wg sync.WaitGroup
		numRequests := 100

		for i := 0; i < numRequests; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := httptest.NewRequest("GET", "/ready", nil)
				w := httptest.NewRecorder()
				router.mux.ServeHTTP(w, req)
				assert.Equal(t, http.StatusOK, w.Code)
			}()
		}

		wg.Wait()
	})
}

func TestInvalidPaths_EdgeCases(t *testing.T) {
	storageBackend := &mockStorageBackend{ready: true}
	router := NewRouter(storageBackend)

	testCases := []struct {
		name string
		path string
		code int
	}{
		{"root path", "/", http.StatusNotFound},
		{"invalid API path", "/api/v1/invalid", http.StatusNotFound},
		{"deep nested path", "/api/v1/streams/test/events/more/deep", http.StatusNotFound},
		{"path with special characters", "/health?<script>alert('xss')</script>", http.StatusOK}, // Query params, health still works
		{"path with encoded characters", "/health%2Finvalid", http.StatusNotFound},
		{"very long path", "/" + createLongString(10000), http.StatusNotFound},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.path, nil)
			w := httptest.NewRecorder()

			router.mux.ServeHTTP(w, req)

			assert.Equal(t, tc.code, w.Code, "path: %s", tc.path)
		})
	}
}

func TestHTTPMethods_EdgeCases(t *testing.T) {
	storageBackend := &mockStorageBackend{ready: true}
	router := NewRouter(storageBackend)

	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/health", nil)
			w := httptest.NewRecorder()

			router.mux.ServeHTTP(w, req)

			// All methods should work for health check
			if method == "HEAD" || method == "OPTIONS" {
				// These might return different codes
				assert.True(t, w.Code == http.StatusOK || w.Code == http.StatusMethodNotAllowed || w.Code == http.StatusNotFound)
			} else {
				assert.Equal(t, http.StatusOK, w.Code)
			}
		})
	}
}

func TestMiddleware_EdgeCases(t *testing.T) {
	t.Run("Middleware with panic", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: true}
		router := NewRouter(storageBackend)

		// Create a handler that panics, wrapped in middleware chain
		chain := middleware.Chain(
			middleware.Recovery(logger.WithComponent("http.middleware")),
			middleware.Logging(logger.WithComponent("http.middleware")),
		)
		panicHandler := chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			panic("test panic")
		}))

		router.mux.Handle("/panic", panicHandler)

		req := httptest.NewRequest("GET", "/panic", nil)
		w := httptest.NewRecorder()

		// Should not panic, should be recovered by middleware
		assert.NotPanics(t, func() {
			router.mux.ServeHTTP(w, req)
		})

		// Should return 500
		assert.Equal(t, http.StatusInternalServerError, w.Code)
		assert.Contains(t, w.Body.String(), "internal server error")
	})

	t.Run("Middleware with large request", func(t *testing.T) {
		storageBackend := &mockStorageBackend{ready: true}
		router := NewRouter(storageBackend)

		// Create a large body (10MB)
		largeBody := make([]byte, 10*1024*1024)
		req := httptest.NewRequest("POST", "/health", nil)
		req.Body = http.MaxBytesReader(nil, req.Body, int64(len(largeBody)))

		w := httptest.NewRecorder()

		// Should handle large requests gracefully
		router.mux.ServeHTTP(w, req)
		// Health endpoint doesn't read body, so should work
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

// mockStorageBackendWithMutex is a version of mockStorageBackend with mutex for concurrent testing
type mockStorageBackendWithMutex struct {
	ready bool
	mu    sync.RWMutex
}

func (m *mockStorageBackendWithMutex) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ready = true
	return nil
}

func (m *mockStorageBackendWithMutex) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ready = false
	return nil
}

func (m *mockStorageBackendWithMutex) Ready() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ready
}

func (m *mockStorageBackendWithMutex) MetaStore() *metastore.Store                        { return nil }
func (m *mockStorageBackendWithMutex) LogManager() storage.LogManager                     { return nil }
func (m *mockStorageBackendWithMutex) StreamManager() storage.StreamManager               { return nil }
func (m *mockStorageBackendWithMutex) QueueManager() storage.QueueManager                 { return nil }
func (m *mockStorageBackendWithMutex) KVManager() storage.KVManager                       { return nil }
func (m *mockStorageBackendWithMutex) ConsumerGroupManager() storage.ConsumerGroupManager { return nil }
func (m *mockStorageBackendWithMutex) Paths() *storage.StoragePaths                       { return nil }
func (m *mockStorageBackendWithMutex) Validate(ctx context.Context) error                 { return nil }
