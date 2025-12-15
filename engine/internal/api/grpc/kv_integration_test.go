package grpc

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/flowmesh/engine/api/proto/flowmeshpb"
	"github.com/flowmesh/engine/internal/storage"
	"github.com/flowmesh/engine/internal/storage/metastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupKVIntegrationTest(t *testing.T) (storage.StorageBackend, func()) {
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

	return storageBackend, cleanup
}

func createKVStoreForGRPC(t *testing.T, storageBackend storage.StorageBackend, tenant, namespace, name string) string {
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

func TestKVService_Integration_SetGet(t *testing.T) {
	storageBackend, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	service := NewKVService(storageBackend)
	createKVStoreForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Set a key-value pair
	setReq := &flowmeshpb.SetRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Key:        "test-key",
		Value:      []byte("test-value"),
		TtlSeconds: 0,
	}

	setResp, err := service.Set(context.Background(), setReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), setResp.Status.Code) // codes.OK = 0

	// Get the value
	getReq := &flowmeshpb.GetRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Key: "test-key",
	}

	getResp, err := service.Get(context.Background(), getReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), getResp.Status.Code)
	assert.Equal(t, []byte("test-value"), getResp.Value)
}

func TestKVService_Integration_SetWithTTL(t *testing.T) {
	storageBackend, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	service := NewKVService(storageBackend)
	createKVStoreForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Set a key-value pair with TTL
	setReq := &flowmeshpb.SetRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Key:        "test-key-ttl",
		Value:      []byte("test-value-ttl"),
		TtlSeconds: 1, // 1 second TTL
	}

	setResp, err := service.Set(context.Background(), setReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), setResp.Status.Code)

	// Get immediately - should work
	getReq := &flowmeshpb.GetRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Key: "test-key-ttl",
	}

	getResp, err := service.Get(context.Background(), getReq)
	require.NoError(t, err)
	assert.Equal(t, []byte("test-value-ttl"), getResp.Value)

	// Wait for TTL to expire
	time.Sleep(1100 * time.Millisecond)

	// Get after TTL expires - should return not found
	getResp, err = service.Get(context.Background(), getReq)
	assert.Error(t, err)
	// Error should be NotFound
}

func TestKVService_Integration_Delete(t *testing.T) {
	storageBackend, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	service := NewKVService(storageBackend)
	createKVStoreForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Set a key-value pair
	setReq := &flowmeshpb.SetRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Key:   "test-key",
		Value: []byte("test-value"),
	}

	_, err := service.Set(context.Background(), setReq)
	require.NoError(t, err)

	// Delete the key
	deleteReq := &flowmeshpb.DeleteRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Key: "test-key",
	}

	deleteResp, err := service.Delete(context.Background(), deleteReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), deleteResp.Status.Code)

	// Try to get deleted key - should return not found
	getReq := &flowmeshpb.GetRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Key: "test-key",
	}

	_, err = service.Get(context.Background(), getReq)
	assert.Error(t, err)
	// Error should be NotFound
}

func TestKVService_Integration_Exists(t *testing.T) {
	storageBackend, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	service := NewKVService(storageBackend)
	createKVStoreForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Set a key-value pair
	setReq := &flowmeshpb.SetRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Key:   "test-key",
		Value: []byte("test-value"),
	}

	_, err := service.Set(context.Background(), setReq)
	require.NoError(t, err)

	// Check if key exists
	existsReq := &flowmeshpb.ExistsRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Key: "test-key",
	}

	existsResp, err := service.Exists(context.Background(), existsReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), existsResp.Status.Code)
	assert.True(t, existsResp.Exists)

	// Check if non-existent key exists
	existsReq.Key = "nonexistent"
	existsResp, err = service.Exists(context.Background(), existsReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), existsResp.Status.Code)
	assert.False(t, existsResp.Exists)
}

func TestKVService_Integration_ListKeys(t *testing.T) {
	storageBackend, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	service := NewKVService(storageBackend)
	createKVStoreForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Set multiple keys
	keys := []string{"user:1", "user:2", "order:1", "order:2", "product:1"}
	for _, key := range keys {
		setReq := &flowmeshpb.SetRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key:   key,
			Value: []byte("value-" + key),
		}
		_, err := service.Set(context.Background(), setReq)
		require.NoError(t, err)
	}

	// List all keys
	listReq := &flowmeshpb.ListKeysRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Prefix: "",
	}

	listResp, err := service.ListKeys(context.Background(), listReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), listResp.Status.Code)
	assert.Len(t, listResp.Keys, 5)

	// List keys with prefix
	listReq.Prefix = "user:"
	listResp, err = service.ListKeys(context.Background(), listReq)
	require.NoError(t, err)
	assert.Equal(t, int32(0), listResp.Status.Code)
	assert.Len(t, listResp.Keys, 2)
	assert.Contains(t, listResp.Keys, "user:1")
	assert.Contains(t, listResp.Keys, "user:2")
}

func TestKVService_Integration_ErrorCases(t *testing.T) {
	storageBackend, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	service := NewKVService(storageBackend)

	t.Run("KV store not found", func(t *testing.T) {
		getReq := &flowmeshpb.GetRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "nonexistent",
			},
			Key: "test-key",
		}

		_, err := service.Get(context.Background(), getReq)
		assert.Error(t, err)
		// Error should be NotFound
	})

	t.Run("Key not found", func(t *testing.T) {
		createKVStoreForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-kv")

		getReq := &flowmeshpb.GetRequest{
			ResourcePath: &flowmeshpb.ResourcePath{
				Tenant:       "test-tenant",
				Namespace:    "test-ns",
				ResourceType: "kv",
				Name:         "test-kv",
			},
			Key: "nonexistent",
		}

		_, err := service.Get(context.Background(), getReq)
		assert.Error(t, err)
		// Error should be NotFound
	})
}

func TestKVService_Integration_ConcurrentOperations(t *testing.T) {
	storageBackend, cleanup := setupKVIntegrationTest(t)
	defer cleanup()

	service := NewKVService(storageBackend)
	createKVStoreForGRPC(t, storageBackend, "test-tenant", "test-ns", "test-kv")

	// Concurrent sets
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			setReq := &flowmeshpb.SetRequest{
				ResourcePath: &flowmeshpb.ResourcePath{
					Tenant:       "test-tenant",
					Namespace:    "test-ns",
					ResourceType: "kv",
					Name:         "test-kv",
				},
				Key:   "key-" + strconv.Itoa(idx),
				Value: []byte("value-" + strconv.Itoa(idx)),
			}
			resp, err := service.Set(context.Background(), setReq)
			done <- (err == nil && resp.Status.Code == 0)
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		assert.True(t, <-done)
	}

	// Verify all keys exist
	listReq := &flowmeshpb.ListKeysRequest{
		ResourcePath: &flowmeshpb.ResourcePath{
			Tenant:       "test-tenant",
			Namespace:    "test-ns",
			ResourceType: "kv",
			Name:         "test-kv",
		},
		Prefix: "",
	}

	listResp, err := service.ListKeys(context.Background(), listReq)
	require.NoError(t, err)
	assert.Len(t, listResp.Keys, 10)
}
